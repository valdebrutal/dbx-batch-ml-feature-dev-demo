"""Seed synthetic source data for the demo.

Scroll-shaped mobile game: accounts + 6 event streams + a clan-membership snapshot
(~1M accounts x 90 days by default -> ~2M rows/date in the largest event table, ~500M
rows total). Every run drops and recreates all source tables, so it
doubles as the demo reset button (undoes append_one_day / restate_three_dates).
Source tables enable row tracking and deletion vectors so downstream MVs can refresh
incrementally on later runs.
"""

from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from logging_config import configure_logging

logger = logging.getLogger(__name__)


COUNTRIES = ["US", "JP", "KR", "DE", "BR", "GB", "FR"]
INSTALL_SOURCES = ["organic", "paid", "referral"]
PURCHASE_CATEGORIES = ["gems", "cards", "chest", "pass", "cosmetic"]
SOCIAL_TYPES = ["message", "invite", "gift"]

# All source tables this seed manages. Dropped + recreated on every run.
EXPECTED_SOURCE_TABLES: list[str] = [
    "src_dim_account",
    "src_events_login",
    "src_events_battle",
    "src_events_purchase",
    "src_events_progression",
    "src_events_social",
    "src_events_session",
    "src_clan_membership_daily",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--history-days", type=int, default=90)
    p.add_argument("--num-accounts", type=int, default=1000000)
    return p.parse_args()


def drop_all_source_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    """DROP TABLE IF EXISTS every src_* table in parallel."""

    def _drop(tbl: str) -> str:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{tbl}")
        return tbl

    with ThreadPoolExecutor(max_workers=len(EXPECTED_SOURCE_TABLES)) as pool:
        dropped = list(pool.map(_drop, EXPECTED_SOURCE_TABLES))
    logger.info("Dropped %d source tables: %s", len(dropped), dropped)


def array_pick(values: list[str], seed_col: str) -> F.Column:
    """Pick a random element from ``values`` deterministically given a seed column."""
    arr = F.array(*[F.lit(v) for v in values])
    idx = (F.abs(F.hash(F.col(seed_col))) % F.lit(len(values))).cast("int")
    return arr.getItem(idx)


def overwrite(df, full_name: str, cluster_by: str | None = None) -> None:
    """Overwrite a Delta table, enabling row tracking and deletion vectors at write time.

    Row tracking and DVs let downstream MVs refresh incrementally; they are set
    per-write because the session-conf path is not settable on serverless Spark
    Connect. If `cluster_by` is supplied, the table is liquid-clustered on that
    date-grain column so date-range reads can prune files.
    """
    writer = (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableRowTracking", "true")
        .option("delta.enableDeletionVectors", "true")
    )
    if cluster_by is not None:
        writer = writer.clusterBy(cluster_by)
    writer.saveAsTable(full_name)


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    # Reset: drop every src_* table so the seed rebuilds from a clean slate.
    drop_all_source_tables(spark, args.catalog, args.schema)

    history_days = args.history_days
    num_accounts = args.num_accounts

    # ----- src_dim_account -----------------------------------------------
    # Each account gets a deterministic spender_class (drives purchase rate + amount)
    # and temporal_pattern (drives when they purchase), giving downstream features
    # real variance. spender_class: 20% whale / 25% dolphin / 30% minnow / 25% non.
    # temporal_pattern: 30% "recent" (burst ~14d-7d before history end), 70% "uniform".
    spender_class_h = F.abs(
        F.hash(F.col("account_id"), F.lit("spender_class"))
    ) % F.lit(100)
    temporal_h = F.abs(F.hash(F.col("account_id"), F.lit("temporal"))) % F.lit(100)

    accounts = (
        spark.range(num_accounts)
        .withColumnRenamed("id", "account_id")
        .withColumn(
            "created_at",
            F.expr(f"date_sub(current_date(), cast(rand() * {history_days} as int))"),
        )
        .withColumn("country", array_pick(COUNTRIES, "account_id"))
        .withColumn("install_source", array_pick(INSTALL_SOURCES, "account_id"))
        .withColumn(
            "spender_class",
            F.when(spender_class_h < 20, F.lit("whale"))
            .when(spender_class_h < 45, F.lit("dolphin"))
            .when(spender_class_h < 75, F.lit("minnow"))
            .otherwise(F.lit("non_spender")),
        )
        .withColumn(
            "temporal_pattern",
            F.when(temporal_h < 30, F.lit("recent")).otherwise(F.lit("uniform")),
        )
        .withColumn(
            "churn_offset_days",
            F.when(F.rand() < 0.30, (F.rand() * history_days).cast("int")),
        )
        .withColumn(
            "churn_date",
            F.when(
                F.col("churn_offset_days").isNotNull(),
                F.expr(
                    f"date_sub(current_date(), {history_days}) "
                    f"+ make_interval(0, 0, 0, churn_offset_days)"
                ),
            ),
        )
        .drop("churn_offset_days")
    )
    overwrite(
        accounts.drop("churn_date"), f"{args.catalog}.{args.schema}.src_dim_account"
    )

    # Cross-join accounts with the day spine and apply the churn cutoff, keeping
    # spender_class / temporal_pattern / day_offset for the per-(account, day) rate.
    days = spark.range(history_days).withColumnRenamed("id", "day_offset")
    account_days_active = (
        accounts.crossJoin(days)
        .withColumn(
            "event_date",
            F.expr(
                f"date_sub(current_date(), {history_days} - cast(day_offset as int))"
            ),
        )
        .where(
            (F.col("event_date") >= F.col("created_at"))
            & (
                (F.col("churn_date").isNull())
                | (F.col("event_date") < F.col("churn_date"))
            )
        )
        .select(
            "account_id",
            "event_date",
            "day_offset",
            "spender_class",
            "temporal_pattern",
        )
    )

    def ts_within_day(date_col: str) -> F.Column:
        """Random timestamp within the given event_date (seconds resolution)."""
        return F.expr(
            f"timestampadd(SECOND, cast(rand() * 86400 as int), to_timestamp({date_col}))"
        )

    def with_event_rate(rate_per_day: float, jitter: float = 0.5):
        """Return a DataFrame replicated by per-day event counts.

        For rates >= 1.0, jittered integer round. For sub-1 rates, a
        Bernoulli draw — int() truncation otherwise zeroes out every row.
        """
        if rate_per_day >= 1.0:
            evt = F.greatest(
                F.lit(0),
                (
                    F.lit(rate_per_day) + (F.rand() - 0.5) * 2 * jitter * rate_per_day
                ).cast("int"),
            )
        else:
            evt = F.when(F.rand() < F.lit(rate_per_day), F.lit(1)).otherwise(F.lit(0))
        return (
            account_days_active.withColumn("event_count", evt)
            .where(F.col("event_count") > 0)
            .withColumn("event_idx", F.explode(F.expr("sequence(1, event_count)")))
        )

    # ----- src_events_login ---------------------------------------------
    # event_date is the cluster key (date-grain; login_ts is too high-cardinality).
    logins = (
        with_event_rate(rate_per_day=1.2, jitter=0.8)
        .withColumn("login_ts", ts_within_day("event_date"))
        .select("account_id", "login_ts", "event_date")
    )
    overwrite(
        logins,
        f"{args.catalog}.{args.schema}.src_events_login",
        cluster_by="event_date",
    )

    # ----- src_events_battle --------------------------------------------
    battles = (
        with_event_rate(rate_per_day=2.0, jitter=1.0)
        .withColumn("battle_ts", ts_within_day("event_date"))
        .withColumn("won", (F.rand() < 0.52).cast("int"))
        .withColumn("duration_seconds", (F.rand() * 240 + 30).cast("int"))
        .select("account_id", "battle_ts", "won", "duration_seconds", "event_date")
    )
    overwrite(
        battles,
        f"{args.catalog}.{args.schema}.src_events_battle",
        cluster_by="event_date",
    )

    # ----- src_events_purchase ------------------------------------------
    # Per-(account, day) purchase rate from spender_class (base rate + amount) and
    # temporal_pattern (when purchases concentrate): class drives spender_tier /
    # is_whale variance, recent-vs-uniform drives 7d/30d-ratio / is_high_velocity.
    base_rate = (
        F.when(F.col("spender_class") == "whale", F.lit(0.4))
        .when(F.col("spender_class") == "dolphin", F.lit(0.15))
        .when(F.col("spender_class") == "minnow", F.lit(0.05))
        .otherwise(F.lit(0.001))
    )
    # "recent" accounts: 15x rate in the burst window [history_days-14, history_days-7],
    # 0.3x outside, so velocity > 0.5 lands inside the labels MV window.
    in_burst = (F.col("day_offset") >= F.lit(history_days - 14)) & (
        F.col("day_offset") < F.lit(history_days - 7)
    )
    temporal_mult = (
        F.when((F.col("temporal_pattern") == "recent") & in_burst, F.lit(15.0))
        .when((F.col("temporal_pattern") == "recent") & ~in_burst, F.lit(0.3))
        .otherwise(F.lit(1.0))
    )
    purchase_rate = base_rate * temporal_mult

    amount_expr = (
        F.when(
            F.col("spender_class") == "whale",
            F.round(F.lit(5.0) + F.rand() * F.lit(45.0), 2),
        )
        .when(
            F.col("spender_class") == "dolphin",
            F.round(F.lit(2.0) + F.rand() * F.lit(13.0), 2),
        )
        .when(
            F.col("spender_class") == "minnow",
            F.round(F.lit(0.5) + F.rand() * F.lit(3.0), 2),
        )
        .otherwise(F.round(F.lit(0.5) + F.rand() * F.lit(1.0), 2))
    )

    # For fractional rates (e.g. 0.05/day, 0.001/day for minnows / non_spenders),
    # the integer cast in `with_event_rate` rounds to 0. Use a probabilistic
    # branch for sub-1 rates so low-frequency events still appear.
    purchase_event_count = F.when(
        purchase_rate >= F.lit(1.0),
        F.greatest(
            F.lit(0),
            (purchase_rate + (F.rand() - F.lit(0.5)) * purchase_rate).cast("int"),
        ),
    ).otherwise(F.when(F.rand() < purchase_rate, F.lit(1)).otherwise(F.lit(0)))

    purchases = (
        account_days_active.withColumn("event_count", purchase_event_count)
        .where(F.col("event_count") > 0)
        .withColumn("event_idx", F.explode(F.expr("sequence(1, event_count)")))
        .withColumn("purchase_ts", ts_within_day("event_date"))
        .withColumn("category", array_pick(PURCHASE_CATEGORIES, "purchase_ts"))
        .withColumn("amount_usd", amount_expr)
        .select("account_id", "purchase_ts", "category", "amount_usd", "event_date")
    )
    overwrite(
        purchases,
        f"{args.catalog}.{args.schema}.src_events_purchase",
        cluster_by="event_date",
    )

    # ----- src_events_progression ---------------------------------------
    # Cumulative level per account using a running count.
    progression = (
        with_event_rate(rate_per_day=0.35, jitter=0.8)
        .withColumn("level_up_ts", ts_within_day("event_date"))
        .select("account_id", "level_up_ts", "event_date")
    )
    w = Window.partitionBy("account_id").orderBy("level_up_ts")
    progression = progression.withColumn("new_level", F.row_number().over(w) + F.lit(1))
    overwrite(
        progression,
        f"{args.catalog}.{args.schema}.src_events_progression",
        cluster_by="event_date",
    )

    # ----- src_events_social --------------------------------------------
    socials = (
        with_event_rate(rate_per_day=1.0, jitter=1.0)
        .withColumn("event_ts", ts_within_day("event_date"))
        .withColumn("event_type", array_pick(SOCIAL_TYPES, "event_ts"))
        .select("account_id", "event_ts", "event_type", "event_date")
    )
    overwrite(
        socials,
        f"{args.catalog}.{args.schema}.src_events_social",
        cluster_by="event_date",
    )

    # ----- src_events_session (JSON payload — proto-parser stand-in) -----
    # payload_json = '{"duration_s": int, "device": str, "level_reached": int}'
    sessions = (
        with_event_rate(rate_per_day=1.1, jitter=0.7)
        .withColumn("session_ts", ts_within_day("event_date"))
        .withColumn(
            "payload_json",
            F.to_json(
                F.struct(
                    (F.rand() * 1500 + 60).cast("int").alias("duration_s"),
                    array_pick(["ios", "android"], "session_ts").alias("device"),
                    (F.rand() * 50 + 1).cast("int").alias("level_reached"),
                )
            ),
        )
        .select("account_id", "session_ts", "payload_json", "event_date")
    )
    overwrite(
        sessions,
        f"{args.catalog}.{args.schema}.src_events_session",
        cluster_by="event_date",
    )

    # ----- src_clan_membership_daily ------------------------------------
    # Slowly-changing dim: each account joins a clan at some point with ~60% prob,
    # may leave once. Snapshot one row per (account_id, date).
    clan_status_per_account = (
        accounts.select("account_id")
        .withColumn(
            "join_offset",
            F.when(F.rand() < 0.60, (F.rand() * history_days).cast("int")),
        )
        .withColumn(
            "leave_offset",
            F.when(
                F.col("join_offset").isNotNull() & (F.rand() < 0.30),
                F.col("join_offset") + (F.rand() * 60 + 5).cast("int"),
            ),
        )
    )
    clan = (
        clan_status_per_account.crossJoin(days)
        .withColumn(
            "date",
            F.expr(
                f"date_sub(current_date(), {history_days} - cast(day_offset as int))"
            ),
        )
        .withColumn(
            "is_clan_member",
            (
                F.col("join_offset").isNotNull()
                & (F.col("day_offset") >= F.col("join_offset"))
                & (
                    F.col("leave_offset").isNull()
                    | (F.col("day_offset") < F.col("leave_offset"))
                )
            ).cast("boolean"),
        )
        .select("account_id", "date", "is_clan_member")
    )
    overwrite(
        clan,
        f"{args.catalog}.{args.schema}.src_clan_membership_daily",
        cluster_by="date",
    )

    # ----- summary print -----------------------------------------------
    for tbl in [
        "src_dim_account",
        "src_events_login",
        "src_events_battle",
        "src_events_purchase",
        "src_events_progression",
        "src_events_social",
        "src_events_session",
        "src_clan_membership_daily",
    ]:
        cnt = spark.table(f"{args.catalog}.{args.schema}.{tbl}").count()
        logger.info("  %s: %s rows", tbl, f"{cnt:,}")


if __name__ == "__main__":
    main()
