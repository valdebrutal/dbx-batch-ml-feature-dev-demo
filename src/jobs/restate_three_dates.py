"""Demo action: restate 3 historical dates in the src_* tables.

Picks 3 deterministic dates (25%, 50%, 75% of the span). For every event source,
DELETE rows on those dates and INSERT replacements at the same event rates as the
seed (values differ, volume stays comparable) so downstream MVs refresh
incrementally rather than fully. Demonstrates the upstream-corruption / restated-data
backfill scenario.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from logging_config import configure_logging

logger = logging.getLogger(__name__)

PURCHASE_CATEGORIES = ["gems", "cards", "chest", "pass", "cosmetic"]
SOCIAL_TYPES = ["message", "invite", "gift"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    return p.parse_args()


def array_pick(values: list[str], seed_col: str) -> F.Column:
    arr = F.array(*[F.lit(v) for v in values])
    idx = (F.abs(F.hash(F.col(seed_col))) % F.lit(len(values))).cast("int")
    return arr.getItem(idx)


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    cs = f"{args.catalog}.{args.schema}"

    bounds = spark.sql(
        f"SELECT CAST(MIN(login_ts) AS DATE) AS lo, CAST(MAX(login_ts) AS DATE) AS hi "
        f"FROM {cs}.src_events_login"
    ).first()
    if bounds is None or bounds["lo"] is None:
        raise RuntimeError(f"{cs}.src_events_login is empty; run seed first.")
    lo: dt.date = bounds["lo"]
    hi: dt.date = bounds["hi"]
    span = (hi - lo).days
    target_dates = [
        lo + dt.timedelta(days=int(span * frac)) for frac in (0.25, 0.50, 0.75)
    ]
    logger.info("Restating dates: %s (span lo=%s hi=%s)", target_dates, lo, hi)

    target_dates_sql = ", ".join(f"DATE '{d}'" for d in target_dates)

    # ----- DELETE rows on target dates from every event source -----------
    # Restate only EVENT tables; the clan-membership dim snapshot is left alone
    # (restating a full daily snapshot would force a full recompute, and real
    # restatements usually correct events, not dim snapshots).
    for tbl, ts_col in [
        ("src_events_login", "login_ts"),
        ("src_events_battle", "battle_ts"),
        ("src_events_purchase", "purchase_ts"),
        ("src_events_progression", "level_up_ts"),
        ("src_events_social", "event_ts"),
        ("src_events_session", "session_ts"),
    ]:
        n = spark.sql(
            f"SELECT COUNT(*) AS n FROM {cs}.{tbl} "
            f"WHERE CAST({ts_col} AS DATE) IN ({target_dates_sql})"
        ).first()["n"]
        spark.sql(
            f"DELETE FROM {cs}.{tbl} "
            f"WHERE CAST({ts_col} AS DATE) IN ({target_dates_sql})"
        )
        logger.info("  deleted %s rows from %s", f"{n:,}", tbl)

    # ----- INSERT replacement rows ---------------------------------------
    accounts = spark.table(f"{cs}.src_dim_account")
    target_dates_df = spark.createDataFrame(
        [(d,) for d in target_dates], schema="event_date date"
    )
    # account_dates carries an `event_date` column already (the target date),
    # which is what the LC-clustered src_events_* tables expect.
    account_dates = accounts.crossJoin(target_dates_df).where(
        F.col("event_date") >= F.col("created_at")
    )

    def ts_within_day(date_col: str) -> F.Column:
        return F.expr(
            f"timestampadd(SECOND, cast(rand() * 86400 as int), to_timestamp({date_col}))"
        )

    def explode_by_rate(df, rate_col: F.Column):
        evt_count = F.when(
            rate_col >= F.lit(1.0),
            F.greatest(
                F.lit(0),
                (rate_col + (F.rand() - F.lit(0.5)) * rate_col).cast("int"),
            ),
        ).otherwise(F.when(F.rand() < rate_col, F.lit(1)).otherwise(F.lit(0)))
        return (
            df.withColumn("event_count", evt_count)
            .where(F.col("event_count") > 0)
            .withColumn("event_idx", F.explode(F.expr("sequence(1, event_count)")))
        )

    # Same rates as the seed: values differ but per-day volume is comparable, so
    # downstream MVs refresh incrementally rather than fully.
    logins = (
        explode_by_rate(account_dates, F.lit(1.2))
        .withColumn("login_ts", ts_within_day("event_date"))
        .select("account_id", "login_ts", "event_date")
    )
    logins.write.mode("append").saveAsTable(f"{cs}.src_events_login")

    battles = (
        explode_by_rate(account_dates, F.lit(2.0))
        .withColumn("battle_ts", ts_within_day("event_date"))
        .withColumn("won", (F.rand() < 0.52).cast("int"))
        .withColumn("duration_seconds", (F.rand() * 240 + 30).cast("int"))
        .select("account_id", "battle_ts", "won", "duration_seconds", "event_date")
    )
    battles.write.mode("append").saveAsTable(f"{cs}.src_events_battle")

    base_rate = (
        F.when(F.col("spender_class") == "whale", F.lit(0.4))
        .when(F.col("spender_class") == "dolphin", F.lit(0.15))
        .when(F.col("spender_class") == "minnow", F.lit(0.05))
        .otherwise(F.lit(0.001))
    )
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
    purchases = (
        explode_by_rate(account_dates, base_rate)
        .withColumn("purchase_ts", ts_within_day("event_date"))
        .withColumn("category", array_pick(PURCHASE_CATEGORIES, "purchase_ts"))
        .withColumn("amount_usd", amount_expr)
        .select("account_id", "purchase_ts", "category", "amount_usd", "event_date")
    )
    purchases.write.mode("append").saveAsTable(f"{cs}.src_events_purchase")

    progression = (
        explode_by_rate(account_dates, F.lit(0.35))
        .withColumn("level_up_ts", ts_within_day("event_date"))
        # new_level is best-effort: just bump a small random amount; the
        # exact value isn't load-bearing for the demo.
        .withColumn("new_level", (F.rand() * 50 + 1).cast("int"))
        .select("account_id", "level_up_ts", "new_level", "event_date")
    )
    progression.write.mode("append").saveAsTable(f"{cs}.src_events_progression")

    socials = (
        explode_by_rate(account_dates, F.lit(1.0))
        .withColumn("event_ts", ts_within_day("event_date"))
        .withColumn("event_type", array_pick(SOCIAL_TYPES, "event_ts"))
        .select("account_id", "event_ts", "event_type", "event_date")
    )
    socials.write.mode("append").saveAsTable(f"{cs}.src_events_social")

    sessions = (
        explode_by_rate(account_dates, F.lit(1.1))
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
    sessions.write.mode("append").saveAsTable(f"{cs}.src_events_session")

    # ----- summary -------------------------------------------------------
    for tbl, ts_col in [
        ("src_events_login", "login_ts"),
        ("src_events_battle", "battle_ts"),
        ("src_events_purchase", "purchase_ts"),
        ("src_events_progression", "level_up_ts"),
        ("src_events_social", "event_ts"),
        ("src_events_session", "session_ts"),
    ]:
        n = spark.sql(
            f"SELECT COUNT(*) AS n FROM {cs}.{tbl} "
            f"WHERE CAST({ts_col} AS DATE) IN ({target_dates_sql})"
        ).first()["n"]
        logger.info("  %-30s now %s rows for restated dates", tbl, f"{n:,}")


if __name__ == "__main__":
    main()
