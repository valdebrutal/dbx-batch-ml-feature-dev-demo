"""Append one new day of synthetic events to the src_* tables.

Simulates the daily cadence: events for day N+1 = max(login_ts).date + 1 are
appended to the src_events_* tables (and a snapshot row to src_clan_membership_daily)
so the next pipeline run sees them as new upstream data and refreshes incrementally.
No-ops if the target day already has logins (rerun safety).
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

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


def append_delta(df, full_name: str) -> None:
    df.write.mode("append").saveAsTable(full_name)


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    cs = f"{args.catalog}.{args.schema}"

    # Determine the new day = max(login_ts).date + 1.
    max_date_row = spark.sql(
        f"SELECT CAST(MAX(login_ts) AS DATE) AS d FROM {cs}.src_events_login"
    ).first()
    if max_date_row is None or max_date_row["d"] is None:
        raise RuntimeError(f"{cs}.src_events_login is empty; run the seed first.")
    new_day: dt.date = max_date_row["d"] + dt.timedelta(days=1)
    logger.info("Appending events for new day: %s", new_day)

    # Guard: skip if this day already has logins (rerun safety).
    already = spark.sql(
        f"SELECT COUNT(*) AS n FROM {cs}.src_events_login "
        f"WHERE CAST(login_ts AS DATE) = DATE '{new_day}'"
    ).first()["n"]
    if already > 0:
        logger.info("Day %s already has %d login rows; nothing to append.", new_day, already)
        return

    new_day_lit = F.lit(new_day).cast("date")
    new_day_ts_base = F.lit(new_day).cast("timestamp")

    accounts = spark.table(f"{cs}.src_dim_account").where(F.col("created_at") <= new_day_lit)

    def ts_within_new_day() -> F.Column:
        return F.expr(
            f"timestampadd(SECOND, cast(rand() * 86400 as int), to_timestamp(DATE '{new_day}'))"
        )

    def explode_by_rate(df, rate_col: F.Column):
        """Replicate each (account) row by an integer event count derived from rate_col."""
        evt_count = F.when(
            rate_col >= F.lit(1.0),
            F.greatest(
                F.lit(0),
                (rate_col + (F.rand() - F.lit(0.5)) * rate_col).cast("int"),
            ),
        ).otherwise(
            F.when(F.rand() < rate_col, F.lit(1)).otherwise(F.lit(0))
        )
        return (
            df.withColumn("event_count", evt_count)
            .where(F.col("event_count") > 0)
            .withColumn("event_idx", F.explode(F.expr("sequence(1, event_count)")))
        )

    # ----- logins (uniform rate, ~1.2/day jittered) ---------------------
    logins = (
        explode_by_rate(accounts, F.lit(1.2))
        .withColumn("login_ts", ts_within_new_day())
        .withColumn("event_date", new_day_lit)
        .select("account_id", "login_ts", "event_date")
    )
    append_delta(logins, f"{cs}.src_events_login")

    # ----- battles ------------------------------------------------------
    battles = (
        explode_by_rate(accounts, F.lit(2.0))
        .withColumn("battle_ts", ts_within_new_day())
        .withColumn("won", (F.rand() < 0.52).cast("int"))
        .withColumn("duration_seconds", (F.rand() * 240 + 30).cast("int"))
        .withColumn("event_date", new_day_lit)
        .select("account_id", "battle_ts", "won", "duration_seconds", "event_date")
    )
    append_delta(battles, f"{cs}.src_events_battle")

    # ----- purchases — uses spender_class for class-specific rate + amount
    base_rate = (
        F.when(F.col("spender_class") == "whale",       F.lit(0.4))
         .when(F.col("spender_class") == "dolphin",     F.lit(0.15))
         .when(F.col("spender_class") == "minnow",      F.lit(0.05))
         .otherwise(F.lit(0.001))
    )
    amount_expr = (
        F.when(F.col("spender_class") == "whale",
               F.round(F.lit(5.0) + F.rand() * F.lit(45.0), 2))
         .when(F.col("spender_class") == "dolphin",
               F.round(F.lit(2.0) + F.rand() * F.lit(13.0), 2))
         .when(F.col("spender_class") == "minnow",
               F.round(F.lit(0.5) + F.rand() * F.lit(3.0), 2))
         .otherwise(F.round(F.lit(0.5) + F.rand() * F.lit(1.0), 2))
    )
    purchases = (
        explode_by_rate(accounts, base_rate)
        .withColumn("purchase_ts", ts_within_new_day())
        .withColumn("category", array_pick(PURCHASE_CATEGORIES, "purchase_ts"))
        .withColumn("amount_usd", amount_expr)
        .withColumn("event_date", new_day_lit)
        .select("account_id", "purchase_ts", "category", "amount_usd", "event_date")
    )
    append_delta(purchases, f"{cs}.src_events_purchase")

    # ----- progression --------------------------------------------------
    progression_new = (
        explode_by_rate(accounts, F.lit(0.35))
        .withColumn("level_up_ts", ts_within_new_day())
        .select("account_id", "level_up_ts")
    )
    # Compute the new_level by reading current max level per account and bumping.
    current_max = (
        spark.table(f"{cs}.src_events_progression")
        .groupBy("account_id")
        .agg(F.max("new_level").alias("max_level"))
    )
    w = Window.partitionBy("account_id").orderBy("level_up_ts")
    progression_new = (
        progression_new.join(current_max, on="account_id", how="left")
        .withColumn("max_level", F.coalesce(F.col("max_level"), F.lit(1)))
        .withColumn("new_level", F.col("max_level") + F.row_number().over(w))
        .withColumn("event_date", new_day_lit)
        .select("account_id", "level_up_ts", "new_level", "event_date")
    )
    append_delta(progression_new, f"{cs}.src_events_progression")

    # ----- social -------------------------------------------------------
    socials = (
        explode_by_rate(accounts, F.lit(1.0))
        .withColumn("event_ts", ts_within_new_day())
        .withColumn("event_type", array_pick(SOCIAL_TYPES, "event_ts"))
        .withColumn("event_date", new_day_lit)
        .select("account_id", "event_ts", "event_type", "event_date")
    )
    append_delta(socials, f"{cs}.src_events_social")

    # ----- sessions (JSON payload) --------------------------------------
    sessions = (
        explode_by_rate(accounts, F.lit(1.1))
        .withColumn("session_ts", ts_within_new_day())
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
        .withColumn("event_date", new_day_lit)
        .select("account_id", "session_ts", "payload_json", "event_date")
    )
    append_delta(sessions, f"{cs}.src_events_session")

    # ----- clan membership snapshot -------------------------------------
    # Carry forward each account's last-known is_clan_member value (or FALSE).
    last_known = (
        spark.table(f"{cs}.src_clan_membership_daily")
        .withColumn(
            "rn",
            F.row_number().over(Window.partitionBy("account_id").orderBy(F.col("date").desc())),
        )
        .where(F.col("rn") == 1)
        .select("account_id", "is_clan_member")
    )
    clan_new = (
        accounts.select("account_id")
        .join(last_known, on="account_id", how="left")
        .withColumn("is_clan_member", F.coalesce(F.col("is_clan_member"), F.lit(False)))
        .withColumn("date", new_day_lit)
        .select("account_id", "date", "is_clan_member")
    )
    append_delta(clan_new, f"{cs}.src_clan_membership_daily")

    # ----- summary ------------------------------------------------------
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
            f"WHERE CAST({ts_col} AS DATE) = DATE '{new_day}'"
        ).first()["n"]
        logger.info("  %-30s appended %s rows for %s", tbl, f"{n:,}", new_day)


if __name__ == "__main__":
    main()
