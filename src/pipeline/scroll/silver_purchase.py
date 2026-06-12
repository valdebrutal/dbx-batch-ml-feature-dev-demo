"""Purchase features per (account_id, observation_date): a Python materialized view.

Generates many columns programmatically (the SDP-native replacement for Hilbert's
Jinja/macro loops): the CATEGORIES x WINDOWS comprehension emits 30
``spend_<category>_<window>d`` aggregations from one loop. SQL and Python MVs coexist
in the same pipeline and reference each other by name.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Categories x trailing windows: one aggregation per pair -> 5 x 6 = 30 pivot columns.
CATEGORIES = ["gems", "cards", "chest", "pass", "cosmetic"]
WINDOWS_DAYS = [3, 7, 14, 30, 60, 90]
MAX_WINDOW = max(WINDOWS_DAYS)

# Code-generated pivot column names (also drives the declared schema below).
_PIVOT_COLS = [f"spend_{c}_{w}d" for c in CATEGORIES for w in WINDOWS_DAYS]

# Cross-category keepers consumed downstream (silver_purchase_velocity reads the
# spend_<cat>_7d columns; dim_spender_tier / silver_purchase_enriched read sum_30d;
# the training subset reads purchases_count_7d).
_KEEPERS = [
    ("purchases_count_7d", "BIGINT"),
    ("purchases_amount_sum_30d", "DOUBLE"),
    ("purchases_amount_sum_90d", "DOUBLE"),
]

# Declared schema (DDL) carries the TIMESERIES PK that makes this MV a UC feature
# table. Column order must match the agg produced below.
_SCHEMA = (
    ",\n  ".join(
        ["account_id BIGINT NOT NULL", "observation_date DATE NOT NULL"]
        + [f"{c} DOUBLE" for c in _PIVOT_COLS]
        + [f"{name} {typ}" for name, typ in _KEEPERS]
    )
    + ",\n  CONSTRAINT silver_purchase_pk PRIMARY KEY (account_id, observation_date TIMESERIES)"
)


def _spend(category: str, window: int) -> F.Column:
    """SUM(amount) for one category within a trailing `window`-day window."""
    return F.coalesce(
        F.sum(
            F.when(
                (F.col("category") == category) & F.col("days_back").between(0, window - 1),
                F.col("amount_usd"),
            )
        ),
        F.lit(0.0),
    ).alias(f"spend_{category}_{window}d")


@dp.materialized_view(
    name="silver_purchase",
    comment=(
        "Purchase features per (account_id, observation_date): per-category x window "
        "spend pivot (30 code-generated columns) + 7d count and 30d/90d totals."
    ),
    schema=_SCHEMA,
    cluster_by=["observation_date"],
)
def silver_purchase():
    # obs spine: one row per (account, observation_date) from account creation onward.
    obs = (
        spark.read.table("src_dim_account")
        .join(spark.read.table("date_spine"), F.col("observation_date") >= F.col("created_at"))
        .select("account_id", "observation_date")
        .alias("o")
    )
    # Attach each purchase that falls in the widest trailing window, with days_back.
    windowed = (
        obs.join(
            spark.read.table("src_events_purchase").alias("p"),
            (F.col("p.account_id") == F.col("o.account_id"))
            & (F.col("p.event_date") >= F.date_sub(F.col("o.observation_date"), MAX_WINDOW))
            & (F.col("p.event_date") <= F.col("o.observation_date")),
            "left",
        )
        .select(
            F.col("o.account_id").alias("account_id"),
            F.col("o.observation_date").alias("observation_date"),
            F.col("p.category").alias("category"),
            F.col("p.amount_usd").alias("amount_usd"),
            F.datediff(F.col("o.observation_date"), F.col("p.purchase_ts").cast("date")).alias("days_back"),
        )
    )

    # The macro replacement: a comprehension builds the 30 pivot aggregations.
    pivot_aggs = [_spend(c, w) for c in CATEGORIES for w in WINDOWS_DAYS]
    keepers = [
        F.count(F.when(F.col("days_back").between(0, 6), F.lit(1))).alias("purchases_count_7d"),
        F.coalesce(F.sum(F.when(F.col("days_back").between(0, 29), F.col("amount_usd"))), F.lit(0.0))
            .alias("purchases_amount_sum_30d"),
        F.coalesce(F.sum(F.when(F.col("days_back").between(0, 89), F.col("amount_usd"))), F.lit(0.0))
            .alias("purchases_amount_sum_90d"),
    ]
    return windowed.groupBy("account_id", "observation_date").agg(*pivot_aggs, *keepers)
