"""Assemble the training dataset via FS create_training_set.

Spine: historical (account_id, observation_date) pairs joined to the labels MV.
Features: a subset of the shared catalog, selected by name. Output: UC table
scroll_raw_training. Replaces Hilbert's auto_join for the training shape.
"""

from __future__ import annotations

import argparse
import logging

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import SparkSession

from features import SCROLL_CHURN_FEATURES
from features.feature_catalog import get_features
from logging_config import configure_logging

logger = logging.getLogger(__name__)


# This scenario selects SCROLL_CHURN_FEATURES by name from the shared catalog; a
# different scenario is just a different name list over the same MVs.


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument(
        "--output-table",
        default="scroll_raw_training",
        help="Output UC table name (under catalog.schema).",
    )
    return p.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    fe = FeatureEngineeringClient()

    # Training eligibility set: every (account_id, observation_date) where the
    # forward-window label is observable. Labels MV already filters out the last 7 days.
    eligibility_df = spark.table(
        f"{args.catalog}.{args.schema}.labels_did_login_within_7d"
    ).select("account_id", "observation_date", "did_login_within_7d")

    features = get_features(spark, args.catalog, args.schema, SCROLL_CHURN_FEATURES)

    training_set = fe.create_training_set(
        df=eligibility_df,
        features=features,
        label="did_login_within_7d",
    )
    training_df = training_set.load_df()
    logger.info("  training set schema: %d cols", len(training_df.columns))

    output_full = f"{args.catalog}.{args.schema}.{args.output_table}"
    (
        training_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(output_full)
    )
    logger.info("  wrote %s", output_full)


if __name__ == "__main__":
    main()
