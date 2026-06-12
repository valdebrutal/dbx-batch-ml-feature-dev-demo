"""Assemble the prediction dataset via FS create_training_set (no label).

Eligibility set: the eligibility_prediction MV (accounts active at the latest
observation_date). Features: the same selection as the training branch. Output: UC
table scroll_raw_prediction. One set of feature definitions serves both branches;
the training/prediction split is just the eligibility-set choice (mirrors Hilbert's
branch distinction).
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession

from databricks.feature_engineering import FeatureEngineeringClient

from features import SCROLL_CHURN_FEATURES
from features.feature_catalog import get_features
from logging_config import configure_logging


logger = logging.getLogger(__name__)


# Feature selection must match the training branch (same SCROLL_CHURN_FEATURES).


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument(
        "--output-table",
        default="scroll_raw_prediction",
        help="Output UC table name (under catalog.schema).",
    )
    return p.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    fe = FeatureEngineeringClient()

    # Eligibility set: the eligibility_prediction MV (active accounts at the latest
    # observation_date).
    eligibility_df = spark.table(f"{args.catalog}.{args.schema}.eligibility_prediction")

    features = get_features(spark, args.catalog, args.schema, SCROLL_CHURN_FEATURES)

    # Same FS API as the training branch with label=None: features over the eligibility set.
    prediction_df = fe.create_training_set(
        df=eligibility_df,
        features=features,
        label=None,
    ).load_df()
    logger.info("  prediction set schema: %d cols", len(prediction_df.columns))

    output_full = f"{args.catalog}.{args.schema}.{args.output_table}"
    (
        prediction_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(output_full)
    )
    logger.info("  wrote %s", output_full)


if __name__ == "__main__":
    main()
