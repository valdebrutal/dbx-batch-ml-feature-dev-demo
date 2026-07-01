"""Register the shared feature catalog into Unity Catalog via fe.register_feature.

One-time (idempotent) action: persists each MV-derived Feature as a governed UC feature
object. create_training_set does not require this -- we register so each feature gets
feature-level lineage and catalog discoverability.
"""

from __future__ import annotations

import argparse
import logging

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import SparkSession

from features.feature_catalog import register_features
from logging_config import configure_logging

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    return p.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    fe = FeatureEngineeringClient()
    registered = register_features(fe, spark, args.catalog, args.schema)
    logger.info("registered %d features", len(registered))


if __name__ == "__main__":
    main()
