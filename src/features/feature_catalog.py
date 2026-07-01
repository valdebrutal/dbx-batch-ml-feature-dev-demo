"""Feature catalog for the Declarative Feature Store.

Derived from the SDP MVs: every non-PK column of each feature-table MV becomes a
Feature, so adding a column to an MV adds a feature with no mapping to maintain.
Consumers select a subset by name and pass the Feature objects to
fe.create_training_set. The TIMESERIES PK on each MV makes it a governed,
point-in-time-joinable feature table, so no create_feature step is needed.
"""

from __future__ import annotations

import logging

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    ColumnSelection,
    DeltaTableSource,
    Feature,
)
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

ENTITY: list[str] = ["account_id"]
TIMESERIES_COLUMN: str = "observation_date"

# PK columns (entity + timeseries) are not features.
_NON_FEATURE_COLUMNS: set[str] = set(ENTITY) | {TIMESERIES_COLUMN}

# Explicit allow-list of feature-table MVs. Intermediates, date spines, label and
# eligibility MVs are excluded by omission.
FEATURE_TABLES: list[str] = [
    "silver_account",
    "silver_battle",
    "silver_purchase",
    "silver_progression",
    "silver_social",
    "silver_social_enriched",
    "silver_session",
    "silver_purchase_enriched",
]


def build_catalog(spark: SparkSession, catalog: str, schema: str) -> dict[str, Feature]:
    """Derive ``{feature_name: Feature}`` from the columns of the FEATURE_TABLES MVs.

    Feature name == column name. Reads each MV's schema and wraps every non-PK column
    as a ``ColumnSelection`` Feature, so the catalog tracks the MVs with no mapping to
    maintain.
    """
    catalog_map: dict[str, Feature] = {}
    for table in FEATURE_TABLES:
        fqn = f"{catalog}.{schema}.{table}"
        for column in spark.table(fqn).columns:
            if column in _NON_FEATURE_COLUMNS:
                continue
            if column in catalog_map:
                raise ValueError(
                    f"Duplicate feature name '{column}' (in {table}); feature names "
                    f"must be unique across FEATURE_TABLES."
                )
            catalog_map[column] = Feature(
                source=DeltaTableSource(
                    catalog_name=catalog,
                    schema_name=schema,
                    table_name=table,
                ),
                function=ColumnSelection(column=column),
                entity=ENTITY,
                timeseries_column=TIMESERIES_COLUMN,
                name=column,
            )
    return catalog_map


def register_features(
    fe: FeatureEngineeringClient, spark: SparkSession, catalog: str, schema: str
) -> list[Feature]:
    """Register every catalog Feature in UC via ``fe.register_feature``.

    create_training_set does not require this; we register so each feature is a governed
    UC object with feature-level lineage (source columns -> feature -> the training sets
    and models that consume it). Re-runs that hit an already-registered feature are
    logged and skipped.
    """
    registered: list[Feature] = []
    for name, feature in build_catalog(spark, catalog, schema).items():
        try:
            registered.append(
                fe.register_feature(
                    feature=feature, catalog_name=catalog, schema_name=schema
                )
            )
        except Exception as exc:  # ponytail: exact type unknown until first run; log+continue so re-runs are idempotent
            logger.warning("skip register '%s': %s", name, exc)
    return registered


def get_features(
    spark: SparkSession, catalog: str, schema: str, names: list[str]
) -> list[Feature]:
    """Pick Feature objects from the catalog by name, preserving ``names`` order."""
    catalog_map = build_catalog(spark, catalog, schema)
    unknown = [n for n in names if n not in catalog_map]
    if unknown:
        raise KeyError(
            f"Unknown feature name(s): {unknown}. Known features: {sorted(catalog_map)}"
        )
    return [catalog_map[n] for n in names]
