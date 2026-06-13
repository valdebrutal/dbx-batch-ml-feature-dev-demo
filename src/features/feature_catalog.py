"""Feature catalog for the Declarative Feature Store.

Derived from the SDP MVs: every non-PK column of each feature-table MV becomes a
Feature, so adding a column to an MV adds a feature with no mapping to maintain.
Consumers select a subset by name and pass the Feature objects to
fe.create_training_set. The TIMESERIES PK on each MV makes it a governed,
point-in-time-joinable feature table, so no create_feature step is needed.
"""

from __future__ import annotations

from databricks.feature_engineering.entities import (
    ColumnSelection,
    DeltaTableSource,
    Feature,
)
from pyspark.sql import SparkSession

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
