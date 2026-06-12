-- Utility MV: one row per observation_date over the seeded history.
-- `${max_history_days}` comes from the pipeline `configuration:` block.

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW date_spine
COMMENT "One row per observation_date over the last ${max_history_days} days."
AS
WITH bounds AS (
  SELECT
    GREATEST(
      CAST(MIN(date) AS DATE),
      CAST(MAX(date) AS DATE) - INTERVAL ${max_history_days} DAY
    ) AS lo,
    CAST(MAX(date) AS DATE) AS hi
  FROM src_clan_membership_daily
)
SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS observation_date
FROM bounds;
