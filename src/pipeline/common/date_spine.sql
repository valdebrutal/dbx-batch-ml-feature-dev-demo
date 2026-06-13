-- Observation dates in the rolling window: the last 30 days ending today, via
-- current_date(). No job parameter, no pipeline-parameters preview required. Every
-- feature MV is built on date_spine, so this single window bounds the range of data the
-- whole pipeline processes each run.
-- Trade-off vs a passed partition date: simple and auto-rolling, but not idempotent --
-- a rerun uses today's date, and you cannot reprocess/backfill a past date.

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW date_spine
COMMENT "Observation dates over the last 30 days (ending today, via current_date())."
AS
WITH bounds AS (
  SELECT current_date() - INTERVAL 29 DAY AS lo, current_date() AS hi
)
SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS observation_date
FROM bounds;
