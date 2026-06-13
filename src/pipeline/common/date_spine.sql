-- Observation dates in the run's partition window.
--
-- day_after_partition_date (a per-run JOB parameter, read with the colon prefix) is the
-- RUN date: the job is scheduled at day+1 00:00, so the partition date being processed is
-- :day_after_partition_date - 1. The window is the 30 days ending at that partition date.
-- The window size is a literal, NOT a ${configuration} value: a named parameter (:...)
-- and a ${configuration} reference cannot share one statement, because SDP runs the
-- named-parameter substitution before ${...} substitution and the param pass fails to
-- parse the raw ${...} placeholder.
-- Every feature MV is built on date_spine, so this single window bounds the range of data
-- the whole pipeline processes. Override the param via "Run now with different parameters".

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW date_spine
COMMENT "Observation dates in the run's partition window, ending at the partition date (day_after_partition_date - 1)."
AS
WITH bounds AS (
  SELECT CAST(:day_after_partition_date AS DATE) - INTERVAL 1 DAY AS partition_date
)
SELECT explode(sequence(partition_date - INTERVAL 30 DAY, partition_date, INTERVAL 1 DAY)) AS observation_date
FROM bounds;
