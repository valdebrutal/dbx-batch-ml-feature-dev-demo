-- Rolling "last N days" window over silver_session. Uses a FIXED date parameter
-- (${window_start_date}), not current_date(), so the filter is deterministic and
-- incrementally refreshable; the orchestrator advances the parameter once per run.

CREATE OR REFRESH MATERIALIZED VIEW silver_session_recent (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  sessions_7d BIGINT,
  avg_session_seconds_7d DOUBLE,
  sessions_ios_7d BIGINT,
  sessions_android_7d BIGINT,
  CONSTRAINT silver_session_recent_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "silver_session from ${window_start_date} onward; rolling window via a fixed date parameter (incrementally refreshable)."
AS
SELECT
  account_id,
  observation_date,
  sessions_7d,
  avg_session_seconds_7d,
  sessions_ios_7d,
  sessions_android_7d
FROM silver_session
WHERE observation_date >= DATE '${window_start_date}';
