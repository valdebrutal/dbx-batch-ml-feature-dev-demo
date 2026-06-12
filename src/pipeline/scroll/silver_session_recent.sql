-- Windowed feature table: silver_session over the last 30 days (rolling).
-- Uses current_date() in the WHERE filter so the window auto-advances each day
-- Per the incremental-refresh docs, current_date() is allowed in a WHERE clause.

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
COMMENT "silver_session over the last 30 days; rolling window via current_date()."
AS
SELECT
  account_id,
  observation_date,
  sessions_7d,
  avg_session_seconds_7d,
  sessions_ios_7d,
  sessions_android_7d
FROM silver_session
WHERE observation_date >= current_date() - INTERVAL 29 DAY;
