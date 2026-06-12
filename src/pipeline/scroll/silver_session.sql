-- Session features with from_json payload parsing (Chronos-payload stand-in).
-- TIMESERIES PK makes this a UC feature table.

CREATE OR REFRESH MATERIALIZED VIEW silver_session (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  sessions_7d BIGINT,
  avg_session_seconds_7d DOUBLE,
  sessions_ios_7d BIGINT,
  sessions_android_7d BIGINT,
  CONSTRAINT nonneg_sessions EXPECT (
    sessions_7d >= 0 AND sessions_ios_7d >= 0 AND sessions_android_7d >= 0
  ),
  CONSTRAINT nonneg_session_seconds EXPECT (
    avg_session_seconds_7d IS NULL OR avg_session_seconds_7d >= 0.0
  ),
  CONSTRAINT device_split_subset EXPECT (sessions_ios_7d + sessions_android_7d <= sessions_7d),
  CONSTRAINT silver_session_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "Session features per (account_id, observation_date); parses JSON payload."
AS
WITH
  parsed AS (
    -- Parse the JSON payload into typed struct fields.
    SELECT
      account_id,
      session_ts,
      event_date,
      from_json(
        payload_json,
        'STRUCT<duration_s: INT, device: STRING, level_reached: INT>'
      ) AS payload
    FROM src_events_session
  ),
  obs AS (
    SELECT a.account_id, d.observation_date
    FROM src_dim_account a
    CROSS JOIN date_spine d
    WHERE d.observation_date >= a.created_at
  ),
  windowed AS (
    SELECT
      obs.account_id,
      obs.observation_date,
      parsed.payload.duration_s     AS duration_s,
      parsed.payload.device         AS device
    FROM obs
    LEFT JOIN parsed
      ON parsed.account_id = obs.account_id
     AND parsed.event_date BETWEEN obs.observation_date - INTERVAL 7 DAY AND obs.observation_date
  )
SELECT
  account_id,
  observation_date,
  COUNT(duration_s) AS sessions_7d,
  AVG(CAST(duration_s AS DOUBLE)) AS avg_session_seconds_7d,
  COUNT(CASE WHEN device = 'ios'     THEN 1 END) AS sessions_ios_7d,
  COUNT(CASE WHEN device = 'android' THEN 1 END) AS sessions_android_7d
FROM windowed
GROUP BY account_id, observation_date;
