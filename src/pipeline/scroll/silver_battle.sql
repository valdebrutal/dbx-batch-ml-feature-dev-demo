-- Rolling 7d battle features via a 3-stage CTE. TIMESERIES PK makes this a
-- UC feature table.

CREATE OR REFRESH MATERIALIZED VIEW silver_battle (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  battles_7d BIGINT,
  win_rate_7d DOUBLE,
  avg_duration_7d DOUBLE,
  CONSTRAINT valid_win_rate EXPECT (win_rate_7d IS NULL OR win_rate_7d BETWEEN 0.0 AND 1.0),
  CONSTRAINT nonneg_duration EXPECT (avg_duration_7d IS NULL OR avg_duration_7d >= 0.0),
  CONSTRAINT silver_battle_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "Battle features per (account_id, observation_date), rolling 7d window."
AS
WITH
  obs AS (
    -- Stage 1: enumerate (account, observation_date) pairs from account creation onward.
    SELECT a.account_id, d.observation_date
    FROM src_dim_account a
    CROSS JOIN date_spine d
    WHERE d.observation_date >= a.created_at
  ),
  events_in_window AS (
    -- Stage 2: LEFT JOIN events that fall inside the 7-day backward window for each obs.
    SELECT
      obs.account_id,
      obs.observation_date,
      b.won,
      b.duration_seconds
    FROM obs
    LEFT JOIN src_events_battle b
      ON b.account_id = obs.account_id
     AND b.event_date BETWEEN obs.observation_date - INTERVAL 7 DAY AND obs.observation_date
  ),
  aggregates AS (
    -- Stage 3: per-(account, obs) rollups. NULLs from LEFT JOIN drop out of COUNT/AVG.
    SELECT
      account_id,
      observation_date,
      COUNT(won) AS battles_7d,
      AVG(CAST(won AS DOUBLE)) AS win_rate_7d,
      AVG(CAST(duration_seconds AS DOUBLE)) AS avg_duration_7d
    FROM events_in_window
    GROUP BY account_id, observation_date
  )
SELECT * FROM aggregates;
