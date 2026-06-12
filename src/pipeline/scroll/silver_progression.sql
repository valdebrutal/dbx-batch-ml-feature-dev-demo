-- Level-progression features (7d/30d level-ups + current level). TIMESERIES PK
-- makes this a UC feature table.

CREATE OR REFRESH MATERIALIZED VIEW silver_progression (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  level_ups_7d BIGINT,
  level_ups_30d BIGINT,
  current_level INT,
  CONSTRAINT valid_level EXPECT (current_level >= 1),
  CONSTRAINT nonneg_level_ups EXPECT (level_ups_7d >= 0 AND level_ups_30d >= 0),
  CONSTRAINT consistent_levelup_windows EXPECT (level_ups_7d <= level_ups_30d),
  CONSTRAINT silver_progression_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "Level progression features per (account_id, observation_date)."
AS
WITH
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
      p.new_level,
      DATEDIFF(obs.observation_date, CAST(p.level_up_ts AS DATE)) AS days_back
    FROM obs
    LEFT JOIN src_events_progression p
      ON p.account_id = obs.account_id
     AND p.event_date <= obs.observation_date
  )
SELECT
  account_id,
  observation_date,
  COUNT(CASE WHEN days_back BETWEEN 0 AND 6  THEN 1 END) AS level_ups_7d,
  COUNT(CASE WHEN days_back BETWEEN 0 AND 29 THEN 1 END) AS level_ups_30d,
  COALESCE(MAX(new_level), 1) AS current_level
FROM windowed
GROUP BY account_id, observation_date;
