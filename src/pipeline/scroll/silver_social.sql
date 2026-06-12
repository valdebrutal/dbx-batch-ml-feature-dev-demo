-- 7d social event counts per (account, observation_date). The clan-membership
-- join lives downstream in silver_social_enriched so this MV stays single-source
-- and can incrementalize cleanly.

CREATE OR REFRESH MATERIALIZED VIEW silver_social (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  messages_7d BIGINT,
  invites_7d BIGINT,
  gifts_7d BIGINT,
  CONSTRAINT nonneg_event_counts EXPECT (messages_7d >= 0 AND invites_7d >= 0 AND gifts_7d >= 0),
  CONSTRAINT silver_social_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "7d social event counts per (account_id, observation_date)."
AS
WITH
  obs AS (
    SELECT a.account_id, d.observation_date
    FROM src_dim_account a
    CROSS JOIN date_spine d
    WHERE d.observation_date >= a.created_at
  ),
  windowed_events AS (
    SELECT
      obs.account_id,
      obs.observation_date,
      s.event_type
    FROM obs
    LEFT JOIN src_events_social s
      ON s.account_id = obs.account_id
     AND s.event_date BETWEEN obs.observation_date - INTERVAL 7 DAY AND obs.observation_date
  )
SELECT
  account_id,
  observation_date,
  COUNT(CASE WHEN event_type = 'message' THEN 1 END) AS messages_7d,
  COUNT(CASE WHEN event_type = 'invite'  THEN 1 END) AS invites_7d,
  COUNT(CASE WHEN event_type = 'gift'    THEN 1 END) AS gifts_7d
FROM windowed_events
GROUP BY account_id, observation_date;
