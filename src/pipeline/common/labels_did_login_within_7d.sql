-- Forward-window LABEL (not a feature): looks ahead 1..7 days from observation_date.
-- The observable cutoff is pre-computed in date_spine_observable to keep this plan shallow.

CREATE OR REFRESH MATERIALIZED VIEW labels_did_login_within_7d
CLUSTER BY (observation_date)
COMMENT "Boolean label: account logged in within the 7 days AFTER observation_date."
AS
WITH
  obs AS (
    SELECT a.account_id, d.observation_date
    FROM src_dim_account a
    CROSS JOIN date_spine_observable d
    WHERE d.observation_date >= a.created_at
  )
SELECT
  obs.account_id,
  obs.observation_date,
  CAST(COUNT(l.login_ts) > 0 AS BOOLEAN) AS did_login_within_7d
FROM obs
LEFT JOIN src_events_login l
  ON l.account_id = obs.account_id
 AND l.event_date BETWEEN obs.observation_date + INTERVAL 1 DAY AND obs.observation_date + INTERVAL 7 DAY
GROUP BY obs.account_id, obs.observation_date;
