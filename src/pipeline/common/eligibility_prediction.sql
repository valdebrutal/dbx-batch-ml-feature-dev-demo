-- Prediction eligibility set: one row per account active (>=1 login in the
-- trailing 30 days) as of the latest observation_date. Built in the pipeline so it
-- runs alongside the feature MVs; build_prediction_set scores this set.

CREATE OR REFRESH MATERIALIZED VIEW eligibility_prediction
CLUSTER BY (observation_date)
COMMENT "Prediction eligibility set: accounts active in the trailing 30 days at the latest observation_date."
AS
WITH latest AS (
  SELECT MAX(observation_date) AS observation_date FROM date_spine
)
SELECT DISTINCT
  l.account_id,
  x.observation_date
FROM latest x
JOIN src_events_login l
  ON l.event_date >= x.observation_date - INTERVAL 30 DAY
 AND l.event_date <= x.observation_date;
