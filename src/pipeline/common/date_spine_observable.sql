-- Subset of date_spine restricted to obs_dates whose forward 7-day window is
-- fully observable. Used by labels_did_login_within_7d so the cutoff isn't an
-- inline scalar subquery (which would break incremental refresh).

CREATE OR REFRESH MATERIALIZED VIEW date_spine_observable
COMMENT "obs_dates from date_spine restricted to the labels-observable range."
AS
SELECT d.observation_date
FROM date_spine d
CROSS JOIN (SELECT MAX(observation_date) AS hi FROM date_spine) m
WHERE d.observation_date <= m.hi - INTERVAL 7 DAY;
