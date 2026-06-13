-- Observation dates sourced from the event data itself (events exist for every date).
-- A plain DISTINCT over an append-only source is incrementally maintainable: a new day of
-- events adds exactly one date, so date_spine refreshes ROW_BASED on that single row rather
-- than overwriting. That keeps every feature MV built on it incremental too. (A moving
-- window like "max(event_date) - 29" would instead force a COMPLETE_RECOMPUTE -- the bound
-- depends on a global aggregate -- which overwrites all rows and cascades downstream. If a
-- bounded serving window is needed, apply it in the training/eligibility layer, not here.)

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW date_spine
COMMENT "Distinct observation dates, derived from the event data (incrementally maintainable)."
AS
SELECT DISTINCT event_date AS observation_date
FROM src_events_login;
