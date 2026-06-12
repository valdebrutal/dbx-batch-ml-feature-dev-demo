-- Downstream of silver_social: adds the clan-membership snapshot column via a
-- separate join to src_clan_membership_daily, keeping silver_social on a single
-- event source so it can incrementalize cleanly.

CREATE OR REFRESH MATERIALIZED VIEW silver_social_enriched (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  is_clan_member BOOLEAN,
  CONSTRAINT silver_social_enriched_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "Clan-membership snapshot per (account_id, observation_date), joined from silver_social's obs grain."
AS
SELECT
  s.account_id,
  s.observation_date,
  COALESCE(c.is_clan_member, FALSE) AS is_clan_member
FROM silver_social s
LEFT JOIN src_clan_membership_daily c
  ON c.account_id = s.account_id
 AND c.date       = s.observation_date;
