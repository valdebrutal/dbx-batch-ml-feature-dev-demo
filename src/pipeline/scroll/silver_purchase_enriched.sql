-- Fan-in MV with 3 upstream MVs (silver_purchase, dim_spender_tier,
-- silver_purchase_velocity). Exposes is_whale, is_high_velocity, enriched_spend_score.
-- The JOIN + COALESCE is isolated in a `joined` CTE; the final SELECT only projects.

CREATE OR REFRESH MATERIALIZED VIEW silver_purchase_enriched (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  is_whale BOOLEAN,
  is_high_velocity BOOLEAN,
  enriched_spend_score DOUBLE,
  CONSTRAINT nonneg_score EXPECT (enriched_spend_score IS NULL OR enriched_spend_score >= 0.0),
  CONSTRAINT silver_purchase_enriched_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "Enriched purchase features combining spender-tier dim and 7d/30d velocity intermediate."
AS
WITH joined AS (
  SELECT
    p.account_id,
    p.observation_date,
    p.purchases_amount_sum_30d,
    d.spender_tier,
    COALESCE(v.spend_velocity_7d_over_30d, 0.0) AS velocity_safe
  FROM silver_purchase p
  LEFT JOIN dim_spender_tier d
    ON d.account_id       = p.account_id
   AND d.observation_date = p.observation_date
  LEFT JOIN silver_purchase_velocity v
    ON v.account_id       = p.account_id
   AND v.observation_date = p.observation_date
)
SELECT
  account_id,
  observation_date,
  spender_tier = 'whale' AS is_whale,
  velocity_safe > 0.5    AS is_high_velocity,
  CASE spender_tier
    WHEN 'whale'   THEN 3.0
    WHEN 'dolphin' THEN 2.0
    WHEN 'minnow'  THEN 1.0
    ELSE                0.0
  END * velocity_safe * purchases_amount_sum_30d
    AS enriched_spend_score
FROM joined;
