-- Dimension MV from silver_purchase: spender tier per (account, observation_date)
-- from 30-day purchase amount. Consumed by silver_purchase_enriched.

CREATE OR REFRESH MATERIALIZED VIEW dim_spender_tier (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  spender_tier STRING,
  CONSTRAINT valid_tier_value EXPECT (
    spender_tier IN ('whale', 'dolphin', 'minnow', 'non_spender')
  ),
  CONSTRAINT dim_spender_tier_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
COMMENT "Per-(account, observation_date) spender-tier label derived from 30d purchase sum."
AS
SELECT
  account_id,
  observation_date,
  CASE
    WHEN purchases_amount_sum_30d >= 100.0 THEN 'whale'
    WHEN purchases_amount_sum_30d >=  20.0 THEN 'dolphin'
    WHEN purchases_amount_sum_30d >    0.0 THEN 'minnow'
    ELSE 'non_spender'
  END AS spender_tier
FROM silver_purchase;
