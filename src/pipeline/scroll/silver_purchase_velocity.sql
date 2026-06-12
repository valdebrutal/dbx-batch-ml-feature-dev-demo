-- Intermediate MV from silver_purchase: per-(account, day) velocity ratio
-- (7d sum / 30d sum). Not a feature; consumed by silver_purchase_enriched.

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW silver_purchase_velocity (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  spend_7d_total DOUBLE,
  spend_velocity_7d_over_30d DOUBLE,
  CONSTRAINT nonneg_spend_total EXPECT (spend_7d_total IS NULL OR spend_7d_total >= 0.0),
  CONSTRAINT velocity_in_range EXPECT (
    spend_velocity_7d_over_30d IS NULL
    OR spend_velocity_7d_over_30d BETWEEN 0.0 AND 1.0
  ),
  CONSTRAINT silver_purchase_velocity_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "Intermediate purchase velocity: ratio of 7d spend over 30d spend per account."
AS
SELECT
  account_id,
  observation_date,
  spend_gems_7d + spend_cards_7d + spend_chest_7d + spend_pass_7d + spend_cosmetic_7d
    AS spend_7d_total,
  CASE
    WHEN purchases_amount_sum_30d > 0.0
    THEN (spend_gems_7d + spend_cards_7d + spend_chest_7d + spend_pass_7d + spend_cosmetic_7d)
         / purchases_amount_sum_30d
    ELSE NULL
  END AS spend_velocity_7d_over_30d
FROM silver_purchase;
