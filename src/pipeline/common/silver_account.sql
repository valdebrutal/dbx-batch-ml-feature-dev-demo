-- Account snapshot features (age + categorical attributes). The TIMESERIES PK
-- makes this a point-in-time-joinable UC feature table.

CREATE OR REFRESH MATERIALIZED VIEW silver_account (
  account_id BIGINT NOT NULL,
  observation_date DATE NOT NULL,
  account_age_days INT,
  country STRING,
  install_source STRING,
  CONSTRAINT nonneg_account_age EXPECT (account_age_days IS NULL OR account_age_days >= 0),
  CONSTRAINT silver_account_pk PRIMARY KEY (account_id, observation_date TIMESERIES)
)
CLUSTER BY (observation_date)
COMMENT "Account-level snapshot features per (account_id, observation_date)."
AS
SELECT
  a.account_id,
  d.observation_date,
  DATEDIFF(d.observation_date, a.created_at) AS account_age_days,
  a.country,
  a.install_source
FROM src_dim_account a
CROSS JOIN date_spine d
WHERE d.observation_date >= a.created_at;
