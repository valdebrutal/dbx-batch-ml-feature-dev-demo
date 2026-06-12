# sc_fs_demo

A working demo of **Spark Declarative Pipelines (SDP) + the Declarative Batch Feature Store**, orchestrated by a Lakeflow Job and declared via Databricks Asset Bundles. Fully serverless.

## What it does

1. **Seed** synthetic game data (~5K accounts × 540 days; 6 event streams + a clan-membership snapshot) into `<catalog>.<schema>.src_*`.
2. **SDP pipeline** (mostly SQL `CREATE OR REFRESH MATERIALIZED VIEW`, plus one Python MV) computes the silver feature MVs and a forward-window labels MV. `silver_purchase.py` is the Python MV: it code-generates a 30-column `spend_<category>_<window>d` pivot from a comprehension. SQL and Python MVs share one dataflow graph.
3. **Feature catalog** — `src/features/feature_catalog.py` (importable code, not a job task) derives the catalog from the MVs: every non-PK column of a listed feature MV becomes a `{name: Feature}` entry. No `create_feature`/registration step — each MV's `PRIMARY KEY (account_id, observation_date TIMESERIES)` already makes it a governed, point-in-time-joinable feature table in Unity Catalog.
4. **Two `create_training_set` calls** select a feature subset by name (`SCROLL_CHURN_FEATURES`): one over the historical labeled set (the `labels` MV), one over the latest-date set (the `eligibility_prediction` MV). A different model is just a different list of names — same MVs, same pipeline.

## Source tables (`src_*`)

Six append-only event streams keyed on `account_id` + an event timestamp (with a date-grain `event_date` cluster key), one account dimension, and one daily snapshot.

| Table | Grain | Content |
|---|---|---|
| `src_dim_account` | one row per `account_id` | Account dimension: `created_at`, `country`, `install_source`, `spender_class` (whale / dolphin / minnow / non_spender), `temporal_pattern` (recent / uniform). |
| `src_events_login` | one row per login | Login events (~1.2/account/day). Activity, recency, and the next-7d-login label. |
| `src_events_battle` | one row per battle | Battle outcomes (~2/account/day): `won` flag, `duration_seconds`. |
| `src_events_purchase` | one row per purchase | In-app purchases: `category` (gems / cards / chest / pass / cosmetic), `amount_usd`. |
| `src_events_progression` | one row per level-up | Level-ups (~0.35/account/day) with cumulative `new_level`. |
| `src_events_social` | one row per social event | Social interactions (~1/account/day): `event_type` (message / invite / gift). |
| `src_events_session` | one row per session | Play sessions (~1.1/account/day) with a raw `payload_json` (`{duration_s, device, level_reached}`) parsed via `from_json`. |
| `src_clan_membership_daily` | one row per (`account_id`, `date`) | Daily slowly-changing snapshot of clan membership: `is_clan_member` boolean. |

## Layout

```
demo/
├── databricks.yml      # bundle + artifacts (builds the project wheel)
├── pyproject.toml      # wheel: ships the `features` package + `logging_config`
├── resources/
│   ├── pipeline.yml    # serverless SDP
│   └── job.yml         # serverless Lakeflow Job (pipeline -> training + prediction builds)
└── src/
    ├── logging_config.py   # shared logging setup (in the wheel)
    ├── features/           # library (in the wheel): feature list + feature_catalog
    ├── jobs/               # entry-point scripts run as spark_python_task python_files
    └── pipeline/           # SDP source (.sql + silver_purchase.py)
```

Job scripts import the library with `from features import ...` / `import logging_config`: the bundle builds `src/` into a wheel and attaches it to each task's serverless `environments` block, so there is no `sys.path` manipulation.

## SQL patterns covered

| Pattern | File |
|---|---|
| Multi-stage CTEs | `pipeline/scroll/silver_battle.sql` |
| Conditional-aggregation pivot (code-generated, DataFrame API) | `pipeline/scroll/silver_purchase.py` |
| Forward range-window label | `pipeline/common/labels_did_login_within_7d.sql` |
| JSON struct parsing | `pipeline/scroll/silver_session.sql` (`from_json`) |
| Snapshot / dim joins | `pipeline/common/silver_account.sql`, `pipeline/scroll/silver_social_enriched.sql` |
| Rolling "last N days" window (incrementally refreshable) | `pipeline/scroll/silver_session_recent.sql` |
| Feature subset selection (per-model) | `jobs/build_training_set.py`, `build_prediction_set.py` |

## Deploy + run

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run sc_fs_demo_orchestrator
```

Set your CLI profile in `databricks.yml` (`targets.dev.workspace.profile`) and your `catalog`/`schema` via the bundle variables.

## Outputs to inspect after a run

- `<catalog>.<schema>.src_*` — 8 source tables
- `<catalog>.<schema>.silver_*` / `dim_spender_tier` — feature-table MVs + intermediates
- `labels_did_login_within_7d` — label MV (also the training eligibility set)
- `eligibility_prediction` — prediction eligibility set (active accounts at the latest observation_date)
- `scroll_raw_training` — training dataset (eligibility set + PIT-joined features + label)
- `scroll_raw_prediction` — prediction dataset (eligibility set + PIT-joined features, no label)
