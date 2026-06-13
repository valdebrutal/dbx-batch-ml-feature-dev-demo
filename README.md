# sc_fs_demo

A working demo of **Spark Declarative Pipelines (SDP) + the Declarative Batch Feature Store**, orchestrated by a Lakeflow Job and declared via Databricks Asset Bundles. Fully serverless.

The pipeline turns raw event tables into governed, point-in-time-joinable feature tables (materialized views), then assembles training and prediction datasets from them with `create_training_set` — no feature registration step, no hand-written joins.

## Start here

1. **Run it** — see [Deploy + run](#deploy--run). One command seeds data; one runs the whole DAG.
2. **`src/pipeline/`** — the feature logic. Each `.sql` file is one materialized view (one feature table). Read `common/silver_account.sql` first (simplest), then `scroll/silver_session.sql` (windowed aggregation + JSON parsing).
3. **`src/features/feature_catalog.py`** — how features are discovered from the MVs (no registration).
4. **`src/jobs/build_training_set.py`** — how a model's feature subset becomes a training set via `create_training_set` (the replacement for a wide `auto_join` table).
5. **`resources/`** — how it's wired: `pipeline.yml` (the SDP pipeline) and `job.yml` (the orchestrating DAG).

## Repo map

```
demo/
├── databricks.yml              # Bundle: variables (catalog/schema), wheel artifact, targets
├── pyproject.toml              # Wheel: ships the `features` package + `logging_config`
├── resources/
│   ├── pipeline.yml            # Serverless SDP pipeline (loads src/pipeline/**)
│   ├── job.yml                 # Orchestrator: pipeline -> build_training_set + build_prediction_set
│   ├── seed_initial_data_job.yml      # one-time data seed
│   ├── append_one_day_job.yml         # demo action: advance the data by one day
│   └── restate_three_dates_job.yml    # demo action: restate 3 historical dates
└── src/
    ├── logging_config.py       # shared logging (in the wheel)
    ├── features/
    │   ├── __init__.py             # SCROLL_CHURN_FEATURES: the per-model feature list
    │   └── feature_catalog.py      # derives {name: Feature} from the MVs; get_features()
    ├── jobs/                   # entry-point scripts (run as spark_python_task)
    │   ├── generate_scroll_data.py # seeds the src_* tables
    │   ├── build_training_set.py   # create_training_set over the labeled set
    │   ├── build_prediction_set.py # create_training_set over the latest-date set
    │   ├── append_one_day.py       # appends one new day of events
    │   └── restate_three_dates.py  # rewrites 3 historical dates of events
    └── pipeline/               # SDP source, loaded by the pipeline
        ├── common/             # cross-pipeline shared MVs (date spine, account, labels)
        └── scroll/             # this pipeline's feature MVs (battle, purchase, session, ...)
```

## Runnable entry points

Run with `databricks bundle run <name>` (see [Deploy + run](#deploy--run) for setup):

| Name | What it does |
|---|---|
| `sc_fs_demo_seed_initial_data` | Seeds synthetic game data into `<catalog>.<schema>.src_*`. Run once first. |
| `sc_fs_demo_orchestrator` | The main DAG: refreshes the SDP pipeline, then builds the training and prediction sets. |
| `sc_fs_demo_pipeline` | The SDP pipeline alone (all feature MVs + labels). |
| `sc_fs_demo_append_one_day` | Appends one new day of events — then re-run the pipeline to watch it refresh **incrementally**. |
| `sc_fs_demo_restate_three_dates` | Rewrites 3 historical dates — re-run the pipeline to see only the affected slice recompute. |

## How features work

- **Each MV is a feature table.** Every feature MV declares `PRIMARY KEY (account_id, observation_date TIMESERIES)`. That TIMESERIES key is what makes it a governed, discoverable, point-in-time-joinable feature table in Unity Catalog — so there is no separate `create_feature`/registration step.
- **The catalog is derived from the MVs.** `feature_catalog.py` lists the feature-table MVs; every non-PK column becomes a `Feature`. Adding a column to an MV adds a feature with no mapping to maintain.
- **A model is a list of feature names.** `build_training_set.py` / `build_prediction_set.py` select a subset (`SCROLL_CHURN_FEATURES`) and pass the `Feature` objects to `create_training_set`, which performs the point-in-time join. A different model is just a different list — same MVs, same pipeline.

## Seeing incremental refresh (Enzyme)

The materialized views refresh **incrementally** on serverless: when new data arrives, only the changed slice is recomputed, not the whole table. To observe it:

1. Run `sc_fs_demo_append_one_day` (or `sc_fs_demo_restate_three_dates`).
2. Re-run `sc_fs_demo_pipeline`.
3. In the pipeline UI, the **Tables** tab shows each MV's refresh type (`Incremental` / `No change` / `Full recompute`). MVs whose inputs didn't change are skipped; only the affected ones recompute.

## Rolling window (current_date())

`common/date_spine.sql` emits the last 30 days ending today via `current_date()`, and every feature MV is built on `date_spine` — so each run processes only `[current_date() − 29, current_date()]`. No job parameter and no pipeline-parameters preview required; the window auto-advances with the calendar.

Trade-off: simple and zero-wiring, but not idempotent — a rerun uses today's date, so you can't reprocess/backfill a specific past date.

## Source tables (`src_*`)

Six append-only event streams keyed on `account_id` + an event timestamp (with a date-grain `event_date` cluster key), one account dimension, and one daily snapshot. Seeded at **~1M accounts × 90 days** by default (~316M rows; ~a couple million event records per recent date) — tune via the `num_accounts` / `history_days` bundle variables.

| Table | Grain | Content |
|---|---|---|
| `src_dim_account` | one row per `account_id` | Account dimension: `created_at`, `country`, `install_source`, `spender_class` (whale / dolphin / minnow / non_spender), `temporal_pattern`. |
| `src_events_login` | one row per login | Login events (~1.2/account/day). Activity, recency, and the next-7d-login label. |
| `src_events_battle` | one row per battle | Battle outcomes (~2/account/day): `won` flag, `duration_seconds`. |
| `src_events_purchase` | one row per purchase | In-app purchases: `category` (gems / cards / chest / pass / cosmetic), `amount_usd`. |
| `src_events_progression` | one row per level-up | Level-ups (~0.35/account/day) with cumulative `new_level`. |
| `src_events_social` | one row per social event | Social interactions (~1/account/day): `event_type` (message / invite / gift). |
| `src_events_session` | one row per session | Play sessions (~1.1/account/day) with a raw `payload_json` (`{duration_s, device, level_reached}`) parsed via `from_json`. |
| `src_clan_membership_daily` | one row per (`account_id`, `date`) | Daily slowly-changing snapshot of clan membership: `is_clan_member` boolean. |

## Capabilities & patterns demonstrated

| Capability / pattern | Where |
|---|---|
| Rolling last-30-days window via `current_date()` — windows every feature MV | `pipeline/common/date_spine.sql` |
| MVs as governed, point-in-time-joinable feature tables (`PRIMARY KEY ... TIMESERIES`) | every `pipeline/**/silver_*.sql` |
| Incremental refresh / cheap backfill (Enzyme) | whole pipeline — see [Seeing incremental refresh](#seeing-incremental-refresh-enzyme) |
| Multi-stage CTEs | `pipeline/scroll/silver_battle.sql` |
| Conditional-aggregation pivot (code-generated, DataFrame API) | `pipeline/scroll/silver_purchase.py` |
| Forward range-window label | `pipeline/common/labels_did_login_within_7d.sql` |
| JSON struct parsing | `pipeline/scroll/silver_session.sql` (`from_json`) |
| Snapshot / dim joins | `pipeline/common/silver_account.sql`, `pipeline/scroll/silver_social_enriched.sql` |
| Per-model feature subset selection (`create_training_set`) | `jobs/build_training_set.py`, `build_prediction_set.py` |
| Data-quality expectations (`CONSTRAINT ... EXPECT`) | most feature MVs |

## Deploy + run

Set your CLI profile in `databricks.yml` (`targets.dev.workspace.profile`) and your `catalog`/`schema` via the bundle variables.

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run sc_fs_demo_seed_initial_data   # once, to seed data
databricks bundle run sc_fs_demo_orchestrator        # the full DAG
```

Override catalog/schema without editing files: `--var catalog=my_catalog --var schema=my_schema`.

## Development

Formatting, linting, static type-checking, and secret scanning run automatically on every commit via [pre-commit](https://pre-commit.com): **ruff** (format + lint), **mypy** (config in `pyproject.toml`), and **gitleaks**. One-time setup per clone:

```bash
uv tool install pre-commit
brew install gitleaks          # secret scanner (or see gitleaks.io)
pre-commit install
```

Run the checks manually any time with `pre-commit run --all-files`.

## Outputs to inspect after a run

- `<catalog>.<schema>.src_*` — 8 source tables
- `silver_*` — feature-table MVs (pure intermediates like the date spine, spender tier, and purchase velocity are pipeline-private and not published to the catalog)
- `labels_did_login_within_7d` — label MV (also the training eligibility set)
- `eligibility_prediction` — prediction eligibility set (active accounts at the latest observation_date)
- `scroll_raw_training` — training dataset (eligibility set + PIT-joined features + label)
- `scroll_raw_prediction` — prediction dataset (eligibility set + PIT-joined features, no label)
