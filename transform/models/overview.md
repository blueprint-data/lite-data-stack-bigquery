{% docs __overview__ %}
# Lite data stack on BigQuery
This project extracts GitHub data with Meltano and models it with dbt so you get ready-to-use tables and docs without heavy setup.

## How to navigate
- Raw tables land in `<env>_tap_github` via Meltano.
- Staging models (`stg_*`) normalize raw data in dataset `stg`.
- Marts publish final models in dataset `marts` (prod/ci) or `SANDBOX_<DBT_USER>` in dev.
- Column documentation lives next to each model in YAML files.

## How to run locally
1) Load env vars: `set -a; source ../.env; set +a`.
2) Run extraction for your target: `meltano --environment=prod run tap-github target-bigquery` (or `dev`/`ci`).
3) From `transform/`, install deps and build: `./scripts/setup-local.sh && source venv/bin/activate && dbt deps && dbt build --target prod`.
4) Generate docs when needed: `dbt docs generate --target prod` and serve with `dbt docs serve`.

## Tips
- Prefer `dbt build` over `dbt run` so tests stay enforced.
- Set `DBT_USER` for sandboxed dev builds.
- Add new models under `models/staging` or `models/production/marts` and keep their YAML docs beside them.
{% enddocs %}

{% docs __lite_data_stack_bigquery__ %}
# Lite data stack on BigQuery
Use this project as a ready starter: run Meltano to land GitHub data, then use dbt to clean and publish marts with documentation baked in.

## What to read next
- `README.md` for prerequisites and full quick start.
- `models/staging/*.yml` for source and staging column docs.
- `models/production/marts/*.yml` for final model docs and tests.

## Team workflow
- Keep `.env` in sync with your target project and datasets.
- Build selectively during development: `dbt build --select <model> --target dev`.
- Regenerate docs for reviewers after changes: `dbt docs generate --target prod`.
{% enddocs %}
