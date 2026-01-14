# lite-data-stack-bigquery

## What is this?

This template creates a simple data pipeline: it extracts data from a public API (Rick & Morty), stores it in BigQuery, and then transforms it into ready-to-use tables. You do not need to know Meltano or dbt to use it; the README guides you step by step.

It is aimed at small teams or projects starting their first stack: quick to spin up, easy to understand, and with CI/CD ready to automate tests and documentation.

## What it includes

- Extraction with Meltano (API -> BigQuery)
- Transformation with dbt (staging -> marts)
- Models and columns documented in YAML
- CI/CD workflows and dbt docs on GitHub Pages

## Quick start (Happy Path)

Minimum requirements:
- [req] Python 3.11+
- [req] Git
- [req] A Google Cloud project with BigQuery enabled
- [req] A BigQuery dataset and a service account JSON key

1) [DB] Create a BigQuery dataset

Create a dataset in your GCP project and a service account with BigQuery permissions. Download the JSON key file.

2) [CFG] Configure variables

```bash
cd PROJECT_NAME
cp .env.example .env
```

Edit `.env` with your credentials. Minimal example:

```bash
BIGQUERY_PROJECT_ID=your-gcp-project
BIGQUERY_DATASET_ID=analytics
BIGQUERY_LOCATION=US
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

DBT_USER=local

TAP_GITHUB_AUTH_TOKEN=ghp_xxx
```

> [!] WARNING: dbt and Meltano should point to the same BigQuery project/dataset for the first run.

3) [EXT] Run extraction once (Meltano)

```bash
cd extraction
./scripts/setup-local.sh
source venv/bin/activate
set -a; source ../.env; set +a
meltano --environment=prod run tap-rest-rickandmorty target-bigquery
```

> [i] INFO: The script creates the venv and installs dependencies. When it finishes, activate the venv in your shell to run Meltano.
> [!] WARNING: dbt sources point to the `prod_tap_rest_rickandmorty` dataset. That is why the first Meltano run must load into prod.

4) [DBT] Run transform and build models

```bash
cd ../transform
./scripts/setup-local.sh
source venv/bin/activate
cp profiles.yml.example profiles.yml
set -a; source ../.env; set +a
export DBT_PROFILES_DIR=.
dbt deps
dbt build --target prod
```

> [i] INFO: `dbt build` runs models and tests, so it is used in PR/deploy.
> [i] INFO: Every time you change a model, run `dbt build` again (or a selective build).

5) [SQL] See results in the DB

```sql
select * from marts.character_status limit 10;
select * from marts.episode_summary limit 10;
select * from marts.location_summary limit 10;
```

6) [DOCS] Generate dbt docs (optional)

```bash
cd ../transform
set -a; source ../.env; set +a
export DBT_PROFILES_DIR=.
dbt docs generate --target prod
dbt docs serve --target prod
```

Opens at: http://localhost:8080

## Next steps

1) View dbt docs to explore the DAG and columns.
2) Add a new model and document it.
3) Change the data source and adapt staging.

## Understanding the project

### Data flow

```
Rick & Morty API
  -> Meltano (tap-rest-rickandmorty + target-bigquery)
  -> BigQuery: dataset prod_tap_rest_rickandmorty (raw)
  -> dbt staging: dataset stg (stg_*)
  -> dbt marts: dataset marts (final models)
```

### Staging vs marts

- Staging cleans and normalizes raw data. It keeps consistent names and correct types.
- Marts are final models ready for analysis or BI.

Real example from this project:
- `stg_characters` -> `character_status`

### Table of models and key columns

All column documentation lives in:
- `transform/models/staging/schema.yml`
- `transform/models/production/marts/*.yml`

### Useful query examples

```sql
-- Top 5 episodes with most characters
select episode_code, name, character_count
from marts.episode_summary
order by character_count desc
limit 5;

-- Top 5 locations with most residents
select name, location_type, resident_count
from marts.location_summary
order by resident_count desc
limit 5;

-- Character distribution by status
select status, character_count
from marts.character_status
order by character_count desc;
```

### Environments (dev, ci, prod)

- dev: default target. Writes to `SANDBOX_<DBT_USER>`. Ideal for local development.
- ci: optional target with fixed dataset `analytics_ci` if you need it in your own pipelines.
- prod: writes to `stg` and `marts` datasets. Used for deploy and docs.

If you do not pass `--target prod`, dbt uses the default target (dev).

> [!] WARNING: For `dev`, you need `DBT_USER`. If you do not set it, dbt fails in the on-run-start hook.

### Why we use dbt build (and not dbt run)

- `dbt run` only executes models.
- `dbt build` executes models and tests (and snapshots/seeds if they exist).
- In PR and deploy we use `dbt build` to validate everything passes.

### Modeling conventions

- Staging always uses the `stg_` prefix.
- Marts have no prefix (e.g. `character_status`).
- Each production model has its own `.yml` file with columns and tests.
- Use `ref()` for dependencies between models.

## Local development

### Work in your sandbox (dev)

```bash
export DBT_USER=tu_usuario
cd transform
set -a; source ../.env; set +a
export DBT_PROFILES_DIR=.
dbt build
```

> [i] INFO: Raw data stays in `prod_tap_rest_rickandmorty`, but your models are created in `SANDBOX_<DBT_USER>`.
> [i] INFO: If you modify models or YAML, run `dbt build` again.

### Add a new model

1) Create the SQL in `transform/models/staging` or `transform/models/production/marts`.
2) Create the model YAML with descriptions for all columns and basic tests.
3) Run a selective build.

```bash
dbt build --select <nombre_del_modelo>
```

### Change the data source

1) Edit `extraction/meltano.yml` to point to your new extractor.
2) Update `transform/models/staging/_sources.yml` with the new dataset and tables.
3) Rewrite the staging models to map the new columns.

## Quick repo layout

- `extraction/`: Meltano project
- `transform/`: dbt project
- `.github/workflows/`: CI/CD
- `.env.example`: variables template

<details>
<summary>CI/CD Setup</summary>

### Required GitHub secrets

Configure these secrets in your repo:
- `BIGQUERY_PROJECT_ID`
- `BIGQUERY_DATASET_ID`
- `BIGQUERY_LOCATION`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `DBT_USER` (for sandbox datasets)
- `TAP_GITHUB_AUTH_TOKEN`

Optional:
- `DBT_MANIFEST_URL` for custom slim CI

### Workflows

- `data-pipeline.yml`: schedule + manual. Runs extraction and then the dbt run (no tests).
- `dbt-pr-ci.yml`: on PR. Runs dbt build in sandbox and lints with SQLFluff.
- `dbt-cd-docs.yml`: on push to `main`. Runs dbt build in prod and publishes docs.

> [i] INFO: PR and deploy use `dbt build`; the scheduled pipeline uses only `dbt run`.

### Slim CI (prod manifest)

- The PR workflow tries to download `manifest.json` from prod.
- With `state:modified+` and `--defer`, dbt runs only what changed and uses prod for everything else.
- If there is no manifest, it runs a full build.

### SQLFluff

SQLFluff is a SQL linter. It is used to:
- keep consistent style in models
- catch basic issues before running dbt

In PR, only modified SQL models are linted.

### Enable GitHub Pages

1) Go to Settings -> Pages.
2) In Source, choose GitHub Actions.
3) After a push to `main`, the docs are published.

</details>

<details>
<summary>Common troubleshooting</summary>

Error: Env var required but not provided: BIGQUERY_PROJECT_ID

```bash
set -a; source .env; set +a
```

If you are inside `transform` or `extraction`:

```bash
set -a; source ../.env; set +a
```

Error: source: no such file or directory: .env

```bash
set -a; source ../.env; set +a
```

Error: Source dataset not found

Run extraction in prod (because sources point to `prod_tap_rest_rickandmorty`):

```bash
meltano --environment=prod run tap-rest-rickandmorty target-bigquery
```

Error: Required key is missing from config (Meltano)

Make sure `BIGQUERY_*` and `GOOGLE_APPLICATION_CREDENTIALS` are set in `.env` and reload:

```bash
set -a; source .env; set +a
```

Error: DBT_USER environment variable not set

```bash
export DBT_USER=tu_usuario
```

</details>

<details>
<summary>Template customization</summary>

- For another API: replace the tap in `extraction/meltano.yml`.
- For another database: adjust `BIGQUERY_*` and `GOOGLE_APPLICATION_CREDENTIALS` in `.env`.
- For new models: add SQL in `transform/models/production/marts` and its YAML next to it.

</details>

## License

MIT
