# lite-data-stack-bigquery

## What is this?

This template creates a simple data pipeline: it extracts data from the GitHub API, stores it in BigQuery, and then transforms it into ready-to-use tables. You do not need to know Meltano or dbt to use it; the README guides you step by step.

It is aimed at small teams or projects starting their first stack: quick to spin up, easy to understand, and with CI/CD ready to automate tests and documentation.

## What it includes

- Extraction with Meltano (`tap-github` + `target-bigquery`)
- Transformation with dbt (staging -> marts)
- Models and columns documented in YAML
- CI/CD workflows and dbt docs on GitHub Pages

## Quick start (Happy Path)

Minimum requirements:
- [req] Python 3.11+
- [req] Git
- [req] A Google Cloud project with BigQuery enabled
- [req] BigQuery datasets (raw/stg/marts)
- [req] A service account JSON key
- [req] A GCS bucket for Meltano state

1) [GCP] Create a GCP project and service accounts

If you do not have a Google Cloud account, create one. If you already have GCP, create a new project for this stack.

In IAM & Admin:
- Go to Service Accounts and create one account for dbt and one for Meltano
  (or reuse a single account for both).
- For each service account, create a JSON key and download it.
- Rename the files (for example): `dbt-service-account.json` and `meltano-service-account.json`.
- Go back to IAM and grant access to each service account email
  (example: `meltano@data.iam.gserviceaccount.com`).
- Assign the role `BigQuery Data Owner` (or the minimum you need for datasets/jobs).

2) [GCS] Create a GCS bucket for Meltano state

In Cloud Storage -> Buckets:
- Create a private bucket (example: `template-bigquery`).
- Recommended settings:
  - Public access prevention: Enabled
  - Access control: Uniform
  - Storage class: Standard
  - Encryption: Google-managed

Then in bucket permissions:
- Add the Meltano service account (for example: `meltano@<project-id>.iam.gserviceaccount.com`).
- Grant the role `Storage Object Admin`.

This allows Meltano to create and manage state files.

3) [DB] Create BigQuery datasets (or grant create permissions)

You will need datasets for raw and modeled data:
- Raw: `<env>_tap_github` (for example `prod_tap_github`)
- Modeled: `stg` and `marts` (prod/ci). For dev, dbt uses `SANDBOX_<DBT_USER>`.

4) [CFG] Configure variables

```bash
cd PROJECT_NAME
cp .env.example .env
```

Edit `.env` with your credentials. Minimal example:

```bash
BIGQUERY_PROJECT_ID=your-gcp-project
BIGQUERY_DATASET_ID=analytics
BIGQUERY_LOCATION=US
GOOGLE_APPLICATION_CREDENTIALS=/path/to/dbt-service-account.json
MELTANO_STATE_BACKEND_URI=gs://your-bucket/meltano/state

TARGET_BIGQUERY_PROJECT=${BIGQUERY_PROJECT_ID}
TARGET_BIGQUERY_LOCATION=${BIGQUERY_LOCATION}
TARGET_BIGQUERY_CREDENTIALS_PATH=/path/to/meltano-service-account.json

DBT_USER=local

TAP_GITHUB_AUTH_TOKEN=ghp_xxx
```

> [i] INFO: You can point dbt and Meltano to the same JSON key if you want.
> [!] WARNING: dbt sources read from `<target>_tap_github`. Run Meltano in the
> same environment as the dbt target you plan to build.

5) [LOCAL] Set up the extraction environment

```bash
cd extraction
./scripts/setup-local.sh
source venv/bin/activate
```

This creates the venv, installs Meltano dependencies, and initializes the project.

6) [TEST] Verify the state backend

From `extraction/` with the venv active:

```bash
set -a; source ../.env; set +a
meltano state list
```

Expected: no errors, and either an empty list or existing states.

7) [EXT] Run extraction once (Meltano)

```bash
set -a; source ../.env; set +a
meltano --environment=prod run tap-github target-bigquery
```

> [!] WARNING: dbt sources point to `<target>_tap_github` (for example `prod_tap_github`).
> Run Meltano with the same environment as your dbt target.

8) [DBT] Run transform and build models

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

9) [SQL] See results in the DB

```sql
select * from marts.github_commits limit 10;
select * from marts.github_committers limit 10;
select commit_type, count(*) from marts.github_commits group by commit_type;
```

10) [DOCS] Generate dbt docs (optional)

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
GitHub API
  -> Meltano (tap-github + target-bigquery)
  -> BigQuery: dataset <env>_tap_github (raw)
  -> dbt staging: dataset stg (stg_*)
  -> dbt marts: dataset marts (final models)
```

### Staging vs marts

- Staging cleans and normalizes raw data. It keeps consistent names and correct types.
- Marts are final models ready for analysis or BI.

Real example from this project:
- `stg_github_commits` -> `github_commits`

### Table of models and key columns

All column documentation lives in:
- `transform/models/staging/*.yml`
- `transform/models/production/marts/*.yml`

### Useful query examples

```sql
-- Top 5 repos by commit volume
select full_repo_name, count(*) as commit_count
from marts.github_commits
group by full_repo_name
order by commit_count desc
limit 5;

-- Commit types distribution
select commit_type, count(*) as commit_count
from marts.github_commits
group by commit_type
order by commit_count desc;

-- Most active committers (last 90 days)
select github_login, commits_last_90_days
from marts.github_committers
order by commits_last_90_days desc
limit 10;
```

### Environments (dev, ci, prod)

- dev: default target. Raw data in `dev_tap_github`, models in `SANDBOX_<DBT_USER>`.
- ci: optional target. Raw data in `ci_tap_github`, models in `stg`/`marts`.
- prod: raw data in `prod_tap_github`, models in `stg`/`marts` (deploy/docs).

If you do not pass `--target prod`, dbt uses the default target (dev).

> [!] WARNING: For `dev`, you need `DBT_USER`. If you do not set it, dbt fails in the on-run-start hook.

### Why we use dbt build (and not dbt run)

- `dbt run` only executes models.
- `dbt build` executes models and tests (and snapshots/seeds if they exist).
- In PR and deploy we use `dbt build` to validate everything passes.

### Modeling conventions

- Staging always uses the `stg_` prefix.
- Marts have no prefix (e.g. `github_commits`).
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

> [i] INFO: Raw data stays in `<target>_tap_github` (for example `dev_tap_github`),
> but your models are created in `SANDBOX_<DBT_USER>`.
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
2) Update `transform/models/staging/source_github.yml` with the new dataset and tables.
3) Rewrite the staging models to map the new columns.

## Quick repo layout

- `extraction/`: Meltano project
- `transform/`: dbt project
- `.github/workflows/`: CI/CD
- `.env.example`: variables template

<details>
<summary>CI/CD Setup</summary>

The workflows in `.github/workflows` are templates and should be aligned to this
BigQuery stack (tap-github + target-bigquery) before enabling.

### Required GitHub secrets (after aligning workflows)

- `BIGQUERY_PROJECT_ID`
- `BIGQUERY_DATASET_ID`
- `BIGQUERY_LOCATION`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `TARGET_BIGQUERY_PROJECT`
- `TARGET_BIGQUERY_LOCATION`
- `TARGET_BIGQUERY_CREDENTIALS_PATH`
- `DBT_USER` (for sandbox datasets)
- `TAP_GITHUB_AUTH_TOKEN`

Optional:
- `DBT_MANIFEST_URL` for custom slim CI
- `MELTANO_STATE_BACKEND_URI` if you want Meltano state in GCS

### Workflows

- `data-pipeline.yml`: schedule + manual. Runs extraction and then dbt.
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

Run extraction in the same environment as your dbt target (prod example):

```bash
meltano --environment=prod run tap-github target-bigquery
```

Need a full reload (clear Meltano state)

If you want to reimport everything, clear the saved state first:

```bash
meltano --environment=prod state list
meltano --environment=prod state clear prod:tap-github-to-target-bigquery --force
```

To clear all state IDs:

```bash
meltano --environment=prod state clear --all --force
```

Error: Required key is missing from config (Meltano)

Make sure `BIGQUERY_*`, `TARGET_BIGQUERY_*`, and credentials paths are set in `.env` and reload:

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
- For another warehouse: update `transform/profiles.yml.example` and `BIGQUERY_*`/`TARGET_BIGQUERY_*` in `.env`.
- For new models: add SQL in `transform/models/production/marts` and its YAML next to it.

</details>

## License

MIT
