# Maneuver Marketing Test

A small data pipeline that:

1. Builds a dbt model (`items_by_country_month`) on top of a Google Sheets-backed source in BigQuery.
2. Exports the result to a Google Sheet via the Sheets API.
3. Posts a summary (orders, revenue, top channel, QC stats) to Slack.

It runs on demand locally and on a 12-hour schedule via GitHub Actions.

## Data flow

```
Google Sheet (raw orders)
        │
        ▼
BigQuery external table:  raw_data.orders_raw
        │
        ▼   (dbt: clean_data.sql — dedupe, type-cast, currency conversion, QC flags)
BigQuery table:  ods.clean_data
        │
        ▼   (dbt: items_by_country_month.sql — aggregate by month/country/product)
BigQuery table:  datamarts.items_by_country_month
        │
        ├──► Google Sheet  (load_to_gsheet.BigQueryToSheets)
        │
        └──► Slack message (send_slack_message)
```

All BigQuery objects live in the `maneuver-marketing-test` GCP project.

## Project layout

```
.
├── .github/workflows/
│   └── pipeline.yml              # Cron (every 12h) + manual trigger
├── creds/                        # gitignored — service account key lives here locally
│   └── maneuver-marketing-test-625a0985781e.json
├── dbt/
│   ├── dbt_project.yml           # Project config + per-folder dataset routing
│   ├── macros/
│   │   └── generate_schema_name.sql   # Override so +dataset is used as-is (no target prefix)
│   └── models/
│       ├── sources.yml           # Declares raw_data.orders_raw as a source
│       └── maneuver_marketing/
│           ├── ods/clean_data.sql                # Cleansing layer
│           └── datamarts/items_by_country_month.sql   # Aggregated mart
├── pipeline.py                   # Entry point: dbt → Sheets → Slack
├── load_to_gsheet.py             # BigQueryToSheets class + run_bq_query helper
├── send_slack_message.py         # Posts a message via incoming webhook
├── requirements.txt              # Python deps (matches local versions)
├── .gitignore                    # Keeps creds/, .venv/, dbt artifacts, profiles.yml out of git
└── README.md
```

### Key files

| File | Purpose |
|---|---|
| `pipeline.py` | Orchestrates the run: shells out to `dbt build`, calls `BigQueryToSheets`, computes summary metrics, sends Slack message. Status is `OK` on success, `FAILED` on any exception (the message is sent either way). |
| `load_to_gsheet.py` | `run_bq_query()` returns a DataFrame using the local SA key. `BigQueryToSheets` authenticates with the same key and writes a DataFrame into a target sheet, in chunks, with retry. |
| `send_slack_message.py` | One-shot POST to a Slack incoming webhook. Reads the URL from `SLACK_WEBHOOK_URL` (required). |
| `dbt/models/.../clean_data.sql` | Dedupes by `order_id`, parses mixed date formats, casts numerics, joins currency rates from `vars`, flags rows with `has_incomplete_data` / `qc_flagged`. |
| `dbt/models/.../items_by_country_month.sql` | Aggregates clean rows by month × country × product. |
| `dbt/macros/generate_schema_name.sql` | Without this, dbt builds tables in `<target_dataset>_<custom_dataset>` (e.g. `ods_datamarts`). The override uses the custom name as-is, so models land in `ods` and `datamarts`. |

## Local setup

Prereqs: Python 3.11, gcloud-managed BigQuery access (or just the SA key below).

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Place the service-account key at `creds/maneuver-marketing-test-625a0985781e.json`. The `creds/` directory is gitignored.

Configure dbt at `~/.dbt/profiles.yml`:

```yaml
maneuver_marketing_test:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: maneuver-marketing-test
      dataset: ods
      location: US
      threads: 1
      priority: interactive
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: /absolute/path/to/creds/maneuver-marketing-test-625a0985781e.json
```

Required GCP APIs (enabled in project `maneuver-marketing-test`):
- BigQuery API
- Google Sheets API
- Google Drive API (the `orders_raw` source is a Sheets-backed external table, so BigQuery needs Drive scope to read it)

The service account also needs **Viewer** access to the source sheet and **Editor** access to the destination sheet.

## Running

Locally:

```bash
python3 pipeline.py
```

Manually in CI: GitHub → Actions → **Pipeline** → *Run workflow*.

Scheduled: every 12h at 00:00 and 12:00 UTC (best-effort — GitHub may delay 5–15 min under load).

## GitHub Actions secrets

Set these in *Settings → Secrets and variables → Actions*:

| Secret | Value |
|---|---|
| `GCP_SA_KEY` | Full JSON contents of the service account key |
| `SPREADSHEET_ID` | Destination Google Sheet ID |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |

The workflow writes the SA key to `creds/...json`, generates `dbt/profiles.yml`, then runs `python pipeline.py`.

## Configuration

- Currency rates are set in `dbt/dbt_project.yml` under `vars.currency_rates`.
- The destination sheet tab name and source query are defined in `pipeline.py` (`SHEET_TAB`, `sync_to_sheets()`).
- The metrics SQL (orders processed, gross paid revenue, top channel, QC anomalies, pass rate) is in `pipeline.py:collect_metrics()`.
