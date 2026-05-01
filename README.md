# Maneuver Marketing Test

A small data pipeline that:

1. Cleans raw orders and builds a single dbt mart (`items_by_country_channel_month`) on top of a Google Sheets-backed source in BigQuery.
2. Exports the mart into a tab in a Google Sheet via the Sheets API.
3. Posts a summary (orders, revenue, top channel, QC stats) to Slack.

It runs on demand locally and on a 3-hour schedule via GitHub Actions.

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
        ▼   (dbt: items_by_country_channel_month.sql — group by month × channel × country × product)
BigQuery table:  datamarts.items_by_country_channel_month
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
│   └── pipeline.yml              # Cron (every 3h) + manual trigger
├── creds/                        # gitignored — service account key lives here locally
│   └── maneuver-marketing-test-625a0985781e.json
├── dbt/
│   ├── dbt_project.yml           # Project config + per-folder dataset routing
│   ├── macros/
│   │   └── generate_schema_name.sql            # Use +dataset as-is (no target prefix)
│   └── models/
│       ├── sources.yml                         # Declares raw_data.orders_raw as a source
│       └── maneuver_marketing/
│           ├── ods/clean_data.sql                          # Cleansing layer
│           └── datamarts/items_by_country_channel_month.sql # Aggregated mart
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
| `dbt/models/.../clean_data.sql` | Cleansing layer. See [Transformation logic](#transformation-logic). |
| `dbt/models/.../items_by_country_channel_month.sql` | Aggregates clean rows by month × source_channel × country × product. |
| `dbt/macros/generate_schema_name.sql` | Without this, dbt builds tables in `<target_dataset>_<custom_dataset>` (e.g. `ods_datamarts`). The override uses the custom name as-is, so models land in `ods` and `datamarts`. |

## Transformation logic

Two tiers: a single cleansing layer (`ods.clean_data`) followed by one aggregation mart.

### Cleansing — `ods.clean_data`

Reads `raw_data.orders_raw` (a Sheets-backed external table) and produces a typed, deduped, USD-aware row per order. Materialized as a table.

Steps, in order:

1. **Drop invalid IDs** — exclude rows where `order_id` is null or blank.
2. **Deduplicate** — for each `order_id`, keep the row with the earliest `created_at`:
   ```sql
   row_number() over (partition by order_id order by created_at) = 1
   ```
3. **Parse mixed date formats** — `created_at` is text in raw. Five formats are tried in priority order via `safe.parse_timestamp`; first match wins, result is reformatted to `%Y-%m-%d %H:%M:%S`:
   - `%Y-%m-%d %H:%M:%S` (e.g. `2024-02-14 11:19:00`)
   - `%Y-%m-%d`
   - `%Y/%m/%d`
   - `%d/%m/%Y %H:%M`
   - `%d/%m/%Y`
4. **Type-cast and normalize**:
   - `total_price`, `total_discounts`, `net_revenue` → `numeric` (`safe_cast`).
   - `quantity` → `int64`.
   - `currency`, `country_code` → upper-cased.
   - `source_channel`, `financial_status`, `fulfillment_status` → trimmed; empty strings become `null`. `financial_status` also lower-cased.
5. **Currency conversion** — joins a per-row `to_usd_conversion_rate`. Rates live in `dbt_project.yml` under `vars.currency_rates` (currently GBP, SGD, AUD; USD = 1). Anything else → `null` (those rows won't contribute to USD aggregates).
6. **Quality flags** (computed, not filtered):
   - `has_incomplete_data` = `True` if `financial_status` or `source_channel` is null.
   - `qc_flagged` = `True` if any of:
     - `abs(net_revenue − (total_price − total_discounts)) > 0.01`
     - `total_price` is null or `≤ 0`
     - `quantity` is null or `≤ 0`

Both flags are kept on the row so downstream models and the metrics query can use them.

### Mart — `datamarts.items_by_country_channel_month`

Filters `clean_data` to `has_incomplete_data = False AND qc_flagged = False`, then groups by `month × source_channel × country_code × product_title`. This grain is fine enough that the prior month-only and channel-only marts are derivable by re-aggregation.

| Column | Definition |
|---|---|
| `month` | `date_trunc(date(created_at), month)` |
| `source_channel` | grouping key |
| `country_code` | grouping key |
| `product_title` | grouping key |
| `price_per_item` | `sum(total_price) / sum(quantity)` |
| `quantity_per_order` | `sum(quantity) / count(distinct order_id)` |
| `order_count` | `count(distinct order_id)` |
| `quantity` | `sum(quantity)` |
| `net_revenue_usd` | `sum(net_revenue × to_usd_conversion_rate)` |

Loaded into Sheet tab **`items_by_country_channel_month`**.

### Pipeline metrics (Slack summary)

Computed in `pipeline.py:collect_metrics()` directly against `ods.clean_data` and `raw_data.orders_raw` — not the mart — so the figures reflect the full population (including QC-flagged rows) for the QC numbers to make sense.

| Metric | Definition |
|---|---|
| Orders processed | Rows in `clean_data` (= distinct `order_id` after dedupe). |
| Duplicates removed | Rows in `orders_raw` with valid `order_id` − rows in `clean_data`. |
| Gross revenue (paid only) | `sum(total_price × to_usd_conversion_rate)` where `financial_status = 'paid'`. |
| Top channel by net revenue | Highest `sum(net_revenue × to_usd_conversion_rate)`, restricted to rows passing both QC filters. |
| QC anomalies | `countif(qc_flagged)` in `clean_data`. |
| Data quality pass rate | Share of `clean_data` rows where neither `qc_flagged` nor `has_incomplete_data` is set. |

The Slack message also includes the pipeline start time (UTC) and a link to the Google Sheet tab.

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

The service account also needs **Viewer** access to the source sheet and **Editor** access to the destination sheet. The destination tab `items_by_country_channel_month` must already exist — `BigQueryToSheets` does not auto-create tabs.

Export the Slack webhook before running locally:

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
```

## Running

Locally:

```bash
python3 pipeline.py
```

Manually in CI: GitHub → Actions → **Pipeline** → *Run workflow*.

Scheduled: every 3h on the hour (00:00, 03:00, 06:00, …, 21:00 UTC), best-effort — GitHub may delay 5–15 min under load.

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
- The mart-to-tab mapping is the `SHEETS_TO_LOAD` dict in `pipeline.py`. Add a new entry to load another mart into a new tab.
- The dashboard URL embedded in the Slack message is `DASHBOARD_URL` in `pipeline.py` (uses a hardcoded `gid` for the target tab — update when the tab changes).
- The metrics SQL (orders processed, gross paid revenue, top channel, QC anomalies, pass rate) is in `pipeline.py:collect_metrics()`.
