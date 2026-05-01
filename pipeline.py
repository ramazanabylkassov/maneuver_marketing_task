"""Pipeline: refresh items_by_country_month, sync to Google Sheets, notify Slack."""

import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from load_to_gsheet import BigQueryToSheets, run_bq_query
from send_slack_message import send_slack_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent
DBT_DIR = REPO_ROOT / "dbt"

BQ_PROJECT = "maneuver-marketing-test"
RAW_TABLE = f"{BQ_PROJECT}.raw_data.orders_raw"
CLEAN_TABLE = f"{BQ_PROJECT}.ods.clean_data"
MART_TABLE = f"{BQ_PROJECT}.datamarts.items_by_country_month"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "186hTRRRByB5EF_vQtRWMFZM41BNupjgoIba8Vn7pf_Y")
SHEET_TAB = "items_by_country_month"


def run_dbt() -> None:
    cmd = ["dbt", "build", "--select", "+items_by_country_month", "--full-refresh"]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, cwd=DBT_DIR, check=True)


def collect_metrics() -> dict:
    sql = f"""
    with
        raw as (
            select count(*) as raw_count
            from `{RAW_TABLE}`
            where order_id is not null and trim(order_id) != ''
        ),
        clean as (
            select * from `{CLEAN_TABLE}`
        ),
        totals as (
            select
                (select raw_count from raw)                              as raw_count,
                count(*)                                                 as orders_processed,
                countif(qc_flagged)                                      as qc_anomalies,
                sum(if(financial_status = 'paid',
                       total_price * to_usd_conversion_rate, 0))         as gross_revenue_usd,
                countif(not qc_flagged and not has_incomplete_data)      as clean_count
            from clean
        ),
        top_channel as (
            select source_channel,
                   sum(net_revenue * to_usd_conversion_rate) as net_rev_usd
            from clean
            where not has_incomplete_data and not qc_flagged
            group by source_channel
            order by net_rev_usd desc
            limit 1
        )
    select
        totals.raw_count - totals.orders_processed                            as duplicates_removed,
        totals.orders_processed                                               as orders_processed,
        totals.qc_anomalies                                                   as qc_anomalies,
        round(totals.gross_revenue_usd, 1)                                    as gross_revenue_usd,
        round(safe_divide(totals.clean_count, totals.orders_processed), 4)    as pass_rate,
        top_channel.source_channel                                            as top_channel,
        round(top_channel.net_rev_usd, 1)                                     as top_channel_revenue
    from totals, top_channel
    """
    df = run_bq_query(sql, project_id=BQ_PROJECT)
    return df.iloc[0].to_dict()


def sync_to_sheets() -> None:
    BigQueryToSheets(
        client="maneuver_marketing_test",
        spreadsheet_id=SPREADSHEET_ID,
        spreadsheet_details={
            SHEET_TAB: {
                "query": (
                    f"select * from `{MART_TABLE}` "
                    "order by month, country_code, product_title"
                )
            }
        },
    ).load_bq_to_sheets()


def format_message(status: str, metrics: Optional[dict], error: Optional[str]) -> str:
    icon = ":white_check_mark:" if status == "OK" else ":x:"
    header = f"{icon} Pipeline {status}"
    if not metrics:
        return f"{header}\n• Error: {error}"
    return (
        f"{header}\n"
        f"• Orders processed: {metrics['orders_processed']} "
        f"(duplicates removed: {metrics['duplicates_removed']})\n"
        f"• Gross revenue (paid only): ${metrics['gross_revenue_usd']}\n"
        f"• Top channel by net revenue: {metrics['top_channel']} "
        f"(${metrics['top_channel_revenue']})\n"
        f"• QC anomalies: {metrics['qc_anomalies']}\n"
        f"• Data quality pass rate: {round(metrics['pass_rate'] * 100)}%"
    )


def main() -> None:
    metrics: Optional[dict] = None
    error: Optional[str] = None
    status = "OK"

    try:
        run_dbt()
        sync_to_sheets()
        metrics = collect_metrics()
    except Exception as e:
        logger.exception("Pipeline failed")
        status, error = "FAILED", str(e)
        try:
            metrics = collect_metrics()
        except Exception:
            logger.exception("Could not collect metrics on failure path")

    send_slack_message(message=format_message(status, metrics, error))


if __name__ == "__main__":
    main()
