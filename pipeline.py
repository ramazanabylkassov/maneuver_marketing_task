"""Pipeline: refresh items_by_country_channel_month, sync to Google Sheets, notify Slack."""

import logging
import os
import subprocess
from datetime import datetime, timezone
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

# Maps Google Sheet tab name -> fully-qualified BigQuery table to dump into it.
SHEETS_TO_LOAD = {
    "items_by_country_channel_month": f"{BQ_PROJECT}.datamarts.items_by_country_channel_month",
}

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "186hTRRRByB5EF_vQtRWMFZM41BNupjgoIba8Vn7pf_Y")
DASHBOARD_URL = (
    f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
    "?gid=121810620#gid=121810620"
)
SLACK_USER_ID = "U0B148FR5GC"  # paged on failure

# A successful run is downgraded to FAILED if either threshold is breached.
MAX_QC_ANOMALIES = 0
MIN_PASS_RATE = 0.95

# Monitoring thresholds (independent of the pipeline-status thresholds above).
ORDER_VARIANCE_THRESHOLD = 0.20
MONITOR_MIN_PASS_RATE = 0.90
DUPLICATE_RATE_THRESHOLD = 0.05


def run_dbt() -> None:
    cmd = [
        "dbt", "build",
        "--select", "+items_by_country_channel_month",
        "--full-refresh",
    ]
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
            tab: {"query": f"select * from `{table}` order by 1, 2, 3"}
            for tab, table in SHEETS_TO_LOAD.items()
        },
    ).load_bq_to_sheets()


def format_message(
    status: str,
    metrics: Optional[dict],
    error: Optional[str],
    started_at: datetime,
) -> str:
    started_line = f":clock3: Started: {started_at.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    ping_line = f"<@{SLACK_USER_ID}>"

    # Pipeline crashed before metrics could be computed — short failure message.
    if error is not None:
        return (
            f"{started_line}\n"
            f":x: Pipeline FAILED\n"
            f"• Error: {error}\n"
            f"{ping_line}"
        )

    icon = ":white_check_mark:" if status == "PASSED" else ":x:"
    dashboard_line = f"• Dashboard: <{DASHBOARD_URL}|MAIN TAB>"
    body = (
        f"{started_line}\n"
        f"{icon} Pipeline {status}\n"
        f"• Orders processed: {metrics['orders_processed']} "
        f"(duplicates removed: {metrics['duplicates_removed']})\n"
        f"• Gross revenue (paid only): ${metrics['gross_revenue_usd']}\n"
        f"• Top channel by net revenue: {metrics['top_channel']} "
        f"(${metrics['top_channel_revenue']})\n"
        f"• QC anomalies: {metrics['qc_anomalies']}\n"
        f"• Data quality pass rate: {round(metrics['pass_rate'] * 100)}%\n"
        f"{dashboard_line}"
    )
    if status == "FAILED":
        body += f"\n{ping_line}"
    return body


def runMonitoringChecks() -> None:
    """
    Runs data-quality checks against ods.clean_data + raw_data.orders_raw and
    posts a single consolidated Slack alert listing every failing check.
    No-ops if all checks pass.

    Checks:
      - Order count variance   (WARNING)  today's count is >20% off the daily avg
      - Zero/negative revenue  (CRITICAL) any paid order with total_price <= 0
      - Pass rate drop         (CRITICAL) pass_rate < 90%
      - Duplicate rate         (WARNING)  duplicates > 5% of raw rows
    """
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
        daily as (
            select date(created_at) as d, count(distinct order_id) as cnt
            from clean
            group by d
        ),
        today_block as (
            select count(distinct order_id) as today_count
            from clean
            where date(created_at) = current_date()
        ),
        daily_avg as (
            select avg(cnt) as avg_count from daily
        ),
        bad_paid as (
            select countif(financial_status = 'paid'
                           and (total_price is null or total_price <= 0)) as bad_paid_count
            from clean
        ),
        rates as (
            select
                countif(not qc_flagged and not has_incomplete_data) as clean_count,
                count(*)                                            as total_count
            from clean
        )
    select
        today_block.today_count                                       as today_count,
        daily_avg.avg_count                                           as avg_count,
        bad_paid.bad_paid_count                                       as bad_paid_count,
        safe_divide(rates.clean_count, rates.total_count)             as pass_rate,
        safe_divide(raw.raw_count - rates.total_count, raw.raw_count) as duplicate_rate
    from today_block, daily_avg, bad_paid, rates, raw
    """
    row = run_bq_query(sql, project_id=BQ_PROJECT).iloc[0].to_dict()

    today_count = int(row["today_count"] or 0)
    avg_count = float(row["avg_count"]) if row["avg_count"] is not None else 0.0
    bad_paid = int(row["bad_paid_count"] or 0)
    pass_rate = float(row["pass_rate"]) if row["pass_rate"] is not None else 0.0
    duplicate_rate = float(row["duplicate_rate"]) if row["duplicate_rate"] is not None else 0.0

    alerts = []

    if avg_count > 0:
        variance = abs(today_count - avg_count) / avg_count
        if variance > ORDER_VARIANCE_THRESHOLD:
            direction = "above" if today_count > avg_count else "below"
            alerts.append({
                "name": "Order count variance",
                "severity": "WARNING",
                "detected": f"Today's order count is {variance:.0%} {direction} the dataset daily average.",
                "actual": f"{today_count} orders today",
                "expected": f"~{avg_count:.0f} orders/day (±{ORDER_VARIANCE_THRESHOLD:.0%})",
                "action": "Check upstream feed for missing or duplicate orders today.",
            })

    if bad_paid > 0:
        alerts.append({
            "name": "Zero/negative revenue on paid orders",
            "severity": "CRITICAL",
            "detected": f"{bad_paid} paid order(s) have total_price ≤ 0 after cleaning.",
            "actual": str(bad_paid),
            "expected": "0",
            "action": "Inspect raw rows; reject or correct upstream before next refresh.",
        })

    if pass_rate < MONITOR_MIN_PASS_RATE:
        alerts.append({
            "name": "Pass rate drop",
            "severity": "CRITICAL",
            "detected": f"Data quality pass rate is {pass_rate:.0%}.",
            "actual": f"{pass_rate:.0%}",
            "expected": f"≥ {MONITOR_MIN_PASS_RATE:.0%}",
            "action": "Review qc_flagged / has_incomplete_data rows in clean_data; fix upstream sources.",
        })

    if duplicate_rate > DUPLICATE_RATE_THRESHOLD:
        alerts.append({
            "name": "Duplicate rate",
            "severity": "WARNING",
            "detected": f"{duplicate_rate:.0%} of raw rows were duplicates.",
            "actual": f"{duplicate_rate:.0%}",
            "expected": f"≤ {DUPLICATE_RATE_THRESHOLD:.0%}",
            "action": "Investigate duplicate sources (retried webhooks, double-emitting jobs).",
        })

    if not alerts:
        logger.info("Monitoring checks: all passed.")
        return

    severity_icon = {"WARNING": ":warning:", "CRITICAL": ":rotating_light:"}
    blocks = [
        (
            f"{severity_icon[a['severity']]} *{a['name']}* — {a['severity']}\n"
            f"• Detected: {a['detected']}\n"
            f"• Actual: {a['actual']}\n"
            f"• Expected: {a['expected']}\n"
            f"• Recommended action: {a['action']}"
        )
        for a in alerts
    ]

    has_critical = any(a["severity"] == "CRITICAL" for a in alerts)
    header_icon = ":rotating_light:" if has_critical else ":warning:"
    body = f"{header_icon} Monitoring alert\n\n" + "\n\n".join(blocks)
    if has_critical:
        body += f"\n\n<@{SLACK_USER_ID}>"

    send_slack_message(message=body)


def main() -> None:
    started_at = datetime.now(timezone.utc)
    metrics: Optional[dict] = None
    error: Optional[str] = None
    status = "PASSED"

    try:
        run_dbt()
        sync_to_sheets()
        metrics = collect_metrics()
        if metrics["qc_anomalies"] > MAX_QC_ANOMALIES or metrics["pass_rate"] < MIN_PASS_RATE:
            status = "FAILED"
    except Exception as e:
        logger.exception("Pipeline failed")
        status, error = "FAILED", str(e)

    send_slack_message(message=format_message(status, metrics, error, started_at))

    if error is None:
        try:
            runMonitoringChecks()
        except Exception:
            logger.exception("runMonitoringChecks failed")


if __name__ == "__main__":
    main()
