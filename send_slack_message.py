import logging
import os

import requests

logger = logging.getLogger(__name__)


def send_slack_message(message: str = "Hello, world!"):
    """
    Sends a Slack message via an incoming webhook.
    The channel is determined by the webhook itself.
    Reads the webhook URL from the SLACK_WEBHOOK_URL environment variable.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise RuntimeError("SLACK_WEBHOOK_URL environment variable is not set.")

    response = requests.post(webhook_url, json={"text": message}, timeout=10)
    response.raise_for_status()

    logger.info("Slack message sent via webhook")
    return response
