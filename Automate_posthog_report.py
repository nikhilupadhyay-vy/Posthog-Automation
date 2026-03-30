import requests
import pandas as pd
from collections import defaultdict
import os
import io
from typing import Dict, List
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

POSTHOG_API_KEY = os.getenv("API_KEY")
PROJECT_ID = 1
DASHBOARD_ID = 7

BASE_URL = "https://trace.portfolioiq.co/api"

HEADERS = {
    "Authorization": f"Bearer {POSTHOG_API_KEY}",
    "Content-Type": "application/json",
}

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
CHANNEL_ID = "C03RN1YTMJM"


def get_json(url):
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()


# 1. Fetch dashboard
dashboard_url = f"{BASE_URL}/projects/{PROJECT_ID}/dashboards/{DASHBOARD_ID}/"
dashboard = get_json(dashboard_url)

tiles = dashboard.get("tiles", [])
insight_ids = [
    tile["insight"]["id"]
    for tile in tiles
    if tile.get("insight") and tile["insight"].get("id")
]

print("Insight IDs:", insight_ids)

# Dictionary to store events grouped by email
email_events = defaultdict(list)

for insight_id in insight_ids:
    insight_url = f"{BASE_URL}/projects/{PROJECT_ID}/insights/{insight_id}/"
    insight = get_json(insight_url)

    result = insight.get("result")
    items_to_process = []

    if isinstance(result, list) and len(result) > 0:
        if isinstance(result[0], list):
            for row in result:
                if len(row) >= 2:
                    email = row[0]
                    events_list = row[1]
                    if email and '@' in str(email):
                        if isinstance(events_list, list):
                            formatted_events = []
                            for event in events_list:
                                event_str = str(event).strip()
                                if " | " in event_str:
                                    formatted_events.append(event_str)
                                elif event_str.startswith("click"):
                                    if " " in event_str:
                                        parts = event_str.split(" ", 1)
                                        formatted_events.append(f"click | {parts[1]}" if len(parts) == 2 else event_str)
                                    else:
                                        formatted_events.append(event_str)
                                else:
                                    formatted_events.append(f"click | {event_str}")
                            email_events[email] = formatted_events
                        elif isinstance(events_list, str):
                            if " | " in events_list:
                                event_parts = [e.strip() for e in events_list.split(" | ")]
                                formatted_events = []
                                for event_part in event_parts:
                                    if event_part.startswith("click"):
                                        if " " in event_part:
                                            parts = event_part.split(" ", 1)
                                            formatted_events.append(f"click | {parts[1]}" if len(parts) == 2 else event_part)
                                        else:
                                            formatted_events.append(event_part)
                                    else:
                                        formatted_events.append(f"click | {event_part}")
                                email_events[email] = formatted_events
                            else:
                                if events_list.startswith("click"):
                                    if " " in events_list:
                                        parts = events_list.split(" ", 1)
                                        email_events[email] = [f"click | {parts[1]}"]
                                    else:
                                        email_events[email] = [events_list]
                                else:
                                    email_events[email] = [f"click | {events_list}"]
            continue
        else:
            items_to_process = result
    elif isinstance(result, dict):
        if 'results' in result:
            items_to_process = result['results'] if isinstance(result['results'], list) else [result['results']]
        elif 'data' in result:
            items_to_process = result['data'] if isinstance(result['data'], list) else [result['data']]
        else:
            items_to_process = [result]

    if not items_to_process and not email_events:
        print(f"Skipping insight {insight_id}: result format not recognized")
        continue

    for item in items_to_process:
        if not isinstance(item, dict):
            continue

        email = None
        for field in ['email', 'user_email', 'distinct_id', 'person_email', 'user']:
            if field in item:
                email = item[field]
                break

        if not email and 'person' in item and isinstance(item['person'], dict):
            for field in ['email', 'distinct_id', 'properties']:
                if field in item['person']:
                    if field == 'properties' and isinstance(item['person']['properties'], dict):
                        email = item['person']['properties'].get('email') or item['person']['properties'].get('$email')
                    else:
                        email = item['person'][field]
                    if email:
                        break

        if not email and 'properties' in item and isinstance(item['properties'], dict):
            email = item['properties'].get('email') or item['properties'].get('$email') or item['properties'].get('user_email')

        if not email and isinstance(item, dict):
            for key in ['label', 'name', 'key']:
                if key in item and '@' in str(item[key]):
                    email = item[key]
                    break

        event_name = None
        for field in ['event', 'event_name', 'name', 'action', 'label', 'key']:
            if field in item:
                event_name = item[field]
                break

        if not event_name and 'properties' in item and isinstance(item['properties'], dict):
            event_name = item['properties'].get('event') or item['properties'].get('event_name') or item['properties'].get('$event_name')

        if email and event_name:
            email_events[email].append(f"click | {event_name}")
        elif email:
            label = item.get('label') or item.get('name') or insight.get('name', '')
            if label:
                email_events[email].append(f"click | {label}")
        elif event_name:
            distinct_id = item.get('distinct_id') or item.get('person', {}).get('distinct_id', '')
            if distinct_id and '@' in str(distinct_id):
                email_events[distinct_id].append(f"click | {event_name}")

if not email_events:
    print("\nWarning: No email/event pairs found. Trying alternative extraction...")
    for insight_id in insight_ids:
        insight_url = f"{BASE_URL}/projects/{PROJECT_ID}/insights/{insight_id}/"
        insight = get_json(insight_url)
        result = insight.get("result")
        if isinstance(result, list) and len(result) > 0:
            first_item = result[0]
            if isinstance(first_item, dict) and ('EMAIL' in first_item or 'email' in first_item):
                for item in result:
                    email = item.get('EMAIL') or item.get('email')
                    events = item.get('USER_EVENTS') or item.get('user_events') or item.get('events')
                    if email and events:
                        if isinstance(events, str):
                            event_list = [e.strip() for e in events.split('|')]
                            formatted_events = []
                            for event in event_list:
                                if event.startswith("click"):
                                    if " | " not in event and " " in event:
                                        parts = event.split(" ", 1)
                                        formatted_events.append(f"click | {parts[1]}")
                                    else:
                                        formatted_events.append(event)
                                else:
                                    formatted_events.append(f"click | {event}")
                            email_events[email].extend(formatted_events)
                        elif isinstance(events, list):
                            formatted_events = []
                            for event in events:
                                event_str = str(event).strip()
                                if " | " in event_str:
                                    formatted_events.append(event_str)
                                elif event_str.startswith("click"):
                                    if " " in event_str:
                                        parts = event_str.split(" ", 1)
                                        formatted_events.append(f"click | {parts[1]}")
                                    else:
                                        formatted_events.append(event_str)
                                else:
                                    formatted_events.append(f"click | {event_str}")
                            email_events[email].extend(formatted_events)

    if not email_events:
        raise ValueError("No user event data found. Please check the insight data structure.")


# ── BUILD TXT SUMMARY IN MEMORY ───────────────────────────────────────────────

def parse_event(event: str) -> str:
    """Extract the target from 'click | Target' format."""
    if ' | ' in event:
        _, target = event.split(' | ', 1)
        return target.strip()
    return event.strip()


def build_txt_buffer(email_events: dict) -> io.BytesIO:
    """Build the summary TXT entirely in memory and return a BytesIO buffer."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []
    lines.append("User Activity Summary in last 24 hrs.")
    lines.append(f"Generated: {timestamp}")
    lines.append("=" * 80)
    lines.append("")

    for email in sorted(email_events.keys()):
        events = email_events[email]

        # Deduplicate while preserving order
        seen = set()
        unique_targets = []
        for event in events:
            target = parse_event(event)
            if target and target not in seen:
                seen.add(target)
                unique_targets.append(target)

        if unique_targets:
            targets_str = ", ".join(unique_targets)
            lines.append(f"{email.lower()} -> clicked on {targets_str}")
            lines.append("")  # blank line between users

    content = "\n".join(lines).rstrip() + "\n"
    buf = io.BytesIO(content.encode("utf-8"))
    buf.seek(0)

    print(f"TXT summary built in memory — {len(email_events)} users")
    return buf


# ── SEND TXT TO SLACK ─────────────────────────────────────────────────────────

def send_txt_to_slack(buffer: io.BytesIO, channel: str, token: str) -> None:
    """Upload the in-memory TXT buffer straight to a Slack channel."""
    client = WebClient(token=token)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"user_activity_summary_{timestamp}.txt"

    try:
        client.files_upload_v2(
            channel=channel,
            file=buffer,
            filename=filename,
            title="User Activity Summary",
            initial_comment="Hi Team,\nSharing the User Activity Last 24 Hours report. 📄",
        )
        print("✅ TXT summary sent to Slack successfully.")
    except SlackApiError as e:
        print("❌ Slack error:", e.response["error"])


if __name__ == "__main__":
    txt_buffer = build_txt_buffer(email_events)
    send_txt_to_slack(txt_buffer, CHANNEL_ID, SLACK_BOT_TOKEN)
