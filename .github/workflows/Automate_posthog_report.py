import requests
import pandas as pd
from collections import defaultdict
import csv
import os
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

def get_json(url):
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()  # <-- shows real API error
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

    # PostHog insight result can be a list, dict, or list of lists
    items_to_process = []
    
    if isinstance(result, list) and len(result) > 0:
        # Check if it's a list of lists (tabular data)
        if isinstance(result[0], list):
            # This is tabular data where each row is [email, list_of_events]
            for row in result:
                if len(row) >= 2:
                    email = row[0]
                    events_list = row[1]
                    
                    # Validate email
                    if email and '@' in str(email):
                        # Events might already be formatted or need formatting
                        if isinstance(events_list, list):
                            # Events are in a list - process each one
                            formatted_events = []
                            for event in events_list:
                                event_str = str(event).strip()
                                # Events might be in format "click | event_name" or just "event_name"
                                if " | " in event_str:
                                    # Already in "click | event_name" format, use as is
                                    formatted_events.append(event_str)
                                elif event_str.startswith("click"):
                                    # Format: "click event_name" -> "click | event_name"
                                    if " " in event_str:
                                        parts = event_str.split(" ", 1)
                                        if len(parts) == 2:
                                            formatted_events.append(f"click | {parts[1]}")
                                        else:
                                            formatted_events.append(event_str)
                                    else:
                                        formatted_events.append(event_str)
                                else:
                                    # Just event name, add "click | " prefix
                                    formatted_events.append(f"click | {event_str}")
                            
                            # Store events as a list (not joined)
                            email_events[email] = formatted_events
                        elif isinstance(events_list, str):
                            # Events are already a string - split and format
                            if " | " in events_list:
                                # Split by " | " and keep format
                                event_parts = [e.strip() for e in events_list.split(" | ")]
                                formatted_events = []
                                for event_part in event_parts:
                                    if event_part.startswith("click"):
                                        # Format: "click event_name" -> "click | event_name"
                                        if " " in event_part:
                                            parts = event_part.split(" ", 1)
                                            if len(parts) == 2:
                                                formatted_events.append(f"click | {parts[1]}")
                                            else:
                                                formatted_events.append(event_part)
                                        else:
                                            formatted_events.append(event_part)
                                    else:
                                        formatted_events.append(f"click | {event_part}")
                                email_events[email] = formatted_events
                            else:
                                # Single event string
                                if events_list.startswith("click"):
                                    if " " in events_list:
                                        parts = events_list.split(" ", 1)
                                        email_events[email] = [f"click | {parts[1]}"]
                                    else:
                                        email_events[email] = [events_list]
                                else:
                                    email_events[email] = [f"click | {events_list}"]
            # Skip the dict processing since we handled it above
            continue
        else:
            # Regular list of dicts
            items_to_process = result
    elif isinstance(result, dict):
        # Check if result has a 'results' key or similar
        if 'results' in result:
            items_to_process = result['results'] if isinstance(result['results'], list) else [result['results']]
        elif 'data' in result:
            items_to_process = result['data'] if isinstance(result['data'], list) else [result['data']]
        else:
            # Try to process the dict itself
            items_to_process = [result]
    
    if not items_to_process and not email_events:
        print(f"Skipping insight {insight_id}: result format not recognized")
        continue
    
    for item in items_to_process:
        if not isinstance(item, dict):
            continue
            
        # Try to find email in various possible fields
        email = None
        
        # Check direct fields
        for field in ['email', 'user_email', 'distinct_id', 'person_email', 'user']:
            if field in item:
                email = item[field]
                break
        
        # Check nested person object
        if not email and 'person' in item and isinstance(item['person'], dict):
            for field in ['email', 'distinct_id', 'properties']:
                if field in item['person']:
                    if field == 'properties' and isinstance(item['person']['properties'], dict):
                        email = item['person']['properties'].get('email') or item['person']['properties'].get('$email')
                    else:
                        email = item['person'][field]
                    if email:
                        break
        
        # Check properties object
        if not email and 'properties' in item and isinstance(item['properties'], dict):
            email = item['properties'].get('email') or item['properties'].get('$email') or item['properties'].get('user_email')
        
        # Check if item itself is keyed by email or distinct_id
        if not email and isinstance(item, dict):
            # Sometimes the key might be the email
            for key in ['label', 'name', 'key']:
                if key in item and '@' in str(item[key]):
                    email = item[key]
                    break
        
        # Try to find event name in various possible fields
        event_name = None
        
        # Check direct fields
        for field in ['event', 'event_name', 'name', 'action', 'label', 'key']:
            if field in item:
                event_name = item[field]
                break
        
        # Check properties
        if not event_name and 'properties' in item and isinstance(item['properties'], dict):
            event_name = item['properties'].get('event') or item['properties'].get('event_name') or item['properties'].get('$event_name')
        
        # If we have email and event, add to dictionary
        if email and event_name:
            email_events[email].append(f"click | {event_name}")
        elif email:
            # If we have email but no event name, try to use the insight name or label
            label = item.get('label') or item.get('name') or insight.get('name', '')
            if label:
                email_events[email].append(f"click | {label}")
        elif event_name:
            # If we have event but no email, try to extract from other fields
            # Sometimes email might be in a different format
            distinct_id = item.get('distinct_id') or item.get('person', {}).get('distinct_id', '')
            if distinct_id and '@' in str(distinct_id):
                email_events[distinct_id].append(f"click | {event_name}")

if not email_events:
    print("\nWarning: No email/event pairs found in the data.")
    print("This might mean:")
    print("1. The insight data structure is different than expected")
    print("2. The data needs to be queried differently")
    print("\nTrying alternative data extraction...")
    
    # Try alternative: maybe the data is already in the format we need
    for insight_id in insight_ids:
        insight_url = f"{BASE_URL}/projects/{PROJECT_ID}/insights/{insight_id}/"
        insight = get_json(insight_url)
        result = insight.get("result")
        
        # Try to see if result is already a DataFrame-like structure
        if isinstance(result, list) and len(result) > 0:
            # Check if first item has EMAIL or email column
            first_item = result[0]
            if isinstance(first_item, dict):
                # Check if it's already in the format we want
                if 'EMAIL' in first_item or 'email' in first_item:
                    # Data might already be formatted
                    for item in result:
                        email = item.get('EMAIL') or item.get('email')
                        events = item.get('USER_EVENTS') or item.get('user_events') or item.get('events')
                        if email and events:
                            if isinstance(events, str):
                                # Split events and format them
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
                                # Format each event
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

# Create DataFrame with email and separate user_events columns
# First, find the maximum number of events to determine column count
max_events = max(len(events) for events in email_events.values()) if email_events else 0

# Check if we have any data to export
if not email_events or len(email_events) == 0:
    print("\n⚠️  Warning: No user event data found. Creating empty CSV file with headers only.")
    # Create an empty DataFrame with just the email column
    final_df = pd.DataFrame(columns=["email"])
    csv_path = "C:\\Users\\nikhil.upadhyay\\Desktop\\Posthog_Automate\\user_activity.csv"
    final_df.to_csv(csv_path, index=False)
    print(f"Empty CSV file created: {csv_path}")
    print("Note: The file contains only headers. No user data available.")
else:
    # Create output data with separate columns for each event
    output_data = []
    for email, events in email_events.items():
        row = {"email": email}
        # Add each event to a separate column
        for idx, event in enumerate(events):
            row[f"user_events.{idx}"] = event
        # Fill remaining columns with empty strings if this user has fewer events
        for idx in range(len(events), max_events):
            row[f"user_events.{idx}"] = ""
        output_data.append(row)

    final_df = pd.DataFrame(output_data)

    # Sort by email for consistent output
    final_df = final_df.sort_values("email").reset_index(drop=True)

    # Reorder columns: email first, then user_events.0, user_events.1, etc.
    columns = ["email"] + [f"user_events.{i}" for i in range(max_events)]
    final_df = final_df[columns]

    csv_path = "C:\\Users\\nikhil.upadhyay\\Desktop\\Posthog_Automate\\user_activity.csv"
    final_df.to_csv(csv_path, index=False)

    print(f"Report generated: {csv_path}")
    print(f"Total users: {len(final_df)}")



def read_csv_file(filename: str = "user_activity.csv") -> List[Dict]:
    """Read the CSV file and return a list of dictionaries. Handles any number of rows."""
    data = []
    row_count = 0
    
    # Check if file is empty
    if os.path.getsize(filename) == 0:
        print(f"  Warning: {filename} is empty (0 bytes)")
        return data
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            # Check if file has headers but might be empty
            if reader.fieldnames is None:
                print(f"  Warning: {filename} has no headers")
                return data
            
            for row in reader:
                # Skip completely empty rows
                if not any(row.values()):
                    continue
                data.append(row)
                row_count += 1
                # Progress indicator for large files
                if row_count % 100 == 0:
                    print(f"  Processed {row_count} rows...", end='\r')
        
        if row_count >= 100:
            print()  # New line after progress indicator
    except Exception as e:
        print(f"  Error reading {filename}: {e}")
        return data
    
    return data


def extract_events(row: Dict) -> List[str]:
    """Extract all events from a row, filtering out empty values. Handles any number of columns."""
    events = []
    # Get all column names that contain event data
    # This handles patterns like: user_events.0, user_events.1, ..., user_events.N
    # Also handles any other event column naming patterns
    for key, value in row.items():
        # Skip email column and empty values
        if key == 'email':
            continue
        # Include any column with a non-empty value (handles any column naming pattern)
        if value and str(value).strip():
            events.append(str(value).strip())
    return events


def parse_event(event: str) -> str:
    """Parse event string to extract the target (e.g., 'click | Upload documents' -> 'Upload documents')."""
    if ' | ' in event:
        _, target = event.split(' | ', 1)
        return target.strip()
    return event.strip()


def generate_user_summary(data: List[Dict]) -> str:
    """Generate per-user summary in the requested format. Handles any number of rows and events."""
    if not data or len(data) == 0:
        return ""
    
    summary_lines = []
    total_rows = len(data)
    processed = 0
    users_with_events = 0
    max_events_per_user = 0
    total_events = 0
    
    for row in data:
        email = row.get('email', 'Unknown')
        # Skip rows with empty or invalid email
        if not email or email == 'Unknown' or '@' not in str(email):
            processed += 1
            continue
        
        events = extract_events(row)
        
        if not events:
            processed += 1
            continue
        
        users_with_events += 1
        total_events += len(events)
        max_events_per_user = max(max_events_per_user, len(events))
        
        # Parse events to extract targets
        event_targets = [parse_event(event) for event in events]
        
        # Format: email -> clicked on target1, target2, target3, ...
        # Remove duplicates while preserving order
        seen = set()
        unique_targets = []
        for target in event_targets:
            if target and target.strip():  # Skip empty targets
                if target not in seen:
                    seen.add(target)
                    unique_targets.append(target)
        
        # Only add to summary if there are valid targets
        if unique_targets:
            # Join targets with commas
            targets_str = ", ".join(unique_targets)
            # Format: email (lowercase) -> clicked on target1, target2, ... (with blank line after)
            email_lowercase = email.lower()  # Ensure email is in lowercase
            summary_lines.append(f"{email_lowercase} -> clicked on {targets_str}")
            summary_lines.append("")  # Blank line after each person's summary
        
        processed += 1
        # Progress indicator for large datasets
        if total_rows > 50 and processed % 50 == 0:
            print(f"  Processed {processed}/{total_rows} users...", end='\r')
    
    if total_rows > 50:
        print()  # New line after progress indicator
    
    print(f"  Users with events: {users_with_events}/{total_rows}")
    print(f"  Total events processed: {total_events:,}")
    if max_events_per_user > 0:
        print(f"  Max events per user: {max_events_per_user}")
    
    # Join lines and remove trailing blank lines for clean format
    result = "\n".join(summary_lines)
    return result.rstrip()  # Remove trailing whitespace/newlines


def create_summary_file(content: str, filename: str = "user_activity_summary.txt") -> str:
    """Create a text file with the summary. Handles large content efficiently."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    header = f"""User Activity Summary in last 24 hrs.
Generated: {timestamp}
{'='*80}

"""
    
    # Write file in chunks for large files to be memory efficient
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(header)
        # For very large content, write in chunks
        if len(content) > 1000000:  # 1MB threshold
            # Write in chunks
            chunk_size = 8192  # 8KB chunks
            for i in range(0, len(content), chunk_size):
                f.write(content[i:i+chunk_size])
        else:
            f.write(content)
    
    return filename


def main():
    """Main function to orchestrate the script."""
    # Read CSV file
    csv_file = "user_activity.csv"
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found in the current directory.")
        return
    
    print(f"Reading {csv_file}...")
    # Get file size for context
    file_size = os.path.getsize(csv_file)
    
    # Check if file is empty
    if file_size == 0:
        print(f"  Error: {csv_file} is empty (0 bytes). No data to process.")
        return
    
    print(f"  File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
    
    data = read_csv_file(csv_file)
    
    # Check if we have any data
    if not data or len(data) == 0:
        print(f"  Warning: {csv_file} contains no user records (file may have only headers or be empty).")
        print("  No summary will be generated.")
        return
    
    print(f"✅ Loaded {len(data):,} user records.")
    
    # Check number of columns in first row to show data structure
    if data:
        num_columns = len(data[0])
        event_columns = sum(1 for key in data[0].keys() if key.startswith('user_events.') or (key != 'email' and data[0].get(key)))
        print(f"  Total columns: {num_columns} (Email + {event_columns} event columns)")
    
    # Generate per-user summary
    print("\nGenerating user summaries...")
    text_summary = generate_user_summary(data)
    
    # Check if summary is empty
    if not text_summary or not text_summary.strip():
        print(f"  Warning: No user activity found in the data.")
        print("  Summary file will contain only the header.")
    
    print(f"✅ Generated summary for {len(data):,} users.")
    
    # Use only the email summaries
    final_summary = text_summary
    
    # Create text file
    print("Creating summary text file...")
    summary_file = create_summary_file(final_summary)
    summary_file_path = os.path.abspath(summary_file)
    
    # Verify file was created
    if os.path.exists(summary_file_path):
        file_size = os.path.getsize(summary_file_path)
        print(f"✅ Summary saved to: {summary_file_path}")
        print(f"   File size: {file_size:,} bytes")
    else:
        print(f"❌ Error: File was not created at {summary_file_path}")
        return
    
    # Print summary to console only if there's content
    if final_summary and final_summary.strip():
        print("\n" + "="*80)
        print("SUMMARY PREVIEW")
        print("="*80)
        # Show first 20 lines
        lines = final_summary.split('\n')
        for line in lines[:20]:
            print(line)
        if len(lines) > 20:
            print(f"\n... ({len(lines) - 20} more lines)")
        print("\n" + "="*80)
        print(f"\n📄 Full summary available in: {summary_file_path}")
    else:
        print("\n⚠️  No user activity data found. Summary file contains only header.")


if __name__ == "__main__":
    main()


SLACK_BOT_TOKEN = "xoxb-91986544022-10234221989315-0FDXOpLsbTQiu37RHYJYVYxM"
CHANNEL_ID = "C03RN1YTMJM"   # or channel ID like C01ABCDEF
FILE_PATH = "C:\\Users\\nikhil.upadhyay\\Desktop\\Posthog_Automate\\user_activity_summary.txt"

# Only upload to Slack if file exists and has content
if os.path.exists(FILE_PATH):
    file_size = os.path.getsize(FILE_PATH)
    if file_size > 0:
        client = WebClient(token=SLACK_BOT_TOKEN)
        try:
            client.files_upload_v2(
                channel=CHANNEL_ID,
                file=FILE_PATH,
                title="User Activity Summary",
                initial_comment = "Hi Team,\nSharing the User Activity Last 24 Hours report. 📄"
            )
            print("File sent to Slack successfully")
        except SlackApiError as e:
            print("Slack error:", e.response["error"])
    else:
        print("⚠️  Summary file is empty. Skipping Slack upload.")
else:
    print("⚠️  Summary file not found. Skipping Slack upload.")


