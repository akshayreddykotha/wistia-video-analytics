# Ingest Events Data
import json
import boto3
import os
import requests
from io import BytesIO
from datetime import datetime, timezone
import time

# Constants
BASE_URL = "https://api.wistia.com/v1/stats/events.json"
WISTIA_API_TOKEN = os.getenv("WISTIA_API_TOKEN")
if not WISTIA_API_TOKEN:
    raise Exception("Missing WISTIA_API_TOKEN environment variable")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ak-wistia")
MEDIA_IDS_FILTER = ["gskhw4w4lm", "v08dlrgr7v"]   # comma-separated media ids, optional

# Settings
MAX_PAGES = 5
PER_PAGE = 100
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

s3 = boto3.client("s3")
LATEST_FILE_KEY = "raw_data/events/s3_keys/latest_event_file.json"

def fetch_all_events(per_page=PER_PAGE, max_pages=MAX_PAGES, media_id=None, received_after=None):
    page = 1
    all_events = []
    batch_length = 1
    headers = {
        "Authorization": f"Bearer {WISTIA_API_TOKEN}"
    }

    while batch_length > 0:
        print(f"Fetching page {page}... for {media_id}")
        try:
            response = requests.get(
                BASE_URL,
                params={"page": page, "per_page": per_page, "media_id": media_id },
                headers=headers
            )

            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break

            batch = response.json()
            if not batch:
                print("No more data.")
                break

            # Filter by media_id if provided
            if media_id:
                batch = [event for event in batch]

            # Filter by received_at if provided
            if received_after:
                batch = [event for event in batch if event.get("received_at") and
                         datetime.fromisoformat(event["received_at"].replace("Z", "+00:00")) > received_after]
            
            # Find out the number of elements after filters
            batch_length = len(batch)
            
            all_events.extend(batch)

            page += 1

        except Exception as e:
            print(f"Exception on page {page}: {e}")
            retries = 0
            while retries < MAX_RETRIES:
                print(f"Retrying ({retries + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY)
                retries += 1

    return all_events

def upload_json_to_s3(data, bucket, filename):
    s3 = boto3.client("s3")
    json_str = json.dumps(data, indent=2)
    json_buffer = BytesIO(json_str.encode("utf-8"))
    s3.upload_fileobj(json_buffer, bucket, filename)
    print(f"Uploaded to S3: s3://{bucket}/{filename}")

def update_latest_file_pointer(file_key=None):
    content = {"latest_s3_key": file_key}
    json_buffer = BytesIO(json.dumps(content, indent=2).encode("utf-8"))
    s3.upload_fileobj(json_buffer, S3_BUCKET_NAME, LATEST_FILE_KEY) #uploads the latest file or [] to the latest_event_file.json
    print(f"Updated latest_event_file.json: {file_key}")

def lambda_handler(event, context):
    total_events = []
    try:
        print("Starting Wistia Events Ingestion Lambda...")

        # Pull only events received in last 24 hours (for incremental pull)
        received_after = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Looking for records after: {received_after}")
        for media_id in MEDIA_IDS_FILTER:
            events = fetch_all_events(media_id=media_id, received_after=received_after)
            total_events.extend(events)
        
        if total_events:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"raw_data/events/wistia_events_{timestamp}.json"
            upload_json_to_s3(total_events, S3_BUCKET_NAME, filename)
            update_latest_file_pointer(filename)
        
        else:
            # No events, update pointer to empty list
            filename=None
            update_latest_file_pointer(filename)
            message = "No new events found for today. No file uploaded."

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Fetched {len(total_events)} events and uploaded to S3.",
                "s3_key": filename
            })
        }

    except Exception as e:
        print(f"Error: {e}")
        update_latest_file_pointer(None)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
