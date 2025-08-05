# Ingest Visitors Data
import json
import boto3
import os
import requests
from io import StringIO, BytesIO
from datetime import datetime

# Wistia API constants
BASE_URL = "https://api.wistia.com/v1/stats/visitors.json"
WISTIA_API_TOKEN = os.getenv("WISTIA_API_TOKEN")
if not WISTIA_API_TOKEN:
    raise Exception("Missing WISTIA_API_TOKEN environment variable")

# AWS S3 bucket
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ak-wistia")  # fallback if not set

def fetch_all_visitors(per_page=100, max_pages=3):
    page = 1
    all_visitors = []

    headers = {
        "Authorization": f"Bearer {WISTIA_API_TOKEN}"
    }

    while page <= max_pages:
        print(f"Fetching page {page}...")
        response = requests.get(
            BASE_URL,
            params={"page": page, "per_page": per_page},
            headers=headers
        )

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        visitors_batch = response.json()
        if not visitors_batch:
            print("No more data to fetch. Exiting.")
            break

        all_visitors.extend(visitors_batch)
        page += 1

    return all_visitors

def upload_json_to_s3(data, bucket, filename):
    s3 = boto3.client("s3")
    json_str = json.dumps(data, indent=2)
    json_buffer = BytesIO(json_str.encode("utf-8"))
    
    s3.upload_fileobj(json_buffer, bucket, filename)
    print(f"Uploaded to S3: s3://{bucket}/{filename}")

def lambda_handler(event, context):
    try:
        visitors_data = fetch_all_visitors()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_data/visitors/visitors_data_{timestamp}.json"

        upload_json_to_s3(visitors_data, S3_BUCKET_NAME, filename)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Fetched {len(visitors_data)} records and uploaded to S3.",
                "s3_key": filename
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
