# Ingest Engagement Data based off of each media-id
import json
import boto3
import os
import requests
from io import BytesIO
from datetime import datetime
from urllib.parse import urljoin

# Constants
BASE_ENGAGEMENT_ENDPOINT = "https://api.wistia.com/v1/stats/medias"
WISTIA_API_TOKEN = os.getenv("WISTIA_API_TOKEN")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ak-wistia")

# Auth headers
HEADERS = {"Authorization": f"Bearer {WISTIA_API_TOKEN}"}

def get_latest_media_file_key(bucket, pointer_key="raw_data/media/s3_keys/latest_media_file.json"):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=pointer_key)
    pointer_data = json.loads(response["Body"].read().decode("utf-8"))
    return pointer_data["latest_s3_key"]

def get_latest_media_data(bucket, key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))

def fetch_engagement_for_media(media_id):
    url = f"{BASE_ENGAGEMENT_ENDPOINT}/{media_id}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"[Error] {media_id}: {response.status_code} - {response.text}")
        return {"media_id": media_id, "error": response.text}

    return response.json()

def upload_to_s3(data, bucket, filename):
    s3 = boto3.client("s3")
    json_str = json.dumps(data, indent=2)
    buffer = BytesIO(json_str.encode("utf-8"))
    s3.upload_fileobj(buffer, bucket, filename)
    print(f"Uploaded engagement to: s3://{bucket}/{filename}")

def lambda_handler(event, context):
    try:
        MEDIA_FILE_S3_KEY=get_latest_media_file_key(S3_BUCKET_NAME)
        print(f"Latest object S3 Key: {MEDIA_FILE_S3_KEY}")

        media_list = get_latest_media_data(S3_BUCKET_NAME, MEDIA_FILE_S3_KEY)
        all_engagement_data = []

        for media in media_list:
            media_id = media.get("hashed_id")
            if media_id:
                engagement_data = fetch_engagement_for_media(media_id)
                all_engagement_data.append({
                    "media_id": media_id,
                    "engagement": engagement_data
                })

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"raw_data/engagement/media_engagement_{timestamp}.json"
        upload_to_s3(all_engagement_data, S3_BUCKET_NAME, s3_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Fetched engagement for {len(all_engagement_data)} media",
                "s3_key": s3_key
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

