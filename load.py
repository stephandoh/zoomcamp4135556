import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# config

BUCKET_NAME = "dezoomcamp_hw4_2026"
CREDENTIALS_FILE = "solar-router-483810-s0-219c4dace8e9.json"
YEARS = ["2019", "2020"]
TAXI_TYPES = ["yellow", "green"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DOWNLOAD_DIR = "data"
CHUNK_SIZE = 8 * 1024 * 1024

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)


bucket = client.bucket(BUCKET_NAME)

# function to download files
def download_file(args):
    year, taxi_type, month = args
    file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"
    url = f"{BASE_URL}/{file_name}"
    local_path = os.path.join(DOWNLOAD_DIR, file_name)

    try:
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, local_path)
        return local_path
    except Exception as e:
        print(f"Failed: {url} -> {e}")
        return None
    

# function to create bucket
def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)
    

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

# function execute
if __name__ == "__main__":
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    create_bucket(BUCKET_NAME)

    tasks = [(year, taxi_type, month) for year in YEARS for taxi_type in TAXI_TYPES for month in MONTHS]    

    with ThreadPoolExecutor(max_workers=6) as executor:
        files = list(executor.map(download_file, tasks))

    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(upload_to_gcs, filter(None, files))


    print("All files downloaded and uploaded to GCS successfully.")