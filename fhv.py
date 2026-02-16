import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# ================= CONFIG =================
BUCKET_NAME = "dezoomcamp_hw4_steph_2026"
CREDENTIALS_FILE = r"C:\Users\steph\Downloads\zoomcamp_hw_4\solar-router-483810-s0-219c4dace8e9.json"
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "data"
CHUNK_SIZE = 8 * 1024 * 1024

# Correct GitHub release URL for FHV data
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

# ================= SETUP CLIENT =================
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

# ================= FUNCTIONS =================
def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists, proceeding...")
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but is not accessible. Change the bucket name.")
        sys.exit(1)

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt+1})...")
            blob.upload_from_filename(file_path)
            if verify_gcs_upload(blob_name):
                print(f"Uploaded & verified: gs://{BUCKET_NAME}/{blob_name}")
                return
        except Exception as e:
            print(f"Upload failed: {e}")
        time.sleep(5)
    print(f"Failed to upload {file_path} after {max_retries} attempts.")

def download_file(args):
    year, month = args
    file_name = f"fhv_tripdata_{year}-{month}.csv.gz"
    url = f"{BASE_URL}/{file_name}"
    local_path = os.path.join(DOWNLOAD_DIR, file_name)
    try:
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, local_path)
        return local_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

# ================= EXECUTION =================
if __name__ == "__main__":
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    create_bucket(BUCKET_NAME)

    tasks = [(year, month) for year in YEARS for month in MONTHS]

    # Download files in parallel
    with ThreadPoolExecutor(max_workers=6) as executor:
        files = list(executor.map(download_file, tasks))

    # Upload successfully downloaded files to GCS
    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(upload_to_gcs, filter(None, files))

    print("All FHV files downloaded and uploaded to GCS successfully.")
