import requests
import pandas as pd
import logging
from google.cloud import storage
from datetime import datetime

# ================== Config ==================
BUCKET_NAME = "spacex-raw-data"  # your GCS bucket
FILE_FORMAT = "csv"              # or "parquet"
FILE_NAME = f"spacex_launches_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.{FILE_FORMAT}"
DESTINATION_BLOB_NAME = f"raw/{FILE_NAME}"  # path inside GCS bucket

# Make sure this env var is set to your service account file path
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/spacex-ingestor-key.json"

# ================== Logging ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ================ Extract ===================
def extract_from_spacex():
    url = "https://api.spacexdata.com/v4/launches"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("✅ Successfully fetched launch data from SpaceX API")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to fetch data: {e}")
        return []


# ================ Transform ===================
def transform_launch_data(raw_data):
    logging.info("🔧 Transforming raw data into DataFrame...")

    launches = []
    for launch in raw_data:
        launches.append({
            "id": launch.get("id"),
            "name": launch.get("name"),
            "date_utc": launch.get("date_utc"),
            "success": launch.get("success"),
            "rocket": launch.get("rocket"),
            "details": launch.get("details"),
            "flight_number": launch.get("flight_number")
        })

    df = pd.DataFrame(launches)
    logging.info(f"📊 Transformed {len(df)} rows.")
    return df


# ================ Load ===================
def upload_to_gcs(df, bucket_name, destination_blob_name, file_format="csv"):
    logging.info(f"☁️ Uploading data to GCS bucket: {bucket_name}/{destination_blob_name}")

    # Save locally first
    local_temp_file = f"/tmp/{FILE_NAME}"
    if file_format == "csv":
        df.to_csv(local_temp_file, index=False)
    elif file_format == "parquet":
        df.to_parquet(local_temp_file, index=False)
    else:
        raise ValueError("Unsupported file format. Use 'csv' or 'parquet'.")

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_temp_file)

    logging.info(f"✅ Successfully uploaded to gs://{bucket_name}/{destination_blob_name}")


# ================ ETL Pipeline ===================
    
def load_to_gcs_pipeline():
    raw_data = extract_from_spacex()
    if raw_data:
        df = transform_launch_data(raw_data)
        upload_to_gcs(df, BUCKET_NAME, DESTINATION_BLOB_NAME, FILE_FORMAT)
    else:
        logging.error("No data fetched from SpaceX API")


# ================ Run ETL ===================
if __name__ == "__main__":
    logging.info("🚀 Starting SpaceX ETL process...")

    raw_data = extract_from_spacex()
    if raw_data:
        df = transform_launch_data(raw_data)
        upload_to_gcs(df, BUCKET_NAME, DESTINATION_BLOB_NAME, FILE_FORMAT)

    logging.info("✅ ETL pipeline finished.")
