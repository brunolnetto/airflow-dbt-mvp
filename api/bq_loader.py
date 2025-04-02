from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging

# =================== Config ===================
PROJECT_ID = "spacex-data-pipeline"
DATASET_ID = "raw"  # you can use "bronze" if following medallion
TABLE_ID = "spacex_launches"
BUCKET_NAME = "spacex-raw-data"
FILE_NAME = "raw/spacex_launches_20250402_005801.csv"
FILE_FORMAT = "CSV"  # or "PARQUET"

# Full table path
BQ_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# =================== Logging ===================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_to_bigquery():
    client = bigquery.Client()

    # Build URI
    gcs_uri = f"gs://{BUCKET_NAME}/{FILE_NAME}"
    logging.info(f"Loading file from: {gcs_uri}")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows=1,  # optional: if first row is header
        max_bad_records=10    # ✅ allow up to 10 malformed rows
    )
    
    try:
        load_job = client.load_table_from_uri(
            gcs_uri,
            BQ_TABLE,
            job_config=job_config
        )
        load_job.result()  # wait for the job to complete

        logging.info(f"✅ Loaded data into BigQuery table: {BQ_TABLE}")

        # Confirm table size
        table = client.get_table(BQ_TABLE)
        logging.info(f"📊 {table.num_rows} rows loaded.")

    except NotFound as e:
        logging.error(f"❌ Table or dataset not found: {e}")
    except Exception as e:
        logging.error(f"❌ Load failed: {e}")


if __name__ == "__main__":
    load_to_bigquery()
