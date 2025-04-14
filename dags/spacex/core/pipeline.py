import logging
from io import BytesIO

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from .interface import BasePipeline, AbstractStorage
from .config import load_conn_params
from .utils import check_raw_file_exists
from .extract import extract_entity
from .load import ensure_database_exists, upload_to_postgres
from .transform import transform_generic_data, RawDataType
from .storage import MinIOConfig, MinIOStorage


def get_storage_from_config() -> AbstractStorage:
    config = MinIOConfig()
    return MinIOStorage(config)


class SpaceXPipeline(BasePipeline):
    def __init__(self, entity: str, storage: AbstractStorage = None):
        self.entity = entity
        self.storage = storage or get_storage_from_config()
        self.object_key = f"processed/{self.entity}.parquet"
        self.data = None

    def __str__(self):
        return f"<SpaceXPipeline entity='{self.entity}' object_key='{self.object_key}'>"

    def extract(self) -> list:
        logging.info(f"🚀 [Extract] Starting data extraction for entity: {self.entity}")
        raw_data = extract_entity(self.entity)

        if not raw_data:
            logging.warning(f"⚠️ No data fetched for entity: {self.entity}")
            return []

        return raw_data

    def transform(self, raw_data: RawDataType) -> pd.DataFrame:
        df = transform_generic_data(raw_data)
        if df.empty:
            logging.warning(f"⚠️ Transformed DataFrame for entity '{self.entity}' is empty.")
        return df

    def load_data_to_minio(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"🔧 [Load] Loading data into MinIO for entity: {self.entity}")

        try:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            self.storage.upload(buffer.read(), self.object_key)
            logging.info(f"💾 Parquet file for entity '{self.entity}' uploaded to MinIO.")
            return df
        except Exception as e:
            logging.error(f"❌ Loading to MinIO failed for entity: {self.entity}. Error: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def load_data_to_postgres(self, df: pd.DataFrame) -> None:
        logging.info(f"⬆️ [Load] Loading data into Postgres for entity: {self.entity}")
        conn_params = load_conn_params()
        ensure_database_exists(conn_params)
        upload_to_postgres(df, conn_params, self.entity)

    def log_data_info(self, df: pd.DataFrame) -> None:
        logging.info(f"📊 Data info for entity '{self.entity}':")
        logging.info(f"• Rows: {len(df)} | Columns: {len(df.columns)}")
        logging.debug(f"• Columns: {list(df.columns)} | Dtypes: {df.dtypes.to_dict()}")

    def load(self, df: pd.DataFrame) -> None:
        logging.info(f"⬆️ [Load] Loading data into staging area for entity: {self.entity}")
        self.log_data_info(df)
        self.load_data_to_minio(df)
        self.load_data_to_postgres(df)
        logging.info(f"✅ Processed data for entity '{self.entity}' successfully loaded.")

    def run(self) -> None:
        try:
            logging.info(f"🏁 Running pipeline: {self}")
            raw_data = self.extract()

            if not raw_data:
                logging.warning(f"⚠️ No data to load for entity '{self.entity}'")
                return

            df = self.transform(raw_data)
            if df.empty:
                logging.warning(f"⚠️ No transformed data to load for entity '{self.entity}'")
                return

            self.load(df)

        except Exception as e:
            logging.error(f"❌ Pipeline execution failed for entity: {self.entity}. Error: {e}")
            raise
