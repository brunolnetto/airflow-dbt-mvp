import logging
from io import BytesIO
from typing import Optional
from contextlib import contextmanager

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from core.base import BasePipeline, StorageType, RawDataType, PipelineError
from core.config import load_postgres_conn_params
from core.extract import request_data
from core.load import ensure_database_exists, upload_to_postgres
from core.transform import transform_generic_data

logger = logging.getLogger(__name__)

RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 5

@contextmanager
def pipeline_context(self, name, extras):
    logger.info(f"🏁 Starting pipeline: {name}", extra=extras)
    try:
        yield
    finally:
        logger.info(f"✅ Completed pipeline: {name}", extra=extras)

class SpaceXPipeline(BasePipeline):
    def __init__(self, entity: str, storage: StorageType) -> None:
        self.entity = entity
        self.entity_url = f"https://api.spacexdata.com/v4/{self.entity}"
        self.storage = storage
        self.object_key = f"spacex/{self.entity}.parquet"
        self.data: Optional[pd.DataFrame] = None

    def __str__(self):
        return f"<SpaceXPipeline entity='{self.entity}' object_key='{self.object_key}'>"

    def extract(self) -> list[dict]:
        """
        Extract data from the SpaceX API.
        """
        
        logger.info(f"🚀 [Extract] Starting data extraction", extra={"entity": self.entity})
        raw_data = request_data(self.entity_url)

        if not raw_data:
            logger.warning(f"⚠️ No data fetched", extra={"entity": self.entity})
            return []

        return raw_data

    def transform(self, raw_data: list[dict]) -> pd.DataFrame:
        """
        Transform the raw data into a DataFrame.
        """

        df = transform_generic_data(raw_data)
        if df.empty:
            logger.warning(f"⚠️ Transformed DataFrame is empty", extra={"entity": self.entity})
        return df

    def move_to_failed_folder(self) -> None:
        """
        Move the file to the failed folder in MinIO storage.
        """

        failed_key = f"spacex/failed/{self.entity}.parquet"
        self.storage.copy(self.object_key, failed_key)
        self.storage.delete(self.object_key)
        logger.info(f"📂 File moved to failed folder as {failed_key}", extra={"entity": self.entity})

    def load_to_storage(self, df: pd.DataFrame) -> None:
        """
        Load the DataFrame to MinIO storage.
        """
        logger.info(f"🔧 [Load] Uploading to MinIO", extra={"entity": self.entity})
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.storage.upload(buffer.read(), self.object_key)
        logger.info(f"💾 Successfully uploaded to MinIO", extra={"entity": self.entity})

    @retry(stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_WAIT_SECONDS))
    def load_to_database(self, df: pd.DataFrame) -> None:
        """
        Load the DataFrame to PostgreSQL database.
        """
        logger.info(f"⬆️ [Load] Uploading to Postgres", extra={"entity": self.entity})
        conn_params = load_postgres_conn_params()
        ensure_database_exists(conn_params)
        upload_to_postgres(df, conn_params, self.entity)
        logger.info(f"📥 Successfully uploaded to Postgres", extra={"entity": self.entity})

    def load(self, df: pd.DataFrame) -> None:
        """
        Load the transformed DataFrame to storage and database.
        """
        self.load_to_storage(df)
        try:
            self.load_to_database(df)
        except Exception as e:
            logger.error(f"❌ Postgres upload failed, moving file to failed folder...", extra={"entity": self.entity})
            self.move_to_failed_folder()
            raise PipelineError(f"Failed to upload {self.entity} to Postgres") from e

    def run(self) -> None:
        """
        Run the pipeline: extract, transform, and load data.
        """
        with pipeline_context(self.__class__.__name__, {"entity": self.entity}):
            super().run()