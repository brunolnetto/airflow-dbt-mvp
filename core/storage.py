import os
import json
import logging
from io import BytesIO
from pathlib import Path
from typing import List
from dataclasses import dataclass

import boto3
import pandas as pd

from .base import AbstractStorage
from .config import PROCESSED_DIR, RAW_DIR
from .utils import serialize_to_buffer

@dataclass
class MinIOConfig:
    endpoint_url: str = os.getenv("MINIO_URL", "http://minio:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY")
    secret_key: str = os.getenv("MINIO_SECRET_KEY")
    region_name: str = os.getenv("MINIO_REGION")
    bucket_name: str

def get_s3_client(config: MinIOConfig):
    if not all([config.access_key, config.secret_key, config.bucket_name]):
        raise RuntimeError("❌ MinIO configuration is incomplete.")

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region_name,
    )

class MinIOStorage(AbstractStorage):
    def __init__(self, config: MinIOConfig):
        values=[config.access_key, config.secret_key, config.bucket_name]
        if not all(values):
            raise RuntimeError("❌ Missing required MinIO credentials or bucket name.")
        
        self.config = config
        self.client = get_s3_client(config)
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            buckets = self.client.list_buckets().get("Buckets", [])
            if not any(b["Name"] == self.config.bucket_name for b in buckets):
                self.client.create_bucket(Bucket=self.config.bucket_name)
                logging.info(f"🪣 Bucket '{self.config.bucket_name}' created.")
        except Exception as e:
            logging.error(f"❌ Bucket verification/creation failed: {e}")
            raise

    # 📤 Método genérico de upload
    def upload(self, content: bytes, object_key: str) -> None:
        try:
            buffer = BytesIO(content)
            buffer.seek(0)
            self.client.upload_fileobj(buffer, self.config.bucket_name, object_key)
            logging.info(f"📤 Uploaded to MinIO at {object_key}")
        except Exception as e:
            logging.error(f"❌ Upload failed: {e}")
            raise

    # 📥 Método genérico de download
    def download(self, object_key: str) -> bytes:
        try:
            buffer = BytesIO()
            self.client.download_fileobj(self.config.bucket_name, object_key, buffer)
            buffer.seek(0)
            logging.info(f"📥 Downloaded from MinIO: {object_key}")
            return buffer.read()
        except Exception as e:
            logging.error(f"❌ Download failed: {e}")
            raise

    # 📝 Método genérico de copia
    def copy(self, source_key: str, destination_key: str) -> None:
        self.client.copy_object(
            bucket_name=self.bucket,
            object_name=destination_key,
            object_source=f"{self.bucket}/{source_key}"
        )

    # 📜 Listar objetos com prefixo
    def list_objects(self, prefix: str = "") -> List[str]:
        try:
            response = self.client.list_objects_v2(Bucket=self.config.bucket_name, Prefix=prefix)
            return [item["Key"] for item in response.get("Contents", [])]
        except Exception as e:
            logging.error(f"❌ List objects failed: {e}")
            raise

    # ✅ Métodos utilitários (não fazem parte do contrato de interface)
    def upload_json(self, data: List[dict], object_key: str) -> None:
        content = json.dumps(data).encode("utf-8")
        self.upload(content, object_key)

    def download_json(self, object_key: str) -> List[dict]:
        content = self.download(object_key)
        return json.loads(content)

    def upload_dataframe_parquet(self, df: pd.DataFrame, object_key: str) -> None:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        self.upload(buffer.getvalue(), object_key)
