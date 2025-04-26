from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable, Union, List, Dict, Any
from io import BytesIO
import logging
from typing import Type

import pandas as pd

logger = logging.getLogger(__name__)

class BasePipeline(ABC):
    @abstractmethod
    def extract(self) -> list:
        ...

    @abstractmethod
    def load(self) -> None:
        ...

    @abstractmethod
    def transform(self) -> pd.DataFrame:
        ...

    def run(self) -> None:
        """Standard run method that all pipelines must respect"""
        try:
            raw_data = self.extract()

            if not raw_data:
                logger.warning(f"⚠️ No data extracted")
                return
            
            logger.info(f"📥 Extracted {len(raw_data)} records")
            df = self.transform(raw_data)

            if df.empty:
                logger.warning("❌ Transformation resulted in empty dataframe")
                return
            
            logger.info(f"📊 Transformed DataFrame: Rows={len(df)}, Columns={len(df.columns)}")
            self.load(df)
        except Exception as e:
            raise RuntimeError(f"❌ Pipeline execution failed for {self.entity}: {e}")


@runtime_checkable
class StorageProtocol(Protocol):
    def upload(self, content: bytes, object_key: str) -> None:
        ...

    def download(self, object_key: str) -> bytes:
        ...

    def list_objects(self, prefix: str = "") -> list[str]:
        ...


class AbstractStorage(ABC):
    @abstractmethod
    def upload(self, content: bytes, object_key: str) -> None:
        pass

    @abstractmethod
    def download(self, object_key: str) -> bytes:
        pass

    @abstractmethod
    def list_objects(self, prefix: str = "") -> list[str]:
        pass

    @abstractmethod
    def copy(self, source_key: str, destination_key: str) -> None:
        pass

class PipelineError(Exception):
    """Custom exception for pipeline errors."""
    pass

RawDataType=List[Dict[str, Any]]
PipelineType = Type[BasePipeline]
StorageType = Type[AbstractStorage]