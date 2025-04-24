from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable, Union
from io import BytesIO

import pandas as pd

class BasePipeline(ABC):
    def __init__(self, entity: str):
        self.entity = entity

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
        try:
            self.extract()
            self.load()
            self.transform()
        except Exception as e:
            raise RuntimeError(f"Pipeline execution failed for {self.entity}: {e}")


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
