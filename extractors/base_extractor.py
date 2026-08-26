"""
BaseExtractor: abstract class for watermark-based incremental extraction
from the source (OLTP) database into the Bronze layer.

Bronze partitions now land in MinIO (the data lake), as Parquet objects,
key pattern: bronze/<table_name>/<partition_key>.parquet

partition_key defaults to a full UTC timestamp (not just a date) -- Bronze
now runs hourly, so a date-only partition name would let a later run in
the same day silently overwrite an earlier one. The Bronze DAG passes
Airflow's `ts_nodash` as partition_key, and forwards that same value to
Silver via TriggerDagRunOperator's conf, so both layers agree on which
slice of data a given run produced.

Template Method pattern, same as data_generator.base.BaseGenerator:
- extract() is the shared orchestration (get watermark -> query -> land -> advance watermark)
- table_name / watermark_column are declared per subclass
"""

import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import pandas as pd

from storage.minio_client import MinioClient
from watermark.watermark_store import WatermarkStore

SOURCE_DB_PATH = "source_db/core_banking.db"
BRONZE_BUCKET = "bronze"


class BaseExtractor(ABC):
    table_name: str = ""
    watermark_column: str = "updated_at"

    def __init__(
        self,
        source_db_path: str = SOURCE_DB_PATH,
        watermark_store: Optional[WatermarkStore] = None,
        minio_client: Optional[MinioClient] = None,
    ):
        if not self.table_name:
            raise ValueError(f"{self.__class__.__name__} must set table_name")
        self.source_db_path = source_db_path
        self.watermark_store = watermark_store or WatermarkStore()
        self.minio = minio_client or MinioClient()

    def _read_incremental(self, last_watermark: Optional[str]) -> pd.DataFrame:
        conn = sqlite3.connect(self.source_db_path)
        if last_watermark is None:
            query = f"SELECT * FROM {self.table_name}"
            df = pd.read_sql(query, conn)
        else:
            query = f"SELECT * FROM {self.table_name} WHERE {self.watermark_column} > ?"
            df = pd.read_sql(query, conn, params=(last_watermark,))
        conn.close()
        return df

    def extract(self, partition_key: Optional[str] = None) -> dict:
        """
        Runs one incremental extraction cycle. Returns a small summary dict
        so the calling Airflow task can log/XCom it.
        """
        partition_key = partition_key or datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        last_watermark = self.watermark_store.get_last_watermark(self.table_name)

        df = self._read_incremental(last_watermark)

        if df.empty:
            return {"table": self.table_name, "rows_extracted": 0, "watermark_advanced": False}

        object_key = f"{self.table_name}/{partition_key}.parquet"
        self.minio.write_parquet(df, bucket=BRONZE_BUCKET, key=object_key)

        new_watermark = df[self.watermark_column].max()
        self.watermark_store.set_watermark(self.table_name, str(new_watermark))

        return {
            "table": self.table_name,
            "rows_extracted": len(df),
            "bucket": BRONZE_BUCKET,
            "object_key": object_key,
            "new_watermark": str(new_watermark),
            "watermark_advanced": True,
        }