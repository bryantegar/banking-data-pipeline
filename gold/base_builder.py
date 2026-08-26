"""
base_builder.py

Abstract base class for Gold-layer dimensional/fact table builders.

Unlike Bronze/Silver (which land incremental partitions per run), Gold
tables here are full snapshots: each run reads the FULL accumulated state
from Silver -- every partition object in MinIO for that table, merged and
deduplicated by primary key keeping the row with the latest updated_at --
and rewrites the Gold table in one shot. This is the common pattern for a
reporting/dimensional layer where dashboards need "the current picture",
not a stream of deltas.

Gold output still lands as local CSV for now (output/gold/) -- moving it
into the Postgres warehouse is the next step.
"""

import os
from abc import ABC, abstractmethod

import pandas as pd

from storage.minio_client import MinioClient

SILVER_BUCKET = "silver"
GOLD_DIR = "output/gold"


class BaseGoldBuilder(ABC):
    output_table: str = ""

    def __init__(self, minio_client: "MinioClient | None" = None):
        self.minio = minio_client or MinioClient()

    def _load_silver_table(self, table_name: str, primary_key: str) -> pd.DataFrame:
        """Read every Silver partition object for a table from MinIO,
        concatenate them, and keep only the most recent row per primary
        key (by updated_at). This reconstructs "current state" from an
        append-only partition history -- same idea as a MERGE/upsert in a
        real warehouse."""
        keys = self.minio.list_keys(SILVER_BUCKET, prefix=f"{table_name}/")
        if not keys:
            return pd.DataFrame()

        df = pd.concat(
            (self.minio.read_parquet(SILVER_BUCKET, key) for key in keys),
            ignore_index=True,
        )
        df["updated_at"] = pd.to_datetime(df["updated_at"], format="mixed")
        df = df.sort_values("updated_at").drop_duplicates(subset=[primary_key], keep="last")
        return df.reset_index(drop=True)

    @abstractmethod
    def build(self) -> pd.DataFrame:
        """Returns the finished Gold table as a DataFrame."""
        raise NotImplementedError

    def run(self) -> dict:
        if not self.output_table:
            raise ValueError(f"{self.__class__.__name__} must set output_table")

        df = self.build()
        os.makedirs(GOLD_DIR, exist_ok=True)
        out_path = os.path.join(GOLD_DIR, f"{self.output_table}.csv")
        df.to_csv(out_path, index=False)

        return {"table": self.output_table, "rows": len(df), "output_path": out_path}