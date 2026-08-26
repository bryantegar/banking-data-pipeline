"""
BaseSilverTransformer: abstract class for Bronze -> Silver transformation.

Template Method pattern, mirroring extractors.base_extractor.BaseExtractor:
- run() is the shared orchestration (read Bronze partition from MinIO ->
  clean -> DQ check -> drop invalid rows -> mask PII -> register keys ->
  write Silver partition to MinIO + DQ report to local disk).
- clean() / pii_columns / build_dq_checks() are declared per subclass.

Storage: Bronze and Silver partitions both live in MinIO (the data lake)
as Parquet objects -- key pattern: <bucket>/<table_name>/<partition_key>.parquet.
partition_key is the same value the Bronze DAG used when it wrote this
data (Airflow's ts_nodash), passed through TriggerDagRunOperator's conf so
Silver reads exactly the slice Bronze just produced -- not a guess based on
date alone (Bronze runs hourly, so a date-only key would be ambiguous).

The DQ report JSON stays on local disk (output/silver/_dq_reports/) -- it's
a small audit artifact, not part of the lake's actual data.

Design note on referential integrity: Bronze extraction is incremental (only
new/changed rows per run), so a given `accounts` partition may reference
customers extracted hours ago -- not present in *this* partition. Validating
referential integrity against only the current partition would produce
false-positive violations for perfectly valid rows. Instead, every
transformer registers its primary keys into a persistent ReferenceRegistry
after a successful run, and child tables validate foreign keys against that
accumulated registry rather than the current partition alone.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd

from silver.data_quality import DataQualityChecker, DataQualityReport
from silver.pii_masking import PiiMasker
from silver.reference_registry import ReferenceRegistry
from storage.minio_client import MinioClient

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
DQ_REPORT_DIR = "output/silver/_dq_reports"


@dataclass
class TransformResult:
    table: str
    rows_in: int
    rows_out: int
    output_key: Optional[str]
    dq_report: DataQualityReport
    skipped: bool = False


class BaseSilverTransformer(ABC):
    table_name: str = ""
    primary_key: str = ""
    # {column_name: masking_strategy}, e.g. {"nik": "hash"}. Empty by default
    # -- most tables in this dataset carry no PII except customers.
    pii_columns: Dict[str, str] = {}

    def __init__(
        self,
        registry: Optional[ReferenceRegistry] = None,
        minio_client: Optional[MinioClient] = None,
    ):
        if not self.table_name or not self.primary_key:
            raise ValueError(f"{self.__class__.__name__} must set table_name and primary_key")
        self.registry = registry or ReferenceRegistry()
        self.masker = PiiMasker()
        self.minio = minio_client or MinioClient()

    def _read_bronze_partition(self, partition_key: str) -> Optional[pd.DataFrame]:
        object_key = f"{self.table_name}/{partition_key}.parquet"
        if not self.minio.object_exists(BRONZE_BUCKET, object_key):
            return None
        return self.minio.read_parquet(BRONZE_BUCKET, object_key)

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Default cleaning: drop exact duplicate rows and rows missing the
        primary key entirely. Override per table for anything more specific
        (e.g. trimming whitespace, normalizing casing)."""
        df = df.drop_duplicates()
        df = df.dropna(subset=[self.primary_key])
        return df

    @abstractmethod
    def build_dq_checks(self, checker: DataQualityChecker) -> None:
        """Register this table's data-quality rules on `checker`."""
        raise NotImplementedError

    def mask_pii(self, df: pd.DataFrame) -> pd.DataFrame:
        for column, strategy in self.pii_columns.items():
            if column in df.columns:
                df[column] = self.masker.apply(df[column], strategy)
        return df

    def run(self, partition_key: str) -> TransformResult:
        df = self._read_bronze_partition(partition_key)
        if df is None or df.empty:
            return TransformResult(
                table=self.table_name,
                rows_in=0,
                rows_out=0,
                output_key=None,
                dq_report=DataQualityReport(table=self.table_name, checks=[]),
                skipped=True,
            )

        rows_in = len(df)
        df = self.clean(df)

        checker = DataQualityChecker(self.table_name, df, self.registry)
        self.build_dq_checks(checker)
        report = checker.run()

        # Quarantine rather than crash: rows that fail a DQ rule are dropped
        # from Silver and logged, so one bad row doesn't block an entire
        # partition.
        df = df[~df[self.primary_key].isin(report.invalid_ids)]
        df = self.mask_pii(df)

        self.registry.register_ids(self.table_name, df[self.primary_key].tolist())

        output_key = f"{self.table_name}/{partition_key}.parquet"
        self.minio.write_parquet(df, bucket=SILVER_BUCKET, key=output_key)

        os.makedirs(DQ_REPORT_DIR, exist_ok=True)
        report.save_json(os.path.join(DQ_REPORT_DIR, f"{self.table_name}_{partition_key}.json"))

        return TransformResult(
            table=self.table_name,
            rows_in=rows_in,
            rows_out=len(df),
            output_key=output_key,
            dq_report=report,
            skipped=False,
        )