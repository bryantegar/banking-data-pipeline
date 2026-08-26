import io
import os
from typing import List

import boto3
import pandas as pd
from botocore.client import Config
from botocore.exceptions import ClientError


class MinioClient:
    def __init__(
        self,
        # "minio:9000" only resolves from inside the Docker network (e.g. an
        # Airflow container). Running this from the host/WSL needs
        # "localhost:9000" instead -- MINIO_ENDPOINT_URL lets each context
        # override without touching code.
        endpoint_url: str = None,
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin123",
    ):
        endpoint_url = endpoint_url or os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def ensure_bucket(self, bucket: str) -> None:
        existing = [b["Name"] for b in self.client.list_buckets().get("Buckets", [])]
        if bucket not in existing:
            self.client.create_bucket(Bucket=bucket)

    def write_parquet(self, df: pd.DataFrame, bucket: str, key: str) -> None:
        self.ensure_bucket(bucket)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    def read_parquet(self, bucket: str, key: str) -> pd.DataFrame:
        obj = self.client.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))

    def list_keys(self, bucket: str, prefix: str = "") -> List[str]:
        """List every object key under a prefix, e.g. 'bronze/customers/'."""
        self.ensure_bucket(bucket)
        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def object_exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False