"""
postgres_loader.py

Loads finished Gold tables (output/gold/*.csv, written by run_builders.py)
into the Postgres warehouse (`warehouse-db` in docker-compose). Loading is
kept separate from building -- builders only know how to turn Silver data
into a DataFrame; getting that DataFrame into the warehouse is a distinct
concern (Single Responsibility), and keeping them separate means a loader
bug can't corrupt a build, and vice versa.

Connection defaults assume running from the host/WSL (localhost:5433, the
port mapped in docker-compose). If this is ever run from inside a Docker
container on the same network as warehouse-db, override via env vars:
WAREHOUSE_DB_HOST=warehouse-db and WAREHOUSE_DB_PORT=5432 (the internal
port) -- same host-vs-container distinction as MinioClient.
"""

import glob
import os
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class WarehouseConfig:
    host: str = field(default_factory=lambda: os.environ.get("WAREHOUSE_DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.environ.get("WAREHOUSE_DB_PORT", "5433")))
    database: str = field(default_factory=lambda: os.environ.get("WAREHOUSE_DB_NAME", "banking_warehouse"))
    user: str = field(default_factory=lambda: os.environ.get("WAREHOUSE_DB_USER", "warehouse"))
    password: str = field(default_factory=lambda: os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse"))

    @property
    def dsn(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class PostgresLoader:
    """Writes Gold DataFrames into the warehouse as full-snapshot replace
    loads. This matches BaseGoldBuilder's own semantics -- each run rebuilds
    the whole table from Silver -- so a full REPLACE here is consistent
    with how the table was produced, not a shortcut."""

    def __init__(self, config: WarehouseConfig = None):
        self.config = config or WarehouseConfig()
        self._engine = create_engine(self.config.dsn)

    def load_dataframe(
    self,
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public",
    ) -> int:

        if df.empty:
            print(f"  [{table_name}] skipped -- DataFrame is empty")
            return 0

        with self._engine.begin() as conn:
            conn.execute(
                text(f'DELETE FROM "{schema}"."{table_name}"')
            )

            df.to_sql(
                table_name,
                conn,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        return len(df)

    def load_csv(self, csv_path: str, table_name: str, schema: str = "public") -> int:
        df = pd.read_csv(csv_path)
        return self.load_dataframe(df, table_name, schema=schema)

    def load_all_from_dir(self, gold_dir: str = "output/gold", schema: str = "public") -> None:
        csv_paths = sorted(glob.glob(os.path.join(gold_dir, "*.csv")))
        if not csv_paths:
            print(f"No Gold CSVs found in {gold_dir} -- run gold.run_builders first.")
            return

        for path in csv_paths:
            table_name = os.path.splitext(os.path.basename(path))[0]
            rows = self.load_csv(path, table_name, schema=schema)
            if rows:
                print(f"  [{table_name}] loaded {rows} rows into {schema}.{table_name}")


if __name__ == "__main__":
    PostgresLoader().load_all_from_dir()