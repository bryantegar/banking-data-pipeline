"""
WatermarkStore: persists the last successful extraction timestamp per source
table, backed by a small SQLite metadata table.

This is what makes extraction "incremental": every extractor asks
"what's my last watermark?" before querying the source, and reports back
"here's my new high-water mark" after a successful run.
"""

import sqlite3
from datetime import datetime
from typing import Optional


class WatermarkStore:
    def __init__(self, db_path: str = "watermark/metadata.db"):
        self.db_path = db_path
        self._init_table()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watermarks (
                    table_name TEXT PRIMARY KEY,
                    last_watermark TEXT,
                    updated_at TEXT
                )
                """
            )

    def get_last_watermark(self, table_name: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_watermark FROM watermarks WHERE table_name = ?",
                (table_name,),
            ).fetchone()
        return row[0] if row else None

    def set_watermark(self, table_name: str, watermark: str) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watermarks (table_name, last_watermark, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(table_name) DO UPDATE SET
                    last_watermark = excluded.last_watermark,
                    updated_at = excluded.updated_at
                """,
                (table_name, watermark, now),
            )
