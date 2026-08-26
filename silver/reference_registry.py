"""
ReferenceRegistry: persists known primary keys per Silver table in SQLite.

Mirrors watermark.watermark_store.WatermarkStore in structure and purpose --
except instead of answering "what's the last timestamp I saw", it answers
"which IDs have already been validated into Silver". Referential-integrity
checks need that accumulated history, not just today's Bronze partition
(see BaseSilverTransformer docstring for why).
"""

import sqlite3


class ReferenceRegistry:
    def __init__(self, db_path: str = "silver/reference_registry.db"):
        self.db_path = db_path
        self._init_table()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS known_ids (
                    table_name TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    PRIMARY KEY (table_name, entity_id)
                )
                """
            )

    def register_ids(self, table_name: str, ids: list) -> None:
        """Idempotent: re-registering an already-known ID is a no-op, so
        transformers can safely re-run without corrupting the registry."""
        if not ids:
            return
        with self._connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO known_ids (table_name, entity_id) VALUES (?, ?)",
                [(table_name, str(entity_id)) for entity_id in ids],
            )

    def known_ids(self, table_name: str) -> set:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT entity_id FROM known_ids WHERE table_name = ?", (table_name,)
            ).fetchall()
        # IDs are stored as TEXT (SQLite has no fixed schema for the value),
        # but source data uses integer PKs -- cast back so `.isin()` checks
        # against a pandas int column actually match.
        return {int(value) for (value,) in rows}