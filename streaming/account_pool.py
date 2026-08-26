"""
account_pool.py

Shared helper for loading the current pool of open account IDs from
source_db -- used by both producer.py (to pick sender/receiver accounts)
and consumer.py (to self-calibrate fraud thresholds against the SAME
distribution the producer is actually generating from). Keeping this in
one place means producer and consumer can never quietly drift out of
sync with each other's view of "what accounts exist right now".
"""

import os
import sqlite3

import pandas as pd

SOURCE_DB_PATH = os.environ.get("SOURCE_DB_PATH", "source_db/core_banking.db")


def load_account_pool() -> list:
    conn = sqlite3.connect(SOURCE_DB_PATH)
    ids = pd.read_sql(
        "SELECT account_id FROM accounts WHERE status != 'closed'", conn
    )["account_id"].tolist()
    conn.close()
    return ids
