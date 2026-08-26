"""
BaseGenerator: abstract base class for all synthetic banking data generators.

Design pattern: Template Method.
- generate_one() is the abstract "hook" each subclass must implement.
- generate_many() is the shared orchestration logic (loop + collect).
- _audit_timestamps() is shared logic for created_at / updated_at / deleted_at,
  reused by every entity so the CDC (change-data-capture) simulation stays
  consistent across branches, customers, accounts, and loans.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import random

import pandas as pd
from faker import Faker


class BaseGenerator(ABC):
    def __init__(self, locale: str = "id_ID", seed: Optional[int] = None):
        self.fake = Faker(locale)
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)
        self._records: list[dict] = []

    @abstractmethod
    def generate_one(self, entity_id: int, **kwargs) -> dict:
        """Generate a single record. Must be implemented by each subclass."""
        raise NotImplementedError

    def generate_many(self, count: int, **kwargs) -> list[dict]:
        """Generate `count` records by repeatedly calling generate_one()."""
        self._records = [self.generate_one(i + 1, **kwargs) for i in range(count)]
        return self._records

    def _audit_timestamps(
        self,
        created_start: str = "-3y",
        created_end: str = "-6M",
        update_prob: float = 0.15,
        delete_prob: float = 0.0,
    ) -> tuple[datetime, datetime, Optional[datetime]]:
        """
        Shared CDC-style timestamp logic.

        - created_at: when the record first entered the system
        - updated_at: only diverges from created_at for a fraction of records
          (update_prob) -> gives an incremental/watermark pipeline something
          real to detect
        - deleted_at: soft-delete timestamp for a fraction of records
          (delete_prob), None otherwise
        """
        created = self.fake.date_time_between(start_date=created_start, end_date=created_end)
        updated = created
        if random.random() < update_prob:
            updated = self.fake.date_time_between(start_date=created, end_date="now")

        deleted = None
        if random.random() < delete_prob:
            deleted = self.fake.date_time_between(start_date=updated, end_date="now")

        return created, updated, deleted

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self._records)

    def save_csv(self, path: str) -> None:
        self.to_dataframe().to_csv(path, index=False)
        print(f"Saved {len(self._records)} records -> {path}")
