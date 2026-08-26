import random
from typing import Optional

from .base import BaseGenerator


class AccountGenerator(BaseGenerator):
    ACCOUNT_TYPES = ["tabungan", "giro", "deposito"]

    def __init__(
        self,
        customer_ids: list[int],
        branch_ids: list[int],
        locale: str = "id_ID",
        seed: Optional[int] = None,
    ):
        super().__init__(locale=locale, seed=seed)
        self.customer_ids = customer_ids
        self.branch_ids = branch_ids

    def generate_one(self, entity_id: int, **kwargs) -> dict:
        created, updated, deleted = self._audit_timestamps(
            created_start="-3y", created_end="-1M", update_prob=0.1, delete_prob=0.05
        )
        status = "closed" if deleted else random.choices(["active", "dormant"], weights=[0.9, 0.1])[0]
        return {
            "account_id": entity_id,
            "customer_id": random.choice(self.customer_ids),
            "branch_id": random.choice(self.branch_ids),
            "account_type": random.choice(self.ACCOUNT_TYPES),
            "account_number": self.fake.numerify(text="##########"),
            "open_date": created.date(),
            "status": status,
            "created_at": created,
            "updated_at": updated,
            "deleted_at": deleted,
        }
