import random

from .base import BaseGenerator


class BranchGenerator(BaseGenerator):
    REGIONS = ["Jawa", "Sumatera", "Kalimantan", "Sulawesi", "Bali-Nusra"]

    def generate_one(self, entity_id: int, **kwargs) -> dict:
        created, updated, deleted = self._audit_timestamps(
            created_start="-5y", created_end="-3y", update_prob=0.05, delete_prob=0.01
        )
        city = self.fake.city()
        return {
            "branch_id": entity_id,
            "branch_code": f"BR{entity_id:04d}",
            "branch_name": f"Cabang {city}",
            "city": city,
            "region": random.choice(self.REGIONS),
            "opened_date": created.date(),
            "created_at": created,
            "updated_at": updated,
            "deleted_at": deleted,
        }
