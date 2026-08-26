import random

from .base import BaseGenerator


class CustomerGenerator(BaseGenerator):
    SEGMENTS = ["reguler", "prioritas", "wealth"]
    SEGMENT_WEIGHTS = [0.75, 0.2, 0.05]

    def generate_one(self, entity_id: int, **kwargs) -> dict:
        created, updated, deleted = self._audit_timestamps(
            created_start="-3y", created_end="-6M", update_prob=0.15, delete_prob=0.03
        )
        return {
            "customer_id": entity_id,
            "name": self.fake.name(),
            "nik": self.fake.numerify(text="################"),
            "birth_date": self.fake.date_of_birth(minimum_age=18, maximum_age=65),
            "gender": random.choice(["L", "P"]),
            "city": self.fake.city(),
            "segment": random.choices(self.SEGMENTS, weights=self.SEGMENT_WEIGHTS)[0],
            "created_at": created,
            "updated_at": updated,
            "deleted_at": deleted,
            # SCD Type 2 tracking columns
            "effective_date": created.date(),
            "end_date": None,
            "is_current": deleted is None,
        }
