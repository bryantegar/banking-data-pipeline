import random
from typing import Optional

from .base import BaseGenerator


class LoanGenerator(BaseGenerator):
    DPD_BUCKETS = [0, 30, 60, 90, 120]
    DPD_WEIGHTS = [0.85, 0.06, 0.04, 0.03, 0.02]

    # Only applies to loans currently at dpd == 0 -- a delinquent loan
    # (watchlist/NPL) is by definition still open, so it's never eligible
    # to already be "lunas" at generation time.
    CLOSED_PROBABILITY = 0.25

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

    @staticmethod
    def _status_from_dpd(dpd: int) -> str:
        # dpd (days past due) >= 90 is the standard OJK definition of NPL
        if dpd >= 90:
            return "NPL"
        if dpd >= 30:
            return "watchlist"
        return "current"

    def generate_one(self, entity_id: int, **kwargs) -> dict:
        created = self.fake.date_time_between(start_date="-2y", end_date="-1M")
        dpd = random.choices(self.DPD_BUCKETS, weights=self.DPD_WEIGHTS)[0]
        updated = created if dpd == 0 else self.fake.date_time_between(start_date=created, end_date="now")

        status = self._status_from_dpd(dpd)
        closed_date = None
        if dpd == 0 and random.random() < self.CLOSED_PROBABILITY:
            # Simulates a loan that was disbursed, paid on time, and fully
            # repaid before this snapshot was taken -- realistic for a
            # dataset spanning up to 2 years of disbursement history.
            status = "lunas"
            closed_date = self.fake.date_time_between(start_date=created, end_date="now")
            updated = closed_date

        return {
            "loan_id": entity_id,
            "customer_id": random.choice(self.customer_ids),
            "branch_id": random.choice(self.branch_ids),
            "principal_amount": round(random.uniform(5_000_000, 500_000_000), -5),
            "dpd": dpd,
            "status": status,
            "created_at": created,
            "updated_at": updated,
            "closed_date": closed_date,
        }