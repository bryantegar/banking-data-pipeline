"""
transaction_generator.py

Generates a single synthetic banking transaction -- schema modeled on the
reference dataset the user's mentor recommended (sender/receiver account,
amount, type, status, geolocation, device, network slice/latency/
bandwidth), minus PIN Code, which is never generated or stored anywhere
in this pipeline, synthetic or not.

Unlike CustomerGenerator/AccountGenerator/LoanGenerator (which backdate
created_at to build up historical seed data), transactions here are
always timestamped "now" -- this generator feeds a live, unbounded Kafka
stream, not a one-time historical batch. It's the transaction-side
counterpart to simulate_activity.py: that module simulates account
opening/closing (comparatively rare, low-frequency events extracted in
hourly batch), this simulates customers actually USING those accounts
(high-frequency, extracted via streaming instead).

sender_account_id/receiver_account_id are drawn from a REAL pool of
account IDs (passed in -- typically the current `accounts` table from
source_db) so every streamed transaction references an actual customer
already in the system, not a disconnected fake ID.
"""

import random
from typing import List, Optional

from .base import BaseGenerator


class TransactionGenerator(BaseGenerator):
    TRANSACTION_TYPES = ["Transfer", "Withdrawal", "Deposit"]
    TYPE_WEIGHTS = [0.6, 0.2, 0.2]

    STATUSES = ["Success", "Failed"]
    STATUS_WEIGHTS = [0.9, 0.1]

    DEVICES = ["Mobile", "Desktop", "ATM"]
    DEVICE_WEIGHTS = [0.65, 0.2, 0.15]

    NETWORK_SLICES = ["Slice1", "Slice2", "Slice3"]

    # Rough lat/long centers for a few major Indonesian cities, so
    # geolocation looks like real customer activity clustered around
    # plausible places rather than uniform-random points on the globe.
    CITY_CENTERS = [
        (-6.2088, 106.8456),  # Jakarta
        (-7.2575, 112.7521),  # Surabaya
        (-6.9175, 107.6191),  # Bandung
        (3.5952, 98.6722),  # Medan
        (-5.1477, 119.4327),  # Makassar
    ]

    def __init__(self, account_ids: List[int], locale: str = "id_ID", seed: Optional[int] = None):
        super().__init__(locale=locale, seed=seed)
        if len(account_ids) < 2:
            raise ValueError("Need at least 2 account_ids to generate sender/receiver pairs")
        self.account_ids = account_ids

    def _random_geolocation(self) -> str:
        lat_c, lon_c = random.choice(self.CITY_CENTERS)
        lat = lat_c + random.uniform(-0.05, 0.05)
        lon = lon_c + random.uniform(-0.05, 0.05)
        return f"{lat:.4f}, {lon:.4f}"

    def generate_one(self, entity_id: int, **kwargs) -> dict:
        sender, receiver = random.sample(self.account_ids, 2)
        status = random.choices(self.STATUSES, weights=self.STATUS_WEIGHTS)[0]

        return {
            "transaction_id": f"TXN{random.randint(10_000_000, 99_999_999)}",
            "sender_account_id": sender,
            "receiver_account_id": receiver,
            # Lognormal -> many small transactions, few large ones, same
            # reasoning as the other generators' amount distributions.
            "amount": round(random.lognormvariate(6.5, 1.0), 2),
            "transaction_type": random.choices(self.TRANSACTION_TYPES, weights=self.TYPE_WEIGHTS)[0],
            "status": status,
            "geolocation": self._random_geolocation(),
            "device_used": random.choices(self.DEVICES, weights=self.DEVICE_WEIGHTS)[0],
            "network_slice_id": random.choice(self.NETWORK_SLICES),
            "latency_ms": max(1.0, round(random.gauss(12, 4), 1)),
            "slice_bandwidth_mbps": max(5.0, round(random.gauss(120, 30), 1)),
        }
