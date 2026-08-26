"""
scenario_injector.py

Generates small, clearly-labeled synthetic transaction bursts that
exercise fraud-rule patterns needing transaction HISTORY (velocity,
impossible travel, fan-in, structuring) -- patterns that a stream of
independent, one-off transactions wouldn't naturally produce often
enough to demo live.

Every synthetic transaction is tagged is_synthetic_scenario=True and
scenario_type=<name> -- never silently mixed in as if it were organic
activity. Account IDs are drawn from the SAME real account pool the
main generator uses (not disconnected fake IDs), so a scenario still
reads as "a real customer in this system did something suspicious",
not an obviously synthetic outlier.

Network fields are set to "good" values here on purpose -- these
scenarios exist to exercise FRAUD rules, not the network-quality gate,
so a bad network shouldn't be an incidental extra reason a scenario
transaction gets flagged.
"""

import random
from datetime import datetime, timedelta
from typing import List

_GOOD_NETWORK_DEFAULTS = {"network_slice_id": "Slice1", "latency_ms": 5.0, "slice_bandwidth_mbps": 200.0}


def _base_txn(sender: int, receiver: int, amount: float, status: str, ts: datetime, geo: str, scenario: str) -> dict:
    txn = {
        "transaction_id": f"SYN{random.randint(10_000_000, 99_999_999)}",
        "sender_account_id": sender,
        "receiver_account_id": receiver,
        "amount": round(amount, 2),
        "transaction_type": random.choice(["Transfer", "Withdrawal", "Deposit"]),
        "status": status,
        "timestamp": ts,
        "geolocation": geo,
        "device_used": random.choice(["Mobile", "Desktop"]),
        "is_synthetic_scenario": True,
        "scenario_type": scenario,
    }
    txn.update(_GOOD_NETWORK_DEFAULTS)
    return txn


def _other_accounts(account_ids: List[int], exclude: int, n: int) -> List[int]:
    pool = [a for a in account_ids if a != exclude]
    return random.sample(pool, min(n, len(pool)))


def velocity_burst(account_ids: List[int], start: datetime, n: int = 5) -> List[dict]:
    """One sender firing off several transactions within seconds --
    account-takeover / bot-driven draining pattern."""
    sender = random.choice(account_ids)
    receivers = _other_accounts(account_ids, sender, n)
    geo = "-6.2088, 106.8456"  # Jakarta
    return [
        _base_txn(sender, receiver, random.uniform(100, 400), "Success",
                  start + timedelta(seconds=15 * i), geo, "velocity_burst")
        for i, receiver in enumerate(receivers)
    ]


def impossible_travel(account_ids: List[int], start: datetime) -> List[dict]:
    """Same sender, two transactions minutes apart from geographically
    distant locations -- classic account-takeover signal."""
    sender = random.choice(account_ids)
    r1, r2 = _other_accounts(account_ids, sender, 2)
    return [
        _base_txn(sender, r1, random.uniform(200, 800), "Success",
                  start, "-6.2088, 106.8456", "impossible_travel"),  # Jakarta
        _base_txn(sender, r2, random.uniform(200, 800), "Success",
                  start + timedelta(minutes=3), "51.5072, -0.1276", "impossible_travel"),  # London, 3 min later
    ]


def fan_in_collector(account_ids: List[int], start: datetime, n_senders: int = 6) -> List[dict]:
    """Many distinct senders paying into one receiver in a short window --
    the classic signature of a judol/pinjol proceeds "collector" account."""
    receiver = random.choice(account_ids)
    senders = _other_accounts(account_ids, receiver, n_senders)
    return [
        _base_txn(sender, receiver, random.uniform(50, 300), "Success",
                  start + timedelta(seconds=20 * i), "-6.2088, 106.8456", "fan_in_collector")
        for i, sender in enumerate(senders)
    ]


def structuring(account_ids: List[int], start: datetime, n: int = 4, just_under: float = 950.0) -> List[dict]:
    """Several transactions from the same sender, each just under a
    round/reporting-style threshold -- a common way to dodge reporting
    thresholds (structuring / "smurfing")."""
    sender = random.choice(account_ids)
    receivers = _other_accounts(account_ids, sender, n)
    return [
        _base_txn(sender, receiver, just_under - random.uniform(0, 50), "Success",
                  start + timedelta(minutes=2 * i), "-6.9175, 107.6191", "structuring")
        for i, receiver in enumerate(receivers)
    ]


SCENARIOS = [velocity_burst, impossible_travel, fan_in_collector, structuring]


def random_scenario(account_ids: List[int], start: datetime) -> List[dict]:
    """Picks one scenario type at random and generates it."""
    return random.choice(SCENARIOS)(account_ids, start)
