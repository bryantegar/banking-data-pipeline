"""
fraud_rules.py

Rule-based fraud scoring engine. Each rule is intentionally simple and
explainable -- the point of a first-pass fraud system is transparency
(you can tell an analyst exactly why a transaction got flagged), not
black-box accuracy. Keeps small in-memory state per sender/receiver to
catch patterns that need transaction HISTORY, not just a single row.

NOTE: the source dataset's own "Fraud Flag" column doesn't correlate with
any observable feature here (amount, status, device are all near 50/50
regardless of flag) -- it's decorative, not derived from realistic fraud
behavior. This engine does NOT try to reproduce that column; it implements
independent, explainable heuristics instead, covering patterns relevant to
Indonesian banking risk typologies (illegal online gambling/lending
proceeds, structuring, impossible-travel account takeover):

- large_amount            : above a statistical threshold
- failed_high_value       : failed attempt on an above-median amount
- velocity_burst          : many transactions from one sender in a short
                             window
- impossible_travel       : same sender, location changed too fast to be
                             physically plausible
- fan_in_collector_pattern: one receiver getting money from many distinct
                             senders in a short window -- the classic
                             signature of a judol/pinjol collector
                             ("mule") account
- possible_structuring    : several sub-threshold transactions from the
                             same sender in a short window, a common way
                             to dodge reporting thresholds (money
                             laundering)
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional


@dataclass
class FraudScoreResult:
    risk_score: int
    reasons: list = field(default_factory=list)


class FraudRuleEngine:
    def __init__(
        self,
        large_amount_threshold: float,
        median_amount: float,
        velocity_window_minutes: int = 5,
        velocity_threshold: int = 3,
        fan_in_window_minutes: int = 5,
        fan_in_threshold: int = 4,
        impossible_travel_minutes: int = 10,
        structuring_window_minutes: int = 10,
        structuring_count: int = 3,
    ):
        self.large_amount_threshold = large_amount_threshold
        self.median_amount = median_amount
        self.velocity_window = timedelta(minutes=velocity_window_minutes)
        self.velocity_threshold = velocity_threshold
        self.fan_in_window = timedelta(minutes=fan_in_window_minutes)
        self.fan_in_threshold = fan_in_threshold
        self.impossible_travel_window = timedelta(minutes=impossible_travel_minutes)
        self.structuring_window = timedelta(minutes=structuring_window_minutes)
        self.structuring_count = structuring_count

        self._sender_history = defaultdict(deque)
        self._sender_last_location = {}
        self._receiver_senders = defaultdict(deque)
        self._sender_amounts = defaultdict(deque)

    def score(self, txn: dict) -> FraudScoreResult:
        sender = txn["sender_account_id"]
        receiver = txn["receiver_account_id"]
        amount = txn["amount"]
        status = txn["status"]
        ts = txn["timestamp"]
        location = txn.get("geolocation")

        reasons = []

        if amount > self.large_amount_threshold:
            reasons.append("large_amount")

        if status == "Failed" and amount > self.median_amount:
            reasons.append("failed_high_value")

        hist = self._sender_history[sender]
        while hist and (ts - hist[0]) > self.velocity_window:
            hist.popleft()
        if len(hist) >= self.velocity_threshold:
            reasons.append("velocity_burst")
        hist.append(ts)

        last = self._sender_last_location.get(sender)
        if last and location and last["location"] != location:
            if (ts - last["timestamp"]) < self.impossible_travel_window:
                reasons.append("impossible_travel")
        if location:
            self._sender_last_location[sender] = {"location": location, "timestamp": ts}

        rhist = self._receiver_senders[receiver]
        while rhist and (ts - rhist[0][0]) > self.fan_in_window:
            rhist.popleft()
        distinct_recent_senders = {s for _, s in rhist}
        if len(distinct_recent_senders) >= self.fan_in_threshold:
            reasons.append("fan_in_collector_pattern")
        rhist.append((ts, sender))

        ahist = self._sender_amounts[sender]
        while ahist and (ts - ahist[0][0]) > self.structuring_window:
            ahist.popleft()
        ahist.append((ts, amount))
        below_threshold_recent = [a for _, a in ahist if a < self.large_amount_threshold]
        if len(below_threshold_recent) >= self.structuring_count:
            reasons.append("possible_structuring")

        return FraudScoreResult(risk_score=len(reasons), reasons=reasons)
