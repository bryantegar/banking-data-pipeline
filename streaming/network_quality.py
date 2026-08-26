"""
network_quality.py

Independent pre-transaction network-health gate, separate from fraud
scoring (fraud_rules.py) -- these are different concerns: fraud asks "is
this transaction suspicious", network quality asks "is the connection
reliable enough to even attempt the transaction". A bad network is
grounds to flag/retry regardless of whether the transaction itself looks
fine otherwise.

NOTE on the source dataset, same caveat fraud_rules.py already documents
for Fraud Flag: Transaction Status ("Failed") does NOT meaningfully
correlate with latency here -- failure rate stays ~47-54% across every
latency bucket from 5ms to 20ms, essentially flat. This module does not
claim latency predicts failure in this specific dataset; it implements an
independent, explainable threshold rule using realistic network-
engineering judgment. Thresholds are set from this dataset's own
percentiles (not picked arbitrarily):

    latency_ms      <= 16 (P75)   -> good
                    16-19 (P75-P90) -> degraded
                    > 19 (P90+)     -> poor
    bandwidth_mbps  >= 98 (P25)   -> adequate
                    < 98 (below P25) -> constrained

A transaction is only gated (check_passed=False) when latency is "poor"
AND bandwidth is constrained at the same time -- requiring both signals
to agree avoids over-blocking on one noisy metric, the same "multiple
signals must agree" composition style fraud_rules.py already uses.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class NetworkQualityResult:
    quality: str  # "good" | "degraded" | "poor" | "unknown"
    check_passed: bool
    reasons: list = field(default_factory=list)


class NetworkQualityGate:
    def __init__(
        self,
        latency_degraded_ms: float = 16.0,
        latency_poor_ms: float = 19.0,
        bandwidth_constrained_mbps: float = 98.0,
    ):
        self.latency_degraded_ms = latency_degraded_ms
        self.latency_poor_ms = latency_poor_ms
        self.bandwidth_constrained_mbps = bandwidth_constrained_mbps

    def evaluate(
        self, latency_ms: Optional[float], bandwidth_mbps: Optional[float]
    ) -> NetworkQualityResult:
        if latency_ms is None or bandwidth_mbps is None:
            return NetworkQualityResult(
                quality="unknown", check_passed=True, reasons=["missing_network_metrics"]
            )

        reasons = []
        if latency_ms > self.latency_poor_ms:
            quality = "poor"
            reasons.append(f"latency {latency_ms}ms exceeds poor threshold ({self.latency_poor_ms}ms)")
        elif latency_ms > self.latency_degraded_ms:
            quality = "degraded"
            reasons.append(
                f"latency {latency_ms}ms above degraded threshold ({self.latency_degraded_ms}ms)"
            )
        else:
            quality = "good"

        bandwidth_constrained = bandwidth_mbps < self.bandwidth_constrained_mbps
        if bandwidth_constrained:
            reasons.append(
                f"bandwidth {bandwidth_mbps}Mbps below constrained threshold "
                f"({self.bandwidth_constrained_mbps}Mbps)"
            )

        check_passed = not (quality == "poor" and bandwidth_constrained)

        return NetworkQualityResult(quality=quality, check_passed=check_passed, reasons=reasons)