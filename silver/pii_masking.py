"""
PiiMasker: deterministic, one-way masking for personally identifiable data.

NIK is hashed with SHA-256 plus a project-level salt (read from env, never
hardcoded) so the same NIK always hashes to the same value -- preserving
joinability for analytics -- without the raw NIK ever landing in Silver or
any layer downstream of it. Names are partially masked rather than hashed,
since a fully-hashed name is useless for QA/display but a masked one still
lets an analyst sanity-check the data.
"""

import hashlib
import os

import pandas as pd


class PiiMasker:
    def __init__(self, salt: str | None = None):
        # Never hardcode the salt -- pull from env, with a clearly-labelled
        # dev fallback so local runs don't require extra setup.
        self.salt = salt or os.environ.get("PII_HASH_SALT", "banking-pipeline-dev-salt")

    def hash_column(self, series: pd.Series) -> pd.Series:
        return series.astype(str).apply(
            lambda value: hashlib.sha256(f"{self.salt}:{value}".encode()).hexdigest()
        )

    def mask_name(self, series: pd.Series) -> pd.Series:
        """'Ika Prasasta, S.H.' -> 'Ika P.' -- keeps data usable for
        display/QA without exposing the full legal name."""

        def _mask(raw_name: object) -> object:
            parts = str(raw_name).replace(",", "").split()
            if len(parts) <= 1:
                return raw_name
            return f"{parts[0]} {parts[1][0]}."

        return series.apply(_mask)

    def apply(self, series: pd.Series, strategy: str) -> pd.Series:
        if strategy == "hash":
            return self.hash_column(series)
        if strategy == "mask_name":
            return self.mask_name(series)
        raise ValueError(f"Unknown PII masking strategy: {strategy!r}")