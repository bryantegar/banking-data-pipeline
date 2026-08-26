"""
Reusable data-quality rule engine for the Silver layer.

Rules are composed per table (see concrete_transformers.py) and produce a
DataQualityReport that is both written to disk -- as an audit trail, and as
a foundation for a future DataHub/governance integration -- and used to
filter invalid rows out of the Silver output, so bad data never silently
flows downstream into Gold.
"""

import json
from dataclasses import asdict, dataclass, field

import pandas as pd

from silver.reference_registry import ReferenceRegistry


@dataclass
class CheckResult:
    rule: str
    column: str
    passed: bool
    violation_count: int
    violation_ids: list = field(default_factory=list)


@dataclass
class DataQualityReport:
    table: str
    checks: list[CheckResult]

    @property
    def invalid_ids(self) -> set:
        """Union of every row flagged by any check -- one strike is enough
        to keep a row out of Silver."""
        ids: set = set()
        for check in self.checks:
            ids.update(check.violation_ids)
        return ids

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def save_json(self, path: str) -> None:
        payload = {
            "table": self.table,
            "passed": self.passed,
            "checks": [asdict(check) for check in self.checks],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)


class DataQualityChecker:
    """Accumulates rule checks against a DataFrame. Each rule records the
    primary/foreign key of every violating row (not just a pass/fail count),
    so BaseSilverTransformer can drop exactly those rows before writing
    Silver output."""

    def __init__(self, table_name: str, df: pd.DataFrame, registry: ReferenceRegistry):
        self.table_name = table_name
        self.df = df
        self.registry = registry
        self._results: list[CheckResult] = []

    def not_null(self, column: str, id_column: str) -> None:
        self._record("not_null", column, self.df[column].isna(), id_column)

    def no_duplicates(self, column: str, id_column: str) -> None:
        self._record("no_duplicates", column, self.df[column].duplicated(keep="first"), id_column)

    def value_range(self, column: str, id_column: str, min_value=None, max_value=None) -> None:
        mask = pd.Series(False, index=self.df.index)
        if min_value is not None:
            mask |= self.df[column] < min_value
        if max_value is not None:
            mask |= self.df[column] > max_value
        self._record("value_range", column, mask, id_column)

    def referential_integrity(self, column: str, parent_table: str, id_column: str) -> None:
        """Flags rows whose foreign key isn't a known primary key of
        `parent_table`. Checked against the accumulated ReferenceRegistry
        rather than today's Bronze slice -- see BaseSilverTransformer
        docstring for why that distinction matters."""
        known_parent_ids = self.registry.known_ids(parent_table)
        mask = ~self.df[column].isin(known_parent_ids)
        self._record(f"referential_integrity[{parent_table}]", column, mask, id_column)

    def _record(self, rule: str, column: str, violation_mask: pd.Series, id_column: str) -> None:
        violating_ids = self.df.loc[violation_mask, id_column].tolist()
        self._results.append(
            CheckResult(
                rule=rule,
                column=column,
                passed=len(violating_ids) == 0,
                violation_count=len(violating_ids),
                violation_ids=violating_ids,
            )
        )

    def run(self) -> DataQualityReport:
        return DataQualityReport(table=self.table_name, checks=self._results)