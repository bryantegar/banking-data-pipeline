"""
concrete_builders.py

Gold dimensional/fact table builders:
- dim_branch, dim_customer, dim_time      (dimensions)
- fact_loan                               (risk / NPL analysis)
- fact_account                            (customer segmentation)

All pd.to_datetime() calls use format="mixed": timestamps in this dataset
come from two different writers (Faker's date_time_between -> "YYYY-MM-DD
HH:MM:SS", and simulate_activity.py's datetime.utcnow().isoformat() ->
"YYYY-MM-DDTHH:MM:SS.ffffff") that can land in the same column across
different partitions. format="mixed" parses each value independently
instead of assuming one format for the whole column.
"""

import pandas as pd

from gold.base_builder import BaseGoldBuilder


class DimBranchBuilder(BaseGoldBuilder):
    output_table = "dim_branch"

    def build(self) -> pd.DataFrame:
        branches = self._load_silver_table("branches", primary_key="branch_id")
        if branches.empty:
            return branches
        return branches[
            ["branch_id", "branch_code", "branch_name", "city", "region", "opened_date"]
        ].copy()


class DimCustomerBuilder(BaseGoldBuilder):
    output_table = "dim_customer"

    def build(self) -> pd.DataFrame:
        customers = self._load_silver_table("customers", primary_key="customer_id")
        if customers.empty:
            return customers
        # is_current is stored as int64 (0/1) in Silver, not a Python bool --
        # compare against 1 directly rather than casting to str, which was
        # silently filtering out every row (str(1) == "1", never "True").
        if "is_current" in customers.columns:
            customers = customers[customers["is_current"] == 1]
        return customers[
            ["customer_id", "name", "nik", "birth_date", "gender", "city", "segment"]
        ].copy()


class DimTimeBuilder(BaseGoldBuilder):
    output_table = "dim_time"

    def __init__(self, start: str = "2023-01-01", end: str = "2027-12-31"):
        self.start = start
        self.end = end

    def build(self) -> pd.DataFrame:
        dates = pd.date_range(self.start, self.end, freq="D")
        return pd.DataFrame(
            {
                "date_id": dates.strftime("%Y%m%d").astype(int),
                "date": dates.date,
                "day": dates.day,
                "month": dates.month,
                "quarter": dates.quarter,
                "year": dates.year,
                "day_name": dates.day_name(),
            }
        )


class FactLoanBuilder(BaseGoldBuilder):
    output_table = "fact_loan"

    def build(self) -> pd.DataFrame:
        loans = self._load_silver_table("loans", primary_key="loan_id")
        if loans.empty:
            return loans

        loans = loans.copy()
        loans["disbursement_date_id"] = (
            pd.to_datetime(loans["created_at"], format="mixed").dt.strftime("%Y%m%d").astype(int)
        )
        loans["is_npl"] = loans["status"] == "NPL"
        loans["is_closed"] = loans["status"] == "lunas"

        # closed_date is only populated for "lunas" loans -- nullable
        # Int64 (not plain int) so open loans can hold <NA> instead of
        # forcing a fake 0 or crashing on cast.
        closed_date_id = pd.to_datetime(
            loans["closed_date"], format="mixed", errors="coerce"
        ).dt.strftime("%Y%m%d")
        loans["closed_date_id"] = closed_date_id.astype("Int64")

        return loans[
            [
                "loan_id",
                "customer_id",
                "branch_id",
                "disbursement_date_id",
                "closed_date_id",
                "principal_amount",
                "dpd",
                "status",
                "is_npl",
                "is_closed",
            ]
        ].copy()


class FactAccountBuilder(BaseGoldBuilder):
    output_table = "fact_account"

    def build(self) -> pd.DataFrame:
        accounts = self._load_silver_table("accounts", primary_key="account_id")
        customers = self._load_silver_table("customers", primary_key="customer_id")
        if accounts.empty:
            return accounts

        accounts = accounts[accounts["status"] != "closed"].copy()

        merged = accounts.merge(
            customers[["customer_id", "segment", "city"]].rename(columns={"city": "customer_city"}),
            on="customer_id",
            how="left",
        )
        merged["open_date_id"] = (
            pd.to_datetime(merged["open_date"], format="mixed").dt.strftime("%Y%m%d").astype(int)
        )

        return merged[
            [
                "account_id",
                "customer_id",
                "branch_id",
                "account_type",
                "status",
                "segment",
                "customer_city",
                "open_date_id",
            ]
        ].copy()


class CustomerMonthlyTrendBuilder(BaseGoldBuilder):
    """
    Reconstructs TRUE historical monthly customer counts using each
    customer's created_at (join date) and deleted_at (churn date) -- this
    is exact, not estimated, since every customer's active window is fully
    known. For each calendar month from the earliest signup to now:
      - active_customers: how many customers were active as of month-end
      - new_customers: how many joined that month
      - churned_customers: how many churned that month
      - active_customers_mom_change: month-over-month delta (the "kenaikan/
        penurunan debitur" the dashboard needs)
    """

    output_table = "customer_monthly_trend"

    def build(self) -> pd.DataFrame:
        customers = self._load_silver_table("customers", primary_key="customer_id")
        if customers.empty:
            return customers

        customers = customers.copy()
        customers["created_at"] = pd.to_datetime(customers["created_at"], format="mixed")
        customers["deleted_at"] = pd.to_datetime(customers["deleted_at"], format="mixed")

        start_month = customers["created_at"].min().to_period("M")
        end_month = pd.Timestamp.utcnow().to_period("M")
        months = pd.period_range(start_month, end_month, freq="M")

        rows = []
        for month in months:
            month_end = month.to_timestamp(how="end")
            joined_by = customers["created_at"] <= month_end
            still_active = customers["deleted_at"].isna() | (customers["deleted_at"] > month_end)
            active_count = int((joined_by & still_active).sum())
            new_this_month = int((customers["created_at"].dt.to_period("M") == month).sum())
            churned_this_month = int((customers["deleted_at"].dt.to_period("M") == month).sum())
            rows.append(
                {
                    "month": str(month),
                    "active_customers": active_count,
                    "new_customers": new_this_month,
                    "churned_customers": churned_this_month,
                }
            )

        df = pd.DataFrame(rows)
        df["active_customers_mom_change"] = df["active_customers"].diff()
        return df


class LoanCohortTrendBuilder(BaseGoldBuilder):
    """
    Groups the CURRENT loan book by disbursement (origination) month --
    i.e. "of the loans handed out in month X, how many are NPL right now".

    This is a cohort/vintage view, not a point-in-time portfolio history:
    dpd/status only ever reflects TODAY's value, so we can't know what the
    NPL ratio looked like in a past month (that needs periodic snapshots
    captured going forward -- see PortfolioSnapshotBuilder). What this DOES
    give honestly: which origination cohorts are riskiest right now, a
    real and commonly-used risk-management view in banking.
    """

    output_table = "loan_cohort_trend"

    def build(self) -> pd.DataFrame:
        loans = self._load_silver_table("loans", primary_key="loan_id")
        if loans.empty:
            return loans

        loans = loans.copy()
        loans["created_at"] = pd.to_datetime(loans["created_at"], format="mixed")
        loans["cohort_month"] = loans["created_at"].dt.to_period("M").astype(str)
        loans["is_npl"] = loans["status"] == "NPL"
        loans["npl_principal"] = loans["principal_amount"].where(loans["is_npl"], 0)

        summary = (
            loans.groupby("cohort_month")
            .agg(
                total_loans=("loan_id", "count"),
                total_principal=("principal_amount", "sum"),
                npl_count=("is_npl", "sum"),
                npl_amount=("npl_principal", "sum"),
            )
            .reset_index()
        )
        summary["npl_ratio_pct"] = round(summary["npl_count"] / summary["total_loans"] * 100, 2)
        return summary.sort_values("cohort_month")


class PortfolioSnapshotBuilder(BaseGoldBuilder):
    """
    Computes TODAY's portfolio totals as a single row. Unlike the other
    builders, this is NOT a full-overwrite snapshot -- the Gold DAG loads
    this row into warehouse.portfolio_snapshot_history via an upsert-by-date
    (see PostgresClient.upsert_snapshot), so each day this DAG runs adds
    (or replaces) one row. Enough days of this accumulating is what
    eventually gives a TRUE month-over-month NPL/tunggakan trend -- the
    thing loan_cohort_trend can't honestly provide (see its docstring).
    """

    output_table = "portfolio_snapshot_history"

    def build(self) -> pd.DataFrame:
        loans = self._load_silver_table("loans", primary_key="loan_id")
        customers = self._load_silver_table("customers", primary_key="customer_id")

        snapshot_date = pd.Timestamp.utcnow().normalize()

        total_loans = len(loans)
        npl_count = int((loans["status"] == "NPL").sum()) if not loans.empty else 0
        npl_amount = (
            float(loans.loc[loans["status"] == "NPL", "principal_amount"].sum())
            if not loans.empty
            else 0.0
        )
        total_outstanding = float(loans["principal_amount"].sum()) if not loans.empty else 0.0
        npl_ratio_pct = round(npl_count / total_loans * 100, 2) if total_loans else 0.0

        active_customers = (
            int((customers["is_current"] == 1).sum()) if not customers.empty else 0
        )

        return pd.DataFrame(
            [
                {
                    "snapshot_date": snapshot_date,
                    "total_loans": total_loans,
                    "npl_count": npl_count,
                    "npl_amount": npl_amount,
                    "npl_ratio_pct": npl_ratio_pct,
                    "total_outstanding": total_outstanding,
                    "active_customers": active_customers,
                }
            ]
        )