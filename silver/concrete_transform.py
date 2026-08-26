from silver.base_transform import BaseSilverTransformer
from silver.data_quality import DataQualityChecker


class BranchTransformer(BaseSilverTransformer):
    table_name = "branches"
    primary_key = "branch_id"

    def build_dq_checks(self, checker: DataQualityChecker) -> None:
        checker.not_null("branch_name", id_column=self.primary_key)
        checker.no_duplicates("branch_code", id_column=self.primary_key)


class CustomerTransformer(BaseSilverTransformer):
    table_name = "customers"
    primary_key = "customer_id"
    pii_columns = {"nik": "hash", "name": "mask_name"}

    def build_dq_checks(self, checker: DataQualityChecker) -> None:
        checker.not_null("nik", id_column=self.primary_key)
        # NIK is a national ID -- it must be unique per person. Checked
        # before masking, since the raw value is still available here.
        checker.no_duplicates("nik", id_column=self.primary_key)


class AccountTransformer(BaseSilverTransformer):
    table_name = "accounts"
    primary_key = "account_id"

    def build_dq_checks(self, checker: DataQualityChecker) -> None:
        checker.not_null("customer_id", id_column=self.primary_key)
        checker.referential_integrity("customer_id", parent_table="customers", id_column=self.primary_key)
        checker.referential_integrity("branch_id", parent_table="branches", id_column=self.primary_key)


class LoanTransformer(BaseSilverTransformer):
    table_name = "loans"
    primary_key = "loan_id"

    def build_dq_checks(self, checker: DataQualityChecker) -> None:
        # dpd (days past due) can never be negative -- a negative value
        # means an upstream bug, not a legitimate business state.
        checker.value_range("dpd", id_column=self.primary_key, min_value=0)
        checker.referential_integrity("customer_id", parent_table="customers", id_column=self.primary_key)
        checker.referential_integrity("branch_id", parent_table="branches", id_column=self.primary_key)