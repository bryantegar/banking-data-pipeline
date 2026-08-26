from .base_extractor import BaseExtractor


class BranchExtractor(BaseExtractor):
    table_name = "branches"
    watermark_column = "updated_at"


class CustomerExtractor(BaseExtractor):
    table_name = "customers"
    watermark_column = "updated_at"


class AccountExtractor(BaseExtractor):
    table_name = "accounts"
    watermark_column = "updated_at"


class LoanExtractor(BaseExtractor):
    table_name = "loans"
    watermark_column = "updated_at"
