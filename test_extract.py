from extractors.concrete_extractors import (
    BranchExtractor,
    CustomerExtractor,
    AccountExtractor,
    LoanExtractor,
)

if __name__ == "__main__":
    for extractor_cls in [BranchExtractor, CustomerExtractor, AccountExtractor, LoanExtractor]:
        result = extractor_cls().extract()
        print(result)