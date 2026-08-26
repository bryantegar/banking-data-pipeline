"""
app.py

Streamlit dashboard for the Gold layer: two tabs --
- Risk / NPL: portfolio health, NPL ratio, loan status by branch
- Customer Segmentation: account distribution by segment and city

Reads directly from the Gold CSVs produced by gold_batch_transformation_dag.
Run with: streamlit run dashboard/app.py
"""

import pandas as pd
import streamlit as st

GOLD_DIR = "output/gold"

st.set_page_config(page_title="Core Banking Data Platform", layout="wide")


@st.cache_data(ttl=60)
def load_gold_tables():
    dim_branch = pd.read_csv(f"{GOLD_DIR}/dim_branch.csv")
    dim_customer = pd.read_csv(f"{GOLD_DIR}/dim_customer.csv")
    fact_loan = pd.read_csv(f"{GOLD_DIR}/fact_loan.csv")
    fact_account = pd.read_csv(f"{GOLD_DIR}/fact_account.csv")
    return dim_branch, dim_customer, fact_loan, fact_account


dim_branch, dim_customer, fact_loan, fact_account = load_gold_tables()

st.title("Core Banking Data Platform")
st.caption("Gold layer dashboard \u2014 risk monitoring & customer segmentation")

tab_risk, tab_segmentation = st.tabs(["\U0001F4CA Risk / NPL", "\U0001F465 Customer Segmentation"])

# ---------------------------------------------------------------------------
# TAB 1: Risk / NPL
# ---------------------------------------------------------------------------
with tab_risk:
    total_loans = len(fact_loan)
    npl_count = int(fact_loan["is_npl"].sum())
    npl_ratio = round(npl_count / total_loans * 100, 2) if total_loans else 0
    total_outstanding = fact_loan["principal_amount"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Loans", f"{total_loans:,}")
    col2.metric("NPL Ratio", f"{npl_ratio}%")
    col3.metric("NPL Count", f"{npl_count:,}")
    col4.metric("Total Outstanding", f"Rp {total_outstanding:,.0f}")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Loan Status Distribution")
        status_counts = fact_loan["status"].value_counts()
        st.bar_chart(status_counts)

    with col_right:
        st.subheader("NPL Ratio by Branch")
        loan_branch = fact_loan.merge(dim_branch, on="branch_id", how="left")
        npl_by_branch = (
            loan_branch.groupby("branch_name")["is_npl"]
            .mean()
            .mul(100)
            .round(2)
            .sort_values(ascending=False)
            .head(10)
        )
        st.bar_chart(npl_by_branch)

    st.subheader("Loans by Branch (table)")
    branch_summary = (
        loan_branch.groupby("branch_name")
        .agg(total_loans=("loan_id", "count"), npl_count=("is_npl", "sum"))
        .assign(npl_ratio_pct=lambda d: round(d["npl_count"] / d["total_loans"] * 100, 2))
        .sort_values("npl_ratio_pct", ascending=False)
    )
    st.dataframe(branch_summary, use_container_width=True)

# ---------------------------------------------------------------------------
# TAB 2: Customer Segmentation
# ---------------------------------------------------------------------------
with tab_segmentation:
    total_customers = len(dim_customer)
    total_accounts = len(fact_account)

    col1, col2, col3 = st.columns(3)
    col1.metric("Active Customers", f"{total_customers:,}")
    col2.metric("Active Accounts", f"{total_accounts:,}")
    col3.metric(
        "Accounts / Customer",
        round(total_accounts / total_customers, 2) if total_customers else 0,
    )

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Customers by Segment")
        st.bar_chart(dim_customer["segment"].value_counts())

    with col_right:
        st.subheader("Accounts by Type")
        st.bar_chart(fact_account["account_type"].value_counts())

    st.subheader("Top 10 Cities by Active Customers")
    st.bar_chart(dim_customer["city"].value_counts().head(10))