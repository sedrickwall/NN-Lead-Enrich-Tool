# Create the Lead Cleaner app
import streamlit as st
import pandas as pd
import re
from io import BytesIO

from loaders.google_sheets import load_google_csv, clear_cache
from loaders.schema import validate_columns
from loaders.normalize import (
    extract_email_domain,
    normalize_domain,
    normalize_website_to_domain,
)


# app.py — Lead Enricher (MVP, separate tool)
# Reads library tables from Google Sheets (published CSV URLs) defined in config/data_sources.yaml
# Outputs: enriched_leads.csv, ambiguous_review.csv, dedupe_suggestions.csv

import re
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import yaml

# ---------------------------
# Page config
# ---------------------------
st.set_page_config(page_title="Lead Enricher Tool", page_icon="🔎", layout="wide")
st.title("🔎 Lead Enricher")
st.markdown("**Enrich uploaded leads by matching email domains to Salesforce Accounts.**")

# ---------------------------
# Helpers: config + loading
# ---------------------------
def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

@st.cache_data(ttl=600)
def load_google_csv(url: str) -> pd.DataFrame:
    return pd.read_csv(url)

def validate_columns(df: pd.DataFrame, required_cols: List[str], name: str) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"❌ `{name}` is missing required columns: {missing}")
        st.stop()

def safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x)

# ---------------------------
# Helpers: normalization
# ---------------------------
def extract_email_domain(email: str) -> Optional[str]:
    if pd.isna(email):
        return None
    s = str(email).strip().lower()
    if "@" not in s:
        return None
    # take right side after last @ just in case
    return s.split("@")[-1].strip()

def normalize_domain(domain: Optional[str], collapse_subdomains: bool = True) -> Optional[str]:
    if not domain:
        return None
    d = str(domain).strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/")[0].strip()
    if not d:
        return None

    # NOTE: naive subdomain collapse. If you need accurate .co.uk handling later,
    # we can switch to tldextract with one dependency.
    if collapse_subdomains and d.count(".") >= 2:
        parts = d.split(".")
        d = ".".join(parts[-2:])

    return d

def normalize_website_to_domain(website: Optional[str], collapse_subdomains: bool = True) -> Optional[str]:
    if pd.isna(website) or website is None:
        return None
    return normalize_domain(str(website), collapse_subdomains=collapse_subdomains)

def build_alias_map(alias_df: pd.DataFrame, collapse_subdomains: bool) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for _, r in alias_df.iterrows():
        inp = normalize_domain(r.get("InputDomain"), collapse_subdomains=collapse_subdomains)
        can = normalize_domain(r.get("CanonicalDomain"), collapse_subdomains=collapse_subdomains)
        if inp and can:
            m[inp] = can
    return m

# ---------------------------
# Helpers: dedupe
# ---------------------------
def dedupe_by_email(df: pd.DataFrame, email_col: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Flags duplicates within the uploaded file by exact email match (normalized).
    Returns:
      - updated df with IsPotentialDuplicate, DuplicateGroupId, DuplicateReason
      - a dedupe suggestions table (may be empty)
    """
    work = df.copy()
    work["_email_norm"] = work[email_col].astype(str).str.strip().str.lower()

    # Consider only non-empty, non-"nan"
    valid = work["_email_norm"].ne("") & work["_email_norm"].ne("nan")
    dup_mask = valid & work["_email_norm"].duplicated(keep=False)

    dup_df = work[dup_mask].copy()

    if dup_df.empty:
        work["IsPotentialDuplicate"] = False
        work["DuplicateGroupId"] = ""
        work["DuplicateReason"] = ""
        return work.drop(columns=["_email_norm"]), pd.DataFrame()

    # Create group IDs per duplicated email
    dup_df["DuplicateGroupId"] = (
        dup_df.groupby("_email_norm").ngroup().apply(lambda x: f"DUP-{x+1:03d}")
    )
    dup_df["DuplicateReason"] = "EmailExact"

    # Merge flags back
    flags = dup_df[[email_col, "DuplicateGroupId", "DuplicateReason"]].drop_duplicates()
    work = work.merge(flags, on=email_col, how="left")
    work["IsPotentialDuplicate"] = work["DuplicateGroupId"].notna()
    work["DuplicateGroupId"] = work["DuplicateGroupId"].fillna("")
    work["DuplicateReason"] = work["DuplicateReason"].fillna("")

    return work.drop(columns=["_email_norm"]), dup_df.drop(columns=["_email_norm"])

def df_to_csv_bytes(df: pd.DataFrame) -> BytesIO:
    buf = BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    buf.seek(0)
    return buf

# ---------------------------
# Load library via YAML
# ---------------------------
config = load_yaml("config/data_sources.yaml")
sources = config.get("sources", {})

acct_src = sources.get("sf_accounts")
alias_src = sources.get("domain_alias")
contacts_src = sources.get("sf_contacts")  # optional, not used in MVP

if not acct_src or not alias_src:
    st.error("❌ config/data_sources.yaml must include sources: `sf_accounts` and `domain_alias`.")
    st.stop()

with st.sidebar:
    st.header("⚙️ Settings")
    collapse_subdomains = st.toggle("Collapse subdomains (mail.acme.com → acme.com)", value=True)
    treat_personal_as_unmatched = st.toggle("Do not match personal email domains", value=True)
    if st.button("🔄 Refresh library data"):
        st.cache_data.clear()
        st.toast("Library cache cleared")

# Built-in personal domains (keeps YAML simple)
DEFAULT_PERSONAL_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com",
    "outlook.com", "hotmail.com", "live.com", "msn.com",
    "icloud.com", "me.com", "mac.com",
    "aol.com",
    "proton.me", "protonmail.com",
    "gmx.com", "gmx.net",
}

# Load required library tables
try:
    accounts_df = load_google_csv(acct_src["url"])
    alias_df = load_google_csv(alias_src["url"])
except Exception as e:
    st.error(f"❌ Failed to load Google Sheets CSV library data. Error: {e}")
    st.stop()

validate_columns(accounts_df, acct_src.get("required_columns", []), "sf_accounts")
validate_columns(alias_df, alias_src.get("required_columns", []), "domain_alias")

# Optional contacts library load (not required for MVP)
contacts_df = None
if contacts_src:
    try:
        contacts_df = load_google_csv(contacts_src["url"])
        validate_columns(contacts_df, contacts_src.get("required_columns", []), "sf_contacts")
    except Exception:
        contacts_df = None

# Precompute normalized domains for accounts
accounts_df = accounts_df.copy()
accounts_df["WebsiteDomainNormalized"] = accounts_df["Website"].apply(
    lambda x: normalize_website_to_domain(x, collapse_subdomains=collapse_subdomains)
)
accounts_df = accounts_df.dropna(subset=["WebsiteDomainNormalized"])

# Build domain → accounts index
domain_to_accounts: Dict[str, List[pd.Series]] = {}
for _, r in accounts_df.iterrows():
    d = r["WebsiteDomainNormalized"]
    domain_to_accounts.setdefault(d, []).append(r)

# Alias map (InputDomain → CanonicalDomain)
alias_map = build_alias_map(alias_df, collapse_subdomains=collapse_subdomains)

with st.sidebar:
    st.subheader("✅ Library Status")
    st.write(f"Accounts (with websites): **{len(accounts_df):,}**")
    st.write(f"Alias rows: **{len(alias_df):,}**")
    st.write(f"Personal domains (built-in): **{len(DEFAULT_PERSONAL_DOMAINS):,}**")
    if contacts_df is not None:
        st.write(f"SF Contacts loaded (optional): **{len(contacts_df):,}**")
    else:
        st.write("SF Contacts loaded (optional): **No**")

# ---------------------------
# Upload lead list
# ---------------------------
st.header("📤 Step 1: Upload Lead List")
uploaded_file = st.file_uploader("Upload CSV or Excel file", type=["csv", "xlsx", "xls"])

if not uploaded_file:
    st.info("👆 Upload a lead list to enrich.")
    st.stop()

# Read uploaded
try:
    if uploaded_file.name.lower().endswith(".csv"):
        leads = pd.read_csv(uploaded_file)
    else:
        leads = pd.read_excel(uploaded_file)
except Exception as e:
    st.error(f"❌ Could not read file: {e}")
    st.stop()

st.success(f"✅ File uploaded: {uploaded_file.name} ({len(leads)} rows)")
with st.expander("📋 Preview Uploaded Data (First 10 rows)"):
    st.dataframe(leads.head(10))

# ---------------------------
# Column mapping
# ---------------------------
st.header("🔗 Step 2: Map Columns")
c1, c2, c3 = st.columns(3)

with c1:
    email_col = st.selectbox("Email Column (required)", [""] + list(leads.columns))
with c2:
    company_col = st.selectbox("Company Column (optional)", [""] + list(leads.columns))
with c3:
    st.caption("MVP matching: Email domain → Account website domain (with optional alias mapping).")

# ---------------------------
# Run enrichment
# ---------------------------
if st.button("🚀 Enrich Leads", type="primary"):
    if not email_col:
        st.error("⚠️ Please select an Email column.")
        st.stop()

    with st.spinner("Enriching leads..."):
        work = leads.copy()

        # Extract + normalize email domains
        work["EmailDomainRaw"] = work[email_col].apply(extract_email_domain)
        work["EmailDomainNormalized"] = work["EmailDomainRaw"].apply(
            lambda d: normalize_domain(d, collapse_subdomains=collapse_subdomains)
        )

        # Apply alias canonicalization
        def canonicalize(d: Optional[str]) -> Optional[str]:
            if not d:
                return None
            return alias_map.get(d, d)

        work["DomainCanonical"] = work["EmailDomainNormalized"].apply(canonicalize)

        suggested_ids: List[str] = []
        suggested_names: List[str] = []
        reasons: List[str] = []
        confidences: List[str] = []
        candidate_counts: List[int] = []
        candidates_list: List[str] = []

        for d in work["DomainCanonical"].tolist():
            if not d:
                suggested_ids.append("")
                suggested_names.append("")
                reasons.append("NoEmailDomain")
                confidences.append("Low")
                candidate_counts.append(0)
                candidates_list.append("")
                continue

            if treat_personal_as_unmatched and d in DEFAULT_PERSONAL_DOMAINS:
                suggested_ids.append("")
                suggested_names.append("")
                reasons.append("PersonalEmail")
                confidences.append("Low")
                candidate_counts.append(0)
                candidates_list.append("")
                continue

            candidates = domain_to_accounts.get(d, [])
            candidate_counts.append(len(candidates))

            if len(candidates) == 1:
                a = candidates[0]
                suggested_ids.append(safe_str(a.get("AccountId")))
                suggested_names.append(safe_str(a.get("AccountName")))
                reasons.append("DomainMatch")
                confidences.append("High")
                candidates_list.append(f'{safe_str(a.get("AccountId"))}|{safe_str(a.get("AccountName"))}|{d}')
            elif len(candidates) > 1:
                suggested_ids.append("")
                suggested_names.append("")
                reasons.append("Ambiguous")
                confidences.append("Medium")
                packed = []
                for a in candidates[:10]:
                    packed.append(f'{safe_str(a.get("AccountId"))}|{safe_str(a.get("AccountName"))}|{d}')
                candidates_list.append(" || ".join(packed))
            else:
                suggested_ids.append("")
                suggested_names.append("")
                reasons.append("NoMatch")
                confidences.append("Low")
                candidates_list.append("")

        work["SuggestedAccountId"] = suggested_ids
        work["SuggestedAccountName"] = suggested_names
        work["MatchReason"] = reasons
        work["MatchConfidence"] = confidences
        work["MatchCandidatesCount"] = candidate_counts
        work["MatchCandidates"] = candidates_list

        # Split for review
        matched_df = work[work["MatchConfidence"].eq("High")].copy()
        ambiguous_df = work[work["MatchReason"].eq("Ambiguous")].copy()
        unmatched_df = work[work["MatchReason"].isin(["NoMatch", "NoEmailDomain", "PersonalEmail"])].copy()

        # Dedupe suggestions
        enriched_df, dedupe_df = dedupe_by_email(work, email_col=email_col)

    # ---------------------------
    # Results
    # ---------------------------
    st.subheader("📊 Results")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Matched (High)", len(matched_df))
    m2.metric("Ambiguous", len(ambiguous_df))
    m3.metric("Unmatched", len(unmatched_df))
    m4.metric("Potential duplicates", int(enriched_df["IsPotentialDuplicate"].sum()))

    with st.expander("✅ Matched (sample)"):
        st.dataframe(matched_df.head(50))

    with st.expander("⚠️ Ambiguous (sample)"):
        st.dataframe(ambiguous_df.head(50))

    with st.expander("❌ Unmatched (sample)"):
        st.dataframe(unmatched_df.head(50))

    # ---------------------------
    # Downloads
    # ---------------------------
    st.subheader("📥 Downloads")

    st.download_button(
        "⬇️ Download Enriched Leads (CSV)",
        data=df_to_csv_bytes(enriched_df),
        file_name="enriched_leads.csv",
        mime="text/csv",
        type="primary"
    )

    st.download_button(
        "⬇️ Download Ambiguous Review (CSV)",
        data=df_to_csv_bytes(ambiguous_df),
        file_name="ambiguous_review.csv",
        mime="text/csv"
    )

    # If no dedupes, still provide a file (empty)
    st.download_button(
        "⬇️ Download Dedupe Suggestions (CSV)",
        data=df_to_csv_bytes(dedupe_df if not dedupe_df.empty else pd.DataFrame()),
        file_name="dedupe_suggestions.csv",
        mime="text/csv"
    )

    st.success("🎉 Enrichment complete.")
    st.markdown(
        f"""
**Summary**
- Uploaded leads: **{len(leads)}**
- Matched (High): **{len(matched_df)}**
- Ambiguous: **{len(ambiguous_df)}**
- Unmatched: **{len(unmatched_df)}**
- Potential duplicates: **{int(enriched_df["IsPotentialDuplicate"].sum())}**
"""
    )
