# ============================================================
# Active Loans Report Builder — ONE FILE (Streamlit)
#
# What this file does
# - Prompts user to log in to Salesforce (OAuth Authorization Code + PKCE)
# - Pulls Salesforce *Reports* via REST (/analytics/reports/<id>) using simple-salesforce.sf.restful
# - Parses uploaded servicer files (Statebridge / Berkadia Data Tape / FCI / Midland / CHL)
# - Builds Bridge Asset / Bridge Loan / Term Loan / Term Asset dataframes
# - Writes into the user-provided template workbook while PRESERVING formulas
# - Dynamically labels the UPB column header as "M/D UPB" using the detected report run date
#
# Notes
# - The UPB header date is treated as the REPORT RUN DATE (not necessarily the same as the data cutoff).
#   By default we choose the latest run-date we can detect from the uploaded servicer files.
# - Permission/FLS/report-access errors return empty report data instead of crashing the app.
# - Describe / PKCE state is stored PER USER SESSION (no cross-user token leakage).
# ============================================================

import base64
import hashlib
import re
import secrets
import time
import urllib.parse
from datetime import date
from io import BytesIO
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
from openpyxl import load_workbook
from simple_salesforce import Salesforce


# =============================================================================
# SALESFORCE REPORTS (IDs you gave)
# =============================================================================
REPORTS = {
    "bridge_maturity": ("Bridge Maturity Report v3", "00O5b000005s0aFEAQ"),
    "do_not_lend": ("Do Not Lend", "00OPK000005tu3V2AQ"),
    "valuation": ("Valuation v4 Report", "00OPK000003PXS52AO"),
    "am_assignments": ("AM Assignments Report", "00OPK00000257Kf2AI"),
    "active_rm": ("Active RM Report", "00OPK000005QLAn2AO"),
    "term_export": ("Term Data Export", "00OPK000004p7Uz2AI"),
    "sold_term": ("Sold Term Loans", "00OPK0000030QFJ2A2"),
    "term_asset": ("Term Asset Level - By Deal", "00OPK00000DRwy52AD"),
}


# =============================================================================
# PROVIDED MAPPINGS (Label-based; SF report outputs are label columns)
# =============================================================================
BRIDGE_ASSET_FROM_BRIDGE_MATURITY = {
    "Loan Buyer": "Sold To",
    "Financing": "Warehouse Line",
    "Deal Number": "Deal Loan Number",
    "Servicer ID": "Servicer Loan Number",
    "SF Yardi ID": "Yardi ID",
    "Asset ID": "Asset ID",
    "Deal Name": "Deal Name",
    "Borrower Entity": "Borrower Entity: Business Entity Name",
    "Account Name": "Account Name: Account Name",
    "Primary Contact": "Primary Contact: Full Name",
    "Address": "Address",
    "City": "City",
    "State": "State",
    "Zip": "Zip",
    "County": "County",
    "CBSA": "CBSA",
    "APN": "APN",
    "Additional APNs": "Additional APNs",
    "# of Units": "# of Units",
    "Year Built": "Year Built",
    "Square Feet": "Square Feet",
    "Origination Date": "Close Date",
    "First Funding Date": "First Funding Date",
    "Last Funding Date": "Last Funding Date",
    "Original Loan Maturity date": "Original Loan Maturity Date",
    "Current Loan Maturity date": "Current Loan Maturity date",
    "Original Asset Maturity date": "Original Asset Maturity Date",
    "Current Asset Maturity Date": "Current Asset Maturity date",
    "Remedy Plan": "Remedy Plan",
    "Delinquency Notes": "Delinquency Status Notes",
    "Maturity Status": "Maturity Status",
    "Is Special Asset (Y/N)": "Is Special Asset",
    "Special Asset Status": "Special Asset: Status",
    "Special Asset Reason": "Special Asset: Special Asset Reason",
    "Special Asset: Special Asset Status": "Special Asset: Special Asset Status",
    "Special Asset: Resolved Date": "Special Asset: Resolved Date",
    "Forbearance Term Date": "Forbearance Term Date",
    "REO Date": "REO Date",
    "Initial Disbursement Funded": "Initial Disbursement Funded",
    "Renovation Holdback": "Approved Renovation Advance Amount",
    "Renovation Holdback Funded": "Renovation Advance Amount Funded",
    "Renovation Holdback Remaining": "Reno Advance Amount Remaining",
    "Interest Allocation": "Interest Allocation",
    "Interest Allocation Funded": "Interest Holdback Funded",
    "Title Company": "Title Company: Account Name",
    "Tax Due Date": "Tax Payment Next Due Date",
    "Tax Frequency": "Taxes Payment Frequency",
    "Tax Commentary": "Tax Commentary",
    "Product Type": "Product Type",
    "Product Sub-Type": "Product Sub-Type",
    "Transaction Type": "Transaction Type",
    "Project Strategy": "Project Strategy",
    "Property Type": "Property Type",
    "Originator": "Originator: Originating Company",
    "Deal Intro Sub-Source": "Deal Intro Sub-Source",
    "Referral Source Account": "Referral Source Account: Account Name",
    "Referral Source Contact": "Referral Source Contact: Full Name",
    "Loan Stage": "Stage",
    "Property Status": "Status",
}

BRIDGE_ASSET_FROM_VALUATION = {
    "Origination Value Dt": "Origination Valuation Date",
    "Origination As-Is Value": "Origination As-Is Value",
    "Origination ARV": "Origination After Repair Value",
    "Most Recent Appraisal Order Date": "Order Date",
    "Updated Valuation Date": "Current Appraisal Date",
    "Updated As-Is Value": "Current Appraised As-Is Value",
    "Updated ARV": "Current Appraised After Repair Value",
}

TERM_LOAN_FROM_TERM_EXPORT = {
    "Deal Number": "Deal Loan Number",
    "SF Yardi ID": "Yardi ID",
    "Deal Name": "Deal Name",
    "Borrower Entity": "Borrower Entity",
    "Account Name": "Account Name",
    "Do Not Lend (Y/N)": "Do Not Lend",
    "Financing": "Current Funding Vehicle",
    "Loan Amount": "Loan Amount",
    "Origination Date": "Close Date",
    "Originator": "CAF Originator",
    "Deal Intro Sub-Source": "Deal Intro Sub-Source",
    "Referral Source Account": "Referral Source Account",
    "Referral Source Contact": "Referral Source Contact",
    "AM Commentary": "Comments AM",
}

TERM_LOAN_FROM_SOLD_TERM = {
    "Loan Buyer": "Sold Loan: Sold To",
}

TERM_ASSET_FROM_TERM_ASSET_REPORT = {
    "Deal Number": "Deal Loan Number",
    "Asset ID": "Asset ID",
    "Address": "Address",
    "City": "City",
    "State": "State",
    "Zip": "Zip",
    "CBSA": "CBSA",
    "# Units": "# of Units",
    "Property Type": "Property Type",
    "Property ALA": "ALA",
}


# =============================================================================
# BASIC NORMALIZATION
# =============================================================================

def norm_text(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s or None


def norm_id(x):
    s = norm_text(x)
    if not s:
        return None
    s = re.sub(r"[^0-9A-Za-z]", "", s)
    return s or None


def norm_id_series(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"[^0-9A-Za-z]", "", regex=True)
        .replace({"": pd.NA})
    )


def money_to_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    s = str(x)
    s = re.sub(r"[^0-9\.\-]", "", s)
    return pd.to_numeric(s, errors="coerce")


def to_dt(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NaT
    return pd.to_datetime(x, errors="coerce")


def make_upb_header(run_dt: date) -> str:
    # report run date (month/day) + UPB
    return f"{run_dt.month}/{run_dt.day} UPB"


def dq_bucket(days_past_due: float) -> str:
    if pd.isna(days_past_due):
        return ""
    d = int(max(0, days_past_due))
    if d == 0:
        return "Current"
    if d < 30:
        return "1-29"
    if d < 45:
        return "30-44"
    if d < 60:
        return "45-59"
    if d < 90:
        return "60-89"
    return "90+"


# =============================================================================
# SALESFORCE AUTH (OAuth + PKCE)
# =============================================================================

def _b64url_no_pad(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("utf-8")


def _make_verifier() -> str:
    v = secrets.token_urlsafe(96)
    return v[:128]


def _make_challenge(verifier: str) -> str:
    return _b64url_no_pad(hashlib.sha256(verifier.encode("utf-8")).digest())


def _is_perm_error(msg: str) -> bool:
    m = (msg or "").lower()
    needles = [
        "insufficient",
        "permission",
        "not authorized",
        "not permitted",
        "invalid_type",
        "insufficient_access",
        "insufficient access",
        "insufficient_privileges",
        "insufficient_access_on_cross_reference_entity",
        "entity is not accessible",
        "field is not accessible",
        "no access",
        "access denied",
    ]
    return any(n in m for n in needles)


def sf_restful_safe(sf: Salesforce, path: str, method: str = "GET") -> dict:
    """Return {} on permission/report-access errors instead of crashing the app."""
    try:
        return sf.restful(path, method=method)
    except Exception as e:
        if _is_perm_error(str(e)):
            st.warning(f"⚠️ Salesforce access issue for: {path}. Returning empty results for this item.")
            return {}
        raise


def _exchange_code_for_token(
    token_url: str,
    code: str,
    verifier: str,
    client_id: str,
    redirect_uri: str,
    client_secret: Optional[str] = None,
) -> dict:
    data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code": code,
        "code_verifier": verifier,
    }
    if client_secret:
        data["client_secret"] = client_secret

    resp = requests.post(token_url, data=data, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Token exchange failed ({resp.status_code}): {resp.text}")
    return resp.json()


def ensure_sf_session() -> Salesforce:
    """Forces user login (PKCE) before continuing. Returns simple_salesforce.Salesforce."""

    cfg = st.secrets.get("salesforce")
    if not cfg:
        st.error("Missing Salesforce secrets. Add a [salesforce] section to .streamlit/secrets.toml")
        st.stop()

    client_id = cfg["client_id"]
    client_secret = cfg.get("client_secret")
    auth_host = cfg.get("auth_host", "https://login.salesforce.com").rstrip("/")
    redirect_uri = cfg["redirect_uri"].rstrip("/")

    auth_url = f"{auth_host}/services/oauth2/authorize"
    token_url = f"{auth_host}/services/oauth2/token"

    qp = st.query_params
    code = qp.get("code")
    state = qp.get("state")
    err = qp.get("error")
    err_desc = qp.get("error_description")

    if err:
        st.error(f"Login error: {err}")
        if err_desc:
            st.code(err_desc)
        st.stop()

    if "sf_token" not in st.session_state:
        st.session_state.sf_token = None

    # PKCE state store must be per-session (permissions differ by user)
    if "pkce_store" not in st.session_state:
        st.session_state.pkce_store = {}

    store = st.session_state.pkce_store
    now = time.time()
    ttl = 900  # 15 minutes
    for s, (_v, t0) in list(store.items()):
        if now - t0 > ttl:
            store.pop(s, None)

    # OAuth callback
    if code:
        if not state or state not in store:
            st.error("Login link expired. Click login again.")
            st.stop()

        verifier, _t0 = store.pop(state)
        tok = _exchange_code_for_token(token_url, code, verifier, client_id, redirect_uri, client_secret)
        st.session_state.sf_token = tok
        st.query_params.clear()
        st.rerun()

    # Not logged in -> show login button and stop
    if not st.session_state.sf_token:
        new_state = secrets.token_urlsafe(24)
        verifier = _make_verifier()
        challenge = _make_challenge(verifier)
        store[new_state] = (verifier, time.time())

        login_params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "state": new_state,
            "prompt": "login",
            "scope": "api refresh_token",
        }
        login_url = auth_url + "?" + urllib.parse.urlencode(login_params)

        st.info("Step 1: Log in to Salesforce to pull reports.")
        st.link_button("Login to Salesforce", login_url)
        st.stop()

    tok = st.session_state.sf_token
    access_token = tok.get("access_token")
    instance_url = tok.get("instance_url")

    if not access_token or not instance_url:
        st.error("Login token missing needed values.")
        st.stop()

    return Salesforce(instance_url=instance_url, session_id=access_token)


# =============================================================================
# SALESFORCE REPORT PULL (REST)
# =============================================================================

def get_report_metadata(sf: Salesforce, report_id: str) -> dict:
    return sf_restful_safe(sf, f"analytics/reports/{report_id}", method="GET")


def run_report_page(sf: Salesforce, report_id: str, page: int, page_size: int) -> dict:
    return sf_restful_safe(
        sf,
        f"analytics/reports/{report_id}?includeDetails=true&pageSize={page_size}&page={page}",
        method="GET",
    )


def report_json_to_df(report_json: dict) -> pd.DataFrame:
    if not report_json:
        return pd.DataFrame()

    rm = report_json.get("reportMetadata") or {}
    em = report_json.get("reportExtendedMetadata") or {}
    colinfo = em.get("detailColumnInfo") or {}
    detail_cols = rm.get("detailColumns") or []

    labels: List[str] = []
    for col_key in detail_cols:
        info = colinfo.get(col_key, {}) or {}
        lbl = info.get("label") or col_key
        labels.append(lbl)

    factmap = report_json.get("factMap") or {}
    block = factmap.get("T!T") or {}
    rows = block.get("rows") or []

    data_rows = []
    for r in rows:
        cells = r.get("dataCells") or []
        vals = []
        for c in cells:
            v = c.get("label")
            if v is None:
                v = c.get("value")
            vals.append(v)
        if len(vals) < len(labels):
            vals += [None] * (len(labels) - len(vals))
        data_rows.append(vals[: len(labels)])

    df = pd.DataFrame(data_rows, columns=labels)

    # Avoid duplicate-column headaches
    if df.columns.duplicated().any():
        seen: Dict[str, int] = {}
        new_cols: List[str] = []
        for c in df.columns:
            if c not in seen:
                seen[c] = 1
                new_cols.append(c)
            else:
                seen[c] += 1
                new_cols.append(f"{c} ({seen[c]})")
        df.columns = new_cols

    return df


def run_report_all_rows(sf: Salesforce, report_id: str, page_size: int = 2000, max_pages: int = 5000) -> pd.DataFrame:
    meta = get_report_metadata(sf, report_id)
    if not meta:
        return pd.DataFrame()

    total_rows = (meta.get("attributes") or {}).get("reportTotalRows") or (meta.get("reportMetadata") or {}).get(
        "reportTotalRows"
    )

    chunks: List[pd.DataFrame] = []
    page = 0
    total_seen = 0

    while page < max_pages:
        js = run_report_page(sf, report_id, page=page, page_size=page_size)
        if not js:
            break

        df = report_json_to_df(js)
        n = len(df)
        if n == 0:
            break

        chunks.append(df)
        total_seen += n

        if isinstance(total_rows, int) and total_rows > 0 and total_seen >= total_rows:
            break
        if n < page_size:
            break

        page += 1

    if not chunks:
        return pd.DataFrame()

    out = pd.concat(chunks, ignore_index=True).drop_duplicates()
    return out


# =============================================================================
# SERVICER FILE PARSING (NO HARD-CODED NAMES; DETECT BY COLUMNS)
# =============================================================================

def sniff_excel_header_row(file_bytes: bytes, required_cols: Set[str], max_scan_rows: int = 30) -> Optional[int]:
    """Scan first rows of the ACTIVE sheet; find row that contains all required columns."""
    wb = load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    try:
        for r in range(1, max_scan_rows + 1):
            row_vals = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
            cols = {str(v).strip() for v in row_vals if v is not None and str(v).strip() != ""}
            if required_cols.issubset(cols):
                return r  # 1-indexed
        return None
    finally:
        wb.close()


def date_from_filename(name: str) -> Optional[date]:
    # YYYYMMDD
    m = re.search(r"(20\d{2})(\d{2})(\d{2})", name)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # YYYY-M-D or YYYY-MM-DD
    m = re.search(r"(20\d{2})[-_](\d{1,2})[-_](\d{1,2})", name)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # MM_DD_YYYY or MM-DD-YYYY
    m = re.search(r"(\d{2})[_-](\d{2})[_-](20\d{2})", name)
    if m:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))

    # MMDDYYYY
    m = re.search(r"(\d{2})(\d{2})(20\d{2})", name)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return date(yy, mm, dd)

    return None


def _extract_date_from_text(s: str) -> Optional[date]:
    if not s:
        return None
    # Try a few patterns
    m = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    m = re.search(r"(\d{1,2})/(\d{1,2})/(20\d{2})", s)
    if m:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))

    return None


def parse_servicer_upload(upload) -> pd.DataFrame:
    name = upload.name
    b = upload.getvalue()

    # -----------------------------
    # CHL CSV
    # -----------------------------
    if name.lower().endswith(".csv"):
        df = pd.read_csv(BytesIO(b))
        req = {"Servicer Loan ID", "UPB"}
        if not req.issubset(set(df.columns)):
            raise ValueError(f"CSV doesn't look like CHL Streamline (missing {req - set(df.columns)}).")

        d = date_from_filename(name)
        as_of = pd.to_datetime(d) if d else pd.NaT

        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "CHL",
                "servicer_id": norm_id_series(df["Servicer Loan ID"]),
                "upb": df["UPB"].apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Current Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Performing Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # -----------------------------
    # Excel types detected by required columns
    # -----------------------------
    checks: List[Tuple[str, Set[str]]] = [
        # CHL Streamline Excel export (header row is usually 2)
        ("CHL", {"Servicer Loan ID", "UPB", "Next Due Date", "Current Maturity Date"}),
        ("Statebridge", {"Loan Number", "Current UPB", "Due Date", "Maturity Date", "Loan Status"}),
        # Berkadia "Data Tape" (your CoreVest_Data_Tape file)
        ("Berkadia", {"BCM Loan#", "Principal Balance", "Next Payment Due Date", "Maturity Date"}),
        ("FCI", {"Account", "Current Balance", "Next Due Date", "Maturity Date", "Status"}),
        ("Midland", {"ServicerLoanNumber", "UPB$", "NextPaymentDate", "MaturityDate", "ServicerLoanStatus"}),
    ]

    detected = None
    header_row = None
    for serv, req in checks:
        hr = sniff_excel_header_row(b, req)
        if hr is not None:
            detected = serv
            header_row = hr
            break

    if detected is None:
        raise ValueError(
            "Could not detect servicer file type from columns (Statebridge/Berkadia/FCI/Midland/CHL Streamline)."
        )

    # Read sheet using detected header row
    df = pd.read_excel(BytesIO(b), header=header_row - 1)

    # Best-effort file-name date
    d_file = date_from_filename(name)
    as_of_file = pd.to_datetime(d_file) if d_file else pd.NaT

    # -----------------------------
    # CHL Streamline Excel
    # -----------------------------
    if detected == "CHL":
        # Try to infer run date from A1 title like "Streamline - Servicing-2026-3-2"
        try:
            wb = load_workbook(BytesIO(b), read_only=True, data_only=True)
            ws = wb.active
            title = ws.cell(1, 1).value
            wb.close()
        except Exception:
            title = None

        d_title = _extract_date_from_text(str(title)) if title else None
        as_of_title = pd.to_datetime(d_title) if d_title else pd.NaT

        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "CHL",
                "servicer_id": norm_id_series(df["Servicer Loan ID"]),
                "upb": df["UPB"].apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Current Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Performing Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_title if pd.notna(as_of_title) else as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # -----------------------------
    # Statebridge (CoreVestLoanData_*.xlsx matches this)
    # -----------------------------
    if detected == "Statebridge":
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "Statebridge",
                "servicer_id": norm_id_series(df["Loan Number"]),
                "upb": pd.to_numeric(df["Current UPB"], errors="coerce"),
                "suspense": pd.to_numeric(df.get("Unapplied Balance", np.nan), errors="coerce"),
                "next_payment_date": df["Due Date"].apply(to_dt),
                "maturity_date": df["Maturity Date"].apply(to_dt),
                "status": df["Loan Status"].astype("string"),
                # Prefer the file's own "Date" column if present; fallback to filename
                "as_of": df.get("Date", pd.Series([as_of_file] * len(df))).apply(to_dt).fillna(as_of_file),
            }
        )
        return out.dropna(subset=["servicer_id"])

    # -----------------------------
    # Berkadia Data Tape
    # -----------------------------
    if detected == "Berkadia":
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "Berkadia",
                "servicer_id": norm_id_series(df["BCM Loan#"]),
                "upb": pd.to_numeric(df["Principal Balance"], errors="coerce"),
                "suspense": pd.to_numeric(df.get("Suspense Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Payment Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                # Use "Loan Status" if available (better than B/T)
                "status": df.get("Loan Status", df.get("B/T", pd.Series([None] * len(df)))).astype("string"),
                # Prefer "Run Date" column; fallback to filename
                "as_of": df.get("Run Date", pd.Series([as_of_file] * len(df))).apply(to_dt).fillna(as_of_file),
            }
        )
        return out.dropna(subset=["servicer_id"])

    # -----------------------------
    # FCI
    # -----------------------------
    if detected == "FCI":
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "FCI",
                "servicer_id": norm_id_series(df["Account"]),
                "upb": pd.to_numeric(df["Current Balance"], errors="coerce"),
                "suspense": pd.to_numeric(df.get("Suspense Pmt.", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # -----------------------------
    # Midland
    # -----------------------------
    if detected == "Midland":
        raw = df["ServicerLoanNumber"].astype("string").str.strip()
        raw = raw.str.replace(r"COM$", "", regex=True)
        raw = raw.str.replace(r"[^0-9A-Za-z]", "", regex=True).str.lstrip("0")

        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "Midland",
                "servicer_id": raw.replace({"": pd.NA}),
                "upb": df["UPB$"].apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df["NextPaymentDate"].apply(to_dt),
                "maturity_date": df["MaturityDate"].apply(to_dt),
                "status": df["ServicerLoanStatus"].astype("string"),
                "as_of": df.get("ReportDate", pd.Series([as_of_file] * len(df))).apply(to_dt).fillna(as_of_file),
            }
        )
        return out.dropna(subset=["servicer_id"])

    raise ValueError("Unhandled servicer type.")


def build_servicer_lookup(servicer_uploads: List) -> Tuple[pd.DataFrame, date]:
    frames: List[pd.DataFrame] = []
    asof_candidates: List[date] = []

    for f in servicer_uploads:
        df = parse_servicer_upload(f)
        frames.append(df)
        dmax = pd.to_datetime(df["as_of"], errors="coerce").max()
        if pd.notna(dmax):
            asof_candidates.append(dmax.date())

    lookup = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(
            columns=[
                "source_file",
                "servicer",
                "servicer_id",
                "upb",
                "suspense",
                "next_payment_date",
                "maturity_date",
                "status",
                "as_of",
            ]
        )
    )

    # latest row per (servicer, servicer_id)
    if not lookup.empty:
        lookup = lookup.sort_values("as_of").drop_duplicates(["servicer", "servicer_id"], keep="last")

    # choose "report run date" default = max as_of found; fallback today
    run_date = max(asof_candidates) if asof_candidates else date.today()
    return lookup, run_date


# =============================================================================
# LAST WEEK REPORT CARRY-FORWARD (REO DATE + optional manual columns)
# =============================================================================

def read_tab_df_from_active_loans(file_bytes: bytes, sheet: str) -> pd.DataFrame:
    # header is row 4 => pandas header=3
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=3)
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_prev_maps(prev_bytes: bytes) -> dict:
    out = {}

    # Term Loan REO carry-forward
    try:
        tl = read_tab_df_from_active_loans(prev_bytes, "Term Loan")
        if "Deal Number" in tl.columns and "REO Date" in tl.columns:
            tmp = tl[["Deal Number", "REO Date"]].copy()
            tmp["_deal_key"] = norm_id_series(tmp["Deal Number"])
            out["term_loan_reo"] = tmp.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")
    except Exception:
        pass

    # Optional Bridge Loan manual carry-forward
    try:
        bl = read_tab_df_from_active_loans(prev_bytes, "Bridge Loan")
        keep = [c for c in ["Deal Number", "State(s)", "Loan Level Delinquency", "Special Focus (Y/N)"] if c in bl.columns]
        if "Deal Number" in keep and len(keep) > 1:
            tmp = bl[keep].copy()
            tmp["_deal_key"] = norm_id_series(tmp["Deal Number"])
            out["bridge_loan_manual"] = tmp.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")
    except Exception:
        pass

    return out


# =============================================================================
# BUILD: BRIDGE ASSET
# =============================================================================

def build_bridge_asset(
    sf_spine: pd.DataFrame,
    sf_dnl: pd.DataFrame,
    sf_val: pd.DataFrame,
    sf_am: pd.DataFrame,
    sf_arm: pd.DataFrame,
    serv_lookup: pd.DataFrame,
    upb_col: str,
) -> pd.DataFrame:
    out = pd.DataFrame()

    # Base from Bridge Maturity
    for col, label in BRIDGE_ASSET_FROM_BRIDGE_MATURITY.items():
        out[col] = sf_spine[label] if label in sf_spine.columns else None

    # Keys
    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_serv_id_key"] = norm_id_series(out.get("Servicer ID", pd.Series([None] * len(out))))
    out["_asset_key"] = norm_id_series(out.get("Asset ID", pd.Series([None] * len(out))))

    # Do Not Lend by Deal
    if not sf_dnl.empty and "Deal Loan Number" in sf_dnl.columns:
        dnl = sf_dnl.copy()
        dnl["_deal_key"] = norm_id_series(dnl["Deal Loan Number"])
        if "Do Not Lend" in dnl.columns:
            dnl_flag = dnl.groupby("_deal_key")["Do Not Lend"].max().reset_index()
            out = out.merge(dnl_flag, on="_deal_key", how="left")
            out["Do Not Lend (Y/N)"] = out["Do Not Lend"].fillna(False).map(lambda x: "Y" if bool(x) else "")
            out = out.drop(columns=["Do Not Lend"], errors="ignore")

    # Valuation by Asset ID
    if not sf_val.empty and "Asset ID" in sf_val.columns:
        v = sf_val.copy()
        v["_asset_key"] = norm_id_series(v["Asset ID"])
        keep = ["_asset_key"] + [lbl for lbl in BRIDGE_ASSET_FROM_VALUATION.values() if lbl in v.columns]
        v = v[keep].drop_duplicates("_asset_key")
        out = out.merge(v, on="_asset_key", how="left")
        for tcol, vlabel in BRIDGE_ASSET_FROM_VALUATION.items():
            if vlabel in out.columns:
                out[tcol] = out[vlabel]
                out = out.drop(columns=[vlabel], errors="ignore")

    # AM Assignments pivot by Deal
    if not sf_am.empty and "Deal Loan Number" in sf_am.columns:
        am = sf_am.copy()
        am["_deal_key"] = norm_id_series(am["Deal Loan Number"])
        am["_dt"] = pd.to_datetime(am.get("Date Assigned"), errors="coerce")
        am = am.sort_values(["_deal_key", "Team Role", "_dt"]).drop_duplicates(["_deal_key", "Team Role"], keep="last")

        role_to_namecol = {
            "Asset Manager": "Asset Manager 1",
            "Asset Manager 2": "Asset Manager 2",
            "Construction Manager": "Construction Mgr.",
        }
        role_to_datecol = {
            "Asset Manager": "AM 1 Assigned Date",
            "Asset Manager 2": "AM 2 Assigned Date",
            "Construction Manager": "CM Assigned Date",
        }

        piv_name = am.pivot_table(index="_deal_key", columns="Team Role", values="Team Member Name", aggfunc="first")
        piv_date = am.pivot_table(index="_deal_key", columns="Team Role", values="Date Assigned", aggfunc="first")

        piv_name = piv_name.rename(columns=role_to_namecol).reset_index()
        piv_date = piv_date.rename(columns=role_to_datecol).reset_index()

        out = out.merge(piv_name, on="_deal_key", how="left")
        out = out.merge(piv_date, on="_deal_key", how="left")

    # Active RM by Deal
    if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns:
        arm = sf_arm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        if "CAF Originator" in arm.columns:
            arm = arm[["_deal_key", "CAF Originator"]].drop_duplicates("_deal_key")
            out = out.merge(arm, on="_deal_key", how="left")
            out["Active RM"] = out["CAF Originator"]
            out = out.drop(columns=["CAF Originator"], errors="ignore")

    # Servicer lookup join (by Servicer ID)
    if not serv_lookup.empty:
        s = serv_lookup.dropna(subset=["servicer_id"]).copy()
        out = out.merge(
            s.rename(
                columns={
                    "servicer_id": "_serv_id_key",
                    "servicer": "Servicer",
                    "upb": "_loan_upb",
                    "suspense": "_loan_suspense",
                    "next_payment_date": "Next Payment Date",
                    "maturity_date": "Servicer Maturity Date",
                    "status": "Servicer Status",
                }
            )[
                [
                    "_serv_id_key",
                    "Servicer",
                    "_loan_upb",
                    "_loan_suspense",
                    "Next Payment Date",
                    "Servicer Maturity Date",
                    "Servicer Status",
                ]
            ],
            on="_serv_id_key",
            how="left",
        )

        # Allocate loan-level UPB across assets.
        w = pd.to_numeric(sf_spine.get("Current UPB", pd.Series([np.nan] * len(out))), errors="coerce")
        out["_w"] = w
        out["_w_sum"] = out.groupby("_serv_id_key")["_w"].transform("sum")
        out["_n_in_loan"] = out.groupby("_serv_id_key")["_serv_id_key"].transform("size").replace({0: np.nan})

        out[upb_col] = np.where(
            out["_w_sum"].fillna(0) > 0,
            out["_loan_upb"] * (out["_w"] / out["_w_sum"]),
            out["_loan_upb"] / out["_n_in_loan"],
        )

        out["Suspense Balance"] = np.where(
            out["_w_sum"].fillna(0) > 0,
            out["_loan_suspense"] * (out["_w"] / out["_w_sum"]),
            out["_loan_suspense"] / out["_n_in_loan"],
        )

    # SF Funded Amount: prefer explicit; else sum of funded buckets
    if "Approved Advance Amount Funded" in sf_spine.columns:
        out["SF Funded Amount"] = pd.to_numeric(sf_spine["Approved Advance Amount Funded"], errors="coerce")
    else:
        out["SF Funded Amount"] = (
            pd.to_numeric(out.get("Initial Disbursement Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Renovation Holdback Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Interest Allocation Funded", 0), errors="coerce").fillna(0)
        )

    # Portfolio / Segment / Strategy Grouping — left blank for now
    out.setdefault("Portfolio", "")
    out.setdefault("Segment", "")
    out.setdefault("Strategy Grouping", "")

    # normalize Y/N for Is Special Asset
    if "Is Special Asset (Y/N)" in out.columns:
        out["Is Special Asset (Y/N)"] = out["Is Special Asset (Y/N)"].fillna(False).map(lambda x: "Y" if bool(x) else "")

    return out


# =============================================================================
# BUILD: TERM LOAN
# =============================================================================

def build_term_loan(
    sf_term: pd.DataFrame,
    sf_sold: pd.DataFrame,
    sf_am: pd.DataFrame,
    sf_arm: pd.DataFrame,
    serv_lookup: pd.DataFrame,
    upb_col: str,
    run_dt: date,
    prev_maps: dict,
) -> pd.DataFrame:
    out = pd.DataFrame()

    # Base from Term Data Export
    for col, label in TERM_LOAN_FROM_TERM_EXPORT.items():
        out[col] = sf_term[label] if label in sf_term.columns else None

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))

    # Do Not Lend -> Y/N
    if "Do Not Lend (Y/N)" in out.columns:
        out["Do Not Lend (Y/N)"] = out["Do Not Lend (Y/N)"].fillna(False).map(lambda x: "Y" if bool(x) else "")

    # Loan Buyer from Sold Term Loans
    if not sf_sold.empty and "Deal Loan Number" in sf_sold.columns:
        sold = sf_sold.copy()
        sold["_deal_key"] = norm_id_series(sold["Deal Loan Number"])
        if "Sold Loan: Sold To" in sold.columns:
            sold = sold[["_deal_key", "Sold Loan: Sold To"]].drop_duplicates("_deal_key")
            out = out.merge(sold, on="_deal_key", how="left")
            out["Loan Buyer"] = out["Sold Loan: Sold To"]
            out = out.drop(columns=["Sold Loan: Sold To"], errors="ignore")

    # Pull Servicer ID + optional SF servicer name from Term export if present
    out["Servicer ID"] = sf_term["Servicer Commitment Id"] if "Servicer Commitment Id" in sf_term.columns else None
    servicer_sf = sf_term["Servicer Name"] if "Servicer Name" in sf_term.columns else None

    # Servicer join for UPB + dates/status
    out["_serv_id_key"] = norm_id_series(out.get("Servicer ID", pd.Series([None] * len(out))))

    if not serv_lookup.empty:
        s = serv_lookup.dropna(subset=["servicer_id"]).copy()
        # Midland normalization: compare also lstrip zeros
        out["_serv_id_key_mid"] = out["_serv_id_key"].astype("string").str.lstrip("0")

        s2 = s.copy()
        s2["_sid_mid"] = s2["servicer_id"].astype("string").str.lstrip("0")
        s2 = s2.rename(
            columns={
                "servicer": "_servicer_file",
                "upb": upb_col,
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Maturity Date",
                "status": "Servicer Status",
            }
        )[["_sid_mid", "_servicer_file", upb_col, "Next Payment Date", "Maturity Date", "Servicer Status"]]

        out = out.merge(s2, left_on="_serv_id_key_mid", right_on="_sid_mid", how="left").drop(columns=["_sid_mid"], errors="ignore")

        # Fill Servicer display name
        out["Servicer"] = out.get("_servicer_file")
        if servicer_sf is not None:
            out["Servicer"] = out["Servicer"].fillna(servicer_sf)
        out = out.drop(columns=["_servicer_file"], errors="ignore")

    else:
        out["Servicer"] = servicer_sf if servicer_sf is not None else ""

    # Fallbacks if servicer-file join missing
    if upb_col not in out.columns:
        out[upb_col] = np.nan

    # SF fallback: Current Servicer UPB (exists in Term Data Export)
    if "Current Servicer UPB" in sf_term.columns:
        out[upb_col] = out[upb_col].fillna(pd.to_numeric(sf_term["Current Servicer UPB"], errors="coerce"))

    # Last resort: Loan Amount
    if "Loan Amount" in out.columns:
        out[upb_col] = out[upb_col].fillna(pd.to_numeric(out["Loan Amount"], errors="coerce"))

    # Maturity Date fallback from SF (Original Loan Maturity Date)
    if "Maturity Date" not in out.columns:
        out["Maturity Date"] = pd.NaT
    if "Original Loan Maturity Date" in sf_term.columns:
        out["Maturity Date"] = out["Maturity Date"].fillna(pd.to_datetime(sf_term["Original Loan Maturity Date"], errors="coerce"))

    # Next Payment Date fallback from SF
    if "Next Payment Date" not in out.columns:
        out["Next Payment Date"] = pd.NaT
    if "Next Payment Date" in sf_term.columns:
        out["Next Payment Date"] = out["Next Payment Date"].fillna(pd.to_datetime(sf_term["Next Payment Date"], errors="coerce"))

    # Active RM: if Term export has it, use it; else fallback to Active RM report
    if "Active RM" in sf_term.columns:
        out["Active RM"] = sf_term["Active RM"]
    else:
        out["Active RM"] = ""
        if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns and "CAF Originator" in sf_arm.columns:
            arm = sf_arm.copy()
            arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
            arm = arm[["_deal_key", "CAF Originator"]].drop_duplicates("_deal_key")
            out = out.merge(arm, on="_deal_key", how="left")
            out["Active RM"] = out["CAF Originator"].fillna("")
            out = out.drop(columns=["CAF Originator"], errors="ignore")

    # Asset Manager from AM Assignments (role = Asset Manager)
    if not sf_am.empty and "Deal Loan Number" in sf_am.columns:
        am = sf_am.copy()
        am["_deal_key"] = norm_id_series(am["Deal Loan Number"])
        am["_dt"] = pd.to_datetime(am.get("Date Assigned"), errors="coerce")
        am = am.sort_values(["_deal_key", "Team Role", "_dt"]).drop_duplicates(["_deal_key", "Team Role"], keep="last")

        if "Team Role" in am.columns and "Team Member Name" in am.columns:
            am1 = am[am["Team Role"].astype("string").str.strip().eq("Asset Manager")][["_deal_key", "Team Member Name"]]
            am1 = am1.drop_duplicates("_deal_key")
            out = out.merge(am1, on="_deal_key", how="left")
            out["Asset Manager"] = out["Team Member Name"].fillna("")
            out = out.drop(columns=["Team Member Name"], errors="ignore")
        else:
            out["Asset Manager"] = ""
    else:
        out["Asset Manager"] = ""

    # REO Date carry-forward from last week's report
    out["REO Date"] = ""
    if "term_loan_reo" in prev_maps:
        reo = prev_maps["term_loan_reo"][["_deal_key", "REO Date"]].copy()
        out = out.merge(reo, on="_deal_key", how="left", suffixes=("", "_prev"))
        out["REO Date"] = out["REO Date_prev"].fillna("")
        out = out.drop(columns=["REO Date_prev"], errors="ignore")

    # Portfolio/Segment left blank for now
    out.setdefault("Portfolio", "")
    out.setdefault("Segment", "")

    return out


# =============================================================================
# BUILD: TERM ASSET (ALA-weight UPB from Term Loan)
# =============================================================================

def build_term_asset(sf_term_asset: pd.DataFrame, term_loan: pd.DataFrame, upb_col: str) -> pd.DataFrame:
    out = pd.DataFrame()

    for col, label in TERM_ASSET_FROM_TERM_ASSET_REPORT.items():
        out[col] = sf_term_asset[label] if label in sf_term_asset.columns else None

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["CPP JV"] = ""  # N/A for now

    # allocate UPB from Term Loan across assets by ALA
    tl = term_loan.copy()
    tl["_deal_key"] = norm_id_series(tl.get("Deal Number", pd.Series([None] * len(tl))))

    if upb_col in tl.columns:
        tl = tl[["_deal_key", upb_col]].drop_duplicates("_deal_key")
        out = out.merge(tl, on="_deal_key", how="left")

        ala = pd.to_numeric(out.get("Property ALA", np.nan), errors="coerce")
        ala_sum = ala.groupby(out["_deal_key"]).transform("sum")
        out[upb_col] = np.where(ala_sum > 0, out[upb_col] * (ala / ala_sum), out[upb_col])

    return out


# =============================================================================
# BUILD: BRIDGE LOAN (roll-up Bridge Asset + a few SF fields)
# =============================================================================

def build_bridge_loan(
    bridge_asset: pd.DataFrame,
    sf_spine: pd.DataFrame,
    upb_col: str,
    prev_maps: dict,
) -> pd.DataFrame:
    ba = bridge_asset.copy()

    # roll-up per Deal
    g = ba.groupby("_deal_key", dropna=True)

    def _first(series: pd.Series):
        s = series.dropna()
        return s.iloc[0] if len(s) else ""

    def _max_dt(series: pd.Series):
        s = pd.to_datetime(series, errors="coerce")
        s = s.dropna()
        return s.max() if len(s) else ""

    def _min_dt(series: pd.Series):
        s = pd.to_datetime(series, errors="coerce")
        s = s.dropna()
        return s.min() if len(s) else ""

    out = pd.DataFrame(
        {
            "Deal Number": g["Deal Number"].first(),
            "Portfolio": g["Portfolio"].apply(_first) if "Portfolio" in ba.columns else "",
            "Loan Buyer": g["Loan Buyer"].first() if "Loan Buyer" in ba.columns else "",
            "Financing": g["Financing"].first() if "Financing" in ba.columns else "",
            "Servicer ID": g["Servicer ID"].first() if "Servicer ID" in ba.columns else "",
            "Servicer": g["Servicer"].apply(_first) if "Servicer" in ba.columns else "",
            "Deal Name": g["Deal Name"].first() if "Deal Name" in ba.columns else "",
            "Borrower Name": g["Borrower Entity"].first() if "Borrower Entity" in ba.columns else "",
            "Account": g["Account Name"].first() if "Account Name" in ba.columns else "",
            "Do Not Lend (Y/N)": g["Do Not Lend (Y/N)"].max() if "Do Not Lend (Y/N)" in ba.columns else "",
            "Primary Contact": g["Primary Contact"].first() if "Primary Contact" in ba.columns else "",
            "Number of Assets": g["Asset ID"].nunique() if "Asset ID" in ba.columns else 0,
            "# of Units": pd.to_numeric(g["# of Units"].sum(min_count=1), errors="coerce") if "# of Units" in ba.columns else np.nan,
            "State(s)": g["State"].apply(
                lambda s: ", ".join(sorted({str(x).strip() for x in s.dropna() if str(x).strip() != ""}))
            )
            if "State" in ba.columns
            else "",
            "Origination Date": g["Origination Date"].apply(_min_dt) if "Origination Date" in ba.columns else "",
            "Last Funding Date": g["Last Funding Date"].apply(_max_dt) if "Last Funding Date" in ba.columns else "",
            "Original Maturity Date": g["Original Loan Maturity date"].first() if "Original Loan Maturity date" in ba.columns else "",
            "Current Maturity Date": g["Current Loan Maturity date"].first() if "Current Loan Maturity date" in ba.columns else "",
            # This matches your workbook behavior: Next Advance Maturity Date comes from servicer-side maturity
            "Next Advance Maturity Date": g["Servicer Maturity Date"].first() if "Servicer Maturity Date" in ba.columns else "",
            "Next Payment Date": g["Next Payment Date"].apply(_min_dt) if "Next Payment Date" in ba.columns else "",
            "Loan Level Delinquency": "",  # manual/carry-forward
            "Active Funded Amount": pd.to_numeric(g["SF Funded Amount"].sum(min_count=1), errors="coerce")
            if "SF Funded Amount" in ba.columns
            else np.nan,
            upb_col: pd.to_numeric(g[upb_col].sum(min_count=1), errors="coerce") if upb_col in ba.columns else np.nan,
            "Suspense Balance": pd.to_numeric(g["Suspense Balance"].sum(min_count=1), errors="coerce") if "Suspense Balance" in ba.columns else np.nan,
            "Initial Disbursement Funded": pd.to_numeric(g["Initial Disbursement Funded"].sum(min_count=1), errors="coerce")
            if "Initial Disbursement Funded" in ba.columns
            else np.nan,
            "Renovation Holdback": pd.to_numeric(g["Renovation Holdback"].sum(min_count=1), errors="coerce") if "Renovation Holdback" in ba.columns else np.nan,
            "Renovation HB Funded": pd.to_numeric(g["Renovation Holdback Funded"].sum(min_count=1), errors="coerce")
            if "Renovation Holdback Funded" in ba.columns
            else np.nan,
            "Renovation HB Remaining": pd.to_numeric(g["Renovation Holdback Remaining"].sum(min_count=1), errors="coerce")
            if "Renovation Holdback Remaining" in ba.columns
            else np.nan,
            "Interest Allocation": pd.to_numeric(g["Interest Allocation"].sum(min_count=1), errors="coerce") if "Interest Allocation" in ba.columns else np.nan,
            "Interest Allocation Funded": pd.to_numeric(g["Interest Allocation Funded"].sum(min_count=1), errors="coerce")
            if "Interest Allocation Funded" in ba.columns
            else np.nan,
            "Loan Stage": g["Loan Stage"].first() if "Loan Stage" in ba.columns else "",
            "Segment": g["Segment"].apply(_first) if "Segment" in ba.columns else "",
            "Product Type": g["Product Type"].first() if "Product Type" in ba.columns else "",
            "Product Sub Type": g["Product Sub-Type"].first() if "Product Sub-Type" in ba.columns else "",
            "Transaction Type": g["Transaction Type"].first() if "Transaction Type" in ba.columns else "",
            "Project Strategy": g["Project Strategy"].first() if "Project Strategy" in ba.columns else "",
            "Strategy Grouping": g["Strategy Grouping"].apply(_first) if "Strategy Grouping" in ba.columns else "",
            "CV Originator": g["Originator"].first() if "Originator" in ba.columns else "",
            "Active RM": g["Active RM"].apply(_first) if "Active RM" in ba.columns else "",
            "Deal Intro Sub-Source": g["Deal Intro Sub-Source"].first() if "Deal Intro Sub-Source" in ba.columns else "",
            "Referral Source Account": g["Referral Source Account"].first() if "Referral Source Account" in ba.columns else "",
            "Referral Source Contact": g["Referral Source Contact"].first() if "Referral Source Contact" in ba.columns else "",
            "3/31 NPL": "",
            "Needs NPL Value": "",
            "Special Focus (Y/N)": "",  # manual/carry-forward
            "Asset Manager 1": g["Asset Manager 1"].apply(_first) if "Asset Manager 1" in ba.columns else "",
            "AM 1 Assigned Date": g["AM 1 Assigned Date"].apply(_first) if "AM 1 Assigned Date" in ba.columns else "",
            "Asset Manager 2": g["Asset Manager 2"].apply(_first) if "Asset Manager 2" in ba.columns else "",
            "AM 2 Assigned Date": g["AM 2 Assigned Date"].apply(_first) if "AM 2 Assigned Date" in ba.columns else "",
            "Construction Mgr.": g["Construction Mgr."].apply(_first) if "Construction Mgr." in ba.columns else "",
            "CM Assigned Date": g["CM Assigned Date"].apply(_first) if "CM Assigned Date" in ba.columns else "",
        }
    ).reset_index(drop=True)

    # Loan Commitment + Remaining Commitment + AM Commentary from SF Bridge Maturity report
    if "Deal Loan Number" in sf_spine.columns:
        deal = sf_spine.copy()
        deal["_deal_key"] = norm_id_series(deal["Deal Loan Number"])
        keep = ["_deal_key"]

        if "Loan Commitment" in deal.columns:
            keep.append("Loan Commitment")
        if "Total Remaining Commitment Amount" in deal.columns:
            keep.append("Total Remaining Commitment Amount")
        if "Comments AM" in deal.columns:
            keep.append("Comments AM")

        deal = deal[keep].drop_duplicates("_deal_key")
        out = out.merge(deal, on="_deal_key", how="left")

        if "Total Remaining Commitment Amount" in out.columns:
            out["Remaining Commitment"] = out["Total Remaining Commitment Amount"]

        if "Comments AM" in out.columns:
            out["AM Commentary"] = out["Comments AM"]

        out = out.drop(columns=["Total Remaining Commitment Amount", "Comments AM"], errors="ignore")

    # Carry-forward optional manual columns from last week (if present)
    if "bridge_loan_manual" in prev_maps:
        man = prev_maps["bridge_loan_manual"].copy()
        out = out.merge(man, on="_deal_key", how="left", suffixes=("", "_prev"))
        for c in ["State(s)", "Loan Level Delinquency", "Special Focus (Y/N)"]:
            if f"{c}_prev" in out.columns:
                out[c] = out[f"{c}_prev"].fillna(out.get(c, ""))
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    return out.drop(columns=["_deal_key"], errors="ignore")


# =============================================================================
# EXCEL OUTPUT (template-based; preserve formulas; dynamic UPB header)
# =============================================================================

def header_tuples_from_ws(ws_values, header_row: int = 4) -> List[Tuple[int, str]]:
    """Read header display-values (cached) by column index from a data_only=True workbook."""
    out: List[Tuple[int, str]] = []
    for col_idx, cell in enumerate(ws_values[header_row], start=1):
        v = cell.value
        h = "" if v is None else str(v).strip()
        if h:
            out.append((col_idx, h))
    return out


def formula_col_indices(ws_formula, start_row: int = 5, header_row: int = 4) -> Set[int]:
    """Detect which columns contain formulas on the first data row so we can preserve them."""
    fcols: Set[int] = set()
    for col_idx, _cell in enumerate(ws_formula[header_row], start=1):
        v = ws_formula.cell(start_row, col_idx).value
        if isinstance(v, str) and v.startswith("="):
            fcols.add(col_idx)
    return fcols


def normalize_header_name(h: str, upb_header: str) -> str:
    # Replace any "M/D UPB" header with the chosen upb_header
    if isinstance(h, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", h):
        return upb_header
    if h.strip().upper().endswith("UPB") and "UPB" in upb_header:
        # catch formula headers that cache as an UPB string
        if re.search(r"\b\d{1,2}/\d{1,2}\b", h):
            return upb_header
    return h.strip()


def clear_columns(ws, col_indices: List[int], start_row: int = 5):
    max_r = ws.max_row
    for r in range(start_row, max_r + 1):
        for c in col_indices:
            ws.cell(r, c).value = None


def write_df_to_sheet_preserve_formulas(
    ws_formula,
    df: pd.DataFrame,
    header_tuples: List[Tuple[int, str]],
    formula_cols: Set[int],
    start_row: int = 5,
):
    """Writes only NON-formula columns; leaves formula columns intact in the template."""

    write_cols = [(c, h) for (c, h) in header_tuples if c not in formula_cols]
    col_indices = [c for c, _h in write_cols]
    headers = [h for _c, h in write_cols]

    df_out = df.copy()
    for h in headers:
        if h not in df_out.columns:
            df_out[h] = None
    df_out = df_out[headers]

    clear_columns(ws_formula, col_indices, start_row=start_row)

    for r_offset, row in enumerate(df_out.itertuples(index=False, name=None), start=0):
        r = start_row + r_offset
        for (c, _h), val in zip(write_cols, row):
            ws_formula.cell(r, c).value = val


def set_upb_header_in_sheet(ws_formula, ws_values, new_upb_header: str, header_row: int = 4):
    """Update the UPB header cell in sheets where the header is a literal string.

    If the UPB header is a formula (common in Bridge Loan / Term Asset), we leave it alone.
    Excel will recalc when the user opens the file.
    """
    hdr = header_tuples_from_ws(ws_values, header_row=header_row)
    for col_idx, h in hdr:
        if isinstance(h, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", h):
            cur = ws_formula.cell(header_row, col_idx).value
            if isinstance(cur, str) and cur.startswith("="):
                # keep formula
                return
            ws_formula.cell(header_row, col_idx).value = new_upb_header
            return


# =============================================================================
# STREAMLIT UI
# =============================================================================

st.set_page_config(page_title="Active Loans Builder", layout="wide")
st.title("Active Loans Report Builder")

st.markdown(
    """
**Inputs**
1) Upload the Active Loans template workbook (.xlsx)
2) Upload last week's Active Loans report (optional) — used for REO Date carry-forward
3) Upload current servicer files (csv/xlsx)
4) Log in to Salesforce to pull the reports

**UPB header**
The UPB column header is labeled as **M/D UPB**, where M/D is the **report run date**.
By default, we detect this date from your uploaded servicer files (latest date found).
"""
)

template_upload = st.file_uploader("Upload Active Loans TEMPLATE (.xlsx)", type=["xlsx"])
prev_upload = st.file_uploader(
    "Upload LAST WEEK'S Active Loans report (.xlsx) for REO Date carry-forward (optional)", type=["xlsx"]
)
servicer_uploads = st.file_uploader(
    "Upload current servicer files (csv/xlsx) — any filenames (Statebridge/Berkadia/FCI/Midland/CHL)",
    type=["csv", "xlsx"],
    accept_multiple_files=True,
)

use_sf = st.checkbox("Pull Salesforce via API (required for full automation)", value=True)

sf = None
if use_sf:
    sf = ensure_sf_session()
    c1, c2 = st.columns([3, 1])
    with c1:
        inst = (st.session_state.get("sf_token") or {}).get("instance_url", "")
        st.success("✅ Logged in to Salesforce")
        if inst:
            st.caption(f"Connected to: {inst}")
    with c2:
        if st.button("Log out"):
            st.session_state.sf_token = None
            st.rerun()

# Quick guess for default (filename-based only) — cheap, before full parse
name_guess = date.today()
if servicer_uploads:
    dts = [date_from_filename(u.name) for u in servicer_uploads]
    dts = [d for d in dts if d]
    if dts:
        name_guess = max(dts)

use_detected_run_date = st.checkbox(
    "Use report run date detected from servicer files (recommended)", value=True
)
manual_run_date = st.date_input("Report run date (UPB column header)", value=name_guess, disabled=use_detected_run_date)

build_btn = st.button("Build Active Loans", type="primary")

if build_btn:
    if not template_upload:
        st.error("Upload the template workbook first.")
        st.stop()

    if not servicer_uploads:
        st.error("Upload the servicer files. UPB/Next Payment/Maturity/Status come from them.")
        st.stop()

    if use_sf and sf is None:
        st.error("Salesforce login is required (or uncheck Salesforce option).")
        st.stop()

    # Parse last week maps
    prev_maps = {}
    if prev_upload:
        with st.spinner("Reading last week's report (carry-forward)..."):
            prev_maps = build_prev_maps(prev_upload.getvalue())

    # Parse servicer files
    with st.spinner("Parsing servicer files..."):
        serv_lookup, detected_run_date = build_servicer_lookup(servicer_uploads)

    # Choose run date for UPB header
    run_dt = detected_run_date if use_detected_run_date else manual_run_date
    upb_col = make_upb_header(run_dt)

    st.markdown("### Servicer lookup preview (standardized)")
    st.caption(f"Detected report run date from servicer files: **{detected_run_date.isoformat()}**")
    st.caption(f"UPB column header to be used: **{upb_col}**")
    st.dataframe(serv_lookup.head(25), use_container_width=True)

    # Pull Salesforce reports
    dfs: Dict[str, pd.DataFrame] = {}
    if use_sf:
        for key, (nm, rid) in REPORTS.items():
            with st.spinner(f"Pulling Salesforce report: {nm} ({rid})"):
                dfs[key] = run_report_all_rows(sf, rid, page_size=2000)
    else:
        st.error("This version expects Salesforce API pulls.")
        st.stop()

    # Build sheets
    with st.spinner("Building Bridge Asset..."):
        bridge_asset = build_bridge_asset(
            dfs.get("bridge_maturity", pd.DataFrame()),
            dfs.get("do_not_lend", pd.DataFrame()),
            dfs.get("valuation", pd.DataFrame()),
            dfs.get("am_assignments", pd.DataFrame()),
            dfs.get("active_rm", pd.DataFrame()),
            serv_lookup,
            upb_col,
        )

    with st.spinner("Building Term Loan..."):
        term_loan = build_term_loan(
            dfs.get("term_export", pd.DataFrame()),
            dfs.get("sold_term", pd.DataFrame()),
            dfs.get("am_assignments", pd.DataFrame()),
            dfs.get("active_rm", pd.DataFrame()),
            serv_lookup,
            upb_col,
            run_dt,
            prev_maps,
        )

    with st.spinner("Building Term Asset..."):
        term_asset = build_term_asset(dfs.get("term_asset", pd.DataFrame()), term_loan, upb_col)

    with st.spinner("Building Bridge Loan..."):
        bridge_loan = build_bridge_loan(bridge_asset, dfs.get("bridge_maturity", pd.DataFrame()), upb_col, prev_maps)

    # Diagnostics
    st.subheader("Diagnostics")

    if "_loan_upb" in bridge_asset.columns:
        matched = bridge_asset["_loan_upb"].notna().mean()
        st.write(f"Bridge Asset servicer-join match rate (UPB): {matched:.1%}")

    if upb_col in term_loan.columns:
        matched = term_loan[upb_col].notna().mean()
        st.write(f"Term Loan servicer-join match rate (UPB): {matched:.1%}")

    if "_loan_upb" in bridge_asset.columns and upb_col in bridge_asset.columns:
        rec = (
            bridge_asset.dropna(subset=["_serv_id_key"])
            .groupby("_serv_id_key")
            .agg(loan_upb=("_loan_upb", "max"), sum_asset_upb=(upb_col, "sum"), n_assets=("Asset ID", "nunique"))
            .reset_index()
        )
        rec["diff"] = rec["sum_asset_upb"] - rec["loan_upb"]
        st.write("Bridge Asset UPB reconciliation (top diffs):")
        st.dataframe(rec.sort_values("diff", key=lambda s: s.abs(), ascending=False).head(20), use_container_width=True)

    # Write output workbook (preserve formulas)
    tmpl_bytes = template_upload.getvalue()

    wb = load_workbook(BytesIO(tmpl_bytes), data_only=False)
    wb_vals = load_workbook(BytesIO(tmpl_bytes), data_only=True)

    # Update UPB header where it is a literal string
    for sheet in ["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset"]:
        if sheet in wb.sheetnames and sheet in wb_vals.sheetnames:
            set_upb_header_in_sheet(wb[sheet], wb_vals[sheet], upb_col, header_row=4)

    # Write data preserving formulas
    sheet_to_df = {
        "Bridge Asset": bridge_asset,
        "Bridge Loan": bridge_loan,
        "Term Loan": term_loan,
        "Term Asset": term_asset,
    }

    for sheet_name, df in sheet_to_df.items():
        if sheet_name not in wb.sheetnames or sheet_name not in wb_vals.sheetnames:
            continue

        ws = wb[sheet_name]
        ws_v = wb_vals[sheet_name]

        hdr = header_tuples_from_ws(ws_v, header_row=4)
        hdr = [(c, normalize_header_name(h, upb_col)) for (c, h) in hdr]
        fcols = formula_col_indices(ws, start_row=5, header_row=4)

        write_df_to_sheet_preserve_formulas(ws, df, hdr, fcols, start_row=5)

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    st.success("Built Active Loans workbook.")
    st.download_button(
        "Download Active Loans Output",
        data=out.getvalue(),
        file_name=f"Active Loans_{run_dt.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
