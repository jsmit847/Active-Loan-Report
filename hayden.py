# ============================================================
# Active Loans Report Builder — ONE FILE (Streamlit) — LOGIN FIXED (HUD STYLE)
#
# Updates in this version:
# ✅ Servicer file logic tightened to match Hayden's weekly workflow
#    - Supports the standard weekly servicer files:
#        1) FCI_CVMaster (Account -> Next Due Date)
#        2) FCI_v1805510 (Account -> Next Due Date)
#        3) CoreVestLoanData (Loan Number -> Due Date, pad loan # with 4 leading zeros)
#        4) CoreVest_Data_Tape (BCM Loan# -> Next Payment Due Date)
#        5) CHL Streamline (Servicer Loan ID -> Next Due Date; servicer from "Servicing Company")
#        7) Midland (ServicerLoanNumber -> NextPaymentDate)
#        9) FCI_2012632 (Account -> Next Due Date)
#
# ✅ Active-loan validation (UPB > 0):
#    - Shows conflicts across servicer uploads (same loan ID with different UPB / dates)
#    - Shows servicer-active loans missing from the output
#    - Shows mismatches between selected servicer values and the built workbook
#
# ✅ REO fallback:
#    - If Loan Stage == REO and servicer UPB is missing/0, carry forward prior-week UPB
#      (or 0.00 if prior week not provided)
#
# Secrets required in .streamlit/secrets.toml
#   [salesforce]
#   client_id = "..."
#   auth_host  = "https://cvest.my.salesforce.com"
#   redirect_uri = "https://active-loan-report.streamlit.app/"  # ok (we normalize)
#   client_secret = "..."   # only if connected app requires it
# ============================================================

import base64
import hashlib
import re
import secrets
import time
import urllib.parse
from datetime import date, datetime
from io import BytesIO
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
from openpyxl import load_workbook
from simple_salesforce import Salesforce


# =============================================================================
# PERSONALIZATION (Primary user)
# =============================================================================
PRIMARY_USER_NAME = "Hayden"


def hey(name: str = PRIMARY_USER_NAME) -> str:
    return f"Hi {name} 👋"


# =============================================================================
# SALESFORCE REPORTS (IDs you gave)
# =============================================================================
REPORTS: Dict[str, Tuple[str, str]] = {
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
    "Active RM": "CAF Originator: Full Name",
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
    "Active RM": "Active RM",
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
# NORMALIZATION
# =============================================================================

def norm_id_series(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"[^0-9A-Za-z]", "", regex=True)
        .replace({"": pd.NA})
    )


def id_key_no_leading_zeros(s: pd.Series) -> pd.Series:
    """Normalized join key: alnum only, then lstrip(0)."""
    out = norm_id_series(s)
    out = out.astype("string").str.lstrip("0")
    return out.replace({"": pd.NA})


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
    return f"{run_dt.month}/{run_dt.day} UPB"


def is_reo_stage(val) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return "reo" in s and s != ""


def has_any_value(val) -> bool:
    if val is None:
        return False
    if isinstance(val, float) and np.isnan(val):
        return False
    if isinstance(val, str) and val.strip() == "":
        return False
    return True


def date_only(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NaT
    dt = pd.to_datetime(x, errors="coerce")
    if pd.isna(dt):
        return pd.NaT
    return dt.date()


# =============================================================================
# SALESFORCE AUTH (OAuth + PKCE) — EXACTLY HUD STYLE
# =============================================================================

def b64url_no_pad(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("utf-8")


def make_verifier() -> str:
    v = secrets.token_urlsafe(96)
    return v[:128]


def make_challenge(verifier: str) -> str:
    return b64url_no_pad(hashlib.sha256(verifier.encode("utf-8")).digest())


@st.cache_resource
def pkce_store():
    return {}


def exchange_code_for_token(
    token_url: str,
    code: str,
    verifier: str,
    client_id: str,
    redirect_uri: str,
    client_secret: Optional[str],
):
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
    cfg = st.secrets["salesforce"]

    CLIENT_ID = cfg["client_id"]
    AUTH_HOST = cfg.get("auth_host", "https://cvest.my.salesforce.com").rstrip("/")
    # ✅ THIS IS THE BIG FIX: match HUD behavior exactly
    REDIRECT_URI = cfg["redirect_uri"].rstrip("/")
    CLIENT_SECRET = cfg.get("client_secret")

    AUTH_URL = f"{AUTH_HOST}/services/oauth2/authorize"
    TOKEN_URL = f"{AUTH_HOST}/services/oauth2/token"

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

    store = pkce_store()

    # TTL cleanup (HUD behavior)
    now = time.time()
    TTL = 900
    for s, (_v, t0) in list(store.items()):
        if now - t0 > TTL:
            store.pop(s, None)

    # Callback
    if code:
        if not state or state not in store:
            st.error("Login link expired. Click login again.")
            st.stop()
        verifier, _t0 = store.pop(state)
        tok = exchange_code_for_token(TOKEN_URL, code, verifier, CLIENT_ID, REDIRECT_URI, CLIENT_SECRET)
        st.session_state.sf_token = tok
        st.query_params.clear()
        st.rerun()

    # Not logged in -> show login link
    if not st.session_state.sf_token:
        new_state = secrets.token_urlsafe(24)
        new_verifier = make_verifier()
        new_challenge = make_challenge(new_verifier)
        store[new_state] = (new_verifier, time.time())

        login_params = {
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "code_challenge": new_challenge,
            "code_challenge_method": "S256",
            "state": new_state,
            "prompt": "login",
            "scope": "api refresh_token",
        }
        login_url = AUTH_URL + "?" + urllib.parse.urlencode(login_params)

        st.info("Step 1: Log in to Salesforce.")
        st.link_button("Login", login_url)

        # Debug view that will immediately show if you’re accidentally using a different redirect_uri
        with st.expander("Debug (OAuth values being used)"):
            st.write("AUTH_HOST:")
            st.code(AUTH_HOST)
            st.write("REDIRECT_URI (normalized):")
            st.code(REDIRECT_URI)
            st.write("AUTH_URL:")
            st.code(AUTH_URL)
            st.write("TOKEN_URL:")
            st.code(TOKEN_URL)

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
    try:
        return sf.restful(path, method=method)
    except Exception as e:
        if _is_perm_error(str(e)):
            st.warning(f"⚠️ Salesforce access issue for: {path}. Returning empty results for this item.")
            return {}
        raise


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

    return pd.concat(chunks, ignore_index=True).drop_duplicates()


# =============================================================================
# SERVICER FILE PARSING
# =============================================================================

def date_from_filename(name: str) -> Optional[date]:
    m = re.search(r"(20\d{2})(\d{2})(\d{2})", name)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    m = re.search(r"(20\d{2})[-_](\d{1,2})[-_](\d{1,2})", name)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    m = re.search(r"(\d{2})[_-](\d{2})[_-](20\d{2})", name)
    if m:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))

    m = re.search(r"(\d{2})(\d{2})(20\d{2})", name)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return date(yy, mm, dd)

    return None


def sniff_excel_header(
    file_bytes: bytes,
    required_cols: Set[str],
    max_scan_rows: int = 35,
    sheet_candidates: Optional[Sequence[str]] = None,
) -> Optional[Tuple[str, int]]:
    wb = load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        sheetnames = list(sheet_candidates) if sheet_candidates else wb.sheetnames
        for sn in sheetnames:
            if sn not in wb.sheetnames:
                continue
            ws = wb[sn]
            max_c = min(ws.max_column or 250, 250)
            for r in range(1, max_scan_rows + 1):
                row_vals = [ws.cell(r, c).value for c in range(1, max_c + 1)]
                cols = {str(v).strip() for v in row_vals if v is not None and str(v).strip() != ""}
                if required_cols.issubset(cols):
                    return sn, r
        return None
    finally:
        wb.close()


def _corevest_pad_loan_number(raw: pd.Series) -> pd.Series:
    s = norm_id_series(raw)
    s = s.fillna(pd.NA).astype("string")
    # add four leading zeros unless already present
    s = s.apply(lambda x: x if pd.isna(x) else (x if x.startswith("0000") else f"0000{x}"))
    return s.replace({"": pd.NA})


def parse_servicer_upload(upload) -> pd.DataFrame:
    name = upload.name
    b = upload.getvalue()

    d_file = date_from_filename(name)
    as_of_file = pd.to_datetime(d_file) if d_file else pd.NaT

    # ----------------------------
    # CSV: CHL Streamline
    # ----------------------------
    if name.lower().endswith(".csv"):
        df = pd.read_csv(BytesIO(b))
        req = {"Servicer Loan ID", "UPB"}
        if not req.issubset(set(df.columns)):
            raise ValueError(f"CSV doesn't look like CHL Streamline (missing {req - set(df.columns)}).")

        servicer = df.get("Servicing Company", pd.Series(["CHL Streamline"] * len(df))).astype("string")
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": servicer,
                "servicer_id": norm_id_series(df["Servicer Loan ID"]),
                "upb": df["UPB"].apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Current Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Performing Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # ----------------------------
    # XLSX: detect by columns
    # ----------------------------
    checks: List[Tuple[str, Set[str], Optional[Sequence[str]]]] = [
        ("CHL", {"Servicer Loan ID", "UPB"}, None),
        ("CoreVestLoanData", {"Loan Number", "Current UPB", "Due Date", "Maturity Date", "Loan Status"}, None),
        ("CoreVest_Data_Tape", {"BCM Loan#", "Principal Balance", "Next Payment Due Date", "Maturity Date"}, ["Loan"]),
        ("FCI", {"Account", "Current Balance", "Next Due Date", "Maturity Date", "Status"}, None),
        ("Midland", {"ServicerLoanNumber", "UPB$", "NextPaymentDate", "MaturityDate", "ServicerLoanStatus"}, None),
    ]

    detected = None
    sheet_name = None
    header_row = None
    for serv, req, sheets in checks:
        hit = sniff_excel_header(b, req, sheet_candidates=sheets)
        if hit is not None:
            detected, sheet_name, header_row = serv, hit[0], hit[1]
            break

    if detected is None or sheet_name is None or header_row is None:
        raise ValueError(
            "Could not detect servicer file type from columns (FCI / CoreVestLoanData / CoreVest_Data_Tape / Midland / CHL Streamline)."
        )

    df = pd.read_excel(BytesIO(b), sheet_name=sheet_name, header=header_row - 1)

    # CHL Streamline (xlsx)
    if detected == "CHL":
        servicer = df.get("Servicing Company", pd.Series(["CHL Streamline"] * len(df))).astype("string")
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": servicer,
                "servicer_id": norm_id_series(df["Servicer Loan ID"]),
                "upb": df["UPB"].apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Current Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Performing Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # CoreVestLoanData (Statebridge style)
    if detected == "CoreVestLoanData":
        # Hayden rule: add four leading zeros in front of Loan Number for matching.
        # We only apply padding when the file looks like the CoreVestLoanData export.
        needs_pad = (
            ("corevestloandata" in name.lower())
            or ("Investor ID" in df.columns)
            or (str(sheet_name).lower().find("corevest") >= 0)
        )
        sid = _corevest_pad_loan_number(df["Loan Number"]) if needs_pad else norm_id_series(df["Loan Number"])
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "Statebridge",
                "servicer_id": sid,
                "upb": pd.to_numeric(df.get("Current UPB", np.nan), errors="coerce"),
                "suspense": pd.to_numeric(df.get("Unapplied Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Loan Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # CoreVest Data Tape (Berkadia style)
    if detected == "CoreVest_Data_Tape":
        status = df.get("Loan Status", pd.Series(["Active"] * len(df))).astype("string")
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "Berkadia",
                "servicer_id": norm_id_series(df["BCM Loan#"]),
                "upb": pd.to_numeric(df.get("Principal Balance", np.nan), errors="coerce"),
                "suspense": pd.to_numeric(df.get("Suspense Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Payment Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": status,
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # FCI (all variants)
    if detected == "FCI":
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "FCI",
                "servicer_id": norm_id_series(df["Account"]),
                "upb": pd.to_numeric(df.get("Current Balance", np.nan), errors="coerce"),
                "suspense": pd.to_numeric(df.get("Suspense Pmt.", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    # Midland
    if detected == "Midland":
        raw = df["ServicerLoanNumber"].astype("string").str.strip()
        raw = raw.str.replace(r"COM$", "", regex=True)
        raw = raw.str.replace(r"[^0-9A-Za-z]", "", regex=True).str.lstrip("0")

        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "Midland",
                "servicer_id": raw.replace({"": pd.NA}),
                "upb": df.get("UPB$", pd.Series([np.nan] * len(df))).apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df.get("NextPaymentDate", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("MaturityDate", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("ServicerLoanStatus", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    raise ValueError("Unhandled servicer type.")


def build_servicer_lookup(servicer_uploads: List) -> Tuple[pd.DataFrame, date, pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    file_dates: List[date] = []

    for f in servicer_uploads:
        df = parse_servicer_upload(f)
        frames.append(df)
        d = date_from_filename(f.name)
        if d:
            file_dates.append(d)

    full = (
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

    if not full.empty:
        full = full.dropna(subset=["servicer_id"]).copy()
        full["_sid_key"] = id_key_no_leading_zeros(full["servicer_id"])
        full = full.dropna(subset=["_sid_key"]).copy()

        full["_has_upb"] = full["upb"].notna().astype(int)
        full["_has_npd"] = full["next_payment_date"].notna().astype(int)
        full["_has_mat"] = full["maturity_date"].notna().astype(int)

        # Choose "best" row per loan:
        #   latest as_of, most complete fields, then highest UPB (stable tie-break)
        full = full.sort_values(
            ["_sid_key", "as_of", "_has_upb", "_has_npd", "_has_mat", "upb"],
            ascending=[True, True, True, True, True, True],
        )
        join = full.drop_duplicates(["_sid_key"], keep="last").drop(
            columns=["_has_upb", "_has_npd", "_has_mat"], errors="ignore"
        )
        full = full.drop(columns=["_has_upb", "_has_npd", "_has_mat"], errors="ignore")
    else:
        full["_sid_key"] = pd.Series(dtype="string")
        join = full.copy()

    run_date = max(file_dates) if file_dates else date.today()
    return join, run_date, full


# =============================================================================
# LAST WEEK REPORT CARRY-FORWARD
#   - REO Date (already)
#   - Bridge Loan manual columns (already)
#   - Prior UPB values for REO fallback (NEW)
# =============================================================================

def read_tab_df_from_active_loans(file_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=3)
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _find_upb_col(cols: Sequence[str]) -> Optional[str]:
    for c in cols:
        if isinstance(c, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", c):
            return c
    return None


def build_prev_maps(prev_bytes: bytes) -> dict:
    out: dict = {}

    # Term Loan: carry forward REO Date and (NEW) previous UPB
    try:
        tl = read_tab_df_from_active_loans(prev_bytes, "Term Loan")
        if "Deal Number" in tl.columns and "REO Date" in tl.columns:
            tmp = tl[["Deal Number", "REO Date"]].copy()
            tmp["_deal_key"] = norm_id_series(tmp["Deal Number"])
            out["term_loan_reo"] = tmp.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")

        upb_col_prev = _find_upb_col(tl.columns)
        if upb_col_prev and "Deal Number" in tl.columns:
            tmpu = tl[["Deal Number", upb_col_prev]].copy()
            tmpu["_deal_key"] = norm_id_series(tmpu["Deal Number"])
            tmpu["_prev_upb"] = tmpu[upb_col_prev].apply(money_to_float)
            out["term_loan_upb"] = tmpu.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")[["_deal_key", "_prev_upb"]]
    except Exception:
        pass

    # Bridge Loan: carry forward manual columns and (NEW) previous UPB
    try:
        bl = read_tab_df_from_active_loans(prev_bytes, "Bridge Loan")
        keep = [c for c in ["Deal Number", "State(s)", "Loan Level Delinquency", "Special Focus (Y/N)"] if c in bl.columns]
        if "Deal Number" in keep and len(keep) > 1:
            tmp = bl[keep].copy()
            tmp["_deal_key"] = norm_id_series(tmp["Deal Number"])
            out["bridge_loan_manual"] = tmp.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")

        upb_col_prev = _find_upb_col(bl.columns)
        if upb_col_prev and "Deal Number" in bl.columns:
            tmpu = bl[["Deal Number", upb_col_prev]].copy()
            tmpu["_deal_key"] = norm_id_series(tmpu["Deal Number"])
            tmpu["_prev_upb"] = tmpu[upb_col_prev].apply(money_to_float)
            out["bridge_loan_upb"] = tmpu.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")[["_deal_key", "_prev_upb"]]
    except Exception:
        pass

    return out


# =============================================================================
# BUILD HELPERS
# =============================================================================

def _yn_from_bool_series(s: pd.Series) -> pd.Series:
    return s.fillna(False).map(lambda x: "Y" if bool(x) else "N")


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
    prev_maps: dict,
) -> pd.DataFrame:
    out = pd.DataFrame()

    for col, label in BRIDGE_ASSET_FROM_BRIDGE_MATURITY.items():
        out[col] = sf_spine[label] if label in sf_spine.columns else None

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_sid_key"] = id_key_no_leading_zeros(out.get("Servicer ID", pd.Series([None] * len(out))))
    out["_asset_key"] = norm_id_series(out.get("Asset ID", pd.Series([None] * len(out))))

    # Do Not Lend
    if not sf_dnl.empty and "Deal Loan Number" in sf_dnl.columns:
        dnl = sf_dnl.copy()
        dnl["_deal_key"] = norm_id_series(dnl["Deal Loan Number"])
        if "Do Not Lend" in dnl.columns:
            dnl_flag = dnl.groupby("_deal_key")["Do Not Lend"].max().reset_index()
            out = out.merge(dnl_flag, on="_deal_key", how="left")
            out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend"])
            out = out.drop(columns=["Do Not Lend"], errors="ignore")

    # Valuation
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

    # AM assignments
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

    # Active RM fallback
    if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns and "CAF Originator" in sf_arm.columns:
        arm = sf_arm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key", "CAF Originator"]].drop_duplicates("_deal_key")
        out = out.merge(arm, on="_deal_key", how="left")
        if "Active RM" not in out.columns:
            out["Active RM"] = out["CAF Originator"]
        else:
            out["Active RM"] = out["Active RM"].fillna(out["CAF Originator"])
        out = out.drop(columns=["CAF Originator"], errors="ignore")

    # Servicer join (loan-level values)
    if not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy()
        s = s.rename(
            columns={
                "servicer": "Servicer",
                "upb": "_loan_upb",
                "suspense": "_loan_suspense",
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Servicer Maturity Date",
                "status": "Servicer Status",
            }
        )

        out = out.merge(
            s[["_sid_key", "Servicer", "_loan_upb", "_loan_suspense", "Next Payment Date", "Servicer Maturity Date", "Servicer Status", "source_file"]],
            on="_sid_key",
            how="left",
        )

        # ✅ REO fallback (Hayden rule)
        # If Loan Stage == REO and servicer UPB missing/0, use last week's UPB (or 0.00).
        if "bridge_loan_upb" in prev_maps:
            prev_upb = prev_maps["bridge_loan_upb"].copy()
            out = out.merge(prev_upb, on="_deal_key", how="left")
        else:
            out["_prev_upb"] = np.nan

        stage_series = out.get("Loan Stage", pd.Series([None] * len(out)))
        reo_mask = stage_series.apply(is_reo_stage)
        loan_upb = pd.to_numeric(out.get("_loan_upb", np.nan), errors="coerce")
        prev_upb_vals = pd.to_numeric(out.get("_prev_upb", np.nan), errors="coerce")

        fill_val = prev_upb_vals.fillna(0.0)
        out["_loan_upb"] = np.where(reo_mask & ((loan_upb.isna()) | (loan_upb <= 0)), fill_val, loan_upb)

        # Allocation weights
        w = pd.to_numeric(sf_spine.get("Current UPB", pd.Series([np.nan] * len(out))), errors="coerce")
        out["_w"] = w
        out["_w_sum"] = out.groupby("_sid_key")["_w"].transform("sum")
        out["_n_in_loan"] = out.groupby("_sid_key")["_sid_key"].transform("size").replace({0: np.nan})

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

        out = out.drop(columns=["_prev_upb"], errors="ignore")

    # Funded amount
    if "Approved Advance Amount Funded" in sf_spine.columns:
        out["SF Funded Amount"] = pd.to_numeric(sf_spine["Approved Advance Amount Funded"], errors="coerce")
    else:
        out["SF Funded Amount"] = (
            pd.to_numeric(out.get("Initial Disbursement Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Renovation Holdback Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Interest Allocation Funded", 0), errors="coerce").fillna(0)
        )

    # Ensure required text columns exist
    if "Portfolio" not in out.columns:
        out["Portfolio"] = ""
    if "Segment" not in out.columns:
        out["Segment"] = ""
    if "Strategy Grouping" not in out.columns:
        out["Strategy Grouping"] = ""

    if "Is Special Asset (Y/N)" in out.columns:
        out["Is Special Asset (Y/N)"] = _yn_from_bool_series(out["Is Special Asset (Y/N)"])

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
    prev_maps: dict,
) -> pd.DataFrame:
    out = pd.DataFrame()

    for col, label in TERM_LOAN_FROM_TERM_EXPORT.items():
        out[col] = sf_term[label] if label in sf_term.columns else None

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))

    if "Do Not Lend (Y/N)" in out.columns:
        out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend (Y/N)"])

    # Sold-to
    if not sf_sold.empty and "Deal Loan Number" in sf_sold.columns:
        sold = sf_sold.copy()
        sold["_deal_key"] = norm_id_series(sold["Deal Loan Number"])
        if "Sold Loan: Sold To" in sold.columns:
            sold = sold[["_deal_key", "Sold Loan: Sold To"]].drop_duplicates("_deal_key")
            out = out.merge(sold, on="_deal_key", how="left")
            out["Loan Buyer"] = out["Sold Loan: Sold To"]
            out = out.drop(columns=["Sold Loan: Sold To"], errors="ignore")

    # Active RM fallback
    if "Active RM" not in out.columns:
        out["Active RM"] = ""
    if out["Active RM"].isna().all():
        if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns and "CAF Originator" in sf_arm.columns:
            arm = sf_arm.copy()
            arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
            arm = arm[["_deal_key", "CAF Originator"]].drop_duplicates("_deal_key")
            out = out.merge(arm, on="_deal_key", how="left")
            out["Active RM"] = out["Active RM"].fillna(out["CAF Originator"]).fillna("")
            out = out.drop(columns=["CAF Originator"], errors="ignore")

    # Asset Manager
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

    # Servicer ID from SF term export
    out["Servicer ID"] = sf_term["Servicer Commitment Id"] if "Servicer Commitment Id" in sf_term.columns else None
    out["_sid_key"] = id_key_no_leading_zeros(out["Servicer ID"].astype("string"))

    # Join servicer values: UPB / Next Pay / Maturity
    if not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy()
        s2 = s.rename(
            columns={
                "servicer": "_servicer_file",
                "upb": upb_col,
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Maturity Date",
            }
        )[["_sid_key", "_servicer_file", upb_col, "Next Payment Date", "Maturity Date"]]

        out = out.merge(s2, on="_sid_key", how="left")

        out["Servicer"] = out.get("Servicer", pd.Series(["" for _ in range(len(out))], dtype="string"))
        out["Servicer"] = out["Servicer"].fillna(out["_servicer_file"]).fillna("")
        out = out.drop(columns=["_servicer_file"], errors="ignore")

    # Carry forward REO Date
    out["REO Date"] = ""
    if "term_loan_reo" in prev_maps:
        reo = prev_maps["term_loan_reo"][["_deal_key", "REO Date"]].copy()
        out = out.merge(reo, on="_deal_key", how="left", suffixes=("", "_prev"))
        out["REO Date"] = out["REO Date_prev"].fillna("")
        out = out.drop(columns=["REO Date_prev"], errors="ignore")

    # ✅ REO balance fallback (if REO Date exists and servicer UPB is missing/0)
    if "term_loan_upb" in prev_maps and upb_col in out.columns:
        prevu = prev_maps["term_loan_upb"].copy()
        out = out.merge(prevu, on="_deal_key", how="left")

        reo_mask = out["REO Date"].apply(has_any_value)
        cur_upb = pd.to_numeric(out[upb_col], errors="coerce")
        prev_upb = pd.to_numeric(out.get("_prev_upb", np.nan), errors="coerce")
        fill_val = prev_upb.fillna(0.0)
        out[upb_col] = np.where(reo_mask & ((cur_upb.isna()) | (cur_upb <= 0)), fill_val, cur_upb)
        out = out.drop(columns=["_prev_upb"], errors="ignore")

    if "Portfolio" not in out.columns:
        out["Portfolio"] = ""
    if "Segment" not in out.columns:
        out["Segment"] = ""

    return out


# =============================================================================
# BUILD: TERM ASSET (ALA-weight UPB from Term Loan)
# =============================================================================

def build_term_asset(sf_term_asset: pd.DataFrame, term_loan: pd.DataFrame, upb_col: str) -> pd.DataFrame:
    out = pd.DataFrame()

    for col, label in TERM_ASSET_FROM_TERM_ASSET_REPORT.items():
        out[col] = sf_term_asset[label] if label in sf_term_asset.columns else None

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["CPP JV"] = ""

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
# BUILD: BRIDGE LOAN (roll-up Bridge Asset)
# =============================================================================

def build_bridge_loan(bridge_asset: pd.DataFrame, upb_col: str, prev_maps: dict) -> pd.DataFrame:
    ba = bridge_asset.copy()
    g = ba.groupby("_deal_key", dropna=True)

    def _first(series: pd.Series):
        s = series.dropna()
        return s.iloc[0] if len(s) else ""

    def _max_dt(series: pd.Series):
        s = pd.to_datetime(series, errors="coerce").dropna()
        return s.max() if len(s) else ""

    def _min_dt(series: pd.Series):
        s = pd.to_datetime(series, errors="coerce").dropna()
        return s.min() if len(s) else ""

    out = pd.DataFrame(
        {
            "Deal Number": g["Deal Number"].first() if "Deal Number" in ba.columns else pd.Series(dtype="string"),
            "Portfolio": g["Portfolio"].apply(_first) if "Portfolio" in ba.columns else "",
            "Loan Buyer": g["Loan Buyer"].first() if "Loan Buyer" in ba.columns else "",
            "Financing": g["Financing"].first() if "Financing" in ba.columns else "",
            "Servicer ID": g["Servicer ID"].first() if "Servicer ID" in ba.columns else "",
            "Servicer": g["Servicer"].apply(_first) if "Servicer" in ba.columns else "",
            "Deal Name": g["Deal Name"].first() if "Deal Name" in ba.columns else "",
            "Borrower Name": g["Borrower Entity"].first() if "Borrower Entity" in ba.columns else "",
            "Account ": g["Account Name"].first() if "Account Name" in ba.columns else "",
            "Do Not Lend (Y/N)": g["Do Not Lend (Y/N)"].max() if "Do Not Lend (Y/N)" in ba.columns else "",
            "Primary Contact": g["Primary Contact"].first() if "Primary Contact" in ba.columns else "",
            "Number of Assets": g["Asset ID"].nunique() if "Asset ID" in ba.columns else 0,
            "# of Units": pd.to_numeric(g["# of Units"].sum(min_count=1), errors="coerce") if "# of Units" in ba.columns else np.nan,
            "State(s)": g["State"].apply(lambda s: ", ".join(sorted({str(x).strip() for x in s.dropna() if str(x).strip() != ""}))) if "State" in ba.columns else "",
            "Origination Date": g["Origination Date"].apply(_min_dt) if "Origination Date" in ba.columns else "",
            "Last Funding Date": g["Last Funding Date"].apply(_max_dt) if "Last Funding Date" in ba.columns else "",
            "Original Maturity Date": g["Original Loan Maturity date"].first() if "Original Loan Maturity date" in ba.columns else "",
            "Current Maturity Date": g["Current Loan Maturity date"].first() if "Current Loan Maturity date" in ba.columns else "",
            "Next Advance Maturity Date": g["Servicer Maturity Date"].first() if "Servicer Maturity Date" in ba.columns else "",
            "Next Payment Date": g["Next Payment Date"].apply(_min_dt) if "Next Payment Date" in ba.columns else "",
            "Days Past Due": "",
            "Loan Level Delinquency": "",
            "Loan Commitment": "",
            "Active Funded Amount": pd.to_numeric(g["SF Funded Amount"].sum(min_count=1), errors="coerce") if "SF Funded Amount" in ba.columns else np.nan,
            upb_col: pd.to_numeric(g[upb_col].sum(min_count=1), errors="coerce") if upb_col in ba.columns else np.nan,
            "Suspense Balance": pd.to_numeric(g["Suspense Balance"].sum(min_count=1), errors="coerce") if "Suspense Balance" in ba.columns else np.nan,
            "Remaining Commitment": "",
            "Most Recent Valuation Date": "",
            "Most Recent As-Is Value": np.nan,
            "Most Recent ARV": np.nan,
            "Initial Disbursement Funded": pd.to_numeric(g["Initial Disbursement Funded"].sum(min_count=1), errors="coerce") if "Initial Disbursement Funded" in ba.columns else np.nan,
            "Renovation Holdback": pd.to_numeric(g["Renovation Holdback"].sum(min_count=1), errors="coerce") if "Renovation Holdback" in ba.columns else np.nan,
            "Renovation HB Funded": pd.to_numeric(g["Renovation Holdback Funded"].sum(min_count=1), errors="coerce") if "Renovation Holdback Funded" in ba.columns else np.nan,
            "Renovation HB Remaining": pd.to_numeric(g["Renovation Holdback Remaining"].sum(min_count=1), errors="coerce") if "Renovation Holdback Remaining" in ba.columns else np.nan,
            "Interest Allocation": pd.to_numeric(g["Interest Allocation"].sum(min_count=1), errors="coerce") if "Interest Allocation" in ba.columns else np.nan,
            "Interest Allocation Funded": pd.to_numeric(g["Interest Allocation Funded"].sum(min_count=1), errors="coerce") if "Interest Allocation Funded" in ba.columns else np.nan,
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
            "Special Focus (Y/N)": "",
            "Asset Manager 1": g["Asset Manager 1"].apply(_first) if "Asset Manager 1" in ba.columns else "",
            "AM 1 Assigned Date": g["AM 1 Assigned Date"].apply(_first) if "AM 1 Assigned Date" in ba.columns else "",
            "Asset Manager 2": g["Asset Manager 2"].apply(_first) if "Asset Manager 2" in ba.columns else "",
            "AM 2 Assigned Date": g["AM 2 Assigned Date"].apply(_first) if "AM 2 Assigned Date" in ba.columns else "",
            "Construction Mgr.": g["Construction Mgr."].apply(_first) if "Construction Mgr." in ba.columns else "",
            "CM Assigned Date": g["CM Assigned Date"].apply(_first) if "CM Assigned Date" in ba.columns else "",
            "AM Commentary": "",
        }
    ).reset_index(drop=True)

    # Carry forward manual cols
    if "bridge_loan_manual" in prev_maps and not out.empty:
        man = prev_maps["bridge_loan_manual"].copy()
        out2 = out.copy()
        out2["_deal_key"] = norm_id_series(out2["Deal Number"])
        out2 = out2.merge(man, on="_deal_key", how="left", suffixes=("", "_prev"))
        for c in ["State(s)", "Loan Level Delinquency", "Special Focus (Y/N)"]:
            if f"{c}_prev" in out2.columns:
                out2[c] = out2[c].replace({"": np.nan}).fillna(out2[f"{c}_prev"]).fillna("")
                out2 = out2.drop(columns=[f"{c}_prev"], errors="ignore")
        out2 = out2.drop(columns=["_deal_key"], errors="ignore")
        out = out2

    out["Special Focus (Y/N)"] = out["Special Focus (Y/N)"].replace({"": "N"}).fillna("N")
    return out


# =============================================================================
# EXCEL OUTPUT HELPERS
# =============================================================================

def header_tuples_from_ws(ws_values, header_row: int = 4) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    row = list(ws_values.iter_rows(min_row=header_row, max_row=header_row, values_only=False))[0]
    for col_idx, cell in enumerate(row, start=1):
        v = cell.value
        h = "" if v is None else str(v).strip()
        if h:
            out.append((col_idx, h))
    return out


def formula_col_indices(ws_formula, start_row: int = 5, header_row: int = 4) -> Set[int]:
    fcols: Set[int] = set()
    row = list(ws_formula.iter_rows(min_row=header_row, max_row=header_row, values_only=False))[0]
    for col_idx, _cell in enumerate(row, start=1):
        v = ws_formula.cell(start_row, col_idx).value
        if isinstance(v, str) and v.startswith("="):
            fcols.add(col_idx)
    return fcols


def normalize_header_name(h: str, upb_header: str) -> str:
    if isinstance(h, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", h):
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


def _parse_mmdd_from_upb_header(h: str) -> Optional[Tuple[int, int]]:
    if not isinstance(h, str):
        return None
    m = re.search(r"\b(\d{1,2})/(\d{1,2})\s*UPB\b", h)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def set_upb_header_in_sheet(ws_formula, ws_values, new_upb_header: str, header_row: int = 4):
    hdr = header_tuples_from_ws(ws_values, header_row=header_row)
    for col_idx, h in hdr:
        if isinstance(h, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", h):
            ws_formula.cell(header_row, col_idx).value = new_upb_header
            return


def update_run_date_in_row3(ws_formula, ws_values, run_dt: date, header_row: int = 4, date_row: int = 3):
    hdr = header_tuples_from_ws(ws_values, header_row=header_row)
    old_mmdd: Optional[Tuple[int, int]] = None
    for _c, h in hdr:
        mmdd = _parse_mmdd_from_upb_header(h)
        if mmdd:
            old_mmdd = mmdd
            break

    if not old_mmdd:
        return

    old_m, old_d = old_mmdd

    row = list(ws_values.iter_rows(min_row=date_row, max_row=date_row, values_only=False))[0]
    for col_idx, cell in enumerate(row, start=1):
        v = cell.value
        if isinstance(v, datetime):
            if v.month == old_m and v.day == old_d:
                ws_formula.cell(date_row, col_idx).value = run_dt
        elif isinstance(v, date):
            if v.month == old_m and v.day == old_d:
                ws_formula.cell(date_row, col_idx).value = run_dt


# =============================================================================
# REPORT SELECTION / CACHING
# =============================================================================

def required_report_keys(target: str) -> Set[str]:
    need: Set[str] = set()
    if target in ("Bridge Asset", "Bridge Loan", "All"):
        need |= {"bridge_maturity", "do_not_lend", "valuation", "am_assignments", "active_rm"}
    if target in ("Term Loan", "Term Asset", "All"):
        need |= {"term_export", "sold_term", "am_assignments", "active_rm"}
    if target in ("Term Asset", "All"):
        need |= {"term_asset"}
    return need


def pull_reports(sf: Salesforce, keys: Set[str]) -> Dict[str, pd.DataFrame]:
    if "report_cache" not in st.session_state:
        st.session_state.report_cache = {}

    cache: dict = st.session_state.report_cache
    out: Dict[str, pd.DataFrame] = {}

    for k in keys:
        nm, rid = REPORTS[k]
        if rid in cache:
            out[k] = cache[rid]
            continue
        with st.spinner(f"Pulling Salesforce report: {nm} ({rid})"):
            df = run_report_all_rows(sf, rid, page_size=2000)
        cache[rid] = df
        out[k] = df

    return out


# =============================================================================
# VALIDATION HELPERS (servicer vs built)
# =============================================================================

def _normalize_id_list(series: pd.Series) -> pd.Series:
    return id_key_no_leading_zeros(series)


def summarize_servicer_conflicts(serv_full: pd.DataFrame) -> pd.DataFrame:
    if serv_full is None or serv_full.empty:
        return pd.DataFrame()

    d = serv_full.copy()
    d = d.dropna(subset=["_sid_key"]).copy()

    def _uniq(vals):
        v = [x for x in vals if has_any_value(x)]
        # normalize dates
        out = []
        for x in v:
            if isinstance(x, (datetime, date)):
                out.append(pd.to_datetime(x).date().isoformat())
            else:
                out.append(str(x))
        return sorted(set(out))

    agg = (
        d.groupby("_sid_key")
        .agg(
            servicer=("servicer", lambda s: ", ".join(sorted(set([str(x) for x in s.dropna().unique()])))),
            sources=("source_file", lambda s: ", ".join(sorted(set([str(x) for x in s.dropna().unique()])))),
            n_sources=("source_file", "nunique"),
            upb_values=("upb", lambda s: _uniq([round(float(x), 2) for x in s.dropna().tolist() if pd.notna(x)])),
            next_payment_dates=("next_payment_date", lambda s: _uniq(s.tolist())),
            maturity_dates=("maturity_date", lambda s: _uniq(s.tolist())),
            statuses=("status", lambda s: _uniq(s.tolist())),
            max_upb=("upb", "max"),
        )
        .reset_index()
    )

    # Conflicts = more than one unique value in any field (for active-ish loans)
    def _has_conflict(row) -> bool:
        if row.get("n_sources", 0) <= 1:
            return False
        fields = ["upb_values", "next_payment_dates", "maturity_dates", "statuses"]
        return any(isinstance(row[f], list) and len(row[f]) > 1 for f in fields)

    agg["has_conflict"] = agg.apply(_has_conflict, axis=1)
    # Only show where max_upb > 0 OR there is conflict
    agg = agg[(agg["has_conflict"]) | (pd.to_numeric(agg["max_upb"], errors="coerce").fillna(0) > 0)].copy()

    # Make list columns printable
    for c in ["upb_values", "next_payment_dates", "maturity_dates", "statuses"]:
        agg[c] = agg[c].apply(lambda x: "; ".join(x) if isinstance(x, list) else "")

    return agg.sort_values(["has_conflict", "max_upb"], ascending=[False, False])


def compare_expected_vs_actual(
    expected: pd.DataFrame,
    actual: pd.DataFrame,
    id_col_expected: str,
    id_col_actual: str,
    cols_map: Dict[str, str],
    active_only: bool = True,
    upb_col_expected: str = "upb",
    upb_tolerance: float = 1.0,
) -> pd.DataFrame:
    """Return mismatches between expected(servicer) and actual(output) at loan level."""
    if expected is None or expected.empty or actual is None or actual.empty:
        return pd.DataFrame()

    e = expected.copy()
    a = actual.copy()

    e["_id"] = _normalize_id_list(e[id_col_expected])
    a["_id"] = _normalize_id_list(a[id_col_actual])

    e = e.dropna(subset=["_id"]).copy()
    a = a.dropna(subset=["_id"]).copy()

    if active_only and upb_col_expected in e.columns:
        e = e[pd.to_numeric(e[upb_col_expected], errors="coerce").fillna(0) > 0].copy()

    # Prepare expected fields
    keep_e = ["_id", "servicer", upb_col_expected]
    for e_col in cols_map.keys():
        if e_col in e.columns and e_col not in keep_e:
            keep_e.append(e_col)
    e2 = e[keep_e].drop_duplicates("_id")

    # Prepare actual fields
    keep_a = ["_id"]
    for a_col in cols_map.values():
        if a_col in a.columns and a_col not in keep_a:
            keep_a.append(a_col)
    a2 = a[keep_a].drop_duplicates("_id")

    m = e2.merge(a2, on="_id", how="left", suffixes=("_exp", "_act"))

    # Compute mismatches
    mism = []
    for _, row in m.iterrows():
        # UPB compare
        exp_upb = pd.to_numeric(row.get(upb_col_expected), errors="coerce")
        act_upb = pd.to_numeric(row.get(cols_map.get(upb_col_expected, "")), errors="coerce")
        if pd.notna(exp_upb) and pd.notna(act_upb):
            if abs(float(exp_upb) - float(act_upb)) > upb_tolerance:
                mism.append(True)
                continue
        elif pd.notna(exp_upb) and pd.isna(act_upb):
            # expected active but missing in output
            mism.append(True)
            continue

        # other fields (dates/strings)
        diff_found = False
        for e_col, a_col in cols_map.items():
            if e_col == upb_col_expected:
                continue
            exp = row.get(e_col)
            act = row.get(a_col)

            if "date" in e_col.lower() or "date" in a_col.lower():
                exp_d = date_only(exp)
                act_d = date_only(act)
                if (pd.isna(exp_d) and pd.isna(act_d)):
                    continue
                if exp_d != act_d:
                    diff_found = True
                    break
            else:
                exp_s = "" if exp is None or (isinstance(exp, float) and np.isnan(exp)) else str(exp).strip()
                act_s = "" if act is None or (isinstance(act, float) and np.isnan(act)) else str(act).strip()
                if exp_s != act_s:
                    diff_found = True
                    break

        mism.append(diff_found)

    m["mismatch"] = mism

    # Build a tidy output
    if not m["mismatch"].any():
        return pd.DataFrame()

    show_cols = ["_id", "servicer", upb_col_expected]
    for e_col, a_col in cols_map.items():
        if e_col != upb_col_expected:
            show_cols += [e_col, a_col]
    show_cols = [c for c in show_cols if c in m.columns]

    out = m[m["mismatch"]].copy()
    out = out[show_cols]
    out = out.rename(columns={"_id": "servicer_id_key"})
    return out


# =============================================================================
# STREAMLIT UI
# =============================================================================

st.set_page_config(page_title="Active Loans Builder", layout="wide")
st.title("Active Loans Report Builder")
st.subheader(hey())

st.markdown(
    """
Welcome! This tool builds the **Active Loans** workbook using **Salesforce report pulls** and **servicer uploads**.

### What you’ll do
1) Upload the **Active Loans TEMPLATE** workbook  
2) Upload the **current servicer files**  
3) Log in to **Salesforce** when prompted  
4) Choose **which sheet to build** (fast) or **All** (slower)
"""
)

colA, colB = st.columns([1.3, 1.0])
with colA:
    template_upload = st.file_uploader("Upload Active Loans TEMPLATE (.xlsx)", type=["xlsx"])
    prev_upload = st.file_uploader(
        "Upload LAST WEEK'S Active Loans report (.xlsx) for carry-forward (optional)", type=["xlsx"]
    )
with colB:
    servicer_uploads = st.file_uploader(
        "Upload current servicer files (csv/xlsx)", type=["csv", "xlsx"], accept_multiple_files=True
    )

build_target = st.selectbox(
    "Which sheet do you want to build right now?",
    options=["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset", "All"],
    index=0,
)

use_sf = st.checkbox("Use Salesforce (recommended)", value=True)
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

name_guess = date.today()
if servicer_uploads:
    dts = [date_from_filename(u.name) for u in servicer_uploads]
    dts = [d for d in dts if d]
    if dts:
        name_guess = max(dts)

use_filename_date = st.checkbox("Use filename date for UPB header (recommended)", value=True)
manual_run_date = st.date_input("UPB header date (M/D UPB)", value=name_guess, disabled=use_filename_date)

if st.button("Clear cached Salesforce reports", type="secondary"):
    st.session_state.report_cache = {}
    st.success("Cleared Salesforce report cache for this session.")

build_btn = st.button("Build", type="primary")

if build_btn:
    if not template_upload:
        st.error("Upload the template workbook first.")
        st.stop()

    if not servicer_uploads:
        st.error("Upload the servicer files. UPB/Next Payment/Maturity/Status come from them.")
        st.stop()

    if use_sf and sf is None:
        st.error("Salesforce login is required (or uncheck the Salesforce option).")
        st.stop()

    prev_maps: dict = {}
    if prev_upload:
        with st.spinner("Reading last week's report (carry-forward)..."):
            prev_maps = build_prev_maps(prev_upload.getvalue())

    with st.spinner("Parsing servicer files..."):
        serv_join, detected_run_date, serv_full = build_servicer_lookup(servicer_uploads)

    run_dt = detected_run_date if use_filename_date else manual_run_date
    upb_col = make_upb_header(run_dt)

    st.markdown("### Servicer lookup preview")
    st.caption(f"Detected run date from filenames: **{detected_run_date.isoformat()}**")
    st.caption(f"UPB column header to be used: **{upb_col}**")
    st.dataframe(serv_full.head(30), use_container_width=True)

    if use_sf:
        need = required_report_keys(build_target)
        dfs = pull_reports(sf, need)
    else:
        st.error("This version requires Salesforce API pulls.")
        st.stop()

    bridge_asset = None
    bridge_loan = None
    term_loan = None
    term_asset = None

    if build_target in ("Bridge Asset", "Bridge Loan", "All"):
        with st.spinner("Building Bridge Asset..."):
            bridge_asset = build_bridge_asset(
                dfs.get("bridge_maturity", pd.DataFrame()),
                dfs.get("do_not_lend", pd.DataFrame()),
                dfs.get("valuation", pd.DataFrame()),
                dfs.get("am_assignments", pd.DataFrame()),
                dfs.get("active_rm", pd.DataFrame()),
                serv_join,
                upb_col,
                prev_maps,
            )

    if build_target in ("Bridge Loan", "All"):
        if bridge_asset is None:
            bridge_asset = build_bridge_asset(
                dfs.get("bridge_maturity", pd.DataFrame()),
                dfs.get("do_not_lend", pd.DataFrame()),
                dfs.get("valuation", pd.DataFrame()),
                dfs.get("am_assignments", pd.DataFrame()),
                dfs.get("active_rm", pd.DataFrame()),
                serv_join,
                upb_col,
                prev_maps,
            )
        with st.spinner("Building Bridge Loan..."):
            bridge_loan = build_bridge_loan(bridge_asset, upb_col, prev_maps)

    if build_target in ("Term Loan", "Term Asset", "All"):
        with st.spinner("Building Term Loan..."):
            term_loan = build_term_loan(
                dfs.get("term_export", pd.DataFrame()),
                dfs.get("sold_term", pd.DataFrame()),
                dfs.get("am_assignments", pd.DataFrame()),
                dfs.get("active_rm", pd.DataFrame()),
                serv_join,
                upb_col,
                prev_maps,
            )

    if build_target in ("Term Asset", "All"):
        if term_loan is None:
            term_loan = build_term_loan(
                dfs.get("term_export", pd.DataFrame()),
                dfs.get("sold_term", pd.DataFrame()),
                dfs.get("am_assignments", pd.DataFrame()),
                dfs.get("active_rm", pd.DataFrame()),
                serv_join,
                upb_col,
                prev_maps,
            )
        with st.spinner("Building Term Asset..."):
            term_asset = build_term_asset(dfs.get("term_asset", pd.DataFrame()), term_loan, upb_col)

    # -----------------------------
    # Diagnostics + validation
    # -----------------------------
    st.subheader("Diagnostics")

    if bridge_asset is not None and "_loan_upb" in bridge_asset.columns:
        st.write(f"Bridge Asset servicer-join match rate (UPB): {bridge_asset['_loan_upb'].notna().mean():.1%}")
    if term_loan is not None and upb_col in term_loan.columns:
        st.write(f"Term Loan servicer-join match rate (UPB): {term_loan[upb_col].notna().mean():.1%}")

    with st.expander("Servicer validation (Active loans: UPB > 0)"):
        # 1) Conflicts across servicer uploads
        conflicts = summarize_servicer_conflicts(serv_full)
        if conflicts.empty:
            st.success("No multi-source conflicts detected (or no active loans in servicer uploads).")
        else:
            st.warning(f"Found {len(conflicts)} loan IDs with multiple sources and/or conflicts.")
            st.dataframe(conflicts.head(200), use_container_width=True)
            st.download_button(
                "Download conflicts CSV",
                data=conflicts.to_csv(index=False).encode("utf-8"),
                file_name=f"servicer_conflicts_{run_dt.isoformat()}.csv",
                mime="text/csv",
            )

        # 2) Active loans missing from the output (Bridge Loan + Term Loan)
        expected_active = serv_join.copy()
        expected_active = expected_active[pd.to_numeric(expected_active.get("upb", 0), errors="coerce").fillna(0) > 0].copy()
        expected_active["_id"] = _normalize_id_list(expected_active["servicer_id"]) if "servicer_id" in expected_active.columns else expected_active.get("_sid_key")

        built_ids = []
        if bridge_loan is not None and "Servicer ID" in bridge_loan.columns:
            built_ids.append(_normalize_id_list(bridge_loan["Servicer ID"]))
        if term_loan is not None and "Servicer ID" in term_loan.columns:
            built_ids.append(_normalize_id_list(term_loan["Servicer ID"]))

        built_union = pd.Series([], dtype="string")
        if built_ids:
            built_union = pd.concat(built_ids, ignore_index=True).dropna().drop_duplicates()

        missing = expected_active.dropna(subset=["_id"]).copy()
        missing = missing[~missing["_id"].isin(set(built_union.tolist()))].copy()

        if missing.empty:
            st.success("All active servicer loans appear in the built Bridge/Term loan tabs (by Servicer ID).")
        else:
            st.error(f"{len(missing)} active servicer loans did NOT appear in Bridge Loan or Term Loan outputs.")
            show = missing[["servicer", "servicer_id", "upb", "next_payment_date", "maturity_date", "source_file"]].copy()
            st.dataframe(show.head(300), use_container_width=True)
            st.download_button(
                "Download missing-active-loans CSV",
                data=show.to_csv(index=False).encode("utf-8"),
                file_name=f"missing_active_servicer_loans_{run_dt.isoformat()}.csv",
                mime="text/csv",
            )

        # 2b) Hayden's weekly check: FCI Master loans with balance should be in the report
        st.markdown("#### FCI Master check (loans with UPB > 0)")
        if serv_full is None or serv_full.empty:
            st.info("No servicer rows to validate.")
        else:
            fci_master = serv_full[serv_full["source_file"].astype("string").str.contains("cvmaster", case=False, na=False)].copy()
            fci_master = fci_master[pd.to_numeric(fci_master.get("upb", 0), errors="coerce").fillna(0) > 0].copy()
            if fci_master.empty:
                st.info("No FCI Master file detected (or no loans with UPB > 0 in that file).")
            else:
                fci_master = fci_master.dropna(subset=["_sid_key"]).drop_duplicates("_sid_key")
                missing_fci = fci_master[~fci_master["_sid_key"].isin(set(built_union.tolist()))].copy()
                if missing_fci.empty:
                    st.success("FCI Master: all loans with balance appear in Bridge Loan or Term Loan outputs.")
                else:
                    st.error(f"FCI Master: {len(missing_fci)} loans with UPB > 0 are missing from the output.")
                    show_fci = missing_fci[["servicer_id", "upb", "next_payment_date", "maturity_date", "status", "source_file"]].copy()
                    st.dataframe(show_fci.head(300), use_container_width=True)
                    st.download_button(
                        "Download FCI Master missing CSV",
                        data=show_fci.to_csv(index=False).encode("utf-8"),
                        file_name=f"missing_fci_master_loans_{run_dt.isoformat()}.csv",
                        mime="text/csv",
                    )

        # 3) Value mismatches (expected servicer vs built)
        if bridge_loan is not None:
            # Build expected vs actual for Bridge Loan
            # expected uses serv_join (upb/next/maturity). actual uses Bridge Loan tab.
            cols_map = {
                "upb": upb_col,
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Next Advance Maturity Date",
            }
            mism_bl = compare_expected_vs_actual(
                expected=serv_join,
                actual=bridge_loan,
                id_col_expected="servicer_id",
                id_col_actual="Servicer ID",
                cols_map=cols_map,
                active_only=True,
                upb_col_expected="upb",
                upb_tolerance=1.0,
            )
            if mism_bl.empty:
                st.success("Bridge Loan: no mismatches vs selected servicer values (for active loans).")
            else:
                st.warning(f"Bridge Loan mismatches: {len(mism_bl)}")
                st.dataframe(mism_bl.head(200), use_container_width=True)
                st.download_button(
                    "Download Bridge Loan mismatches CSV",
                    data=mism_bl.to_csv(index=False).encode("utf-8"),
                    file_name=f"bridge_loan_mismatches_{run_dt.isoformat()}.csv",
                    mime="text/csv",
                )

        if term_loan is not None:
            cols_map = {
                "upb": upb_col,
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Maturity Date",
            }
            mism_tl = compare_expected_vs_actual(
                expected=serv_join,
                actual=term_loan,
                id_col_expected="servicer_id",
                id_col_actual="Servicer ID",
                cols_map=cols_map,
                active_only=True,
                upb_col_expected="upb",
                upb_tolerance=1.0,
            )
            if mism_tl.empty:
                st.success("Term Loan: no mismatches vs selected servicer values (for active loans).")
            else:
                st.warning(f"Term Loan mismatches: {len(mism_tl)}")
                st.dataframe(mism_tl.head(200), use_container_width=True)
                st.download_button(
                    "Download Term Loan mismatches CSV",
                    data=mism_tl.to_csv(index=False).encode("utf-8"),
                    file_name=f"term_loan_mismatches_{run_dt.isoformat()}.csv",
                    mime="text/csv",
                )

    # -----------------------------
    # Write workbook
    # -----------------------------
    tmpl_bytes = template_upload.getvalue()
    wb = load_workbook(BytesIO(tmpl_bytes), data_only=False)
    wb_vals = load_workbook(BytesIO(tmpl_bytes), data_only=True)

    for sheet in ["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset"]:
        if sheet in wb.sheetnames and sheet in wb_vals.sheetnames:
            set_upb_header_in_sheet(wb[sheet], wb_vals[sheet], upb_col, header_row=4)
            update_run_date_in_row3(wb[sheet], wb_vals[sheet], run_dt, header_row=4, date_row=3)

    sheet_to_df = {
        "Bridge Asset": bridge_asset,
        "Bridge Loan": bridge_loan,
        "Term Loan": term_loan,
        "Term Asset": term_asset,
    }

    targets: List[str]
    if build_target == "All":
        targets = ["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset"]
    else:
        targets = [build_target]

    for sheet_name in targets:
        df = sheet_to_df.get(sheet_name)
        if df is None:
            continue
        if sheet_name not in wb.sheetnames or sheet_name not in wb_vals.sheetnames:
            continue

        ws = wb[sheet_name]
        ws_v = wb_vals[sheet_name]

        hdr = header_tuples_from_ws(ws_v, header_row=4)
        hdr = [(c, normalize_header_name(h, upb_col)) for (c, h) in hdr]
        fcols = formula_col_indices(ws, start_row=5, header_row=4)

        write_df_to_sheet_preserve_formulas(ws, df, hdr, fcols, start_row=5)

    out_bytes = BytesIO()
    wb.save(out_bytes)
    out_bytes.seek(0)

    fname_target = build_target.replace(" ", "_")
    st.success("✅ Workbook built")
    st.download_button(
        "Download",
        data=out_bytes.getvalue(),
        file_name=f"Active_Loans_{fname_target}_{run_dt.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
