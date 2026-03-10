# ============================================================
# Active Loans Report Builder — ONE FILE (Streamlit) — LOGIN FIXED (HUD STYLE)
#
# Fix:
# ✅ Make redirect_uri behave EXACTLY like the HUD app:
#    - redirect_uri is normalized with rstrip("/")
#    - same normalized redirect used for BOTH authorize + token exchange
#
# Why this matters:
# - Salesforce Connected App callback URL must match redirect_uri EXACTLY.
# - Trailing slash mismatch commonly causes state mismatch -> "login link expired".
#
# Secrets required in .streamlit/secrets.toml
#   [salesforce]
#   client_id = "..."
#   auth_host  = "https://cvest.my.salesforce.com"   # ok
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


def exchange_code_for_token(token_url: str, code: str, verifier: str, client_id: str, redirect_uri: str, client_secret: Optional[str]):
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
            max_c = min(ws.max_column, 250)
            for r in range(1, max_scan_rows + 1):
                row_vals = [ws.cell(r, c).value for c in range(1, max_c + 1)]
                cols = {str(v).strip() for v in row_vals if v is not None and str(v).strip() != ""}
                if required_cols.issubset(cols):
                    return sn, r
        return None
    finally:
        wb.close()


def _servicer_label(detected: str, filename: str) -> str:
    fn = (filename or "").lower()
    if detected == "CHL":
        return "FCI CHL Streamline"
    if detected == "FCI":
        if "v1805510" in fn:
            return "FCI v1805510"
        if "2012632" in fn:
            return "FCI 2012632"
        return "FCI"
    return detected


def parse_servicer_upload(upload) -> pd.DataFrame:
    name = upload.name
    b = upload.getvalue()

    d_file = date_from_filename(name)
    as_of_file = pd.to_datetime(d_file) if d_file else pd.NaT

    if name.lower().endswith(".csv"):
        df = pd.read_csv(BytesIO(b))
        req = {"Servicer Loan ID", "UPB"}
        if not req.issubset(set(df.columns)):
            raise ValueError(f"CSV doesn't look like CHL Streamline (missing {req - set(df.columns)}).")

        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": _servicer_label("CHL", name),
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

    checks: List[Tuple[str, Set[str], Optional[Sequence[str]]]] = [
        ("CHL", {"Servicer Loan ID", "UPB"}, None),
        ("Statebridge", {"Loan Number", "Current UPB", "Due Date", "Maturity Date", "Loan Status"}, None),
        ("Berkadia", {"BCM Loan#", "Principal Balance", "Next Payment Due Date", "Maturity Date"}, ["Loan"]),
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
            "Could not detect servicer file type from columns (Statebridge/Berkadia/FCI/Midland/CHL Streamline)."
        )

    df = pd.read_excel(BytesIO(b), sheet_name=sheet_name, header=header_row - 1)

    if detected == "CHL":
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": _servicer_label("CHL", name),
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

    if detected == "Statebridge":
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": "Statebridge",
                "servicer_id": norm_id_series(df["Loan Number"]),
                "upb": pd.to_numeric(df["Current UPB"], errors="coerce"),
                "suspense": pd.to_numeric(df.get("Unapplied Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Loan Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": as_of_file,
            }
        )
        return out.dropna(subset=["servicer_id"])

    if detected == "Berkadia":
        status = pd.Series(["Active"] * len(df), dtype="string")
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

    if detected == "FCI":
        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": _servicer_label("FCI", name),
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
        full["_has_upb"] = full["upb"].notna().astype(int)
        full["_has_npd"] = full["next_payment_date"].notna().astype(int)
        full["_has_mat"] = full["maturity_date"].notna().astype(int)
        full = full.sort_values(["as_of", "_has_upb", "_has_npd", "_has_mat"], ascending=[True, True, True, True])

        join = full.drop_duplicates(["servicer_id"], keep="last").drop(
            columns=["_has_upb", "_has_npd", "_has_mat"], errors="ignore"
        )
        full = full.drop(columns=["_has_upb", "_has_npd", "_has_mat"], errors="ignore")
    else:
        join = full.copy()

    run_date = max(file_dates) if file_dates else date.today()
    return join, run_date, full


# =============================================================================
# LAST WEEK REPORT CARRY-FORWARD (REO DATE + optional manual columns)
# =============================================================================
def read_tab_df_from_active_loans(file_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=3)
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_prev_maps(prev_bytes: bytes) -> dict:
    out: dict = {}

    try:
        tl = read_tab_df_from_active_loans(prev_bytes, "Term Loan")
        if "Deal Number" in tl.columns and "REO Date" in tl.columns:
            tmp = tl[["Deal Number", "REO Date"]].copy()
            tmp["_deal_key"] = norm_id_series(tmp["Deal Number"])
            out["term_loan_reo"] = tmp.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")
    except Exception:
        pass

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
) -> pd.DataFrame:
    out = pd.DataFrame()

    for col, label in BRIDGE_ASSET_FROM_BRIDGE_MATURITY.items():
        out[col] = sf_spine[label] if label in sf_spine.columns else None

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_serv_id_key"] = norm_id_series(out.get("Servicer ID", pd.Series([None] * len(out))))
    out["_asset_key"] = norm_id_series(out.get("Asset ID", pd.Series([None] * len(out))))

    if not sf_dnl.empty and "Deal Loan Number" in sf_dnl.columns:
        dnl = sf_dnl.copy()
        dnl["_deal_key"] = norm_id_series(dnl["Deal Loan Number"])
        if "Do Not Lend" in dnl.columns:
            dnl_flag = dnl.groupby("_deal_key")["Do Not Lend"].max().reset_index()
            out = out.merge(dnl_flag, on="_deal_key", how="left")
            out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend"])
            out = out.drop(columns=["Do Not Lend"], errors="ignore")

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

    if "Approved Advance Amount Funded" in sf_spine.columns:
        out["SF Funded Amount"] = pd.to_numeric(sf_spine["Approved Advance Amount Funded"], errors="coerce")
    else:
        out["SF Funded Amount"] = (
            pd.to_numeric(out.get("Initial Disbursement Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Renovation Holdback Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Interest Allocation Funded", 0), errors="coerce").fillna(0)
        )

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

    if not sf_sold.empty and "Deal Loan Number" in sf_sold.columns:
        sold = sf_sold.copy()
        sold["_deal_key"] = norm_id_series(sold["Deal Loan Number"])
        if "Sold Loan: Sold To" in sold.columns:
            sold = sold[["_deal_key", "Sold Loan: Sold To"]].drop_duplicates("_deal_key")
            out = out.merge(sold, on="_deal_key", how="left")
            out["Loan Buyer"] = out["Sold Loan: Sold To"]
            out = out.drop(columns=["Sold Loan: Sold To"], errors="ignore")

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

    out["Servicer ID"] = sf_term["Servicer Commitment Id"] if "Servicer Commitment Id" in sf_term.columns else None
    out["_serv_id_key"] = norm_id_series(out["Servicer ID"].astype("string"))
    out["_serv_id_key_mid"] = out["_serv_id_key"].astype("string").str.lstrip("0")

    if not serv_lookup.empty:
        s = serv_lookup.dropna(subset=["servicer_id"]).copy()
        s2 = s.rename(
            columns={
                "servicer_id": "_sid",
                "servicer": "_servicer_file",
                "upb": upb_col,
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Maturity Date",
            }
        )[["_sid", "_servicer_file", upb_col, "Next Payment Date", "Maturity Date"]]

        out = (
            out.merge(s2, left_on=out["_serv_id_key_mid"], right_on="_sid", how="left")
            .drop(columns=["_sid", "key_0"], errors="ignore")
        )

        out["Servicer"] = out.get("Servicer", pd.Series(["" for _ in range(len(out))], dtype="string"))
        out["Servicer"] = out["Servicer"].fillna(out["_servicer_file"]).fillna("")
        out = out.drop(columns=["_servicer_file"], errors="ignore")

    out["REO Date"] = ""
    if "term_loan_reo" in prev_maps:
        reo = prev_maps["term_loan_reo"][["_deal_key", "REO Date"]].copy()
        out = out.merge(reo, on="_deal_key", how="left", suffixes=("", "_prev"))
        out["REO Date"] = out["REO Date_prev"].fillna("")
        out = out.drop(columns=["REO Date_prev"], errors="ignore")

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
def build_bridge_loan(bridge_asset: pd.DataFrame, sf_spine: pd.DataFrame, upb_col: str, prev_maps: dict) -> pd.DataFrame:
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

    out["Special Focus (Y/N)"] = out["Special Focus (Y/N)"].replace({"": "N"}).fillna("N")
    return out.drop(columns=["_deal_key"], errors="ignore")


# =============================================================================
# EXCEL OUTPUT HELPERS
# =============================================================================
def header_tuples_from_ws(ws_values, header_row: int = 4) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for col_idx, cell in enumerate(ws_values[header_row], start=1):
        v = cell.value
        h = "" if v is None else str(v).strip()
        if h:
            out.append((col_idx, h))
    return out


def formula_col_indices(ws_formula, start_row: int = 5, header_row: int = 4) -> Set[int]:
    fcols: Set[int] = set()
    for col_idx, _cell in enumerate(ws_formula[header_row], start=1):
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

    for col_idx, cell in enumerate(ws_values[date_row], start=1):
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
# STREAMLIT UI
# =============================================================================
st.set_page_config(page_title="Active Loans Builder", layout="wide")
st.title("Active Loans Report Builder")
st.subheader(hey())

st.markdown(
    f"""
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
    prev_upload = st.file_uploader("Upload LAST WEEK'S Active Loans report (.xlsx) for carry-forward (optional)", type=["xlsx"])
with colB:
    servicer_uploads = st.file_uploader("Upload current servicer files (csv/xlsx)", type=["csv", "xlsx"], accept_multiple_files=True)

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
            )
        with st.spinner("Building Bridge Loan..."):
            bridge_loan = build_bridge_loan(bridge_asset, dfs.get("bridge_maturity", pd.DataFrame()), upb_col, prev_maps)

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

    st.subheader("Diagnostics")
    if bridge_asset is not None and "_loan_upb" in bridge_asset.columns:
        st.write(f"Bridge Asset servicer-join match rate (UPB): {bridge_asset['_loan_upb'].notna().mean():.1%}")
    if term_loan is not None and upb_col in term_loan.columns:
        st.write(f"Term Loan servicer-join match rate (UPB): {term_loan[upb_col].notna().mean():.1%}")

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

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    fname_target = build_target.replace(" ", "_")
    st.success("✅ Workbook built")
    st.download_button(
        "Download",
        data=out.getvalue(),
        file_name=f"Active_Loans_{fname_target}_{run_dt.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
