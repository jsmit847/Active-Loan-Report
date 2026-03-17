# ============================================================
# Active Loans Report Builder — ONE FILE (Streamlit)
#
# Features
# - Salesforce OAuth (PKCE) login
# - Salesforce Bulk API 2.0 for large pulls
# - Optional servicer file parsing (can be skipped)
# - Repo template workbook (no template upload)
# - Uses today's ET date for UPB header
# ============================================================

import base64
import hashlib
import io
import re
import secrets
import time
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import streamlit as st
from openpyxl import load_workbook
from simple_salesforce import Salesforce

# =============================================================================
# PERSONALIZATION
# =============================================================================
PRIMARY_USER_NAME = "Hayden"


def hey(name: str = PRIMARY_USER_NAME) -> str:
    return f"Hi {name} 👋"


# =============================================================================
# TEMPLATE / REFERENCE FILES
# =============================================================================
TEMPLATE_FILENAME = "Active Loan Report Template.xlsx"
REFERENCE_WORKBOOK_FILENAME = "20260302 Active Loans_Bridge Asset Column Mapping.xlsx"
API_VERSION = "v66.0"


@st.cache_data(show_spinner=False)
def load_repo_template_bytes() -> Tuple[bytes, str]:
    here = Path(__file__).resolve().parent
    candidates = [
        here / TEMPLATE_FILENAME,
        here / "templates" / TEMPLATE_FILENAME,
        here / "assets" / TEMPLATE_FILENAME,
        Path.cwd() / TEMPLATE_FILENAME,
        Path(TEMPLATE_FILENAME),
    ]

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                return p.read_bytes(), str(p)
        except Exception:
            continue

    tried = "\n".join(str(p) for p in candidates)
    raise FileNotFoundError(
        f"Could not find '{TEMPLATE_FILENAME}' in your repo.\n\n"
        f"Tried:\n{tried}\n\n"
        f"Fix: Commit '{TEMPLATE_FILENAME}' to your GitHub repo."
    )


@st.cache_data(show_spinner=False)
def load_reference_workbook_tables() -> dict:
    here = Path(__file__).resolve().parent
    candidates = [
        here / REFERENCE_WORKBOOK_FILENAME,
        here / "assets" / REFERENCE_WORKBOOK_FILENAME,
        here / "templates" / REFERENCE_WORKBOOK_FILENAME,
        Path.cwd() / REFERENCE_WORKBOOK_FILENAME,
        Path(REFERENCE_WORKBOOK_FILENAME),
    ]

    path = None
    for p in candidates:
        try:
            if p.exists() and p.is_file():
                path = p
                break
        except Exception:
            continue

    if path is None:
        return {
            "source_path": None,
            "strategy_grouping_map": {},
            "legacy_term_keys": set(),
        }

    wb = load_workbook(path, data_only=True, read_only=True)
    try:
        strategy_grouping_map: Dict[str, str] = {}
        legacy_term_keys: Set[str] = set()

        if "Strategy Groupings" in wb.sheetnames:
            ws = wb["Strategy Groupings"]
            for row in ws.iter_rows(min_row=5, values_only=True):
                strategy = row[1] if len(row) > 1 else None
                grouping = row[2] if len(row) > 2 else None
                if strategy and grouping:
                    strategy_grouping_map[str(strategy).strip()] = str(grouping).strip()

        if "Legacy" in wb.sheetnames:
            ws = wb["Legacy"]
            for row in ws.iter_rows(min_row=6, values_only=True):
                term_deal = row[6] if len(row) > 6 else None
                if term_deal is not None and str(term_deal).strip() != "":
                    legacy_term_keys.add(str(term_deal).strip().replace(".0", ""))

        return {
            "source_path": str(path),
            "strategy_grouping_map": strategy_grouping_map,
            "legacy_term_keys": legacy_term_keys,
        }
    finally:
        wb.close()


def today_et() -> date:
    return datetime.now(ZoneInfo("America/New_York")).date()


# =============================================================================
# REPORTS / LABEL MAPS
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

# Active RM intentionally NOT here; merged separately from its own dataset
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

TERM_LOAN_FROM_SOLD_TERM = {"Loan Buyer": "Sold Loan: Sold To"}

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


def _yn_from_bool_series(s: pd.Series) -> pd.Series:
    return s.fillna(False).map(lambda x: "Y" if bool(x) else "N")


# =============================================================================
# SALESFORCE AUTH (OAuth + PKCE)
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


def show_salesforce_login_helper():
    st.info(
        "Step 1: Log in to Salesforce.\n\n"
        "Step 2: Approve access.\n\n"
        "Step 3: Click Build. This app uses the Salesforce API and Bulk API 2.0 to pull larger datasets."
    )


def ensure_sf_session() -> Salesforce:
    cfg = st.secrets["salesforce"]

    CLIENT_ID = cfg["client_id"]
    AUTH_HOST = cfg.get("auth_host", "https://cvest.my.salesforce.com").rstrip("/")
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

    now = time.time()
    TTL = 900
    for s, (_v, t0) in list(store.items()):
        if now - t0 > TTL:
            store.pop(s, None)

    if code:
        if not state or state not in store:
            st.error("Login link expired. Click login again.")
            st.stop()
        verifier, _t0 = store.pop(state)
        tok = exchange_code_for_token(TOKEN_URL, code, verifier, CLIENT_ID, REDIRECT_URI, CLIENT_SECRET)
        st.session_state.sf_token = tok
        st.query_params.clear()
        st.rerun()

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
# BULK API 2.0 HELPERS
# =============================================================================
def _session_cache(bucket: str) -> dict:
    if bucket not in st.session_state:
        st.session_state[bucket] = {}
    return st.session_state[bucket]


def _sf_auth_parts() -> Tuple[str, str]:
    tok = st.session_state.get("sf_token") or {}
    access_token = tok.get("access_token")
    instance_url = (tok.get("instance_url") or "").rstrip("/")
    if not access_token or not instance_url:
        raise RuntimeError("Salesforce session missing. Please log in again.")
    return access_token, instance_url


def _sf_headers(extra: Optional[dict] = None) -> dict:
    access_token, _instance_url = _sf_auth_parts()
    hdrs = {"Authorization": f"Bearer {access_token}"}
    if extra:
        hdrs.update(extra)
    return hdrs


def _sf_request(
    path: str,
    method: str = "GET",
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
    headers: Optional[dict] = None,
    expect_json: bool = True,
    timeout: int = 180,
):
    _access_token, instance_url = _sf_auth_parts()
    url = f"{instance_url}/services/data/{API_VERSION}/{path.lstrip('/')}"
    hdrs = _sf_headers(headers)
    if json_body is not None:
        hdrs.setdefault("Content-Type", "application/json")

    resp = requests.request(
        method=method,
        url=url,
        headers=hdrs,
        params=params,
        json=json_body,
        timeout=timeout,
    )

    if resp.status_code >= 400:
        msg = resp.text[:2000]
        raise RuntimeError(f"Salesforce API {method} failed ({resp.status_code}) for {path}: {msg}")

    if expect_json:
        return resp.json()
    return resp


def _bulk_query_create_job(soql: str) -> str:
    payload = {
        "operation": "query",
        "query": soql,
        "columnDelimiter": "COMMA",
        "lineEnding": "LF",
    }
    js = _sf_request("jobs/query", method="POST", json_body=payload)
    job_id = js.get("id")
    if not job_id:
        raise RuntimeError(f"Bulk query job creation failed: {js}")
    return job_id


def _bulk_query_wait(job_id: str, poll_seconds: float = 1.5, timeout_seconds: int = 1800) -> dict:
    t0 = time.time()
    while True:
        js = _sf_request(f"jobs/query/{job_id}", method="GET")
        state = js.get("state")

        if state == "JobComplete":
            return js
        if state in {"Aborted", "Failed"}:
            raise RuntimeError(
                f"Bulk query job {job_id} failed: state={state}; message={js.get('errorMessage') or js}"
            )
        if time.time() - t0 > timeout_seconds:
            raise TimeoutError(f"Timed out waiting for Bulk query job {job_id}.")
        time.sleep(poll_seconds)


def _bulk_query_results_to_df(job_id: str, max_records: int = 100000) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    locator: Optional[str] = None

    while True:
        params = {"maxRecords": max_records}
        if locator:
            params["locator"] = locator

        resp = _sf_request(
            f"jobs/query/{job_id}/results",
            method="GET",
            params=params,
            headers={"Accept": "text/csv"},
            expect_json=False,
            timeout=300,
        )

        text = resp.text
        if text.strip():
            df = pd.read_csv(io.StringIO(text), keep_default_na=True, low_memory=False)
            if not df.empty:
                frames.append(df)

        locator = resp.headers.get("Sforce-Locator") or resp.headers.get("sforce-locator")
        if not locator or locator.lower() == "null":
            break

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def run_bulk_query(soql: str) -> pd.DataFrame:
    job_id = _bulk_query_create_job(soql)
    _bulk_query_wait(job_id)
    return _bulk_query_results_to_df(job_id)


def describe_sobject(sobject: str) -> dict:
    cache = _session_cache("sobject_describe_cache")
    if sobject in cache:
        return cache[sobject]
    js = _sf_request(f"sobjects/{sobject}/describe", method="GET")
    cache[sobject] = js
    return js


def _field_map_by_name(sobject: str) -> Dict[str, dict]:
    return {f["name"]: f for f in describe_sobject(sobject).get("fields", [])}


def _relationship_name_for_field(sobject: str, field_api: str) -> str:
    fld = _field_map_by_name(sobject).get(field_api)
    if not fld:
        raise KeyError(f"{sobject}.{field_api} not found in describe().")
    rel = fld.get("relationshipName")
    if not rel:
        raise KeyError(f"{sobject}.{field_api} is not a relationship field.")
    return rel


def _find_ref_field(
    sobject: str,
    *,
    label_options: Sequence[str] = (),
    contains_options: Sequence[str] = (),
    api_name_options: Sequence[str] = (),
    reference_to: Sequence[str] = (),
) -> dict:
    fields = describe_sobject(sobject).get("fields", [])
    label_options_l = {x.lower() for x in label_options}
    contains_options_l = [x.lower() for x in contains_options]
    refset = set(reference_to)

    best = None
    best_score = -10**9

    for f in fields:
        if f.get("type") != "reference" or not f.get("relationshipName"):
            continue

        refs = set(f.get("referenceTo") or [])
        if refset and not refs.intersection(refset):
            continue

        name = f.get("name") or ""
        label = (f.get("label") or "").strip().lower()
        score = 0

        if name in api_name_options:
            score += 1000
        if label in label_options_l:
            score += 500
        for opt in contains_options_l:
            if opt and opt in label:
                score += 100
        if name.endswith("__c"):
            score += 5

        if score > best_score:
            best_score = score
            best = f

    if not best:
        raise KeyError(
            f"Could not resolve reference field on {sobject}; labels={label_options or contains_options}; apis={api_name_options}; refs={reference_to}"
        )
    return best


def _find_property_to_opportunity_link() -> dict:
    return _find_ref_field(
        "Property__c",
        label_options=("Opportunity", "Deal", "Loan", "Line of Credit"),
        contains_options=("opportunity", "deal", "loan", "line"),
        api_name_options=("Opportunity__c", "Deal__c", "Loan__c", "Line_Of_Credit__c", "Line_of_Credit__c", "LOC__c"),
        reference_to=("Opportunity",),
    )


def _find_appraisal_to_property_link() -> dict:
    return _find_ref_field(
        "Appraisal__c",
        label_options=("Property", "Subject Property"),
        contains_options=("property",),
        api_name_options=("Property__c", "Subject_Property__c"),
        reference_to=("Property__c",),
    )


def _expr_account_name(sobject: str, field_label: str, *, api_candidates: Sequence[str] = ()) -> str:
    f = _find_ref_field(
        sobject,
        label_options=(field_label,),
        contains_options=(field_label,),
        api_name_options=api_candidates,
        reference_to=("Account",),
    )
    return f"{f['relationshipName']}.Name"


def _expr_contact_name(sobject: str, field_label: str, *, api_candidates: Sequence[str] = ()) -> str:
    f = _find_ref_field(
        sobject,
        label_options=(field_label, "Contact", "Primary Contact"),
        contains_options=(field_label, "contact"),
        api_name_options=api_candidates,
        reference_to=("Contact",),
    )
    return f"{f['relationshipName']}.Name"


def _expr_user_name(sobject: str, field_label: str, *, api_candidates: Sequence[str] = ()) -> str:
    f = _find_ref_field(
        sobject,
        label_options=(field_label,),
        contains_options=(field_label,),
        api_name_options=api_candidates,
        reference_to=("User",),
    )
    return f"{f['relationshipName']}.Name"


def _expr_borrower_entity_name() -> str:
    fld = _find_ref_field(
        "Opportunity",
        label_options=("Borrower Entity",),
        contains_options=("borrower",),
        api_name_options=("Borrower_Entity__c",),
    )
    return f"{fld['relationshipName']}.Name"


def _expr_referral_source_account() -> str:
    return _expr_account_name("Opportunity", "Referral Source Account", api_candidates=("Referral_Source__c",))


def _expr_referral_source_contact() -> str:
    return _expr_contact_name("Opportunity", "Referral Source Contact", api_candidates=("Referral_Source_Contact__c",))


def _expr_caf_originator_name() -> str:
    return _expr_user_name("Opportunity", "CAF Originator", api_candidates=("CAF_Originator__c",))


def _expr_title_company_name() -> str:
    return _expr_account_name("Property__c", "Title Company", api_candidates=("Title_Company__c",))


def _expr_bridge_sold_to_name(opp_rel: str) -> str:
    fld = _find_ref_field(
        "Opportunity",
        label_options=("Sold To",),
        contains_options=("sold to",),
        api_name_options=("Sold_To__c",),
        reference_to=("Account",),
    )
    return f"{opp_rel}.{fld['relationshipName']}.Name"


def _expr_sold_term_buyer_name() -> str:
    sold_pool = _find_ref_field(
        "Opportunity",
        label_options=("Sold Loan Pool", "Sold Loan"),
        contains_options=("sold loan",),
        api_name_options=("FK_Sold_Loan_Pool__c", "Sold_Loan_Pool__c"),
    )
    sold_pool_obj = (sold_pool.get("referenceTo") or [None])[0]
    if not sold_pool_obj:
        raise KeyError("Could not determine Sold Loan Pool target object from Opportunity describe().")

    sold_to = _find_ref_field(
        sold_pool_obj,
        label_options=("Sold To",),
        contains_options=("sold to",),
        api_name_options=("Sold_To__c",),
        reference_to=("Account",),
    )
    return f"{sold_pool['relationshipName']}.{sold_to['relationshipName']}.Name"


def _expr_special_asset_rel(child_field: str) -> str:
    rel = _relationship_name_for_field("Property__c", "Special_Asset__c")
    return f"{rel}.{child_field}"


def _normalize_bulk_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    numeric_hints = ("amount", "value", "upb", "balance", "feet", "units", "year", "rate", "commitment", "ala")
    date_hints = ("date", "maturity", "close", "funding", "order", "resolved")

    for c in out.columns:
        s = out[c]
        cl = c.lower()

        if any(h in cl for h in date_hints):
            parsed = pd.to_datetime(s, errors="coerce")
            if parsed.notna().sum() > 0:
                out[c] = parsed
                continue

        if any(h in cl for h in numeric_hints):
            cleaned = (
                s.astype("string")
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.replace("%", "", regex=False)
            )
            parsed = pd.to_numeric(cleaned, errors="coerce")
            if parsed.notna().sum() > 0:
                out[c] = parsed

    return out


# =============================================================================
# BULK API DATA BUILDERS
# =============================================================================
def _build_bridge_maturity_like() -> pd.DataFrame:
    prop_to_opp = _find_property_to_opportunity_link()
    opp_rel = prop_to_opp["relationshipName"]

    exprs: Dict[str, str] = {
        "Sold To": _expr_bridge_sold_to_name(opp_rel),
        "Warehouse Line": f"{opp_rel}.Warehouse_Line__c",
        "Deal Loan Number": f"{opp_rel}.Deal_Loan_Number__c",
        "Servicer Loan Number": "Servicer_Loan_Number__c",
        "Servicer Commitment Id": f"{opp_rel}.Servicer_Commitment_Id__c",
        "Yardi ID": "Yardi_Id__c",
        "Asset ID": "Asset_ID__c",
        "Deal Name": f"{opp_rel}.Name",
        "Borrower Entity: Business Entity Name": f"{opp_rel}.{_expr_borrower_entity_name()}",
        "Account Name: Account Name": f"{opp_rel}.Account.Name",
        "Primary Contact: Full Name": f"{opp_rel}.{_expr_contact_name('Opportunity', 'Primary Contact', api_candidates=('Contact__c', 'Primary_Contact__c'))}",
        "Address": "Name",
        "City": "City__c",
        "State": "State__c",
        "Zip": "ZipCode__c",
        "County": "County__c",
        "CBSA": "MSA__c",
        "APN": "APN__c",
        "Additional APNs": "Additional_APNs__c",
        "# of Units": "Number_of_Units__c",
        "Year Built": "Year_Built__c",
        "Square Feet": "Square_Feet__c",
        "Close Date": f"{opp_rel}.CloseDate",
        "First Funding Date": "First_Funding_Date__c",
        "Last Funding Date": "Funding_Date__c",
        "Original Loan Maturity Date": f"{opp_rel}.Stated_Maturity_Date__c",
        "Current Loan Maturity date": f"{opp_rel}.Current_Line_Maturity_Date__c",
        "Original Asset Maturity Date": "Asset_Maturity_Date_Override__c",
        "Current Asset Maturity date": "Current_Asset_Maturity_Date__c",
        "Remedy Plan": "Remedy_Plan__c",
        "Delinquency Status Notes": "Delinquency_Status_Notes__c",
        "Maturity Status": "Maturity_Status__c",
        "Is Special Asset": "Is_Special_Asset__c",
        "Special Asset: Status": _expr_special_asset_rel("Status_Comment__c"),
        "Special Asset: Special Asset Reason": _expr_special_asset_rel("Special_Asset_Reason__c"),
        "Special Asset: Special Asset Status": _expr_special_asset_rel("Severity_Level__c"),
        "Special Asset: Resolved Date": _expr_special_asset_rel("Resolved_Date__c"),
        "Forbearance Term Date": "Forbearance_Term_Date__c",
        "REO Date": "REO_Date__c",
        "Initial Disbursement Funded": "Initial_Disbursement_Used__c",
        "Approved Renovation Advance Amount": "Approved_Renovation_Holdback__c",
        "Renovation Advance Amount Funded": "Renovation_Advance_Amount_Used__c",
        "Reno Advance Amount Remaining": "Reno_Advance_Amount_Remaining__c",
        "Interest Allocation": "Interest_Allocation__c",
        "Interest Holdback Funded": "Interest_Reserves__c",
        "Title Company: Account Name": _expr_title_company_name(),
        "Tax Payment Next Due Date": "Tax_Payment_Next_Due_Date__c",
        "Taxes Payment Frequency": "Taxes_Payment_Frequency__c",
        "Tax Commentary": "Tax_Commentary__c",
        "Product Type": f"{opp_rel}.LOC_Loan_Type__c",
        "Product Sub-Type": f"{opp_rel}.Product_Sub_Type__c",
        "Transaction Type": f"{opp_rel}.Transaction_Type__c",
        "Project Strategy": f"{opp_rel}.Project_Strategy__c",
        "Property Type": "Property_Type__c",
        "Originator: Originating Company": f"{opp_rel}.Owner.Originating_Company__c",
        "Deal Intro Sub-Source": f"{opp_rel}.Deal_Intro_Sub_Source__c",
        "Referral Source Account: Account Name": f"{opp_rel}.{_expr_referral_source_account()}",
        "Referral Source Contact: Full Name": f"{opp_rel}.{_expr_referral_source_contact()}",
        "Stage": f"{opp_rel}.StageName",
        "Status": "Status__c",
        "Current UPB": "Current_UPB__c",
        "Approved Advance Amount Funded": "Approved_Advance_Amount_Used__c",
    }

    rename_map = {expr: label for label, expr in exprs.items()}
    soql = "SELECT " + ", ".join(exprs.values()) + " FROM Property__c"
    df = run_bulk_query(soql)
    if df.empty:
        return df

    df = df.rename(columns=rename_map)
    df = _normalize_bulk_df(df)

    for c in ["Servicer Loan Number", "Servicer Commitment Id", "Deal Loan Number"]:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip().replace({"": pd.NA})

    if {"Deal Loan Number", "Servicer Loan Number", "Servicer Commitment Id"}.issubset(df.columns):
        def _first_non_na(s: pd.Series):
            s = s.dropna()
            return s.iloc[0] if len(s) else pd.NA

        deal_servicer = df.groupby("Deal Loan Number")["Servicer Loan Number"].transform(_first_non_na)
        deal_commit = df.groupby("Deal Loan Number")["Servicer Commitment Id"].transform(_first_non_na)
        authoritative = deal_servicer.fillna(deal_commit)
        df["Servicer Loan Number"] = df["Servicer Loan Number"].fillna(authoritative)
        df.loc[authoritative.notna(), "Servicer Loan Number"] = authoritative[authoritative.notna()]

    return df


def _build_valuation_like() -> pd.DataFrame:
    appr_to_prop = _find_appraisal_to_property_link()
    prop_rel = appr_to_prop["relationshipName"]

    exprs = {
        "Asset ID": f"{prop_rel}.Asset_ID__c",
        "Order Date": "Order_Received_Date__c",
        "Current Appraisal Date": f"{prop_rel}.BPO_Appraisal_Date__c",
        "Current Appraised As-Is Value": f"{prop_rel}.Appraised_Value_Amount__c",
        "Current Appraised After Repair Value": f"{prop_rel}.After_Repair_Value__c",
        "Origination Valuation Date": f"{prop_rel}.Origination_Date_Valuation_Date__c",
        "Origination As-Is Value": f"{prop_rel}.Origination_Date_Value__c",
        "Origination After Repair Value": f"{prop_rel}.Origination_After_Repair_Value__c",
        "Appraisal: Created Date": "CreatedDate",
    }

    rename_map = {expr: label for label, expr in exprs.items()}
    soql = "SELECT " + ", ".join(exprs.values()) + " FROM Appraisal__c"
    df = run_bulk_query(soql)
    if df.empty:
        return df

    df = df.rename(columns=rename_map)
    df = _normalize_bulk_df(df)

    if "Asset ID" in df.columns:
        df["_asset_key"] = norm_id_series(df["Asset ID"])
        df["_order_dt"] = pd.to_datetime(df.get("Order Date"), errors="coerce")
        df["_created_dt"] = pd.to_datetime(df.get("Appraisal: Created Date"), errors="coerce")
        df = df.sort_values(["_asset_key", "_order_dt", "_created_dt"], ascending=[True, True, True])
        df = df.drop_duplicates(["_asset_key"], keep="last")
        df = df.drop(columns=["_asset_key", "_order_dt", "_created_dt", "Appraisal: Created Date"], errors="ignore")

    return df


def _build_opportunity_wide() -> pd.DataFrame:
    exprs = {
        "Deal Loan Number": "Deal_Loan_Number__c",
        "Deal Name": "Name",
        "Account Name": "Account.Name",
        "Do Not Lend": "Account.Do_Not_Lend__c",
        "Borrower Entity": _expr_borrower_entity_name(),
        "CAF Originator": _expr_caf_originator_name(),
        "Close Date": "CloseDate",
        "Current Funding Vehicle": "Current_Funding_Vehicle__c",
        "Loan Amount": "Amount",
        "Comments AM": "Asset_Management_Comments__c",
        "Deal Intro Sub-Source": "Deal_Intro_Sub_Source__c",
        "Referral Source Account": _expr_referral_source_account(),
        "Referral Source Contact": _expr_referral_source_contact(),
        "Servicer Commitment Id": "Servicer_Commitment_Id__c",
        "Yardi ID": "Yardi_ID__c",
        "Stage": "StageName",
        "Type": "Type",
        "Current Servicer UPB": "Current_UPB__c",
        "Sold Loan: Sold To": _expr_sold_term_buyer_name(),
    }

    rename_map = {expr: label for label, expr in exprs.items()}
    soql = "SELECT " + ", ".join(exprs.values()) + " FROM Opportunity WHERE Deal_Loan_Number__c != NULL"
    df = run_bulk_query(soql)
    if df.empty:
        return df

    df = df.rename(columns=rename_map)
    df = _normalize_bulk_df(df)
    return df


def _build_am_assignments_like() -> pd.DataFrame:
    exprs = {
        "Deal Loan Number": "Opportunity.Deal_Loan_Number__c",
        "Deal Name": "Opportunity.Name",
        "Team Member Name": "TeamMember.Name",
        "Team Role": "TeamMemberRole",
        "Date Assigned": "Date_Assigned__c",
    }

    rename_map = {expr: label for label, expr in exprs.items()}
    soql = (
        "SELECT Opportunity.Deal_Loan_Number__c, Opportunity.Name, TeamMember.Name, "
        "TeamMemberRole, Date_Assigned__c "
        "FROM OpportunityTeamMember "
        "WHERE Opportunity.Deal_Loan_Number__c != NULL"
    )

    df = run_bulk_query(soql)
    if df.empty:
        return df

    df = df.rename(columns=rename_map)
    df = _normalize_bulk_df(df)
    return df


def _build_term_asset_like() -> pd.DataFrame:
    prop_to_opp = _find_property_to_opportunity_link()
    opp_rel = prop_to_opp["relationshipName"]

    exprs = {
        "Deal Loan Number": f"{opp_rel}.Deal_Loan_Number__c",
        "Asset ID": "Asset_ID__c",
        "Address": "Name",
        "City": "City__c",
        "State": "State__c",
        "Zip": "ZipCode__c",
        "CBSA": "MSA__c",
        "# of Units": "Number_of_Units__c",
        "Property Type": "Property_Type__c",
        "ALA": "ALA__c",
    }

    rename_map = {expr: label for label, expr in exprs.items()}
    soql = "SELECT " + ", ".join(exprs.values()) + " FROM Property__c"
    df = run_bulk_query(soql)
    if df.empty:
        return df

    df = df.rename(columns=rename_map)
    df = _normalize_bulk_df(df)
    return df


def _slice_report_from_opp(opp: pd.DataFrame, which: str) -> pd.DataFrame:
    if opp.empty:
        return pd.DataFrame()

    if which == "do_not_lend":
        cols = [c for c in ["Deal Loan Number", "Do Not Lend", "Account Name"] if c in opp.columns]
        return opp[cols].drop_duplicates()

    if which == "active_rm":
        cols = [c for c in ["Deal Loan Number", "Deal Name", "CAF Originator"] if c in opp.columns]
        return opp[cols].drop_duplicates()

    if which == "term_export":
        cols = [c for c in [
            "Deal Loan Number", "Yardi ID", "Deal Name", "Borrower Entity", "Account Name", "Do Not Lend",
            "Current Funding Vehicle", "Loan Amount", "Close Date", "CAF Originator", "Deal Intro Sub-Source",
            "Referral Source Account", "Referral Source Contact", "Comments AM", "Servicer Commitment Id",
            "Current Servicer UPB", "Stage", "Type"
        ] if c in opp.columns]
        return opp[cols].drop_duplicates()

    if which == "sold_term":
        cols = [c for c in ["Deal Loan Number", "Deal Name", "Servicer Commitment Id", "Yardi ID", "Type", "Sold Loan: Sold To"] if c in opp.columns]
        return opp[cols].drop_duplicates()

    raise KeyError(f"Unhandled opp-slice key: {which}")


def pull_reports(sf, keys: Set[str]) -> Dict[str, pd.DataFrame]:
    day_key = today_et().isoformat()
    cache = _session_cache("report_cache")
    out: Dict[str, pd.DataFrame] = {}

    need_bridge_prop = bool({"bridge_maturity"}.intersection(keys))
    need_valuation = bool({"valuation"}.intersection(keys))
    need_term_asset = bool({"term_asset"}.intersection(keys))
    need_opp = bool({"do_not_lend", "active_rm", "term_export", "sold_term"}.intersection(keys))
    need_am = bool({"am_assignments"}.intersection(keys))

    if need_bridge_prop:
        ck = f"bridge_prop:{day_key}"
        if ck not in cache:
            with st.spinner("Pulling bridge/property data from Salesforce Bulk API..."):
                cache[ck] = _build_bridge_maturity_like()
        out["bridge_maturity"] = cache[ck]

    if need_valuation:
        ck = f"valuation:{day_key}"
        if ck not in cache:
            with st.spinner("Pulling valuation data from Salesforce Bulk API..."):
                cache[ck] = _build_valuation_like()
        out["valuation"] = cache[ck]

    if need_term_asset:
        ck = f"term_asset:{day_key}"
        if ck not in cache:
            with st.spinner("Pulling term asset data from Salesforce Bulk API..."):
                cache[ck] = _build_term_asset_like()
        out["term_asset"] = cache[ck]

    if need_opp:
        ck = f"opp_wide:{day_key}"
        if ck not in cache:
            with st.spinner("Pulling opportunity data from Salesforce Bulk API..."):
                cache[ck] = _build_opportunity_wide()
        opp = cache[ck]
        for k in ["do_not_lend", "active_rm", "term_export", "sold_term"]:
            if k in keys:
                out[k] = _slice_report_from_opp(opp, k)

    if need_am:
        ck = f"am_assignments:{day_key}"
        if ck not in cache:
            with st.spinner("Pulling AM assignments from Salesforce Bulk API..."):
                cache[ck] = _build_am_assignments_like()
        out["am_assignments"] = cache[ck]

    return out


# =============================================================================
# SERVICER FILE PARSING (CACHED)
# =============================================================================
@dataclass(frozen=True)
class UploadBlob:
    filename: str
    file_hash: str
    data: bytes


def _md5_hex(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def make_upload_blob(upload) -> UploadBlob:
    b = upload.getvalue()
    return UploadBlob(filename=upload.name, file_hash=_md5_hex(b), data=b)


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
    s = s.apply(lambda x: x if pd.isna(x) else (x if x.startswith("0000") else f"0000{x}"))
    return s.replace({"": pd.NA})


def parse_servicer_bytes(filename: str, b: bytes) -> pd.DataFrame:
    name = filename
    d_file = date_from_filename(name)
    as_of_file = pd.to_datetime(d_file) if d_file else pd.NaT

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
        raise ValueError("Could not detect servicer file type.")

    df = pd.read_excel(BytesIO(b), sheet_name=sheet_name, header=header_row - 1)

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

    if detected == "CoreVestLoanData":
        needs_pad = "corevestloandata" in name.lower()
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


@st.cache_data(
    show_spinner=False,
    ttl=6 * 60 * 60,
    max_entries=128,
    hash_funcs={UploadBlob: lambda b: f"{b.filename}:{b.file_hash}"},
)
def parse_servicer_cached(blob: UploadBlob) -> pd.DataFrame:
    return parse_servicer_bytes(blob.filename, blob.data)


def build_servicer_lookup(servicer_uploads: List) -> Tuple[pd.DataFrame, date, pd.DataFrame]:
    blobs: List[UploadBlob] = [make_upload_blob(u) for u in servicer_uploads]

    frames: List[pd.DataFrame] = []
    file_dates: List[date] = []

    for blob in blobs:
        frames.append(parse_servicer_cached(blob))
        d = date_from_filename(blob.filename)
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
# BUILDERS
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

    out["Portfolio"] = out.get("Portfolio", "")
    out["Segment"] = out.get("Segment", "")
    out["Strategy Grouping"] = out.get("Strategy Grouping", "")
    out["Do Not Lend (Y/N)"] = None
    out["Active RM"] = None

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_sid_key"] = id_key_no_leading_zeros(out.get("Servicer ID", pd.Series([None] * len(out))))
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
        out["Active RM"] = out["CAF Originator"]
        out = out.drop(columns=["CAF Originator"], errors="ignore")

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
    else:
        # Salesforce-only fallback
        if "Current UPB" in sf_spine.columns:
            out[upb_col] = pd.to_numeric(sf_spine["Current UPB"], errors="coerce")
        else:
            out[upb_col] = np.nan
        out["Servicer"] = ""
        out["Next Payment Date"] = pd.NaT
        out["Servicer Maturity Date"] = pd.NaT
        out["Servicer Status"] = ""
        out["Suspense Balance"] = np.nan

    if "Approved Advance Amount Funded" in sf_spine.columns:
        out["SF Funded Amount"] = pd.to_numeric(sf_spine["Approved Advance Amount Funded"], errors="coerce")
    else:
        out["SF Funded Amount"] = (
            pd.to_numeric(out.get("Initial Disbursement Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Renovation Holdback Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Interest Allocation Funded", 0), errors="coerce").fillna(0)
        )

    ref_tables = load_reference_workbook_tables()
    sg_map = ref_tables.get("strategy_grouping_map") or {}
    if "Project Strategy" in out.columns:
        out["Strategy Grouping"] = out["Strategy Grouping"].replace({"": pd.NA}).fillna(
            out["Project Strategy"].map(sg_map)
        ).fillna("")

    if "Is Special Asset (Y/N)" in out.columns:
        out["Is Special Asset (Y/N)"] = _yn_from_bool_series(out["Is Special Asset (Y/N)"])

    return out


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

    out["Active RM"] = pd.NA
    if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns and "CAF Originator" in sf_arm.columns:
        arm = sf_arm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key", "CAF Originator"]].drop_duplicates("_deal_key")
        out = out.merge(arm, on="_deal_key", how="left")
        out["Active RM"] = out["CAF Originator"].replace({"": pd.NA}).fillna(out["Active RM"])
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
    out["_sid_key"] = id_key_no_leading_zeros(out["Servicer ID"].astype("string"))

    if not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy()
        s2 = s.rename(
            columns={
                "servicer": "_servicer_file",
                "servicer_id": "_matched_servicer_id",
                "upb": upb_col,
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Maturity Date",
            }
        )[["_sid_key", "_servicer_file", "_matched_servicer_id", upb_col, "Next Payment Date", "Maturity Date"]]

        out = out.merge(s2, on="_sid_key", how="left")

        out["Servicer"] = out.get("Servicer", pd.Series(["" for _ in range(len(out))], dtype="string"))
        out["Servicer"] = out["Servicer"].fillna(out["_servicer_file"]).fillna("")
        out["Servicer ID"] = out["_matched_servicer_id"].fillna(out["Servicer ID"])
        out = out.drop(columns=["_servicer_file", "_matched_servicer_id"], errors="ignore")
    else:
        out["Servicer"] = ""
        out["Maturity Date"] = pd.NaT
        out["Next Payment Date"] = pd.NaT
        if "Current Servicer UPB" in sf_term.columns:
            out[upb_col] = pd.to_numeric(sf_term["Current Servicer UPB"], errors="coerce")
        else:
            out[upb_col] = np.nan

    out["REO Date"] = ""
    if "term_loan_reo" in prev_maps:
        reo = prev_maps["term_loan_reo"][["_deal_key", "REO Date"]].copy()
        out = out.merge(reo, on="_deal_key", how="left", suffixes=("", "_prev"))
        out["REO Date"] = out["REO Date_prev"].fillna("")
        out = out.drop(columns=["REO Date_prev"], errors="ignore")

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

    ref_tables = load_reference_workbook_tables()
    legacy_term_keys = ref_tables.get("legacy_term_keys") or set()
    if legacy_term_keys:
        out["Segment"] = np.where(out["_deal_key"].isin(legacy_term_keys), "Legacy", out["Segment"])

    return out


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


# =============================================================================
# STREAMLIT UI
# =============================================================================
st.set_page_config(page_title="Active Loans Builder", layout="wide")
st.title("Active Loans Report Builder")
st.subheader(hey())

run_dt = today_et()
upb_col = make_upb_header(run_dt)

st.markdown(
    f"""
Welcome! This tool builds the **Active Loans** workbook using **Salesforce** and optional **servicer uploads**.

### What you’ll do
1) Upload the **current servicer files** or skip them
2) (Optional) Upload **last week’s Active Loans report** for carry-forward
3) Log in to **Salesforce**
4) Choose **which sheet to build** or **All**

### Template (from GitHub repo)
This app always uses: **{TEMPLATE_FILENAME}**

### UPB header
Always uses today's date (ET): **{run_dt.isoformat()}** → **{upb_col}**
"""
)

try:
    _tmpl_bytes_preview, _tmpl_path_used = load_repo_template_bytes()
    st.success(f"✅ Using repo template: {_tmpl_path_used}")
except Exception as e:
    st.error(str(e))
    st.stop()

ref_tables = load_reference_workbook_tables()
if ref_tables.get("source_path"):
    st.caption(f"Optional mapping workbook found: {ref_tables['source_path']}")
else:
    st.caption("Optional mapping workbook not found. Strategy Grouping / Legacy helpers will be skipped.")

colA, colB = st.columns([1.3, 1.0])
with colA:
    prev_upload = st.file_uploader(
        "Upload LAST WEEK'S Active Loans report (.xlsx) for carry-forward (optional)",
        type=["xlsx"],
    )
with colB:
    servicer_uploads = st.file_uploader(
        "Upload current servicer files (csv/xlsx) (optional if skipped below)",
        type=["csv", "xlsx"],
        accept_multiple_files=True,
    )

skip_servicer_files = st.checkbox(
    "Skip servicer files and build Salesforce-only version",
    value=False,
    help="Leaves servicer-driven columns blank or Salesforce-fallback where available.",
)

build_target = st.selectbox(
    "Which sheet do you want to build right now?",
    options=["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset", "All"],
    index=0,
)

use_sf = st.checkbox("Use Salesforce API (recommended)", value=True)
sf = None
if use_sf:
    show_salesforce_login_helper()
    sf = ensure_sf_session()
    c1, c2 = st.columns([3, 1])
    with c1:
        inst = (st.session_state.get("sf_token") or {}).get("instance_url", "")
        st.success("✅ Logged in to Salesforce API")
        if inst:
            st.caption(f"Connected to: {inst}")
            st.caption("Bulk API 2.0 will be used automatically for large pulls.")
    with c2:
        if st.button("Log out"):
            st.session_state.sf_token = None
            st.rerun()

if st.button("Clear cached Salesforce reports", type="secondary"):
    st.session_state.report_cache = {}
    st.session_state.sobject_describe_cache = {}
    st.success("Cleared Salesforce caches for this session.")

if st.button("Clear cached servicer parsing", type="secondary"):
    st.cache_data.clear()
    st.success("Cleared Streamlit data cache.")

build_btn = st.button("Build", type="primary")

if build_btn:
    if not skip_servicer_files and not servicer_uploads:
        st.error("Upload the servicer files, or check 'Skip servicer files and build Salesforce-only version'.")
        st.stop()

    if use_sf and sf is None:
        st.error("Salesforce login is required.")
        st.stop()

    prev_maps: dict = {}
    if prev_upload:
        with st.spinner("Reading last week's report (carry-forward)..."):
            prev_maps = build_prev_maps(prev_upload.getvalue())

    if skip_servicer_files:
        serv_join = pd.DataFrame(columns=[
            "source_file", "servicer", "servicer_id", "upb", "suspense",
            "next_payment_date", "maturity_date", "status", "as_of", "_sid_key",
        ])
        detected_run_date = run_dt
        serv_full = serv_join.copy()

        st.markdown("### Servicer lookup preview")
        st.caption("Servicer files were skipped. Servicer-driven columns will be blank or Salesforce-fallback where available.")
        st.caption(f"UPB header (always today): **{upb_col}**")
    else:
        with st.spinner("Parsing servicer files (cached)..."):
            serv_join, detected_run_date, serv_full = build_servicer_lookup(servicer_uploads)

        st.markdown("### Servicer lookup preview")
        st.caption(f"Detected latest file date from filenames (info only): **{detected_run_date.isoformat()}**")
        st.caption(f"UPB header (always today): **{upb_col}**")
        st.dataframe(serv_full.head(30), use_container_width=True)

    need = required_report_keys(build_target)
    dfs = pull_reports(sf, need)

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

    st.subheader("Diagnostics")
    if bridge_asset is not None and upb_col in bridge_asset.columns:
        st.write(f"Bridge Asset nonblank {upb_col}: {bridge_asset[upb_col].notna().mean():.1%}")
    if term_loan is not None and upb_col in term_loan.columns:
        st.write(f"Term Loan nonblank {upb_col}: {term_loan[upb_col].notna().mean():.1%}")

    tmpl_bytes, tmpl_path_used = load_repo_template_bytes()

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

    targets = ["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset"] if build_target == "All" else [build_target]

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
    st.caption(f"Built from repo template: {tmpl_path_used}")
    st.download_button(
        "Download",
        data=out_bytes.getvalue(),
        file_name=f"Active_Loans_{fname_target}_{run_dt.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
