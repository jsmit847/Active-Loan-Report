import base64
import calendar
import gc
import hashlib
import io
import re
import secrets
import time
import urllib.parse
import warnings
from copy import copy
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
from openpyxl.formula.translate import Translator
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter


PRIMARY_USER_NAME = "Hayden"
TEMPLATE_FILENAMES = ("Active Loan Template.xlsx", "Active Loan Report Template.xlsx")
API_VERSION = "v66.0"
BULK_PAGE_SIZE = 5000
BULK_WAIT_TIMEOUT_SECONDS = 600
OUTPUT_TEST_FILENAME = "active loan report test.xlsx"
FORCE_QUARTER_END = None
UPB_HEADER_RE = re.compile(r"\b\d{1,2}/\d{1,2}\s*UPB\b", re.I)

BRIDGE_ACTIVE_STAGES = ["Closed Won", "Expired", "Matured", "REO", "Sold"]
BRIDGE_ACTIVE_PROPERTY_STATUSES = ["Active", "REO"]
BRIDGE_TYPES = ["Bridge Loan", "SAB Loan", "Acquired Bridge Loan"]
BRIDGE_EXCLUDED_PRODUCT_TYPE = "Model Home Lease"

DNL_STAGES = [
    "Closed Won", "Purchased", "Brokered- Closed Won", "Expired", "Matured",
    "Sold", "Paid Off", "REO", "REO-Sold",
]

VALUATION_STAGES = ["Closed Won", "Expired", "Matured", "Sold", "Paid Off", "REO", "REO-Sold"]
VALUATION_PROPERTY_STATUSES = ["Active", "Paid Off", "REO", "REO-Sold"]

TERM_ACTIVE_STAGES = ["Approved by Committee", "Closed Won", "Paid Off", "REO", "REO-Sold", "Sold"]
TERM_TYPES = ["DSCR", "Investor DSCR", "Single Rental Loan", "Term Loan"]
TERM_DSCR_TYPES = {"DSCR", "Investor DSCR"}

ACTIVE_RM_STAGES = [
    "Approved by Committee", "Closed Won", "Purchased", "Brokered- Closed Won",
    "Expired", "Matured", "Sold", "Paid Off", "REO", "REO-Sold",
]

AM_ASSIGNMENT_ROLES = ["Asset Manager", "Asset Manager 2", "Construction Manager"]
EXCLUDED_TEST_ACCOUNT_NAME = "Inhouse Test Account"

BRIDGE_MB_FINANCINGS = {
    "Goldman Sachs",
    "Morgan Stanley",
    "Wells Fargo",
    "Wells Fargo - NPL",
    "Goldman Sachs - NPL",
    "Axos",
    "CAFL 2026-R1",
    "Ineligible",
}

DATE_NUMBER_FORMAT = "mm-dd-yy"
MONEY0_FORMAT = r'#,###;[Red]\(#,###\);"-"'
MONEY2_FORMAT = r'#,###.00;[Red]\(#,###.00\);"-"'
BASE_FONT = Font(name="Aptos Narrow", size=11)
BASE_ALIGNMENT = Alignment(horizontal="center", vertical="center")

SHEET_DATE_HEADERS = {
    "Bridge Asset": {
        "Origination Date", "First Funding Date", "Last Funding Date", "Next Payment Date",
        "Original Loan Maturity date", "Current Loan Maturity date", "Original Asset Maturity date",
        "Current Asset Maturity Date", "AM 1 Assigned Date", "AM 2 Assigned Date", "CM Assigned Date",
        "Special Asset: Resolved Date", "Forbearance Term Date", "REO Date", "Origination Value Dt",
        "Most Recent Appraisal Order Date", "Updated Valuation Date", "Tax Due Date",
        "Servicer Maturity Date", "CV Maturity Date", "Maturity Date", "Most Recent Valuation Date",
    },
    "Bridge Loan": {
        "Origination Date", "Last Funding Date", "Original Maturity Date", "Current Maturity Date",
        "Next Advance Maturity Date", "Next Payment Date", "Most Recent Valuation Date",
        "AM 1 Assigned Date", "AM 2 Assigned Date", "CM Assigned Date",
    },
    "Term Loan": {"Origination Date", "Maturity Date", "Next Payment Date", "REO Date"},
    "Term Asset": {"Value Date"},
}

SHEET_MONEY2_HEADERS = {
    "Bridge Asset": {
        "SF Funded Amount", "Suspense Balance", "Origination As-Is Value", "Origination ARV",
        "Updated As-Is Value", "Updated ARV", "Initial Disbursement Funded", "Renovation Holdback",
        "Renovation Holdback Funded", "Renovation Holdback Remaining", "Interest Allocation",
        "Interest Allocation Funded", "Most Recent As-Is Value", "Most Recent ARV", "Needs NPL Value",
        "Property ALA", "As-Is Value",
    },
    "Term Asset": {"Property ALA", "As-Is Value"},
}

SHEET_MONEY0_HEADERS = {
    "Bridge Loan": {
        "Loan Commitment", "Active Funded Amount", "Suspense Balance", "Remaining Commitment",
        "Most Recent As-Is Value", "Most Recent ARV", "Initial Disbursement Funded",
        "Renovation Holdback", "Renovation HB Funded", "Renovation HB Remaining",
        "Interest Allocation", "Interest Allocation Funded",
    },
    "Term Loan": {"Loan Amount"},
}

BRIDGE_ASSET_FROM_BRIDGE_SPINE = {
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

TERM_LOAN_FROM_TERM_WIDE = {
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
    "Value Date": "Value Date",
    "As-Is Value": "As-Is Value",
}

SHEET_BLUEPRINTS = {
    "Bridge Asset": {
        "row1": {
            34: "CALC", 86: "CALC", 87: "CALC", 88: "CALC", 89: "CALC", 90: "CALC",
            91: "CALC", 92: "CALC", 93: "CALC", 94: "CALC", 95: "CALC", 96: "CALC",
            97: "CALC", 98: "CALC", 99: "CALC", 101: "CALC", 102: "CALC", 103: "CALC",
            104: "CALC", 105: "CALC",
        },
        "row2": {2: "Bridge Asset Data", 104: "__QEND__"},
        "row3": {
            35: "__SUBTOTAL__",
            89: "__RUN_DT__",
            90: "=+$CK$3",
            95: "=EDATE(CZ2,-6)",
            104: "=+$CZ$2-90",
        },
        "row4": {
            2: "Portfolio",
            3: "Loan Buyer",
            4: "Financing",
            5: "Deal Number",
            6: "Servicer ID",
            7: "Servicer",
            8: "SF Yardi ID",
            9: "Asset ID",
            10: "Deal Name",
            11: "Borrower Entity",
            12: "Account Name",
            13: "Do Not Lend (Y/N)",
            14: "Primary Contact",
            15: "Address",
            16: "City",
            17: "State",
            18: "Zip",
            19: "County",
            20: "CBSA",
            21: "APN",
            22: "Additional APNs",
            23: "# of Units",
            24: "Year Built",
            25: "Square Feet",
            26: "Origination Date",
            27: "First Funding Date",
            28: "Last Funding Date",
            29: "Next Payment Date",
            30: "Original Loan Maturity date",
            31: "Current Loan Maturity date",
            32: "Original Asset Maturity date",
            33: "Current Asset Maturity Date",
            34: "SF Funded Amount",
            35: "__UPB__",
            36: "Suspense Balance",
            37: "Asset Manager 1",
            38: "AM 1 Assigned Date",
            39: "Asset Manager 2",
            40: "AM 2 Assigned Date",
            41: "Construction Mgr.",
            42: "CM Assigned Date",
            43: "Remedy Plan",
            44: "Delinquency Notes",
            45: "Maturity Status",
            46: "Is Special Asset (Y/N)",
            47: "Special Asset Status",
            48: "Special Asset Reason",
            49: "Special Asset: Special Asset Status",
            50: "Special Asset: Resolved Date",
            51: "Forbearance Term Date",
            52: "REO Date",
            53: "Origination Value Dt",
            54: "Origination As-Is Value",
            55: "Origination ARV",
            56: "Most Recent Appraisal Order Date",
            57: "Updated Valuation Date",
            58: "Updated As-Is Value",
            59: "Updated ARV",
            60: "Initial Disbursement Funded",
            61: "Renovation Holdback",
            62: "Renovation Holdback Funded",
            63: "Renovation Holdback Remaining",
            64: "Interest Allocation",
            65: "Interest Allocation Funded",
            66: "Title Company",
            67: "Tax Due Date",
            68: "Tax Frequency",
            69: "Tax Commentary",
            70: "Segment",
            71: "Product Type",
            72: "Product Sub-Type",
            73: "Transaction Type",
            74: "Project Strategy",
            75: "Strategy Grouping",
            76: "Property Type",
            77: "Originator",
            78: "Active RM",
            79: "Deal Intro Sub-Source",
            80: "Referral Source Account",
            81: "Referral Source Contact",
            82: "Loan Stage",
            83: "Property Status",
            84: "Servicer Status",
            85: "Servicer Maturity Date",
            86: "CV Maturity Date",
            87: "Maturity Difference",
            88: "Maturity Date",
            89: "Days to Maturity",
            90: "Days Past Due",
            91: "DQ Status",
            92: "Most Recent Valuation Date",
            93: "Most Recent As-Is Value",
            94: "Most Recent ARV",
            95: "Needs NPL Value",
            96: "Securitized (Y/N)",
            97: "SSP JV (Y/N)",
            98: "CPP JV (Y/N)",
            99: "Oaktree JV (Y/N)",
            100: "Legacy (Y/N)",
            101: "Matured Loan (YN)",
            102: "DQ 45+ Loan (Y/N)",
            103: "SA Loan (Y/N)",
            104: "3/31 NPL (Y/N)",
            105: "Special Flag",
        },
        "subtotal_col": 35,
    },
    "Bridge Loan": {
        "row1": {},
        "row2": {},
        "row3": {22: "=+'Bridge Asset'!$CK$3", 26: "__SUBTOTAL__"},
        "row4": {
            2: "Portfolio",
            3: "Loan Buyer",
            4: "Financing",
            5: "Deal Number",
            6: "Servicer ID",
            7: "Servicer",
            8: "Deal Name",
            9: "Borrower Name",
            10: "Account",
            11: "Do Not Lend (Y/N)",
            12: "Primary Contact",
            13: "Number of Assets",
            14: "# of Units",
            15: "State(s)",
            16: "Origination Date",
            17: "Last Funding Date",
            18: "Original Maturity Date",
            19: "Current Maturity Date",
            20: "Next Advance Maturity Date",
            21: "Next Payment Date",
            22: "Days Past Due",
            23: "Loan Level Delinquency",
            24: "Loan Commitment",
            25: "Active Funded Amount",
            26: "=+'Bridge Asset'!$AI$4",
            27: "Suspense Balance",
            28: "Remaining Commitment",
            29: "Most Recent Valuation Date",
            30: "Most Recent As-Is Value",
            31: "Most Recent ARV",
            32: "Initial Disbursement Funded",
            33: "Renovation Holdback",
            34: "Renovation HB Funded",
            35: "Renovation HB Remaining",
            36: "Interest Allocation",
            37: "Interest Allocation Funded",
            38: "Loan Stage",
            39: "Segment",
            40: "Product Type",
            41: "Product Sub Type",
            42: "Transaction Type",
            43: "Project Strategy",
            44: "Strategy Grouping",
            45: "CV Originator",
            46: "Active RM",
            47: "Deal Intro Sub-Source",
            48: "Referral Source Account",
            49: "Referral Source Contact",
            50: "3/31 NPL",
            51: "Needs NPL Value",
            52: "Special Focus (Y/N)",
            53: "Asset Manager 1",
            54: "AM 1 Assigned Date",
            55: "Asset Manager 2",
            56: "AM 2 Assigned Date",
            57: "Construction Mgr.",
            58: "CM Assigned Date",
            59: "AM Commentary",
        },
        "subtotal_col": 26,
    },
    "Term Loan": {
        "row1": {30: "__QEND__"},
        "row2": {2: "Term Loan Data"},
        "row3": {16: "__SUBTOTAL__", 21: "__RUN_DT__", 30: "=+$AD$1-90"},
        "row4": {
            2: "Deal Number",
            3: "Servicer ID",
            4: "Servicer",
            5: "SF Yardi ID",
            6: "Deal Name",
            7: "Borrower Entity",
            8: "Account Name",
            9: "Do Not Lend (Y/N)",
            10: "Portfolio",
            11: "Segment",
            12: "Financing",
            13: "CPP JV",
            14: "Loan Buyer",
            15: "Loan Amount",
            16: "__UPB__",
            17: "Origination Date",
            18: "Maturity Date",
            19: "Next Payment Date",
            20: "REO Date",
            21: "Days Past Due",
            22: "DQ Status",
            23: "Asset Manager",
            24: "Originator",
            25: "Active RM",
            26: "Deal Intro Sub-Source",
            27: "Referral Source Account",
            28: "Referral Source Contact",
            29: "AM Commentary",
            30: "Special Loans List (Y/N)",
        },
        "subtotal_col": 16,
    },
    "Term Asset": {
        "row1": {},
        "row2": {},
        "row3": {12: "__SUBTOTAL__"},
        "row4": {
            2: "Deal Number",
            3: "Asset ID",
            4: "Address",
            5: "City",
            6: "State",
            7: "Zip",
            8: "CBSA",
            9: "# Units",
            10: "Property Type",
            11: "Property ALA",
            12: "=+'Term Loan'!$P$4",
            13: "Special (Y/N)",
            14: "Value Date",
            15: "As-Is Value",
        },
        "subtotal_col": 12,
    },
}


def hey(name: str = PRIMARY_USER_NAME) -> str:
    return f"Hi {name} 👋"


def today_et() -> date:
    return datetime.now(ZoneInfo("America/New_York")).date()


def quarter_end_for_run(run_dt: date) -> date:
    if FORCE_QUARTER_END is not None:
        return FORCE_QUARTER_END
    q_month = ((run_dt.month - 1) // 3 + 1) * 3
    last_day = calendar.monthrange(run_dt.year, q_month)[1]
    return date(run_dt.year, q_month, last_day)


def make_upb_header(run_dt: date) -> str:
    return f"{run_dt.month}/{run_dt.day} UPB"


def normalize_header_name(x) -> str:
    return re.sub(r"[^0-9a-z]+", "", str(x).strip().lower())


def header_lookup(columns: Sequence[str]) -> Dict[str, str]:
    return {normalize_header_name(c): c for c in columns}


def first_matching_col(df: pd.DataFrame, aliases: Sequence[str]) -> Optional[str]:
    lookup = header_lookup(df.columns)
    for alias in aliases:
        k = normalize_header_name(alias)
        if k in lookup:
            return lookup[k]
    return None


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


def clean_text(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    s = str(val).strip()
    if s.lower() in {"nan", "none", "<na>", "nat"}:
        return ""
    return s


def blankish_mask(s: pd.Series) -> pd.Series:
    base = pd.Series(list(pd.Series(s, copy=False)), index=pd.Series(s, copy=False).index, dtype="object")
    s_text = base.astype("string").str.strip().str.lower()
    return base.isna() | s_text.isin(["", "nan", "none", "<na>", "nat"])


def coalesce_keep_nonblank(primary: pd.Series, fallback: pd.Series) -> pd.Series:
    p = pd.Series(list(pd.Series(primary, copy=False)), index=pd.Series(primary, copy=False).index)
    f = pd.Series(list(pd.Series(fallback, copy=False)), index=p.index)
    return p.where(~blankish_mask(p), f)


def deal_key(value) -> str:
    s = clean_text(value)
    if not s:
        return ""
    s = re.sub(r"\.0$", "", s)
    return s


def deal_lookup_keys(value) -> List[str]:
    s = deal_key(value)
    if not s:
        return []
    keys = [s]
    m = re.match(r"^(\d+)-", s)
    if m:
        keys.append(m.group(1))
    return keys


def deal_in_lookup(value, lookup: Set[str]) -> bool:
    return any(k in lookup for k in deal_lookup_keys(value))


def first_nonblank(series: pd.Series):
    for v in series:
        if has_any_value(v):
            return v
    return pd.NA


def first_or_various(series: pd.Series):
    vals = []
    seen = set()
    for v in series:
        if not has_any_value(v):
            continue
        key = clean_text(v)
        if key not in seen:
            seen.add(key)
            vals.append(v)
    if not vals:
        return pd.NA
    if len(vals) == 1:
        return vals[0]
    return "Various"


def _yn_from_bool_series(s: pd.Series) -> pd.Series:
    truthy = {"true", "t", "y", "yes", "1"}
    falsy = {"false", "f", "n", "no", "0", ""}

    def _one(x):
        if x is None:
            return "N"
        try:
            if pd.isna(x):
                return "N"
        except Exception:
            pass
        if isinstance(x, str):
            xs = x.strip().lower()
            if xs in truthy:
                return "Y"
            if xs in falsy:
                return "N"
        return "Y" if bool(x) else "N"

    base = pd.Series(list(pd.Series(s, copy=False)), index=pd.Series(s, copy=False).index, dtype="object")
    return base.map(_one)


LIKELY_DATE_PATTERNS = (
    re.compile(r"^\d{4}-\d{1,2}-\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?$") ,
    re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?)?$", re.I),
    re.compile(r"^\d{1,2}-\d{1,2}-\d{2,4}$"),
)
MONTH_NAME_RE = re.compile(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b", re.I)


def _looks_like_date_string(x) -> bool:
    if x is None:
        return False
    try:
        if pd.isna(x):
            return False
    except Exception:
        pass
    s = str(x).strip()
    if not s or s.lower() in {"nan", "nat", "none", "<na>"}:
        return False
    if any(p.match(s) for p in LIKELY_DATE_PATTERNS):
        return True
    if MONTH_NAME_RE.search(s):
        return True
    return False


def _to_datetime_series_mixed(s: pd.Series) -> pd.Series:
    s_str = pd.Series(s, copy=False).astype("string").str.strip()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        warnings.simplefilter("ignore", FutureWarning)
        try:
            return pd.to_datetime(s_str, errors="coerce", format="mixed")
        except TypeError:
            return pd.to_datetime(s_str, errors="coerce")


def downcast_numeric_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        s = out[c]
        try:
            if pd.api.types.is_integer_dtype(s):
                out.loc[:, c] = pd.to_numeric(s, errors="coerce", downcast="integer")
            elif pd.api.types.is_float_dtype(s):
                out.loc[:, c] = pd.to_numeric(s, errors="coerce", downcast="float")
        except Exception:
            pass
    return out


def _normalize_bulk_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    numeric_hints = ("amount", "value", "upb", "balance", "feet", "units", "year", "rate", "commitment", "ala")

    def _is_date_like(col_name: str) -> bool:
        cl = str(col_name).lower()
        if "status" in cl:
            return False
        return (
            "date" in cl
            or "funding" in cl
            or "close" in cl
            or "order" in cl
            or "resolved" in cl
            or "maturity" in cl
        )

    for c in out.columns:
        s = out[c]
        cl = str(c).lower()

        if _is_date_like(c) and not pd.api.types.is_datetime64_any_dtype(s):
            s_str = s.astype("string").str.strip()
            nonblank_mask = s_str.notna() & s_str.ne("")
            nonblank_count = int(nonblank_mask.sum())
            if nonblank_count > 0:
                likely_date_mask = s_str.map(_looks_like_date_string)
                likely_count = int((likely_date_mask & nonblank_mask).sum())
                if likely_count > 0 and (likely_count / nonblank_count) >= 0.60:
                    parsed_input = s_str.where(likely_date_mask, pd.NA)
                    parsed = _to_datetime_series_mixed(parsed_input)
                    parsed_count = int(parsed[nonblank_mask].notna().sum())
                    if parsed_count > 0 and (parsed_count / nonblank_count) >= 0.60:
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

    return downcast_numeric_frame(out)


def normalize_servicer_family(val) -> str:
    s = clean_text(val).lower()
    if not s:
        return ""
    if "berkadia" in s:
        return "berkadia"
    if "midland" in s:
        return "midland"
    if "statebridge" in s:
        return "statebridge"
    if "shellpoint" in s:
        return "shellpoint"
    if "selene" in s:
        return "selene"
    if s == "sps" or "specialized" in s or "select portfolio" in s:
        return "sps"
    if "fci" in s:
        return "fci"
    if "fay" in s:
        return "fay"
    if "cornerstone" in s:
        return "cornerstone"
    return s


def fci_servicer_label_from_filename(filename: str) -> str:
    n = filename.lower()
    if "2012632" in n:
        return "FCI 2012632"
    if "18105510" in n or "1805510" in n:
        return "FCI v1805510"
    return "FCI"


def _soql_quote(v: str) -> str:
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _soql_in(field: str, values) -> str:
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return "1 = 1"
    return f"{field} IN ({', '.join(_soql_quote(v) for v in vals)})"


def _soql_not_equal_or_null(field: str, bad_value: str) -> str:
    q = _soql_quote(bad_value)
    return f"({field} = NULL OR {field} != {q})"


def _soql_parent_name_not_equal_or_no_parent(parent_id_field: str, parent_name_field: str, bad_value: str) -> str:
    q = _soql_quote(bad_value)
    return f"({parent_id_field} = NULL OR {parent_name_field} != {q})"


def _chunked(seq, size=200):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _nonblank_unique(values):
    out = []
    seen = set()
    for x in values:
        s = clean_text(x)
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _run_bulk_union(soql_list, rename_map=None):
    frames = []
    for soql in soql_list:
        df = run_bulk_query(soql, rename_map=rename_map)
        if not df.empty:
            frames.append(df)
        del df
        gc.collect()

    if not frames:
        return pd.DataFrame()

    if len(frames) == 1:
        out = frames[0]
    else:
        out = pd.concat(frames, ignore_index=True, copy=False)

    del frames
    gc.collect()
    return downcast_numeric_frame(out)


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


@st.cache_resource
def http_session():
    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


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
    resp = http_session().post(token_url, data=data, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Token exchange failed ({resp.status_code}): {resp.text}")
    return resp.json()


def show_salesforce_login_helper():
    st.info(
        "Step 1: Log in to Salesforce.\n\n"
        "Step 2: Approve access.\n\n"
        "Step 3: Click Build. This app uses Salesforce Bulk API 2.0 to pull the full datasets."
    )


def ensure_sf_session() -> dict:
    cfg = st.secrets["salesforce"]
    client_id = cfg["client_id"]
    auth_host = cfg.get("auth_host", "https://cvest.my.salesforce.com").rstrip("/")
    redirect_uri = cfg["redirect_uri"].rstrip("/")
    client_secret = cfg.get("client_secret")
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

    store = pkce_store()
    now = time.time()
    ttl = 900
    for s, (_v, t0) in list(store.items()):
        if now - t0 > ttl:
            store.pop(s, None)

    if code:
        if not state or state not in store:
            st.error("Login link expired. Click login again.")
            st.stop()
        verifier, _t0 = store.pop(state)
        tok = exchange_code_for_token(token_url, code, verifier, client_id, redirect_uri, client_secret)
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
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code_challenge": new_challenge,
            "code_challenge_method": "S256",
            "state": new_state,
            "prompt": "login",
            "scope": "api refresh_token",
        }
        login_url = auth_url + "?" + urllib.parse.urlencode(login_params)
        st.link_button("Login to Salesforce", login_url)
        st.stop()

    tok = st.session_state.sf_token
    access_token = tok.get("access_token")
    instance_url = tok.get("instance_url")
    if not access_token or not instance_url:
        st.error("Login token missing needed values.")
        st.stop()
    return {"access_token": access_token, "instance_url": instance_url.rstrip("/")}


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
    resp = http_session().request(
        method=method,
        url=url,
        headers=hdrs,
        params=params,
        json=json_body,
        timeout=timeout,
    )
    if resp.status_code >= 400:
        msg = resp.text[:4000]
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


def _bulk_query_wait(job_id: str, poll_seconds: float = 1.25, timeout_seconds: int = BULK_WAIT_TIMEOUT_SECONDS) -> dict:
    t0 = time.time()
    while True:
        js = _sf_request(f"jobs/query/{job_id}", method="GET")
        state = js.get("state")
        if state == "JobComplete":
            return js
        if state in {"Aborted", "Failed"}:
            raise RuntimeError(f"Bulk query job {job_id} failed: state={state}; message={js.get('errorMessage') or js}")
        if time.time() - t0 > timeout_seconds:
            raise TimeoutError(f"Timed out waiting for Bulk query job {job_id}.")
        time.sleep(poll_seconds)


def _bulk_query_results_pages(job_id: str, max_records: int = BULK_PAGE_SIZE):
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
        yield resp.text
        locator = resp.headers.get("Sforce-Locator") or resp.headers.get("sforce-locator")
        if not locator or locator.lower() == "null":
            break


def run_bulk_query(soql: str, rename_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    job_id = _bulk_query_create_job(soql)
    _bulk_query_wait(job_id)
    frames: List[pd.DataFrame] = []
    for text in _bulk_query_results_pages(job_id):
        if not text.strip():
            continue
        chunk = pd.read_csv(io.StringIO(text), keep_default_na=True, low_memory=True)
        if rename_map:
            chunk = chunk.rename(columns=rename_map)
        chunk = _normalize_bulk_df(chunk)
        chunk = downcast_numeric_frame(chunk)
        frames.append(chunk)
        del chunk, text
        gc.collect()
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, copy=False)
    del frames
    gc.collect()
    return downcast_numeric_frame(out)


def describe_sobject(sobject: str) -> dict:
    cache = _session_cache("sobject_describe_cache")
    if sobject in cache:
        return cache[sobject]
    js = _sf_request(f"sobjects/{sobject}/describe", method="GET")
    cache[sobject] = js
    return js


def _field_map_by_name(sobject: str) -> Dict[str, dict]:
    return {f["name"]: f for f in describe_sobject(sobject).get("fields", [])}


def relationship_name_for(sobject: str, field_api: str) -> str:
    fld = _field_map_by_name(sobject).get(field_api)
    if not fld:
        raise KeyError(f"{sobject}.{field_api} not found in describe().")
    rel = fld.get("relationshipName")
    if not rel:
        raise KeyError(f"{sobject}.{field_api} is not a relationship field.")
    return rel


def first_existing_field_name(sobject: str, candidates: Sequence[str]) -> Optional[str]:
    field_map = _field_map_by_name(sobject)
    for name in candidates:
        if name in field_map:
            return name
    return None


def opportunity_name_expr(field_candidates: Sequence[str]) -> str:
    field_api = first_existing_field_name("Opportunity", field_candidates)
    if not field_api:
        raise KeyError(f"Could not find any Opportunity field in: {field_candidates}")
    try:
        rel = relationship_name_for("Opportunity", field_api)
        return f"{rel}.Name"
    except Exception:
        return field_api


def property_opportunity_relationship_name() -> str:
    field_api = first_existing_field_name(
        "Property__c",
        ["Opportunity__c", "Deal__c", "Loan__c", "Line_Of_Credit__c", "Line_of_Credit__c", "LOC__c"],
    )
    if not field_api:
        raise KeyError("Could not find Property__c -> Opportunity relationship field.")
    return relationship_name_for("Property__c", field_api)


def appraisal_property_relationship_name() -> str:
    field_api = first_existing_field_name("Appraisal__c", ["Property__c", "Subject_Property__c"])
    if not field_api:
        raise KeyError("Could not find Appraisal__c -> Property relationship field.")
    return relationship_name_for("Appraisal__c", field_api)


@st.cache_data(show_spinner=False)
def load_repo_template_bytes() -> Tuple[bytes, str]:
    here = Path(__file__).resolve().parent
    candidates = []
    for filename in TEMPLATE_FILENAMES:
        candidates.extend(
            [
                here / filename,
                here / "templates" / filename,
                here / "assets" / filename,
                Path.cwd() / filename,
                Path(filename),
            ]
        )

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                return p.read_bytes(), str(p)
        except Exception:
            continue

    tried = "\n".join(str(p) for p in candidates)
    raise FileNotFoundError(
        f"Could not find any template file.\n\nTried:\n{tried}\n\n"
        f"Fix: commit one of these to your repo: {', '.join(TEMPLATE_FILENAMES)}"
    )


def resolve_template_bytes(prev_upload) -> Tuple[bytes, str]:
    if prev_upload is not None:
        return prev_upload.getvalue(), f"uploaded workbook template: {prev_upload.name}"
    return load_repo_template_bytes()


@st.cache_data(show_spinner=False)
def load_template_lookup_maps(template_bytes: bytes) -> dict:
    maps = {
        "strategy_map": {},
        "ssp_deals": set(),
        "legacy_bridge_deals": set(),
        "legacy_term_deals": set(),
    }

    xls = pd.ExcelFile(BytesIO(template_bytes))

    if "Strategy Groupings" in xls.sheet_names:
        sg = pd.read_excel(BytesIO(template_bytes), sheet_name="Strategy Groupings", header=3)
        sg = sg.dropna(how="all")
        sg.columns = [str(c).strip() for c in sg.columns]
        strategy_col = first_matching_col(sg, ["Strategy"])
        grouping_col = first_matching_col(sg, ["Grouping"])
        if strategy_col and grouping_col:
            for _, row in sg.iterrows():
                strategy = clean_text(row.get(strategy_col))
                grouping = clean_text(row.get(grouping_col))
                if strategy and grouping:
                    maps["strategy_map"][strategy] = grouping

    if "SSP Loans" in xls.sheet_names:
        ssp = pd.read_excel(BytesIO(template_bytes), sheet_name="SSP Loans", header=3)
        ssp = ssp.dropna(how="all")
        if "Deal No." in ssp.columns:
            maps["ssp_deals"] = set(_nonblank_unique(ssp["Deal No."].tolist()))

    if "Legacy" in xls.sheet_names:
        legacy = pd.read_excel(BytesIO(template_bytes), sheet_name="Legacy", header=4)
        legacy = legacy.dropna(how="all")
        if legacy.shape[1] >= 7:
            maps["legacy_bridge_deals"] = set(_nonblank_unique(legacy.iloc[:, 1].tolist()))
            maps["legacy_term_deals"] = set(_nonblank_unique(legacy.iloc[:, 6].tolist()))

    return maps


def strategy_grouping_from_project_strategy(project_strategy, strategy_map: dict):
    s = clean_text(project_strategy)
    if not s:
        return pd.NA
    return strategy_map.get(s, "Other")


def derive_bridge_segment(deal_number, financing, loan_buyer, template_maps: dict):
    fin = clean_text(financing)
    buyer = clean_text(loan_buyer)

    if fin.startswith("CPP JV"):
        return "CPP JV"
    if fin.startswith("Oaktree JV"):
        return "Oaktree JV"
    if deal_in_lookup(deal_number, template_maps.get("ssp_deals", set())):
        return "SSP"
    if buyer or fin == "Sold":
        return "Sold Servicing Retained"
    if deal_in_lookup(deal_number, template_maps.get("legacy_bridge_deals", set())):
        return "Legacy"
    if fin in BRIDGE_MB_FINANCINGS:
        return "Mortgage Banking"
    return "Securitized Bridge"


def derive_bridge_portfolio(product_type, segment, financing, deal_intro_sub_source, deal_number):
    ptype = clean_text(product_type)
    seg = clean_text(segment)
    fin = clean_text(financing)
    intro = clean_text(deal_intro_sub_source)
    deal = clean_text(deal_number)

    if intro == "Churchill Real Estate":
        return "TPO"
    if deal.startswith("5A-") or intro == "5arch":
        return "5A"
    if ptype in {"Single Asset (1-4 Unit)", "Single Asset (5-10 Unit)", "Single Asset (11+ Unit)", "Portfolio"}:
        return "RB"
    if ptype == "Multifamily/CRE" and (seg in {"SSP", "Legacy"} or "NPL" in fin):
        return "CLO"
    return "CV"


def derive_term_portfolio_segment(loan_type, financing, loan_buyer, deal_number, template_maps: dict):
    typ = clean_text(loan_type)
    fin = clean_text(financing)
    buyer = clean_text(loan_buyer)

    if typ in TERM_DSCR_TYPES:
        return "DSCR", "DSCR", "N"
    if fin.startswith("CPP JV"):
        return "Active Term", "CPP JV", "Y"
    if fin == "Sold" or buyer:
        return "Sold Term", "Sold Servcing Retained", "N"
    if deal_in_lookup(deal_number, template_maps.get("legacy_term_deals", set())):
        return "Active Term", "Legacy", "N"
    if re.match(r"^\d{4}[-A-Za-z0-9]+$", fin):
        return "Securitized Term", "Securitized Term", "N"
    return "Active Term", "Mortgage Banking", "N"


def _build_bridge_spine_like() -> pd.DataFrame:
    opp_rel = property_opportunity_relationship_name()

    sold_pool_field = first_existing_field_name("Opportunity", ["Sold_Loan_Pool__c", "FK_Sold_Loan_Pool__c"])
    sold_pool_rel = relationship_name_for("Opportunity", sold_pool_field) if sold_pool_field else None

    contact_field = first_existing_field_name("Opportunity", ["Contact__c", "Primary_Contact__c"])
    contact_rel = relationship_name_for("Opportunity", contact_field) if contact_field else None

    special_asset_rel = relationship_name_for("Property__c", "Special_Asset__c")

    sold_to_expr = f"{opp_rel}.{sold_pool_rel}.Sold_To__r.Name" if sold_pool_rel else f"{opp_rel}.Account.Name"
    primary_contact_expr = f"{opp_rel}.{contact_rel}.Name" if contact_rel else f"{opp_rel}.Account.Name"

    select_pairs = [
        ("Sold To", sold_to_expr),
        ("Warehouse Line", f"{opp_rel}.Warehouse_Line__c"),
        ("Deal Loan Number", f"{opp_rel}.Deal_Loan_Number__c"),
        ("Servicer Loan Number", "Servicer_Loan_Number__c"),
        ("Servicer Commitment Id", f"{opp_rel}.Servicer_Commitment_Id__c"),
        ("Yardi ID", "Yardi_Id__c"),
        ("Asset ID", "Asset_ID__c"),
        ("Deal Name", f"{opp_rel}.Name"),
        ("Borrower Entity: Business Entity Name", f"{opp_rel}.Borrower_Entity__r.Name"),
        ("Account Name: Account Name", f"{opp_rel}.Account.Name"),
        ("Primary Contact: Full Name", primary_contact_expr),
        ("Address", "Name"),
        ("City", "City__c"),
        ("State", "State__c"),
        ("Zip", "ZipCode__c"),
        ("County", "County__c"),
        ("CBSA", "MSA__c"),
        ("APN", "APN__c"),
        ("Additional APNs", "Additional_APNs__c"),
        ("# of Units", "Number_of_Units__c"),
        ("Year Built", "Year_Built__c"),
        ("Square Feet", "Square_Feet__c"),
        ("Close Date", f"{opp_rel}.CloseDate"),
        ("First Funding Date", "First_Funding_Date__c"),
        ("Last Funding Date", "Funding_Date__c"),
        ("Property Next Payment Date", "Next_Payment_Date__c"),
        ("Opportunity Next Payment Date", f"{opp_rel}.Next_Payment_Date__c"),
        ("Original Loan Maturity Date", f"{opp_rel}.Stated_Maturity_Date__c"),
        ("Current Loan Maturity date", f"{opp_rel}.Current_Line_Maturity_Date__c"),
        ("Original Asset Maturity Date", "Asset_Maturity_Date_Override__c"),
        ("Current Asset Maturity date", "Current_Asset_Maturity_Date__c"),
        ("Loan Commitment", f"{opp_rel}.LOC_Commitment__c"),
        ("Remaining Commitment", f"{opp_rel}.Outstanding_Facility_Amount__c"),
        ("Salesforce Suspense Balance", f"{opp_rel}.Suspense_Balance__c"),
        ("Remedy Plan", "Remedy_Plan__c"),
        ("Delinquency Status Notes", "Delinquency_Status_Notes__c"),
        ("Maturity Status", "Maturity_Status__c"),
        ("Is Special Asset", "Is_Special_Asset__c"),
        ("Special Asset: Status", f"{special_asset_rel}.Status_Comment__c"),
        ("Special Asset: Special Asset Reason", f"{special_asset_rel}.Special_Asset_Reason__c"),
        ("Special Asset: Special Asset Status", f"{special_asset_rel}.Severity_Level__c"),
        ("Special Asset: Resolved Date", f"{special_asset_rel}.Resolved_Date__c"),
        ("Forbearance Term Date", "Forbearance_Term_Date__c"),
        ("REO Date", "REO_Date__c"),
        ("Initial Disbursement Funded", "Initial_Disbursement_Used__c"),
        ("Approved Renovation Advance Amount", "Approved_Renovation_Holdback__c"),
        ("Renovation Advance Amount Funded", "Renovation_Advance_Amount_Used__c"),
        ("Reno Advance Amount Remaining", "Reno_Advance_Amount_Remaining__c"),
        ("Interest Allocation", "Interest_Allocation__c"),
        ("Interest Holdback Funded", "Interest_Reserves__c"),
        ("Title Company: Account Name", "Title_Company__r.Name"),
        ("Tax Payment Next Due Date", "Tax_Payment_Next_Due_Date__c"),
        ("Taxes Payment Frequency", "Taxes_Payment_Frequency__c"),
        ("Tax Commentary", "Tax_Commentary__c"),
        ("Product Type", f"{opp_rel}.LOC_Loan_Type__c"),
        ("Product Sub-Type", f"{opp_rel}.Product_Sub_Type__c"),
        ("Transaction Type", f"{opp_rel}.Transaction_Type__c"),
        ("Project Strategy", f"{opp_rel}.Project_Strategy__c"),
        ("Property Type", "Property_Type__c"),
        ("Originator: Originating Company", f"{opp_rel}.Owner.Originating_Company__c"),
        ("Deal Intro Sub-Source", f"{opp_rel}.Deal_Intro_Sub_Source__c"),
        ("Referral Source Account: Account Name", f"{opp_rel}.Referral_Source__r.Name"),
        ("Referral Source Contact: Full Name", f"{opp_rel}.Referral_Source_Contact__r.Name"),
        ("Stage", f"{opp_rel}.StageName"),
        ("Status", "Status__c"),
        ("Current UPB", "Current_UPB__c"),
        ("Approved Advance Amount Funded", "Approved_Advance_Amount_Used__c"),
        ("Comments AM", f"{opp_rel}.Asset_Management_Comments__c"),
    ]

    rename_map = {expr: label for label, expr in select_pairs}

    where_parts = [
        f"{opp_rel}.Deal_Loan_Number__c != NULL",
        _soql_in(f"{opp_rel}.StageName", BRIDGE_ACTIVE_STAGES),
        _soql_in(f"{opp_rel}.Type", BRIDGE_TYPES),
        _soql_in("Status__c", BRIDGE_ACTIVE_PROPERTY_STATUSES),
        _soql_not_equal_or_null(f"{opp_rel}.LOC_Loan_Type__c", BRIDGE_EXCLUDED_PRODUCT_TYPE),
    ]

    soql = (
        "SELECT "
        + ", ".join(expr for _label, expr in select_pairs)
        + " FROM Property__c WHERE "
        + " AND ".join(where_parts)
    )

    df = run_bulk_query(soql, rename_map=rename_map)

    if df.empty:
        return df

    for c in ["Servicer Loan Number", "Servicer Commitment Id", "Deal Loan Number"]:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip().replace({"": pd.NA})

    if {"Servicer Loan Number", "Servicer Commitment Id"}.issubset(df.columns):
        df["Servicer Loan Number"] = coalesce_keep_nonblank(
            df["Servicer Loan Number"],
            df["Servicer Commitment Id"],
        )

    return downcast_numeric_frame(df)


def _build_do_not_lend_like() -> pd.DataFrame:
    soql = (
        "SELECT Deal_Loan_Number__c, Account.Name, Account.Do_Not_Lend__c "
        "FROM Opportunity WHERE "
        "Account.Do_Not_Lend__c = TRUE "
        f"AND {_soql_in('StageName', DNL_STAGES)} "
        "AND Deal_Loan_Number__c != NULL"
    )
    df = run_bulk_query(soql)
    rename_map = {
        "Deal_Loan_Number__c": "Deal Loan Number",
        "Account.Name": "Account Name",
        "Account.Do_Not_Lend__c": "Do Not Lend",
    }
    return downcast_numeric_frame(_normalize_bulk_df(df.rename(columns=rename_map)))


def _build_valuation_like(asset_ids=None) -> pd.DataFrame:
    prop_rel = appraisal_property_relationship_name()
    asset_ids = _nonblank_unique(asset_ids or [])
    soqls = []

    select_pairs = [
        ("Asset ID", f"{prop_rel}.Asset_ID__c"),
        ("Order Date", "Order_Received_Date__c"),
        ("Current Appraisal Date", f"{prop_rel}.BPO_Appraisal_Date__c"),
        ("Current Appraised As-Is Value", f"{prop_rel}.Appraised_Value_Amount__c"),
        ("Current Appraised After Repair Value", f"{prop_rel}.After_Repair_Value__c"),
        ("Origination Valuation Date", f"{prop_rel}.Origination_Date_Valuation_Date__c"),
        ("Origination As-Is Value", f"{prop_rel}.Origination_Date_Value__c"),
        ("Origination After Repair Value", f"{prop_rel}.Origination_After_Repair_Value__c"),
        ("Appraisal: Created Date", "CreatedDate"),
    ]
    rename_map = {expr: label for label, expr in select_pairs}

    if asset_ids:
        for chunk in _chunked(asset_ids, size=200):
            soqls.append(
                "SELECT "
                + ", ".join(expr for _label, expr in select_pairs)
                + " FROM Appraisal__c WHERE "
                + _soql_in(f"{prop_rel}.Asset_ID__c", chunk)
            )
    else:
        soqls.append(
            "SELECT "
            + ", ".join(expr for _label, expr in select_pairs)
            + " FROM Appraisal__c"
        )

    df = _run_bulk_union(soqls, rename_map=rename_map)

    if df.empty:
        return df

    if "Asset ID" in df.columns:
        df["_asset_key"] = norm_id_series(df["Asset ID"])
        df["_order_dt"] = pd.to_datetime(df.get("Order Date"), errors="coerce")
        df["_created_dt"] = pd.to_datetime(df.get("Appraisal: Created Date"), errors="coerce")
        df = df.sort_values(["_asset_key", "_order_dt", "_created_dt"], ascending=[True, True, True])
        df = df.drop_duplicates(["_asset_key"], keep="last")
        df = df.drop(columns=["_asset_key", "_order_dt", "_created_dt", "Appraisal: Created Date"], errors="ignore")

    return downcast_numeric_frame(df)


def _build_am_assignments_like() -> pd.DataFrame:
    soql = (
        "SELECT Opportunity.Deal_Loan_Number__c, Opportunity.Name, User.Name, TeamMemberRole, Date_Assigned__c "
        "FROM OpportunityTeamMember WHERE "
        "Opportunity.Deal_Loan_Number__c != NULL AND "
        + _soql_parent_name_not_equal_or_no_parent("Opportunity.AccountId", "Opportunity.Account.Name", EXCLUDED_TEST_ACCOUNT_NAME)
        + " AND "
        + _soql_in("TeamMemberRole", AM_ASSIGNMENT_ROLES)
    )

    df = run_bulk_query(soql)
    rename_map = {
        "Opportunity.Deal_Loan_Number__c": "Deal Loan Number",
        "Opportunity.Name": "Deal Name",
        "User.Name": "Team Member Name",
        "TeamMemberRole": "Team Role",
        "Date_Assigned__c": "Date Assigned",
    }
    df = df.rename(columns=rename_map)
    return downcast_numeric_frame(_normalize_bulk_df(df))


def _build_active_rm_like() -> pd.DataFrame:
    soql = (
        "SELECT Opportunity.Deal_Loan_Number__c "
        "FROM OpportunityTeamMember WHERE "
        "Opportunity.Deal_Loan_Number__c != NULL AND "
        + _soql_parent_name_not_equal_or_no_parent("Opportunity.AccountId", "Opportunity.Account.Name", EXCLUDED_TEST_ACCOUNT_NAME)
        + " AND "
        + _soql_in("Opportunity.StageName", ACTIVE_RM_STAGES)
    )
    df = run_bulk_query(soql)
    if df.empty:
        return df
    df = df.rename(columns={"Opportunity.Deal_Loan_Number__c": "Deal Loan Number"})
    return downcast_numeric_frame(_normalize_bulk_df(df))


def _build_sold_term_like() -> pd.DataFrame:
    sold_pool_field = first_existing_field_name("Opportunity", ["FK_Sold_Loan_Pool__c", "Sold_Loan_Pool__c"])
    sold_pool_rel = relationship_name_for("Opportunity", sold_pool_field) if sold_pool_field else None

    select_pairs = [
        ("Deal Loan Number", "Deal_Loan_Number__c"),
        ("Yardi ID", "Yardi_ID__c"),
        ("Servicer Commitment Id", "Servicer_Commitment_Id__c"),
        ("Deal Name", "Name"),
        ("Type", "Type"),
    ]
    if sold_pool_rel:
        select_pairs.extend(
            [
                ("Sold Loan: Sold To", f"{sold_pool_rel}.Sold_To__r.Name"),
                ("Sold Loan: Sold Date", f"{sold_pool_rel}.Sold_Date__c"),
                ("Sold Loan: Servicing Status", f"{sold_pool_rel}.Servicing_Status__c"),
            ]
        )
    rename_map = {expr: label for label, expr in select_pairs}

    soql = (
        "SELECT "
        + ", ".join(expr for _label, expr in select_pairs)
        + " FROM Opportunity WHERE "
        + _soql_in("Type", ["DSCR", "Term Loan"])
        + " AND Deal_Loan_Number__c != NULL AND Probability > 0"
    )
    return run_bulk_query(soql, rename_map=rename_map)


def _build_term_wide_like() -> pd.DataFrame:
    contact_field = first_existing_field_name("Opportunity", ["Contact__c", "Primary_Contact__c"])
    contact_rel = relationship_name_for("Opportunity", contact_field) if contact_field else None
    primary_contact_expr = f"{contact_rel}.Name" if contact_rel else "Account.Name"
    funding_expr = opportunity_name_expr(["Current_Funding_Vehicle__c", "FK_Current_Funding_Vehicle__c"])

    select_pairs = [
        ("Servicer Name", "Servicer_Name__c"),
        ("Servicer Commitment Id", "Servicer_Commitment_Id__c"),
        ("Deal Loan Number", "Deal_Loan_Number__c"),
        ("Yardi ID", "Yardi_ID__c"),
        ("Deal Name", "Name"),
        ("Borrower Entity", "Borrower_Entity__r.Name"),
        ("Account Name", "Account.Name"),
        ("Do Not Lend", "Account.Do_Not_Lend__c"),
        ("Primary Contact", primary_contact_expr),
        ("Close Date", "CloseDate"),
        ("Stage", "StageName"),
        ("Current Funding Vehicle", funding_expr),
        ("Next Payment Date", "Next_Payment_Date__c"),
        ("Loan Amount", "Amount"),
        ("Current Servicer UPB", "Current_UPB__c"),
        ("Original Loan Maturity Date", "Stated_Maturity_Date__c"),
        ("CAF Originator", "Owner.Name"),
        ("CAF Originator: Active", "Owner.IsActive"),
        ("Product Type", "LOC_Loan_Type__c"),
        ("Product Sub-Type", "Product_Sub_Type__c"),
        ("Type", "Type"),
        ("Comments AM", "Asset_Management_Comments__c"),
        ("Deal Intro Sub-Source", "Deal_Intro_Sub_Source__c"),
        ("Referral Source Account", "Referral_Source__r.Name"),
        ("Referral Source Contact", "Referral_Source_Contact__r.Name"),
    ]
    rename_map = {expr: label for label, expr in select_pairs}

    soql = (
        "SELECT "
        + ", ".join(expr for _label, expr in select_pairs)
        + " FROM Opportunity WHERE "
        "Deal_Loan_Number__c != NULL AND Probability > 0 AND "
        + _soql_in("Type", TERM_TYPES)
        + " AND "
        + _soql_in("StageName", TERM_ACTIVE_STAGES)
    )

    term_df = run_bulk_query(soql, rename_map=rename_map)
    sold_df = _build_sold_term_like()

    if term_df.empty:
        return term_df

    if not sold_df.empty and "Deal Loan Number" in sold_df.columns:
        sold_keep = [c for c in ["Deal Loan Number", "Sold Loan: Sold To"] if c in sold_df.columns]
        sold_df["_deal_key"] = norm_id_series(sold_df["Deal Loan Number"])
        sold_df = sold_df[["_deal_key"] + [c for c in sold_keep if c != "Deal Loan Number"]].drop_duplicates("_deal_key")
        term_df["_deal_key"] = norm_id_series(term_df["Deal Loan Number"])
        term_df = term_df.merge(sold_df, on="_deal_key", how="left")

    return downcast_numeric_frame(term_df)


def _build_term_asset_like(deal_numbers=None) -> pd.DataFrame:
    opp_rel = property_opportunity_relationship_name()
    deal_numbers = _nonblank_unique(deal_numbers or [])
    soqls = []

    select_pairs = [
        ("Deal Loan Number", f"{opp_rel}.Deal_Loan_Number__c"),
        ("Asset ID", "Asset_ID__c"),
        ("Address", "Name"),
        ("City", "City__c"),
        ("State", "State__c"),
        ("Zip", "ZipCode__c"),
        ("CBSA", "MSA__c"),
        ("# of Units", "Number_of_Units__c"),
        ("Property Type", "Property_Type__c"),
        ("ALA", "ALA__c"),
        ("Value Date", "BPO_Appraisal_Date__c"),
        ("As-Is Value", "Appraised_Value_Amount__c"),
    ]
    rename_map = {expr: label for label, expr in select_pairs}

    base_where = [
        f"{opp_rel}.Deal_Loan_Number__c != NULL",
        f"{opp_rel}.Probability > 0",
        _soql_in(f"{opp_rel}.Type", TERM_TYPES),
        _soql_in(f"{opp_rel}.StageName", TERM_ACTIVE_STAGES),
        "ALA__c > 0",
    ]

    if deal_numbers:
        for chunk in _chunked(deal_numbers, size=200):
            where_parts = base_where + [_soql_in(f"{opp_rel}.Deal_Loan_Number__c", chunk)]
            soqls.append(
                "SELECT "
                + ", ".join(expr for _label, expr in select_pairs)
                + " FROM Property__c WHERE "
                + " AND ".join(where_parts)
            )
    else:
        soqls.append(
            "SELECT "
            + ", ".join(expr for _label, expr in select_pairs)
            + " FROM Property__c WHERE "
            + " AND ".join(base_where)
        )

    return _run_bulk_union(soqls, rename_map=rename_map)


def _bridge_asset_ids_from_spine(bridge_spine: pd.DataFrame):
    if bridge_spine is None or bridge_spine.empty or "Asset ID" not in bridge_spine.columns:
        return []
    return _nonblank_unique(bridge_spine["Asset ID"].tolist())


def _term_deal_numbers_from_wide(term_wide: pd.DataFrame):
    if term_wide is None or term_wide.empty or "Deal Loan Number" not in term_wide.columns:
        return []
    return _nonblank_unique(term_wide["Deal Loan Number"].tolist())


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


def detect_servicer_type(filename: str) -> str:
    n = filename.lower()
    if "shellpoint" in n:
        return "Shellpoint"
    if "corevest_data_tape" in n:
        return "CoreVest_Data_Tape"
    if "corevestloandata" in n:
        return "CoreVestLoanData"
    if "midland" in n:
        return "Midland"
    if "fci" in n:
        return "FCI"
    if n.endswith(".csv"):
        return "CHL"
    raise ValueError(
        "Could not detect servicer file type from the filename. "
        "Use one of these naming patterns: Shellpoint, CHL, CoreVest_Data_Tape, CoreVestLoanData, FCI, Midland."
    )


def report_date_from_scalar(value) -> Optional[date]:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def read_fci_report_date(file_bytes: bytes, sheet_name: str) -> Optional[date]:
    try:
        top = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=None, nrows=1)
        if top.shape[1] >= 2:
            return report_date_from_scalar(top.iloc[0, 1])
    except Exception:
        return None
    return None



def _servicer_specificity_rank(val) -> int:
    s = clean_text(val).lower()
    if not s:
        return 0
    if s == "fci chl streamline":
        return 6
    if "fci 2012632" in s or "fci v1805510" in s:
        return 5
    if "shellpoint" in s:
        return 4
    if any(x in s for x in ["statebridge", "berkadia", "midland", "selene", "sps", "fay", "cornerstone"]):
        return 3
    if "fci" in s or "chl" in s:
        return 2
    return 1


def _servicer_checkpoint_ok(sf_servicer, file_servicer) -> bool:
    file_txt = clean_text(file_servicer)
    if not file_txt:
        return False

    sf_txt = clean_text(sf_servicer)
    if not sf_txt or sf_txt.upper() == "N/A":
        return True

    sf_fam = normalize_servicer_family(sf_txt)
    file_fam = normalize_servicer_family(file_txt)
    if sf_fam and file_fam and sf_fam == file_fam:
        return True

    sf_low = sf_txt.lower()
    file_low = file_txt.lower()
    return sf_low in file_low or file_low in sf_low


def _fill_text_defaults(df: pd.DataFrame, columns: Sequence[str], default: str = "N/A") -> pd.DataFrame:
    out = df
    for c in columns:
        if c in out.columns:
            s = pd.Series(out[c], index=out.index)
            out[c] = s.where(~blankish_mask(s), default)
    return out


def _filter_term_population(sf_term: pd.DataFrame) -> pd.DataFrame:
    if sf_term is None or sf_term.empty:
        return sf_term

    out = sf_term.copy()
    typ = out.get("Type", pd.Series([""] * len(out), index=out.index)).astype("string").str.strip()
    stage = out.get("Stage", pd.Series([""] * len(out), index=out.index)).astype("string").str.strip()
    funding = out.get("Current Funding Vehicle", pd.Series([pd.NA] * len(out), index=out.index))
    current_upb = pd.to_numeric(out.get("Current Servicer UPB", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")

    dscr_like = typ.isin(["DSCR", "Investor DSCR", "Single Rental Loan"])
    term_like = typ.eq("Term Loan")

    dscr_mask = dscr_like & stage.isin(["Approved by Committee", "Closed Won", "REO", "REO-Sold"])
    sold_term_mask = term_like & stage.eq("Sold") & (
        current_upb.fillna(0).gt(0) | (~blankish_mask(pd.Series(funding, index=out.index)))
    )
    live_term_mask = term_like & stage.isin(["Approved by Committee", "Closed Won", "REO", "REO-Sold"])

    return out.loc[dscr_mask | sold_term_mask | live_term_mask].copy()

def _best_header_read_excel(
    file_bytes: bytes,
    required_alias_groups: List[List[str]],
    preferred_sheets: Optional[List[str]] = None,
    max_header_scan: int = 8,
):
    xls = pd.ExcelFile(BytesIO(file_bytes))
    sheet_names = list(xls.sheet_names)

    if preferred_sheets:
        preferred = []
        others = []
        for s in sheet_names:
            if any(p.lower() in s.lower() for p in preferred_sheets):
                preferred.append(s)
            else:
                others.append(s)
        ordered = preferred + others
    else:
        ordered = sheet_names

    best = None
    best_score = -1

    for sheet in ordered:
        for header_row in range(max_header_scan):
            try:
                df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=header_row)
                df = df.dropna(how="all")
                if df.empty:
                    continue
                df.columns = [str(c).strip() for c in df.columns]
                score = sum(first_matching_col(df, aliases) is not None for aliases in required_alias_groups)
                if score > best_score:
                    best_score = score
                    best = (df, sheet, header_row, score)
            except Exception:
                continue

    if best is None or best_score <= 0:
        raise ValueError("Could not find a matching header row.")

    return best


def _best_header_read_csv(file_bytes: bytes, required_alias_groups: List[List[str]], max_header_scan: int = 3):
    best = None
    best_score = -1

    for header_row in range(max_header_scan):
        try:
            df = pd.read_csv(BytesIO(file_bytes), header=header_row)
            df = df.dropna(how="all")
            if df.empty:
                continue
            df.columns = [str(c).strip() for c in df.columns]
            score = sum(first_matching_col(df, aliases) is not None for aliases in required_alias_groups)
            if score > best_score:
                best_score = score
                best = (df, header_row, score)
        except Exception:
            continue

    if best is None or best_score <= 0:
        raise ValueError("Could not find a matching CSV header row.")

    return best


def _series_to_num(df: pd.DataFrame, aliases: Sequence[str]) -> pd.Series:
    col = first_matching_col(df, aliases)
    if not col:
        return pd.Series([np.nan] * len(df), index=df.index)
    return df[col].apply(money_to_float)


def _series_to_dt(df: pd.DataFrame, aliases: Sequence[str]) -> pd.Series:
    col = first_matching_col(df, aliases)
    if not col:
        return pd.Series([pd.NaT] * len(df), index=df.index)
    return df[col].apply(to_dt)


def _series_to_text(df: pd.DataFrame, aliases: Sequence[str]) -> pd.Series:
    col = first_matching_col(df, aliases)
    if not col:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
    return df[col].astype("string")


def _series_to_id(df: pd.DataFrame, aliases: Sequence[str], transform=None) -> pd.Series:
    col = first_matching_col(df, aliases)
    if not col:
        raise ValueError("Required ID column not found.")
    s = df[col]
    return transform(s) if transform else norm_id_series(s)


def _as_of_for_df(df: pd.DataFrame, filename: str, aliases: Sequence[str]) -> date:
    col = first_matching_col(df, aliases)
    if col and df[col].notna().any():
        d = report_date_from_scalar(df[col].dropna().iloc[0])
        if d:
            return d
    return date_from_filename(filename) or today_et()


def parse_servicer_bytes(filename: str, b: bytes) -> pd.DataFrame:
    servicer_type = detect_servicer_type(filename)

    if servicer_type == "Shellpoint":
        df, _hdr, _score = _best_header_read_csv(
            b,
            [["LoanID", "Servicer Loan ID", "Loan Number"], ["PrincipalBalance", "UPB", "Current UPB"]],
            max_header_scan=2,
        )
        sid_col = first_matching_col(df, ["LoanID", "Servicer Loan ID", "Loan Number"])
        if not sid_col:
            sid_col = first_matching_col(df, ["InvestorLoanID", "Investor Loan ID"])
        if not sid_col:
            raise ValueError("Shellpoint file is missing a loan identifier column.")

        out = pd.DataFrame(
            {
                "source_file": filename,
                "servicer": "Shellpoint",
                "servicer_family": "shellpoint",
                "servicer_id": norm_id_series(df[sid_col]),
                "upb": _series_to_num(df, ["PrincipalBalance", "UPB", "Current UPB"]),
                "suspense": _series_to_num(df, ["SuspenseBalance", "Suspense Balance"]),
                "next_payment_date": _series_to_dt(df, ["NextDueDate", "Next Due Date", "Next Payment Date"]),
                "maturity_date": pd.Series([pd.NaT] * len(df), index=df.index),
                "status": _series_to_text(df, ["LoanStatus", "Status", "PayString"]),
                "as_of": pd.to_datetime(_as_of_for_df(df, filename, ["DataAsOf", "Report Date", "As Of Date", "Run Date"])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=["servicer_id"]))

    if servicer_type == "CHL":
        df, _hdr, _score = _best_header_read_csv(
            b,
            [["Servicer Loan ID", "Loan ID", "Loan Number"], ["UPB", "Principal Balance", "Current UPB"]],
        )
        servicer_col = first_matching_col(df, ["Servicing Company", "Servicer", "Servicer Name"])
        servicer = df[servicer_col].astype("string") if servicer_col else pd.Series(["CHL Streamline"] * len(df))
        servicer = servicer.fillna("CHL Streamline")
        servicer = servicer.where(~servicer.astype("string").str.upper().eq("FCI"), "FCI CHL Streamline")
        out = pd.DataFrame(
            {
                "source_file": filename,
                "servicer": servicer,
                "servicer_family": servicer.map(normalize_servicer_family),
                "servicer_id": _series_to_id(df, ["Servicer Loan ID", "Loan ID", "Loan Number"]),
                "upb": _series_to_num(df, ["UPB", "Principal Balance", "Current UPB"]),
                "suspense": np.nan,
                "next_payment_date": _series_to_dt(df, ["Next Due Date", "Due Date", "Next Payment Date"]),
                "maturity_date": _series_to_dt(df, ["Current Maturity Date", "Maturity Date"]),
                "status": _series_to_text(df, ["Performing Status", "Status", "Loan Status"]),
                "as_of": pd.to_datetime(_as_of_for_df(df, filename, ["Report Date", "As Of Date", "Run Date"])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=["servicer_id"]))

    if servicer_type == "CoreVestLoanData":
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [["Loan Number", "Loan No", "BCM Loan#", "Servicer Loan Number"], ["Current UPB", "Principal Balance", "UPB"]],
            preferred_sheets=["loan"],
        )

        def _idfix(s: pd.Series) -> pd.Series:
            sid = norm_id_series(s).astype("string")
            return sid.apply(lambda x: x if pd.isna(x) else (x if x.startswith("0000") else f"0000{x}"))

        out = pd.DataFrame(
            {
                "source_file": filename,
                "servicer": "Statebridge",
                "servicer_family": "statebridge",
                "servicer_id": _idfix(df[first_matching_col(df, ["Loan Number", "Loan No", "Servicer Loan Number"])]),
                "upb": _series_to_num(df, ["Current UPB", "Principal Balance", "UPB"]),
                "suspense": _series_to_num(df, ["Unapplied Balance", "Suspense Balance", "Suspense"]),
                "next_payment_date": _series_to_dt(df, ["Due Date", "Next Due Date", "Next Payment Date"]),
                "maturity_date": _series_to_dt(df, ["Maturity Date", "Current Maturity Date"]),
                "status": _series_to_text(df, ["Loan Status", "Status"]),
                "as_of": pd.to_datetime(_as_of_for_df(df, filename, ["Date", "Run Date", "Report Date", "As Of Date"])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=["servicer_id"]))

    if servicer_type == "CoreVest_Data_Tape":
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [["BCM Loan#", "Loan Number", "Loan No"], ["Principal Balance", "Current UPB", "UPB"]],
            preferred_sheets=["loan"],
        )
        out = pd.DataFrame(
            {
                "source_file": filename,
                "servicer": "Berkadia",
                "servicer_family": "berkadia",
                "servicer_id": _series_to_id(df, ["BCM Loan#", "Loan Number", "Loan No"]),
                "upb": _series_to_num(df, ["Principal Balance", "Current UPB", "UPB"]),
                "suspense": _series_to_num(df, ["Suspense Balance", "Unapplied Balance", "Suspense"]),
                "next_payment_date": _series_to_dt(df, ["Next Payment Due Date", "Next Due Date", "Due Date"]),
                "maturity_date": _series_to_dt(df, ["Maturity Date", "Current Maturity Date"]),
                "status": _series_to_text(df, ["Loan Status", "Status"]),
                "as_of": pd.to_datetime(_as_of_for_df(df, filename, ["Run Date", "Date", "Report Date", "As Of Date"])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=["servicer_id"]))

    if servicer_type == "FCI":
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [["Account", "Loan Number", "Loan No"], ["Current Balance", "Current UPB", "UPB", "Principal Balance"]],
            preferred_sheets=["fci", "cvmaster", "v1805510", "report"],
        )
        servicer = fci_servicer_label_from_filename(filename)
        out = pd.DataFrame(
            {
                "source_file": filename,
                "servicer": servicer,
                "servicer_family": "fci",
                "servicer_id": _series_to_id(df, ["Account", "Loan Number", "Loan No"]),
                "upb": _series_to_num(df, ["Current Balance", "Current UPB", "UPB", "Principal Balance"]),
                "suspense": _series_to_num(df, ["Suspense Pmt.", "Suspense Payment", "Suspense Balance", "Unapplied Balance"]),
                "next_payment_date": _series_to_dt(df, ["Next Due Date", "Due Date", "Next Payment Date"]),
                "maturity_date": _series_to_dt(df, ["Maturity Date", "Current Maturity Date"]),
                "status": _series_to_text(df, ["Status", "Loan Status"]),
                "as_of": pd.to_datetime(_as_of_for_df(df, filename, ["Report Date", "As Of Date", "Date", "Run Date"])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=["servicer_id"]))

    if servicer_type == "Midland":
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [["ServicerLoanNumber", "Servicer Loan Number", "Loan Number"], ["UPB$", "UPB", "Current UPB", "Principal Balance"]],
            preferred_sheets=["export", "midland", "loan"],
        )

        def _idfix(s: pd.Series) -> pd.Series:
            raw = s.astype("string").str.strip()
            raw = raw.str.replace(r"COM$", "", regex=True)
            raw = raw.str.replace(r"[^0-9A-Za-z]", "", regex=True).str.lstrip("0")
            return raw.replace({"": pd.NA})

        out = pd.DataFrame(
            {
                "source_file": filename,
                "servicer": "Midland",
                "servicer_family": "midland",
                "servicer_id": _idfix(df[first_matching_col(df, ["ServicerLoanNumber", "Servicer Loan Number", "Loan Number"])]),
                "upb": _series_to_num(df, ["UPB$", "UPB", "Current UPB", "Principal Balance"]),
                "suspense": np.nan,
                "next_payment_date": _series_to_dt(df, ["NextPaymentDate", "Next Payment Date", "Next Due Date"]),
                "maturity_date": _series_to_dt(df, ["MaturityDate", "Maturity Date"]),
                "status": _series_to_text(df, ["ServicerLoanStatus", "Loan Status", "Status"]),
                "as_of": pd.to_datetime(_as_of_for_df(df, filename, ["ReportDate", "Report Date", "As Of Date", "Run Date"])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=["servicer_id"]))

    raise ValueError("Unhandled servicer type.")



@st.cache_data(show_spinner=False, ttl=6 * 60 * 60, max_entries=128, hash_funcs={UploadBlob: lambda b: f"{b.filename}:{b.file_hash}"})
def parse_servicer_cached(blob: UploadBlob) -> pd.DataFrame:
    return parse_servicer_bytes(blob.filename, blob.data)


def build_servicer_lookup(servicer_uploads: List) -> Tuple[pd.DataFrame, date, pd.DataFrame]:
    blobs: List[UploadBlob] = [make_upload_blob(u) for u in servicer_uploads]
    frames: List[pd.DataFrame] = []
    file_dates: List[date] = []

    for blob in blobs:
        parsed = parse_servicer_cached(blob)
        if parsed.empty:
            continue
        frames.append(parsed)

        if "as_of" in parsed.columns and parsed["as_of"].notna().any():
            d = pd.to_datetime(parsed["as_of"].dropna().iloc[0]).date()
        else:
            d = date_from_filename(blob.filename)

        if d:
            file_dates.append(d)

    full = (
        pd.concat(frames, ignore_index=True, copy=False)
        if frames
        else pd.DataFrame(columns=["source_file", "servicer", "servicer_family", "servicer_id", "upb", "suspense", "next_payment_date", "maturity_date", "status", "as_of"])
    )

    if not full.empty:
        full = full.dropna(subset=["servicer_id"]).copy()
        full["_sid_key"] = id_key_no_leading_zeros(full["servicer_id"])
        full = full.dropna(subset=["_sid_key"]).copy()

        full["_has_upb"] = full["upb"].notna().astype("int8")
        full["_has_nonzero_upb"] = (pd.to_numeric(full["upb"], errors="coerce").fillna(0) > 0).astype("int8")
        full["_has_suspense"] = full["suspense"].notna().astype("int8")
        full["_has_npd"] = full["next_payment_date"].notna().astype("int8")
        full["_has_mat"] = full["maturity_date"].notna().astype("int8")
        full["_label_rank"] = full["servicer"].map(_servicer_specificity_rank).fillna(0).astype("int16")

        full = full.sort_values(
            ["_sid_key", "as_of", "_has_nonzero_upb", "_has_upb", "_has_suspense", "_has_npd", "_has_mat", "_label_rank", "upb"],
            ascending=[True, True, True, True, True, True, True, True, True],
        )

        join = full.drop_duplicates(["_sid_key"], keep="last").drop(
            columns=["_has_upb", "_has_nonzero_upb", "_has_suspense", "_has_npd", "_has_mat", "_label_rank"], errors="ignore"
        )
        preview = full.head(200).copy()
        full = full.drop(columns=["_has_upb", "_has_nonzero_upb", "_has_suspense", "_has_npd", "_has_mat", "_label_rank"], errors="ignore")
    else:
        full["_sid_key"] = pd.Series(dtype="string")
        join = full.copy()
        preview = full.copy()

    run_date = max(file_dates) if file_dates else today_et()
    return downcast_numeric_frame(join), run_date, downcast_numeric_frame(preview)



def _find_upb_col(cols: Sequence[str]) -> Optional[str]:
    for c in cols:
        if isinstance(c, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", c):
            return c
    return None


def read_tab_df_from_active_loans(file_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=3)
    df = df.dropna(how="all").copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_prev_maps(prev_bytes: bytes) -> dict:
    out: dict = {}

    try:
        ba = read_tab_df_from_active_loans(prev_bytes, "Bridge Asset")
        if "Asset ID" in ba.columns:
            keep = [
                c for c in [
                    "Asset ID", "Portfolio", "Segment", "Strategy Grouping", "REO Date",
                    "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag",
                    "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
                    "Construction Mgr.", "CM Assigned Date", "Servicer", "Servicer Status",
                    "Remedy Plan", "Delinquency Notes", "Maturity Status", "Title Company",
                    "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact",
                ] if c in ba.columns
            ]
            tmp = ba[keep].copy()
            tmp["_asset_key"] = norm_id_series(tmp["Asset ID"])
            out["bridge_asset_manual"] = tmp.dropna(subset=["_asset_key"]).drop_duplicates("_asset_key")
    except Exception:
        pass

    try:
        bl = read_tab_df_from_active_loans(prev_bytes, "Bridge Loan")
        keep = [
            c for c in [
                "Deal Number", "Portfolio", "Segment", "Strategy Grouping", "Loan Level Delinquency",
                "Special Focus (Y/N)", "AM Commentary", "3/31 NPL", "Needs NPL Value",
                "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
                "Construction Mgr.", "CM Assigned Date",
            ] if c in bl.columns
        ]
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

    try:
        tl = read_tab_df_from_active_loans(prev_bytes, "Term Loan")
        if "Deal Number" in tl.columns and "REO Date" in tl.columns:
            tmp = tl[["Deal Number", "REO Date"]].copy()
            tmp["_deal_key"] = norm_id_series(tmp["Deal Number"])
            out["term_loan_reo"] = tmp.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")

        keep = [
            c for c in [
                "Deal Number", "Portfolio", "Segment", "CPP JV", "Special Loans List (Y/N)",
                "Asset Manager", "Deal Intro Sub-Source", "Referral Source Account",
                "Referral Source Contact", "AM Commentary", "Servicer", "Loan Buyer",
            ] if c in tl.columns
        ]
        if "Deal Number" in keep and len(keep) > 1:
            tmpm = tl[keep].copy()
            tmpm["_deal_key"] = norm_id_series(tmpm["Deal Number"])
            out["term_loan_manual"] = tmpm.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")

        upb_col_prev = _find_upb_col(tl.columns)
        if upb_col_prev and "Deal Number" in tl.columns:
            tmpu = tl[["Deal Number", upb_col_prev]].copy()
            tmpu["_deal_key"] = norm_id_series(tmpu["Deal Number"])
            tmpu["_prev_upb"] = tmpu[upb_col_prev].apply(money_to_float)
            out["term_loan_upb"] = tmpu.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")[["_deal_key", "_prev_upb"]]
    except Exception:
        pass

    gc.collect()
    return out



def _parse_npl_or_reo_sheet(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=4)
    df = df.dropna(how="all").copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def parse_npl_reo_bytes(file_bytes: bytes) -> dict:
    out = {
        "loan_flags": pd.DataFrame(columns=["_deal_key", "NPL Flag", "Needs NPL Value", "Special Focus (Y/N)"]),
        "asset_flags": pd.DataFrame(columns=["_deal_key", "_asset_key", "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]),
    }

    try:
        npl = _parse_npl_or_reo_sheet(file_bytes, "NPL")
        if "Deal Number" in npl.columns:
            loan_flags = pd.DataFrame(
                {
                    "_deal_key": norm_id_series(npl["Deal Number"]),
                    "NPL Flag": "Y",
                    "Needs NPL Value": "N",
                    "Special Focus (Y/N)": "Y",
                }
            ).dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")

            asset_flags = pd.DataFrame(
                {
                    "_deal_key": norm_id_series(npl["Deal Number"]),
                    "_asset_key": pd.NA,
                    "3/31 NPL (Y/N)": "Y",
                    "Needs NPL Value": "N",
                    "Special Flag": "Y",
                }
            ).dropna(subset=["_deal_key"])

            out["loan_flags"] = pd.concat([out["loan_flags"], loan_flags], ignore_index=True, copy=False)
            out["asset_flags"] = pd.concat([out["asset_flags"], asset_flags], ignore_index=True, copy=False)
    except Exception:
        pass

    try:
        reo = _parse_npl_or_reo_sheet(file_bytes, "REO")
        if "Deal Number" in reo.columns:
            loan_flags = pd.DataFrame(
                {
                    "_deal_key": norm_id_series(reo["Deal Number"]),
                    "NPL Flag": "N",
                    "Needs NPL Value": "N",
                    "Special Focus (Y/N)": "Y",
                }
            ).dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")

            asset_flags = pd.DataFrame(
                {
                    "_deal_key": norm_id_series(reo["Deal Number"]),
                    "_asset_key": norm_id_series(reo["Asset ID"]) if "Asset ID" in reo.columns else pd.Series([pd.NA] * len(reo)),
                    "3/31 NPL (Y/N)": "N",
                    "Needs NPL Value": "N",
                    "Special Flag": "Y",
                }
            ).dropna(subset=["_deal_key"])

            out["loan_flags"] = pd.concat([out["loan_flags"], loan_flags], ignore_index=True, copy=False)
            out["asset_flags"] = pd.concat([out["asset_flags"], asset_flags], ignore_index=True, copy=False)
    except Exception:
        pass

    if not out["loan_flags"].empty:
        out["loan_flags"] = out["loan_flags"].sort_values(["_deal_key", "Special Focus (Y/N)", "NPL Flag"]).drop_duplicates("_deal_key", keep="last")
    if not out["asset_flags"].empty:
        out["asset_flags"] = out["asset_flags"].drop_duplicates(["_deal_key", "_asset_key"], keep="last")

    return out


def build_bridge_asset(
    sf_spine: pd.DataFrame,
    sf_dnl: pd.DataFrame,
    sf_val: pd.DataFrame,
    sf_am: pd.DataFrame,
    sf_active_rm: pd.DataFrame,
    serv_lookup: pd.DataFrame,
    upb_col: str,
    prev_maps: dict,
    template_maps: dict,
    npl_maps: Optional[dict] = None,
) -> pd.DataFrame:
    out = pd.DataFrame(index=sf_spine.index)

    for col, label in BRIDGE_ASSET_FROM_BRIDGE_SPINE.items():
        out[col] = sf_spine[label] if label in sf_spine.columns else pd.NA

    for extra in ["Loan Commitment", "Remaining Commitment", "Current UPB", "Comments AM"]:
        if extra in sf_spine.columns:
            out[extra] = sf_spine[extra]

    out["Portfolio"] = pd.NA
    out["Segment"] = pd.NA
    out["Strategy Grouping"] = pd.NA
    out["Do Not Lend (Y/N)"] = "N"
    out["Active RM"] = "N"
    out["3/31 NPL (Y/N)"] = pd.NA
    out["Needs NPL Value"] = pd.NA
    out["Special Flag"] = pd.NA

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_sid_key"] = id_key_no_leading_zeros(out.get("Servicer ID", pd.Series([None] * len(out))))
    out["_asset_key"] = norm_id_series(out.get("Asset ID", pd.Series([None] * len(out))))

    if not sf_dnl.empty and "Deal Loan Number" in sf_dnl.columns:
        dnl = sf_dnl.copy()
        dnl["_deal_key"] = norm_id_series(dnl["Deal Loan Number"])
        if "Do Not Lend" in dnl.columns:
            dnl = dnl[["_deal_key", "Do Not Lend"]].drop_duplicates("_deal_key")
            out = out.merge(dnl, on="_deal_key", how="left")
            out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend"])
            out = out.drop(columns=["Do Not Lend"], errors="ignore")

    if not sf_active_rm.empty and "Deal Loan Number" in sf_active_rm.columns:
        arm = sf_active_rm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key"]].drop_duplicates("_deal_key")
        arm["Active RM"] = "Y"
        out = out.merge(arm, on="_deal_key", how="left", suffixes=("", "_active"))
        out["Active RM"] = coalesce_keep_nonblank(out.get("Active RM_active", pd.Series([pd.NA] * len(out))), out["Active RM"])
        out = out.drop(columns=["Active RM_active"], errors="ignore")

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

    if npl_maps and not npl_maps.get("asset_flags", pd.DataFrame()).empty:
        af = npl_maps["asset_flags"].copy()
        if "_asset_key" in af.columns:
            af_asset = af[af["_asset_key"].notna()].copy()
            af_deal = af[af["_asset_key"].isna()].copy()
            if not af_deal.empty:
                keep = ["_deal_key", "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]
                af_deal = af_deal[keep].drop_duplicates("_deal_key")
                out = out.merge(af_deal, on="_deal_key", how="left", suffixes=("", "_npldeal"))
                for c in ["3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]:
                    out[c] = coalesce_keep_nonblank(out.get(f"{c}_npldeal", pd.Series([pd.NA] * len(out))), out[c])
                    out = out.drop(columns=[f"{c}_npldeal"], errors="ignore")
            if not af_asset.empty:
                keep = ["_deal_key", "_asset_key", "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]
                af_asset = af_asset[keep].drop_duplicates(["_deal_key", "_asset_key"])
                out = out.merge(af_asset, on=["_deal_key", "_asset_key"], how="left", suffixes=("", "_nplasset"))
                for c in ["3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]:
                    out[c] = coalesce_keep_nonblank(out.get(f"{c}_nplasset", pd.Series([pd.NA] * len(out))), out[c])
                    out = out.drop(columns=[f"{c}_nplasset"], errors="ignore")

    if "bridge_asset_manual" in prev_maps:
        man = prev_maps["bridge_asset_manual"].copy()
        keep_cols = ["_asset_key"] + [c for c in [
            "Portfolio", "Segment", "Strategy Grouping", "REO Date",
            "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag",
            "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
            "Construction Mgr.", "CM Assigned Date", "Servicer", "Servicer Status",
            "Remedy Plan", "Delinquency Notes", "Maturity Status", "Title Company",
            "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact",
        ] if c in man.columns]
        out = out.merge(man[keep_cols], on="_asset_key", how="left", suffixes=("", "_prev"))
        for c in [
            "Portfolio", "Segment", "Strategy Grouping", "REO Date",
            "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag",
            "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
            "Construction Mgr.", "CM Assigned Date", "Servicer", "Servicer Status",
            "Remedy Plan", "Delinquency Notes", "Maturity Status", "Title Company",
            "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact",
        ]:
            if f"{c}_prev" in out.columns:
                out[c] = coalesce_keep_nonblank(out[f"{c}_prev"], out.get(c, pd.Series([pd.NA] * len(out))))
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    seg_guess = out.apply(
        lambda r: derive_bridge_segment(r.get("Deal Number"), r.get("Financing"), r.get("Loan Buyer"), template_maps),
        axis=1,
    )
    strat_guess = out["Project Strategy"].map(lambda x: strategy_grouping_from_project_strategy(x, template_maps.get("strategy_map", {})))
    port_guess = out.apply(
        lambda r: derive_bridge_portfolio(
            r.get("Product Type"),
            r.get("Segment") if has_any_value(r.get("Segment")) else derive_bridge_segment(r.get("Deal Number"), r.get("Financing"), r.get("Loan Buyer"), template_maps),
            r.get("Financing"),
            r.get("Deal Intro Sub-Source"),
            r.get("Deal Number"),
        ),
        axis=1,
    )

    out["Segment"] = coalesce_keep_nonblank(out["Segment"], seg_guess)
    out["Strategy Grouping"] = coalesce_keep_nonblank(out["Strategy Grouping"], strat_guess)
    out["Portfolio"] = coalesce_keep_nonblank(out["Portfolio"], port_guess)

    prop_npd = pd.to_datetime(sf_spine.get("Property Next Payment Date", pd.Series([pd.NaT] * len(out))), errors="coerce")
    opp_npd = pd.to_datetime(sf_spine.get("Opportunity Next Payment Date", pd.Series([pd.NaT] * len(out))), errors="coerce")
    sf_next_payment = prop_npd.where(prop_npd.notna(), opp_npd)
    sf_current_upb = pd.to_numeric(sf_spine.get("Current UPB", pd.Series([np.nan] * len(out))), errors="coerce")

    if not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy()
        s = s.rename(
            columns={
                "servicer": "Servicer",
                "upb": "_loan_upb",
                "suspense": "_loan_suspense",
                "next_payment_date": "_serv_next_payment_date",
                "maturity_date": "Servicer Maturity Date",
                "status": "Servicer Status",
            }
        )

        out = out.merge(
            s[["_sid_key", "Servicer", "_loan_upb", "_loan_suspense", "_serv_next_payment_date", "Servicer Maturity Date", "Servicer Status", "source_file"]],
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

        loan_upb = pd.to_numeric(out.get("_loan_upb", pd.Series([np.nan] * len(out))), errors="coerce")
        prev_upb_vals = pd.to_numeric(out.get("_prev_upb", pd.Series([np.nan] * len(out))), errors="coerce")
        fill_val = prev_upb_vals.fillna(0.0)
        out["_loan_upb"] = np.where(reo_mask & ((loan_upb.isna()) | (loan_upb <= 0)), fill_val, loan_upb)

        out["_w"] = sf_current_upb
        out["_w_sum"] = out.groupby("_sid_key")["_w"].transform("sum")
        out["_n_in_loan"] = out.groupby("_sid_key")["_sid_key"].transform("size").replace({0: np.nan})

        out[upb_col] = np.where(
            out["_w_sum"].fillna(0) > 0,
            out["_loan_upb"] * (out["_w"] / out["_w_sum"]),
            out["_loan_upb"] / out["_n_in_loan"],
        )

        serv_suspense_alloc = np.where(
            out["_w_sum"].fillna(0) > 0,
            out["_loan_suspense"] * (out["_w"] / out["_w_sum"]),
            out["_loan_suspense"] / out["_n_in_loan"],
        )
        out["Suspense Balance"] = pd.to_numeric(serv_suspense_alloc, errors="coerce")

        current_upb_series = pd.to_numeric(out[upb_col], errors="coerce")
        out[upb_col] = current_upb_series.where(current_upb_series.notna(), sf_current_upb)

        out["Next Payment Date"] = pd.to_datetime(out.get("_serv_next_payment_date"), errors="coerce")
        out["Next Payment Date"] = pd.to_datetime(out["Next Payment Date"], errors="coerce").where(
            pd.to_datetime(out["Next Payment Date"], errors="coerce").notna(),
            sf_next_payment,
        )

        out = out.drop(columns=["_prev_upb"], errors="ignore")
    else:
        out[upb_col] = sf_current_upb
        out["Servicer"] = pd.NA
        out["Next Payment Date"] = sf_next_payment
        out["Servicer Maturity Date"] = pd.NaT
        out["Servicer Status"] = pd.NA
        out["Suspense Balance"] = np.nan

    if "Approved Advance Amount Funded" in sf_spine.columns:
        out["SF Funded Amount"] = pd.to_numeric(sf_spine["Approved Advance Amount Funded"], errors="coerce")
    else:
        out["SF Funded Amount"] = (
            pd.to_numeric(out.get("Initial Disbursement Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Renovation Holdback Funded", 0), errors="coerce").fillna(0)
            + pd.to_numeric(out.get("Interest Allocation Funded", 0), errors="coerce").fillna(0)
        )

    if "Is Special Asset (Y/N)" in out.columns:
        out["Is Special Asset (Y/N)"] = _yn_from_bool_series(out["Is Special Asset (Y/N)"])

    out["3/31 NPL (Y/N)"] = coalesce_keep_nonblank(out["3/31 NPL (Y/N)"], pd.Series(["N"] * len(out), index=out.index))
    out["Needs NPL Value"] = coalesce_keep_nonblank(out["Needs NPL Value"], pd.Series(["N"] * len(out), index=out.index))
    out["Special Flag"] = coalesce_keep_nonblank(out["Special Flag"], pd.Series(["N"] * len(out), index=out.index))

    out = _fill_text_defaults(
        out,
        [
            "Loan Buyer", "Servicer", "Primary Contact",
            "AM 1 Assigned Date", "AM 2 Assigned Date", "CM Assigned Date",
            "Remedy Plan", "Delinquency Notes", "Maturity Status",
            "Special Asset Status", "Special Asset Reason", "Special Asset: Special Asset Status",
            "Special Asset: Resolved Date", "Forbearance Term Date", "REO Date",
            "Most Recent Appraisal Order Date", "Updated Valuation Date",
            "Title Company", "Tax Due Date", "Tax Frequency", "Tax Commentary",
            "Originator", "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact",
            "Servicer Status", "Asset Manager 1", "Asset Manager 2", "Construction Mgr.",
        ],
    )

    return downcast_numeric_frame(out)



def build_term_loan(
    sf_term: pd.DataFrame,
    sf_am: pd.DataFrame,
    sf_active_rm: pd.DataFrame,
    serv_lookup: pd.DataFrame,
    upb_col: str,
    prev_maps: dict,
    template_maps: dict,
) -> pd.DataFrame:
    sf_term = _filter_term_population(sf_term)
    out = pd.DataFrame(index=sf_term.index)

    for col, label in TERM_LOAN_FROM_TERM_WIDE.items():
        out[col] = sf_term[label] if label in sf_term.columns else pd.NA

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))

    if "Do Not Lend (Y/N)" in out.columns:
        out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend (Y/N)"])

    out["Loan Buyer"] = sf_term["Sold Loan: Sold To"] if "Sold Loan: Sold To" in sf_term.columns else pd.NA
    out["Active RM"] = "N"
    out["Servicer"] = sf_term["Servicer Name"] if "Servicer Name" in sf_term.columns else pd.NA
    out["Maturity Date"] = pd.to_datetime(sf_term["Original Loan Maturity Date"], errors="coerce") if "Original Loan Maturity Date" in sf_term.columns else pd.NaT
    out["Next Payment Date"] = pd.to_datetime(sf_term["Next Payment Date"], errors="coerce") if "Next Payment Date" in sf_term.columns else pd.NaT

    if not sf_active_rm.empty and "Deal Loan Number" in sf_active_rm.columns:
        arm = sf_active_rm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key"]].drop_duplicates("_deal_key")
        arm["Active RM"] = "Y"
        out = out.merge(arm, on="_deal_key", how="left", suffixes=("", "_active"))
        out["Active RM"] = coalesce_keep_nonblank(out.get("Active RM_active", pd.Series([pd.NA] * len(out))), out["Active RM"])
        out = out.drop(columns=["Active RM_active"], errors="ignore")

    cls = sf_term.apply(
        lambda r: pd.Series(
            derive_term_portfolio_segment(
                r.get("Type"),
                r.get("Current Funding Vehicle"),
                r.get("Sold Loan: Sold To"),
                r.get("Deal Loan Number"),
                template_maps,
            ),
            index=["Portfolio", "Segment", "CPP JV"],
        ),
        axis=1,
    )
    out["Portfolio"] = cls["Portfolio"]
    out["Segment"] = cls["Segment"]
    out["CPP JV"] = cls["CPP JV"]

    if "term_loan_manual" in prev_maps:
        man = prev_maps["term_loan_manual"].copy()
        out = out.merge(
            man[["_deal_key"] + [c for c in [
                "Portfolio", "Segment", "CPP JV", "Special Loans List (Y/N)",
                "Asset Manager", "Deal Intro Sub-Source", "Referral Source Account",
                "Referral Source Contact", "AM Commentary", "Servicer", "Loan Buyer",
            ] if c in man.columns]],
            on="_deal_key",
            how="left",
            suffixes=("", "_prev"),
        )
        for c in [
            "Portfolio", "Segment", "CPP JV", "Special Loans List (Y/N)",
            "Asset Manager", "Deal Intro Sub-Source", "Referral Source Account",
            "Referral Source Contact", "AM Commentary", "Servicer", "Loan Buyer",
        ]:
            if f"{c}_prev" in out.columns:
                out[c] = coalesce_keep_nonblank(out[f"{c}_prev"], out.get(c, pd.Series([pd.NA] * len(out))))
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    if not sf_am.empty and "Deal Loan Number" in sf_am.columns:
        am = sf_am.copy()
        am["_deal_key"] = norm_id_series(am["Deal Loan Number"])
        am["_dt"] = pd.to_datetime(am.get("Date Assigned"), errors="coerce")
        am = am.sort_values(["_deal_key", "Team Role", "_dt"]).drop_duplicates(["_deal_key", "Team Role"], keep="last")

        am1 = am[am["Team Role"].astype("string").str.strip().eq("Asset Manager")][["_deal_key", "Team Member Name"]]
        am1 = am1.drop_duplicates("_deal_key")
        out = out.merge(am1, on="_deal_key", how="left")
        out["Asset Manager"] = coalesce_keep_nonblank(out.get("Asset Manager", pd.Series([pd.NA] * len(out))), out["Team Member Name"])
        out = out.drop(columns=["Team Member Name"], errors="ignore")
    else:
        out["Asset Manager"] = out.get("Asset Manager", pd.Series([pd.NA] * len(out)))

    out["Servicer ID"] = sf_term["Servicer Commitment Id"] if "Servicer Commitment Id" in sf_term.columns else pd.NA
    out["_sid_key"] = id_key_no_leading_zeros(out["Servicer ID"].astype("string"))

    sf_upb_fallback = pd.to_numeric(
        sf_term["Current Servicer UPB"] if "Current Servicer UPB" in sf_term.columns else pd.Series([np.nan] * len(out)),
        errors="coerce",
    )

    if not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy()
        s2 = s.rename(
            columns={
                "servicer": "_servicer_file",
                "upb": "_servicer_upb",
                "next_payment_date": "_servicer_next_payment_date",
                "maturity_date": "_servicer_maturity_date",
            }
        )[["_sid_key", "_servicer_file", "_servicer_upb", "_servicer_next_payment_date", "_servicer_maturity_date"]]

        out = out.merge(s2, on="_sid_key", how="left")
        checkpoint_ok = pd.Series(
            [_servicer_checkpoint_ok(sf_val, file_val) for sf_val, file_val in zip(out["Servicer"], out["_servicer_file"])],
            index=out.index,
        )

        file_serv = pd.Series(out["_servicer_file"], index=out.index)
        out["Servicer"] = file_serv.where(checkpoint_ok & (~blankish_mask(file_serv)), out["Servicer"])

        file_mat = pd.to_datetime(out["_servicer_maturity_date"], errors="coerce")
        cur_mat = pd.to_datetime(out["Maturity Date"], errors="coerce")
        out["Maturity Date"] = file_mat.where(checkpoint_ok & file_mat.notna(), cur_mat)

        file_npd = pd.to_datetime(out["_servicer_next_payment_date"], errors="coerce")
        cur_npd = pd.to_datetime(out["Next Payment Date"], errors="coerce")
        out["Next Payment Date"] = file_npd.where(checkpoint_ok & file_npd.notna(), cur_npd)

        file_upb = pd.to_numeric(out["_servicer_upb"], errors="coerce")
        out[upb_col] = file_upb.where(checkpoint_ok & file_upb.notna(), sf_upb_fallback)
        out = out.drop(columns=["_servicer_file", "_servicer_upb", "_servicer_next_payment_date", "_servicer_maturity_date"], errors="ignore")
    else:
        out[upb_col] = sf_upb_fallback

    out["REO Date"] = pd.NaT
    if "term_loan_reo" in prev_maps:
        reo = prev_maps["term_loan_reo"][["_deal_key", "REO Date"]].copy()
        out = out.merge(reo, on="_deal_key", how="left", suffixes=("", "_prev"))
        out["REO Date"] = pd.to_datetime(out["REO Date_prev"], errors="coerce").where(
            pd.to_datetime(out["REO Date_prev"], errors="coerce").notna(),
            pd.to_datetime(out["REO Date"], errors="coerce"),
        )
        out = out.drop(columns=["REO Date_prev"], errors="ignore")

    if "term_loan_upb" in prev_maps and upb_col in out.columns:
        prevu = prev_maps["term_loan_upb"].copy()
        out = out.merge(prevu, on="_deal_key", how="left")

        reo_mask = pd.to_datetime(out["REO Date"], errors="coerce").notna()
        cur_upb = pd.to_numeric(out[upb_col], errors="coerce")
        prev_upb = pd.to_numeric(out.get("_prev_upb", np.nan), errors="coerce")
        fill_val = prev_upb.fillna(0.0)
        out[upb_col] = np.where(reo_mask & ((cur_upb.isna()) | (cur_upb <= 0)), fill_val, cur_upb)
        out = out.drop(columns=["_prev_upb"], errors="ignore")

    keep_mask = pd.to_numeric(out[upb_col], errors="coerce").fillna(0).gt(0)
    out = out.loc[keep_mask].copy()

    out["Special Loans List (Y/N)"] = coalesce_keep_nonblank(
        out.get("Special Loans List (Y/N)", pd.Series([pd.NA] * len(out), index=out.index)),
        pd.Series(["N"] * len(out), index=out.index),
    )

    out = _fill_text_defaults(
        out,
        [
            "Servicer ID", "Servicer", "Loan Buyer", "REO Date", "Asset Manager",
            "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact", "AM Commentary",
        ],
    )

    return downcast_numeric_frame(out)



def build_term_asset(sf_term_asset: pd.DataFrame, term_loan: pd.DataFrame, upb_col: str) -> pd.DataFrame:
    out = pd.DataFrame(index=sf_term_asset.index)

    for col, label in TERM_ASSET_FROM_TERM_ASSET_REPORT.items():
        out[col] = sf_term_asset[label] if label in sf_term_asset.columns else pd.NA

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["CPP JV"] = pd.NA

    tl = term_loan.copy()
    tl["_deal_key"] = norm_id_series(tl.get("Deal Number", pd.Series([None] * len(tl))))

    valid_deals = set(tl["_deal_key"].dropna().tolist())
    out = out[out["_deal_key"].isin(valid_deals)].copy()

    if "CPP JV" in tl.columns:
        tl_cpp = tl[["_deal_key", "CPP JV"]].drop_duplicates("_deal_key")
        out = out.merge(tl_cpp, on="_deal_key", how="left", suffixes=("", "_loan"))
        out["CPP JV"] = coalesce_keep_nonblank(out.get("CPP JV_loan", pd.Series([pd.NA] * len(out))), out["CPP JV"])
        out = out.drop(columns=["CPP JV_loan"], errors="ignore")

    if upb_col in tl.columns:
        tl_upb = tl[["_deal_key", upb_col]].drop_duplicates("_deal_key")
        out = out.merge(tl_upb, on="_deal_key", how="left")

        ala = pd.to_numeric(out.get("Property ALA", np.nan), errors="coerce")
        ala_sum = ala.groupby(out["_deal_key"]).transform("sum")
        out[upb_col] = np.where(ala_sum > 0, out[upb_col] * (ala / ala_sum), out[upb_col])

    for c in ["CPP JV"]:
        if c in out.columns:
            out[c] = out[c].replace({"": pd.NA})

    return downcast_numeric_frame(out)


def build_bridge_loan(bridge_asset: pd.DataFrame, upb_col: str, prev_maps: dict, npl_maps: Optional[dict] = None) -> pd.DataFrame:
    ba = bridge_asset.copy()
    g = ba.groupby("_deal_key", dropna=True)

    def _first(series: pd.Series):
        return first_nonblank(series)

    def _max_dt(series: pd.Series):
        s = pd.to_datetime(series, errors="coerce").dropna()
        return s.max() if len(s) else pd.NaT

    def _min_dt(series: pd.Series):
        s = pd.to_datetime(series, errors="coerce").dropna()
        return s.min() if len(s) else pd.NaT

    def _yn_any(series: pd.Series):
        vals = pd.Series(series).astype("string").str.strip().str.upper()
        return "Y" if vals.eq("Y").any() else "N"

    out = pd.DataFrame(
        {
            "Deal Number": g["Deal Number"].first() if "Deal Number" in ba.columns else pd.Series(dtype="string"),
            "Portfolio": g["Portfolio"].apply(_first) if "Portfolio" in ba.columns else pd.Series(dtype="string"),
            "Loan Buyer": g["Loan Buyer"].apply(_first) if "Loan Buyer" in ba.columns else pd.Series(dtype="string"),
            "Financing": g["Financing"].apply(_first) if "Financing" in ba.columns else pd.Series(dtype="string"),
            "Servicer ID": g["Servicer ID"].apply(first_or_various) if "Servicer ID" in ba.columns else pd.Series(dtype="string"),
            "Servicer": g["Servicer"].apply(first_or_various) if "Servicer" in ba.columns else pd.Series(dtype="string"),
            "Deal Name": g["Deal Name"].apply(_first) if "Deal Name" in ba.columns else pd.Series(dtype="string"),
            "Borrower Name": g["Borrower Entity"].apply(_first) if "Borrower Entity" in ba.columns else pd.Series(dtype="string"),
            "Account": g["Account Name"].apply(_first) if "Account Name" in ba.columns else pd.Series(dtype="string"),
            "Do Not Lend (Y/N)": g["Do Not Lend (Y/N)"].max() if "Do Not Lend (Y/N)" in ba.columns else pd.Series(dtype="string"),
            "Primary Contact": g["Primary Contact"].apply(_first) if "Primary Contact" in ba.columns else pd.Series(dtype="string"),
            "Number of Assets": g["Asset ID"].nunique() if "Asset ID" in ba.columns else pd.Series(dtype="float"),
            "# of Units": pd.to_numeric(g["# of Units"].sum(min_count=1), errors="coerce") if "# of Units" in ba.columns else np.nan,
            "State(s)": g["State"].apply(lambda s: ", ".join(sorted({clean_text(x) for x in s if clean_text(x)}))) if "State" in ba.columns else pd.Series(dtype="string"),
            "Origination Date": g["Origination Date"].apply(_min_dt) if "Origination Date" in ba.columns else pd.NaT,
            "Last Funding Date": g["Last Funding Date"].apply(_max_dt) if "Last Funding Date" in ba.columns else pd.NaT,
            "Original Maturity Date": g["Original Loan Maturity date"].apply(_first) if "Original Loan Maturity date" in ba.columns else pd.NaT,
            "Current Maturity Date": g["Current Loan Maturity date"].apply(_first) if "Current Loan Maturity date" in ba.columns else pd.NaT,
            "Next Advance Maturity Date": g["Servicer Maturity Date"].apply(_first) if "Servicer Maturity Date" in ba.columns else pd.NaT,
            "Next Payment Date": g["Next Payment Date"].apply(_min_dt) if "Next Payment Date" in ba.columns else pd.NaT,
            "Days Past Due": pd.NA,
            "Loan Level Delinquency": pd.NA,
            "Loan Commitment": g["Loan Commitment"].apply(_first) if "Loan Commitment" in ba.columns else np.nan,
            "Active Funded Amount": pd.to_numeric(g["SF Funded Amount"].sum(min_count=1), errors="coerce") if "SF Funded Amount" in ba.columns else np.nan,
            upb_col: pd.to_numeric(g[upb_col].sum(min_count=1), errors="coerce") if upb_col in ba.columns else np.nan,
            "Suspense Balance": pd.to_numeric(g["Suspense Balance"].sum(min_count=1), errors="coerce") if "Suspense Balance" in ba.columns else np.nan,
            "Remaining Commitment": g["Remaining Commitment"].apply(_first) if "Remaining Commitment" in ba.columns else np.nan,
            "Most Recent Valuation Date": g["Updated Valuation Date"].apply(_max_dt) if "Updated Valuation Date" in ba.columns else pd.NaT,
            "Most Recent As-Is Value": pd.to_numeric(g["Updated As-Is Value"].sum(min_count=1), errors="coerce") if "Updated As-Is Value" in ba.columns else np.nan,
            "Most Recent ARV": pd.to_numeric(g["Updated ARV"].sum(min_count=1), errors="coerce") if "Updated ARV" in ba.columns else np.nan,
            "Initial Disbursement Funded": pd.to_numeric(g["Initial Disbursement Funded"].sum(min_count=1), errors="coerce") if "Initial Disbursement Funded" in ba.columns else np.nan,
            "Renovation Holdback": pd.to_numeric(g["Renovation Holdback"].sum(min_count=1), errors="coerce") if "Renovation Holdback" in ba.columns else np.nan,
            "Renovation HB Funded": pd.to_numeric(g["Renovation Holdback Funded"].sum(min_count=1), errors="coerce") if "Renovation Holdback Funded" in ba.columns else np.nan,
            "Renovation HB Remaining": pd.to_numeric(g["Renovation Holdback Remaining"].sum(min_count=1), errors="coerce") if "Renovation Holdback Remaining" in ba.columns else np.nan,
            "Interest Allocation": pd.to_numeric(g["Interest Allocation"].sum(min_count=1), errors="coerce") if "Interest Allocation" in ba.columns else np.nan,
            "Interest Allocation Funded": pd.to_numeric(g["Interest Allocation Funded"].sum(min_count=1), errors="coerce") if "Interest Allocation Funded" in ba.columns else np.nan,
            "Loan Stage": g["Loan Stage"].apply(_first) if "Loan Stage" in ba.columns else pd.Series(dtype="string"),
            "Segment": g["Segment"].apply(_first) if "Segment" in ba.columns else pd.Series(dtype="string"),
            "Product Type": g["Product Type"].apply(_first) if "Product Type" in ba.columns else pd.Series(dtype="string"),
            "Product Sub Type": g["Product Sub-Type"].apply(_first) if "Product Sub-Type" in ba.columns else pd.Series(dtype="string"),
            "Transaction Type": g["Transaction Type"].apply(_first) if "Transaction Type" in ba.columns else pd.Series(dtype="string"),
            "Project Strategy": g["Project Strategy"].apply(_first) if "Project Strategy" in ba.columns else pd.Series(dtype="string"),
            "Strategy Grouping": g["Strategy Grouping"].apply(_first) if "Strategy Grouping" in ba.columns else pd.Series(dtype="string"),
            "CV Originator": g["Originator"].apply(_first) if "Originator" in ba.columns else pd.Series(dtype="string"),
            "Active RM": g["Active RM"].apply(_first) if "Active RM" in ba.columns else pd.Series(dtype="string"),
            "Deal Intro Sub-Source": g["Deal Intro Sub-Source"].apply(_first) if "Deal Intro Sub-Source" in ba.columns else pd.Series(dtype="string"),
            "Referral Source Account": g["Referral Source Account"].apply(_first) if "Referral Source Account" in ba.columns else pd.Series(dtype="string"),
            "Referral Source Contact": g["Referral Source Contact"].apply(_first) if "Referral Source Contact" in ba.columns else pd.Series(dtype="string"),
            "3/31 NPL": g["3/31 NPL (Y/N)"].apply(_yn_any) if "3/31 NPL (Y/N)" in ba.columns else "N",
            "Needs NPL Value": g["Needs NPL Value"].apply(_yn_any) if "Needs NPL Value" in ba.columns else "N",
            "Special Focus (Y/N)": g["Special Flag"].apply(_yn_any) if "Special Flag" in ba.columns else "N",
            "Asset Manager 1": g["Asset Manager 1"].apply(_first) if "Asset Manager 1" in ba.columns else pd.Series(dtype="string"),
            "AM 1 Assigned Date": g["AM 1 Assigned Date"].apply(_first) if "AM 1 Assigned Date" in ba.columns else pd.NaT,
            "Asset Manager 2": g["Asset Manager 2"].apply(_first) if "Asset Manager 2" in ba.columns else pd.Series(dtype="string"),
            "AM 2 Assigned Date": g["AM 2 Assigned Date"].apply(_first) if "AM 2 Assigned Date" in ba.columns else pd.NaT,
            "Construction Mgr.": g["Construction Mgr."].apply(_first) if "Construction Mgr." in ba.columns else pd.Series(dtype="string"),
            "CM Assigned Date": g["CM Assigned Date"].apply(_first) if "CM Assigned Date" in ba.columns else pd.NaT,
            "AM Commentary": g["Comments AM"].apply(_first) if "Comments AM" in ba.columns else pd.Series(dtype="string"),
        }
    ).reset_index(drop=True)

    out["_deal_key"] = norm_id_series(out["Deal Number"])

    if npl_maps and not npl_maps.get("loan_flags", pd.DataFrame()).empty:
        loan_flags = npl_maps["loan_flags"].copy().drop_duplicates("_deal_key")
        out = out.merge(loan_flags, on="_deal_key", how="left", suffixes=("", "_npl"))
        if "NPL Flag_npl" in out.columns:
            out["3/31 NPL"] = coalesce_keep_nonblank(out["NPL Flag_npl"], out["3/31 NPL"])
            out = out.drop(columns=["NPL Flag_npl"], errors="ignore")
        for c in ["Needs NPL Value", "Special Focus (Y/N)"]:
            if f"{c}_npl" in out.columns:
                out[c] = coalesce_keep_nonblank(out[f"{c}_npl"], out[c])
                out = out.drop(columns=[f"{c}_npl"], errors="ignore")

    if "bridge_loan_manual" in prev_maps and not out.empty:
        man = prev_maps["bridge_loan_manual"].copy()
        out = out.merge(man, on="_deal_key", how="left", suffixes=("", "_prev"))
        for c in [
            "Portfolio", "Segment", "Strategy Grouping", "Loan Level Delinquency", "Special Focus (Y/N)",
            "AM Commentary", "3/31 NPL", "Needs NPL Value",
            "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
            "Construction Mgr.", "CM Assigned Date",
        ]:
            if f"{c}_prev" in out.columns:
                out[c] = coalesce_keep_nonblank(out[f"{c}_prev"], out.get(c, pd.Series([pd.NA] * len(out))))
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    out["Special Focus (Y/N)"] = coalesce_keep_nonblank(out["Special Focus (Y/N)"], pd.Series(["N"] * len(out), index=out.index))
    out["3/31 NPL"] = coalesce_keep_nonblank(out["3/31 NPL"], pd.Series(["N"] * len(out), index=out.index))
    out["Needs NPL Value"] = coalesce_keep_nonblank(out["Needs NPL Value"], pd.Series(["N"] * len(out), index=out.index))

    out = _fill_text_defaults(
        out,
        [
            "Loan Buyer", "Servicer ID", "Servicer", "Primary Contact", "Loan Level Delinquency",
            "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
            "Construction Mgr.", "CM Assigned Date", "Deal Intro Sub-Source",
            "Referral Source Account", "Referral Source Contact", "AM Commentary",
        ],
    )

    return downcast_numeric_frame(out.drop(columns=["_deal_key"], errors="ignore"))



def _set_scaffold_cell(ws, row_idx: int, col_idx: int, value):
    cell = ws.cell(row_idx, col_idx)
    cell.value = value
    if isinstance(value, (date, datetime)):
        cell.number_format = DATE_NUMBER_FORMAT


def restore_template_scaffold(wb, run_dt: date, upb_header: str):
    q_end = quarter_end_for_run(run_dt)

    for sheet_name, blueprint in SHEET_BLUEPRINTS.items():
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]

        for col_idx, val in blueprint.get("row1", {}).items():
            _set_scaffold_cell(ws, 1, col_idx, q_end if val == "__QEND__" else val)

        for col_idx, val in blueprint.get("row2", {}).items():
            _set_scaffold_cell(ws, 2, col_idx, q_end if val == "__QEND__" else val)

        subtotal_col = blueprint.get("subtotal_col")
        for col_idx, val in blueprint.get("row3", {}).items():
            if val == "__RUN_DT__":
                _set_scaffold_cell(ws, 3, col_idx, run_dt)
            elif val == "__SUBTOTAL__":
                col_letter = get_column_letter(subtotal_col)
                ws.cell(3, col_idx).value = f"=SUBTOTAL(9,{col_letter}5:{col_letter}{max(5, ws.max_row)})"
            else:
                ws.cell(3, col_idx).value = val

        for col_idx, val in blueprint.get("row4", {}).items():
            ws.cell(4, col_idx).value = upb_header if val == "__UPB__" else val


def _parse_direct_ref_formula(formula_text: str):
    if not isinstance(formula_text, str):
        return None
    txt = formula_text.strip()
    if not txt.startswith("="):
        return None
    txt = txt[1:].lstrip("+").strip()

    m = re.fullmatch(r"'([^']+)'!\$?([A-Z]{1,3})\$?(\d+)", txt)
    if m:
        return m.group(1), f"{m.group(2)}{m.group(3)}"

    m = re.fullmatch(r"([A-Za-z0-9_ ]+)!\$?([A-Z]{1,3})\$?(\d+)", txt)
    if m:
        return m.group(1), f"{m.group(2)}{m.group(3)}"

    return None


def _resolve_header_value(wb, ws, cell, upb_header: str, max_depth: int = 6) -> str:
    cur_val = cell.value

    for _ in range(max_depth):
        if cur_val is None:
            return ""
        if not isinstance(cur_val, str):
            return str(cur_val).strip()

        txt = cur_val.strip()
        if UPB_HEADER_RE.search(txt):
            return upb_header

        ref = _parse_direct_ref_formula(txt)
        if not ref:
            return txt

        ref_sheet, ref_cell = ref
        if ref_sheet not in wb.sheetnames:
            return txt
        cur_val = wb[ref_sheet][ref_cell].value

    if cur_val is None:
        return ""
    if isinstance(cur_val, str) and UPB_HEADER_RE.search(cur_val.strip()):
        return upb_header
    return str(cur_val).strip()


def header_tuples_from_ws(ws, header_row: int = 4, wb=None, upb_header: Optional[str] = None) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    row = list(ws.iter_rows(min_row=header_row, max_row=header_row, values_only=False))[0]

    for col_idx, cell in enumerate(row, start=1):
        if wb is not None and upb_header is not None:
            header = _resolve_header_value(wb, ws, cell, upb_header)
        else:
            v = cell.value
            header = "" if v is None else str(v).strip()
        if header:
            out.append((col_idx, header.strip()))
    return out


def formula_col_indices(ws_formula, start_row: int = 5, header_row: int = 4, scan_rows: int = 50) -> Set[int]:
    fcols: Set[int] = set()
    max_scan_row = min(ws_formula.max_row, start_row + scan_rows - 1)

    for r in range(start_row, max_scan_row + 1):
        for col_idx in range(1, ws_formula.max_column + 1):
            v = ws_formula.cell(r, col_idx).value
            if isinstance(v, str) and v.startswith("="):
                fcols.add(col_idx)
    return fcols


def _capture_formula_seeds(ws_formula, formula_cols: Set[int], start_row: int = 5, scan_rows: int = 50):
    seeds = {}
    max_scan_row = min(ws_formula.max_row, start_row + scan_rows - 1)

    for col_idx in sorted(formula_cols):
        for r in range(start_row, max_scan_row + 1):
            v = ws_formula.cell(r, col_idx).value
            if isinstance(v, str) and v.startswith("="):
                seeds[col_idx] = {"origin_row": r, "formula": v}
                break
    return seeds


def _used_output_columns(ws, wb, upb_header: str, header_row: int = 4, start_row: int = 5) -> Set[int]:
    hdr = header_tuples_from_ws(ws, header_row=header_row, wb=wb, upb_header=upb_header)
    cols = {c for c, _h in hdr}
    cols |= formula_col_indices(ws, start_row=start_row, header_row=header_row)
    return cols


def _clear_sheet_body(ws, used_cols: Set[int], start_row: int = 5):
    if not used_cols:
        return
    max_r = ws.max_row
    for r in range(start_row, max_r + 1):
        for c in used_cols:
            ws.cell(r, c).value = None


def _trim_sheet_body_rows(ws, row_count: int, start_row: int = 5):
    keep_last = max(start_row, start_row + row_count - 1)
    if ws.max_row > keep_last:
        ws.delete_rows(keep_last + 1, ws.max_row - keep_last)


def _excel_safe_value(val):
    if val is None or val is pd.NA:
        return None
    if isinstance(val, pd.Timestamp):
        return None if pd.isna(val) else val.to_pydatetime()
    if isinstance(val, np.generic):
        val = val.item()
    if isinstance(val, (list, dict, set, tuple)):
        return str(val)
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    return val


def _coerce_excel_date_value(val):
    if val is None:
        return None
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime().date()
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        parsed = pd.to_datetime(val, errors="coerce")
        if pd.isna(parsed):
            return val
        return parsed.to_pydatetime().date()
    except Exception:
        return val


def _money_format_for_header(sheet_name: str, header: str, upb_header: str) -> Optional[str]:
    if header == upb_header:
        return MONEY2_FORMAT if sheet_name in {"Bridge Asset", "Term Asset"} else MONEY0_FORMAT
    if header in SHEET_MONEY2_HEADERS.get(sheet_name, set()):
        return MONEY2_FORMAT
    if header in SHEET_MONEY0_HEADERS.get(sheet_name, set()):
        return MONEY0_FORMAT
    return None


def _is_date_header(sheet_name: str, header: str) -> bool:
    return header in SHEET_DATE_HEADERS.get(sheet_name, set())


def _copy_reference_row_style(ws_formula, col_idx: int, target_cell):
    ref_cell = ws_formula.cell(5, col_idx)
    if ref_cell.has_style:
        target_cell._style = copy(ref_cell._style)


def _apply_display_style(ws_formula, row_idx: int, col_idx: int, header: str, upb_header: str):
    cell = ws_formula.cell(row_idx, col_idx)
    _copy_reference_row_style(ws_formula, col_idx, cell)
    cell.font = copy(BASE_FONT)
    cell.alignment = copy(BASE_ALIGNMENT)

    if _is_date_header(ws_formula.title, header):
        cell.number_format = DATE_NUMBER_FORMAT
    else:
        money_fmt = _money_format_for_header(ws_formula.title, header, upb_header)
        if money_fmt:
            cell.number_format = money_fmt


def _copy_formula_columns_down(ws_formula, formula_seeds: dict, row_count: int, start_row: int = 5):
    if row_count <= 0:
        return

    for col_idx, seed in formula_seeds.items():
        origin_row = seed["origin_row"]
        origin_formula = seed["formula"]
        origin_ref = f"{get_column_letter(col_idx)}{origin_row}"

        for r in range(start_row, start_row + row_count):
            target = ws_formula.cell(r, col_idx)
            if r == origin_row:
                target.value = origin_formula
            else:
                target.value = Translator(origin_formula, origin=origin_ref).translate_formula(f"{get_column_letter(col_idx)}{r}")
            _copy_reference_row_style(ws_formula, col_idx, target)


def _refresh_subtotal_formula(ws_formula, row_count: int, subtotal_row: int = 3, start_row: int = 5):
    blueprint = SHEET_BLUEPRINTS.get(ws_formula.title, {})
    subtotal_col = blueprint.get("subtotal_col")
    if not subtotal_col:
        return
    col_letter = get_column_letter(subtotal_col)
    end_row = max(start_row, start_row + row_count - 1)
    ws_formula.cell(subtotal_row, subtotal_col).value = f"=SUBTOTAL(9,{col_letter}{start_row}:{col_letter}{end_row})"


def write_df_to_sheet_preserve_formulas(
    ws_formula,
    df: pd.DataFrame,
    header_tuples: List[Tuple[int, str]],
    formula_cols: Set[int],
    upb_header: str,
    start_row: int = 5,
):
    write_cols = [(c, h) for (c, h) in header_tuples if c not in formula_cols]
    headers = [h for _c, h in write_cols]

    missing = {h: pd.NA for h in headers if h not in df.columns}
    df_out = df.assign(**missing) if missing else df
    df_out = df_out[headers]

    for r_offset, row in enumerate(df_out.itertuples(index=False, name=None), start=0):
        r = start_row + r_offset
        for (c, h), val in zip(write_cols, row):
            safe_val = _excel_safe_value(val)
            if _is_date_header(ws_formula.title, h):
                safe_val = _coerce_excel_date_value(safe_val)
            ws_formula.cell(r, c).value = safe_val
            _apply_display_style(ws_formula, r, c, h, upb_header)


def write_output_sheet(wb, sheet_name: str, df: pd.DataFrame, upb_col: str):
    if sheet_name not in wb.sheetnames:
        return

    ws = wb[sheet_name]
    hdr = header_tuples_from_ws(ws, header_row=4, wb=wb, upb_header=upb_col)
    fcols = formula_col_indices(ws, start_row=5, header_row=4)
    formula_seeds = _capture_formula_seeds(ws, fcols, start_row=5)

    used_cols = _used_output_columns(ws, wb=wb, upb_header=upb_col, header_row=4, start_row=5)
    _clear_sheet_body(ws, used_cols, start_row=5)

    write_df_to_sheet_preserve_formulas(ws, df, hdr, fcols, upb_col, start_row=5)
    _copy_formula_columns_down(ws, formula_seeds, row_count=len(df), start_row=5)
    _refresh_subtotal_formula(ws, row_count=len(df), subtotal_row=3, start_row=5)
    _trim_sheet_body_rows(ws, row_count=max(len(df), 1), start_row=5)


def init_build_state():
    if "built_workbook_bytes" not in st.session_state:
        st.session_state.built_workbook_bytes = None
    if "built_workbook_name" not in st.session_state:
        st.session_state.built_workbook_name = None
    if "built_template_path" not in st.session_state:
        st.session_state.built_template_path = None
    if "show_download_prompt" not in st.session_state:
        st.session_state.show_download_prompt = False
    if "download_choice" not in st.session_state:
        st.session_state.download_choice = "Not yet"


def reset_build_state():
    st.session_state.built_workbook_bytes = None
    st.session_state.built_workbook_name = None
    st.session_state.built_template_path = None
    st.session_state.show_download_prompt = False
    st.session_state.download_choice = "Not yet"


st.set_page_config(page_title="Active Loans Builder", layout="wide")
st.title("Active Loans Report Builder")
st.subheader(hey())

run_dt = today_et()
upb_col = make_upb_header(run_dt)
init_build_state()

st.markdown(
    f"""
Welcome! This tool builds the **Active Loans** workbook using **Salesforce Bulk API 2.0** and optional **servicer uploads**.

### What you’ll do
1) Upload the **current servicer files** or skip them
2) (Optional) Upload **last week’s / completed Active Loans report** for carry-forward
3) (Optional) Upload **CV NPL / REO workbook**
4) Log in to **Salesforce**
5) Choose **which sheet to build** or **All**

### UPB header
Always uses today's date (ET): **{run_dt.isoformat()}** → **{upb_col}**
"""
)

_repo_template_available = False
try:
    _tmpl_bytes_preview, _tmpl_path_used = load_repo_template_bytes()
    _repo_template_available = True
    st.success(f"✅ Using repo template: {_tmpl_path_used}")
except Exception as e:
    st.warning(
        "Repo template not found right now. You can still build by uploading a completed Active Loans workbook "
        "to use as the template base, or by committing one of the expected template files to the repo."
    )
    st.caption(str(e))

st.caption(
    "This merged version uses your repo template by default, can use the uploaded completed report as the build base, "
    "adds NPL/REO flag integration, scans servicer files more flexibly, resolves formula-linked UPB headers, "
    "fills formulas down, trims extra blank rows, and keeps row-level Salesforce Servicer IDs intact."
)

col_a, col_b = st.columns([1.3, 1.0])
with col_a:
    prev_upload = st.file_uploader(
        "Upload LAST WEEK'S or COMPLETED Active Loans report (.xlsx) for carry-forward (optional)",
        type=["xlsx"],
    )
with col_b:
    servicer_uploads = st.file_uploader(
        "Upload current servicer files (csv/xlsx) (optional if skipped below)",
        type=["csv", "xlsx"],
        accept_multiple_files=True,
    )

npl_reo_upload = st.file_uploader(
    "Upload CV NPL / REO workbook (.xlsx) (optional)",
    type=["xlsx"],
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
sf_ready = False
if use_sf:
    show_salesforce_login_helper()
    sf_info = ensure_sf_session()
    sf_ready = bool(sf_info)

    c1, c2 = st.columns([3, 1])
    with c1:
        inst = (st.session_state.get("sf_token") or {}).get("instance_url", "")
        st.success("✅ Logged in to Salesforce API")
        if inst:
            st.caption(f"Connected to: {inst}")
            st.caption("Bulk API 2.0 is used with chunked result pages so the pull is not capped at report-api row counts.")
    with c2:
        if st.button("Log out"):
            st.session_state.sf_token = None
            st.rerun()

if st.button("Clear cached Salesforce metadata", type="secondary"):
    st.session_state.sobject_describe_cache = {}
    st.success("Cleared Salesforce metadata cache for this session.")

if st.button("Clear cached servicer parsing", type="secondary"):
    st.cache_data.clear()
    st.success("Cleared Streamlit data cache.")

build_btn = st.button("Build", type="primary")

if build_btn:
    reset_build_state()

    if not use_sf:
        st.error("This version requires Salesforce API to build the report.")
    elif not skip_servicer_files and not servicer_uploads:
        st.error("Upload the servicer files, or check 'Skip servicer files and build Salesforce-only version'.")
    elif not sf_ready:
        st.error("Salesforce login is required.")
    else:
        wb = None
        try:
            status = st.status("Preparing build...", expanded=True)
            diagnostics: List[str] = []
            prev_maps: dict = {}
            npl_maps = {"loan_flags": pd.DataFrame(), "asset_flags": pd.DataFrame()}

            if prev_upload:
                status.update(label="Reading uploaded completed report for carry-forward...")
                prev_maps = build_prev_maps(prev_upload.getvalue())

            if npl_reo_upload is not None:
                status.update(label="Reading CV NPL / REO workbook...")
                npl_maps = parse_npl_reo_bytes(npl_reo_upload.getvalue())

            if skip_servicer_files:
                serv_join = pd.DataFrame(columns=["source_file", "servicer", "servicer_family", "servicer_id", "upb", "suspense", "next_payment_date", "maturity_date", "status", "as_of", "_sid_key"])
                detected_run_date = run_dt
                serv_preview = serv_join.copy()

                st.markdown("### Servicer lookup preview")
                st.caption("Servicer files were skipped. Servicer-driven columns will use Salesforce fallback where available.")
                st.caption(f"UPB header (always today): **{upb_col}**")
            else:
                status.update(label="Parsing servicer files...")
                serv_join, detected_run_date, serv_preview = build_servicer_lookup(servicer_uploads)

                st.markdown("### Servicer lookup preview")
                st.caption(f"Detected latest servicer report date from file contents / report tabs: **{detected_run_date.isoformat()}**")
                st.caption(f"UPB header (always today): **{upb_col}**")
                st.dataframe(serv_preview.head(30), use_container_width=True)

            status.update(label="Loading Excel template...")
            if prev_upload is None and not _repo_template_available:
                raise FileNotFoundError(
                    "No repo template was found and no completed Active Loans workbook was uploaded. "
                    "Upload the prior completed workbook or add one of the expected template files to the repo."
                )
            tmpl_bytes, tmpl_path_used = resolve_template_bytes(prev_upload)
            template_maps = load_template_lookup_maps(tmpl_bytes)
            wb = load_workbook(BytesIO(tmpl_bytes), data_only=False, keep_links=True)
            restore_template_scaffold(wb, run_dt, upb_col)

            need_bridge = build_target in ("Bridge Asset", "Bridge Loan", "All")
            need_term = build_target in ("Term Loan", "Term Asset", "All")
            need_term_asset = build_target in ("Term Asset", "All")
            need_am = need_bridge or need_term

            sf_am = pd.DataFrame()
            if need_am:
                status.update(label="Pulling AM assignments from Salesforce...")
                sf_am = _build_am_assignments_like()

            status.update(label="Pulling Active RM deals from Salesforce...")
            sf_active_rm = _build_active_rm_like()

            if need_bridge:
                status.update(label="Pulling bridge/property data from Salesforce...")
                bridge_spine = _build_bridge_spine_like()
                bridge_asset_ids = _bridge_asset_ids_from_spine(bridge_spine)

                status.update(label="Pulling Do Not Lend deals from Salesforce...")
                bridge_dnl = _build_do_not_lend_like()

                status.update(label="Pulling valuation data from Salesforce...")
                bridge_val = _build_valuation_like(asset_ids=bridge_asset_ids)

                status.update(label="Building Bridge Asset...")
                bridge_asset_df = build_bridge_asset(
                    bridge_spine,
                    bridge_dnl,
                    bridge_val,
                    sf_am,
                    sf_active_rm,
                    serv_join,
                    upb_col,
                    prev_maps,
                    template_maps,
                    npl_maps=npl_maps,
                )

                diagnostics.append(f"Bridge Asset rows: {len(bridge_asset_df):,}")
                diagnostics.append(
                    f"Bridge Asset nonblank {upb_col}: {bridge_asset_df[upb_col].notna().mean():.1%}"
                    if upb_col in bridge_asset_df.columns
                    else f"Bridge Asset nonblank {upb_col}: n/a"
                )

                if build_target in ("Bridge Asset", "All"):
                    status.update(label="Writing Bridge Asset sheet...")
                    write_output_sheet(wb, "Bridge Asset", bridge_asset_df, upb_col)

                if build_target in ("Bridge Loan", "All"):
                    status.update(label="Building Bridge Loan...")
                    bridge_loan_df = build_bridge_loan(bridge_asset_df, upb_col, prev_maps, npl_maps=npl_maps)

                    status.update(label="Writing Bridge Loan sheet...")
                    write_output_sheet(wb, "Bridge Loan", bridge_loan_df, upb_col)
                    del bridge_loan_df

                del bridge_spine, bridge_dnl, bridge_asset_ids, bridge_val, bridge_asset_df
                gc.collect()

            if need_term:
                status.update(label="Pulling term data from Salesforce...")
                term_wide = _build_term_wide_like()

                status.update(label="Building Term Loan...")
                term_loan_df = build_term_loan(
                    term_wide,
                    sf_am,
                    sf_active_rm,
                    serv_join,
                    upb_col,
                    prev_maps,
                    template_maps,
                )

                diagnostics.append(f"Term Loan rows: {len(term_loan_df):,}")
                diagnostics.append(
                    f"Term Loan nonblank {upb_col}: {term_loan_df[upb_col].notna().mean():.1%}"
                    if upb_col in term_loan_df.columns
                    else f"Term Loan nonblank {upb_col}: n/a"
                )

                if build_target in ("Term Loan", "All"):
                    status.update(label="Writing Term Loan sheet...")
                    write_output_sheet(wb, "Term Loan", term_loan_df, upb_col)

                if need_term_asset:
                    term_deal_numbers = _term_deal_numbers_from_wide(term_wide)

                    status.update(label="Pulling term asset data from Salesforce...")
                    term_asset_source = _build_term_asset_like(deal_numbers=term_deal_numbers)

                    status.update(label="Building Term Asset...")
                    term_asset_df = build_term_asset(term_asset_source, term_loan_df, upb_col)

                    status.update(label="Writing Term Asset sheet...")
                    write_output_sheet(wb, "Term Asset", term_asset_df, upb_col)
                    del term_deal_numbers, term_asset_source, term_asset_df

                del term_wide, term_loan_df
                gc.collect()

            del sf_am, sf_active_rm, serv_join, serv_preview
            gc.collect()

            status.update(label="Saving workbook...")
            out_bytes = BytesIO()
            wb.save(out_bytes)
            out_bytes.seek(0)
            wb.close()

            st.session_state.built_workbook_bytes = out_bytes.getvalue()
            st.session_state.built_workbook_name = OUTPUT_TEST_FILENAME
            st.session_state.built_template_path = tmpl_path_used
            st.session_state.show_download_prompt = True
            st.session_state.download_choice = "Not yet"

            status.update(label="Build complete", state="complete")
            st.success("✅ Workbook built")
            st.caption(f"Built from template source: {tmpl_path_used}")

            if diagnostics:
                st.subheader("Diagnostics")
                for msg in diagnostics:
                    st.write(msg)

        except Exception as e:
            st.error("The report builder hit an error. The real traceback is below.")
            st.exception(e)
        finally:
            try:
                if wb is not None:
                    wb.close()
            except Exception:
                pass

if st.session_state.get("show_download_prompt") and st.session_state.get("built_workbook_bytes"):
    st.markdown("### Download")
    st.radio(
        "Your report is ready. Do you want to download the Excel file now?",
        options=["Not yet", "Yes"],
        horizontal=True,
        key="download_choice",
    )

    if st.session_state.get("download_choice") == "Yes":
        st.download_button(
            "Download Excel file",
            data=st.session_state["built_workbook_bytes"],
            file_name=st.session_state["built_workbook_name"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    else:
        st.caption("No problem — the file is ready whenever you want to download it during this session.")
