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

DNL_STAGES = ["Closed Won", "Purchased", "Brokered- Closed Won", "Expired", "Matured", "Sold", "Paid Off", "REO", "REO-Sold"]

TERM_ACTIVE_STAGES = ["Approved by Committee", "Closed Won", "Paid Off", "REO", "REO-Sold", "Sold"]
TERM_TYPES = ["DSCR", "Investor DSCR", "Single Rental Loan", "Term Loan"]
TERM_DSCR_TYPES = {"DSCR", "Investor DSCR"}

ACTIVE_RM_STAGES = ["Approved by Committee", "Closed Won", "Purchased", "Brokered- Closed Won", "Expired", "Matured", "Sold", "Paid Off", "REO", "REO-Sold"]

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
        "row2": {
            2: "Bridge Asset Data",
            104: "__QEND__",
        },
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
        "row3": {
            22: "=+'Bridge Asset'!$CK$3",
            26: "__SUBTOTAL__",
        },
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
        "row1": {
            30: "__QEND__",
        },
        "row2": {
            2: "Term Loan Data",
        },
        "row3": {
            16: "__SUBTOTAL__",
            21: "__RUN_DT__",
            30: "=+$AD$1-90",
        },
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
        "row3": {
            12: "__SUBTOTAL__",
        },
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
    re.compile(r"^\d{4}-\d{1,2}-\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?$"),
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
    out = df
    for c in out.columns:
        s = out[c]
        try:
            if pd.api.types.is_integer_dtype(s):
                out[c] = pd.to_numeric(s, errors="coerce", downcast="integer")
            elif pd.api.types.is_float_dtype(s):
                out[c] = pd.to_numeric(s, errors="coerce", downcast="float")
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

        if any(h in str(c).lower() for h in numeric_hints):
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
        "sa_strategy_set": set(),
    }

    xls = pd.ExcelFile(BytesIO(template_bytes))

    if "Strategy Groupings" in xls.sheet_names:
        sg = pd.read_excel(BytesIO(template_bytes), sheet_name="Strategy Groupings", header=3)
        sg = sg.dropna(how="all")
        sg.columns = [str(c).strip() for c in sg.columns]

        strategy_col = first_matching_col(sg, ["Strategy"])
        grouping_col = first_matching_col(sg, ["Grouping"])
        sa_col = first_matching_col(sg, ["Y"])

        if strategy_col and grouping_col:
            for _, row in sg.iterrows():
                strategy = clean_text(row.get(strategy_col))
                grouping = clean_text(row.get(grouping_col))
                if strategy and grouping:
                    maps["strategy_map"][strategy] = grouping

        if strategy_col and sa_col:
            for _, row in sg.iterrows():
                strategy = clean_text(row.get(strategy_col))
                flag = clean_text(row.get(sa_col)).upper()
                if strategy and flag == "Y":
                    maps["sa_strategy_set"].add(strategy)

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

    # IMPORTANT FIX:
    # only fill blank row-level Servicer Loan Number from Servicer Commitment Id.
    # do NOT overwrite the entire deal with a single servicer id.
    if {"Servicer Loan Number", "Servicer Commitment Id"}.issubset(df.columns):
        df["Servicer Loan Number"] = coalesce_keep_nonblank(df["Servicer Loan Number"], df["Servicer Commitment Id"])

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
    if n.endswith(".csv"):
        return "CHL"
    if "corevest_data_tape" in n:
        return "CoreVest_Data_Tape"
    if "corevestloandata" in n:
        return "CoreVestLoanData"
    if "midland" in n:
        return "Midland"
    if "fci" in n:
        return "FCI"
    raise ValueError(
        "Could not detect servicer file type from the filename. "
        "Use one of these naming patterns: CHL, CoreVest_Data_Tape, CoreVestLoanData, FCI, Midland."
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


def parse_servicer_bytes(filename: str, b: bytes) -> pd.DataFrame:
    name = filename
    servicer_type = detect_servicer_type(name)
    file_date = date_from_filename(name)

    if servicer_type == "CHL":
        usecols = lambda c: c in {
            "Servicer Loan ID", "UPB", "Next Due Date", "Current Maturity Date", "Performing Status", "Servicing Company"
        }
        df = pd.read_csv(BytesIO(b), usecols=usecols)
        as_of = file_date

        servicer = df.get("Servicing Company", pd.Series(["CHL Streamline"] * len(df))).astype("string")
        servicer = servicer.fillna("CHL Streamline")
        servicer = servicer.where(~servicer.astype("string").str.upper().eq("FCI"), "FCI CHL Streamline")

        out = pd.DataFrame(
            {
                "source_file": name,
                "servicer": servicer,
                "servicer_family": servicer.map(normalize_servicer_family),
                "servicer_id": norm_id_series(df["Servicer Loan ID"]),
                "upb": df["UPB"].apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("
