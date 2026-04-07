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
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import streamlit as st
from openpyxl import load_workbook
from openpyxl.formula.translate import Translator
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter


try:
    from active_loan_postbuild_audit import (
        audit_diagnostic_lines,
        audit_openpyxl_workbook,
        write_audit_sheets,
        workbook_needs_attention,
    )
    POSTBUILD_AUDIT_AVAILABLE = True
    POSTBUILD_AUDIT_IMPORT_ERROR = ""
except Exception as _audit_import_exc:
    audit_diagnostic_lines = None
    audit_openpyxl_workbook = None
    write_audit_sheets = None
    workbook_needs_attention = None
    POSTBUILD_AUDIT_AVAILABLE = False
    POSTBUILD_AUDIT_IMPORT_ERROR = str(_audit_import_exc)


PRIMARY_USER_NAME = "Hayden"
TEMPLATE_FILENAMES = (
    "20260330 Active Loans vDRAFT.xlsx",
    "Active Loan Template.xlsx",
    "Active Loan Report Template.xlsx",
)
API_VERSION = "v66.0"
BULK_PAGE_SIZE = 5000
BULK_WAIT_TIMEOUT_SECONDS = 300
OUTPUT_TEST_FILENAME = "active loan report test.xlsx"
SERVICER_PARSE_CACHE_ENABLED = False
SERVICER_HEADER_SCAN_ROWS = 60
SERVICER_PREVIEW_ROWS = 50
ENFORCE_ZERO_FILLABLE_BLANKS = True
ZERO_BLANK_MAX_ROUNDS = 4
FORCE_QUARTER_END = None
UPB_HEADER_RE = re.compile(r"\b\d{1,2}/\d{1,2}\s*UPB\b", re.I)

VALID_STAGES = ["Active", "Closed Won", "Expired", "Matured", "Sold", "REO"]
BRIDGE_ACTIVE_STAGES = VALID_STAGES.copy()
BRIDGE_LOAN_SOURCE_STAGES = VALID_STAGES.copy()
BRIDGE_ACTIVE_PROPERTY_STATUSES = ["Active", "REO"]
BRIDGE_TYPES = ["Bridge Loan", "SAB Loan", "Acquired Bridge Loan", "Single Asset Bridge Loan"]
BRIDGE_TYPE_CONTAINS = ["SAB", "Single Asset Bridge"]
TERM_TYPES = ["DSCR", "Investor DSCR", "Single Rental Loan", "Term Loan"]
LOAN_ACTIVE_STATUS = "Active"
BRIDGE_EXCLUDED_PRODUCT_TYPE = "Model Home Lease"

DNL_STAGES = [
    "Closed Won", "Purchased", "Brokered- Closed Won", "Expired", "Matured",
    "Sold", "Paid Off", "REO", "REO-Sold",
]

VALUATION_STAGES = VALID_STAGES.copy()
VALUATION_PROPERTY_STATUSES = ["Active", "REO"]

EXPIRED_OR_MATURED_STAGES = ["Expired", "Matured"]
REO_FAMILY_STAGES = ["REO"]
TERM_ACTIVE_STAGES = VALID_STAGES.copy()
TERM_ACTIVE_PROPERTY_STATUSES = ["Active", "REO"]
TERM_RECORDTYPE_NAMES = {"term loan", "dscr"}
BRIDGE_RT_EXACT = {"acquired bridge loan", "bridge loan", "sab loan", "single asset bridge loan"}
BRIDGE_RT_CONTAINS = {"sab", "single asset bridge"}
TERM_DSCR_TYPES = {"DSCR", "Investor DSCR"}
TERM_ALWAYS_INCLUDE_DEALS = {"43422", "43462"}
TERM_SPINE_SERVICER_FAMILIES = {"midland", "fci", "berkadia"}
TERM_SOLD_SERVICING_RETAINED_SEGMENT = "Sold Servicing Retained"
TERM_SOLD_RETAINED_SEGMENT_VALUES = {
    TERM_SOLD_SERVICING_RETAINED_SEGMENT,
    "Sold Servcing Retained",
}

ACTIVE_RM_STAGES = [
    "Closed Won", "Expired", "Matured", "Sold", "REO", "REO-Sold",
]

ACTIVE_RM_DIRECT_FIELD_CANDIDATES = [
    "Active_RM__c",
    "Is_Active_RM__c",
    "Active_Relationship_Manager__c",
    "Relationship_Manager_Active__c",
    "Relationship_Manager__c",
    "Relationship_Manager_1__c",
    "Relationship_Manager_2__c",
    "Primary_RM__c",
    "Secondary_RM__c",
    "RM__c",
    "RM_1__c",
    "RM_2__c",
    "Current_RM__c",
]

TERM_SERVICER_NAME_FIELD_CANDIDATES = [
    "Servicer_Name__c",
    "Master_Servicer_Name__c",
    "Primary_Servicer_Name__c",
]

TERM_SERVICER_PRIMARY_FIELD_CANDIDATES = [
    "Servicer_Loan_Number__c",
    "Servicer_Loan_Num__c",
    "Servicer_Loan_ID__c",
    "Servicer_Loan_Id__c",
    "Primary_Servicer_Loan_Number__c",
    "Primary_Servicer_Loan_ID__c",
    "Master_Servicer_Loan_Number__c",
    "Master_Servicer_Loan_ID__c",
]

TERM_SERVICER_FALLBACK_FIELD_CANDIDATES = [
    "Servicer_Commitment_Id__c",
    "Servicer_Commitment_ID__c",
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
    "Origination Value Dt": "Origination Valuation Date",
    "Origination As-Is Value": "Origination As-Is Value",
    "Origination ARV": "Origination After Repair Value",
    "Most Recent Appraisal Order Date": "Most Recent Appraisal Order Date",
    "Updated Valuation Date": "Current Appraisal Date",
    "Updated As-Is Value": "Current Appraised As-Is Value",
    "Updated ARV": "Current Appraised After Repair Value",
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
    "Originator": "CAF Originator: Full Name",
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
    "Most Recent Appraisal Order Date": "Most Recent Appraisal Order Date",
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


DRAFT_FORMULA_OVERRIDES = {
    "Bridge Asset": {
        "SF Funded Amount": "=+$BH5+$BJ5+$BM5",
        "CV Maturity Date": '=IF(OR($BS5="Credit Line",$BT5="Line of Credit"),$AG5,$AE5)',
        "Maturity Difference": '=IFERROR($CH5-$CG5,"N/A")',
        "Maturity Date": '=IF($CG5<>"N/A",$CG5,$CH5)',
        "Days to Maturity": '=+$CJ5-$CK$3',
        "Days Past Due": '=+$CL$3-$AC5',
        "DQ Status": '=IF($AZ5<>"N/A","REO",IF(AND($CL5>0,$CL5<30),"DQ 1-29",IF(AND($CL5>=30,$CL5<60),"DQ 30-59",IF(AND($CL5>=60,$CL5<90),"DQ 60-89",IF($CL5>=90,"DQ 90+","Current")))))',
        "Most Recent Valuation Date": '=IF($BE5<>"N/A",$BE5,$BA5)',
        "Most Recent As-Is Value": '=IF($BE5<>"N/A",$BF5,$BB5)',
        "Most Recent ARV": '=IF($BE5<>"N/A",$BG5,$BC5)',
        "Needs NPL Value": '=IF(AND($CZ5="Y",$CN5<$CQ$3),"Y","N")',
        "Securitized (Y/N)": '=IF($BR5="Securitized Bridge","Y","N")',
        "SSP JV (Y/N)": "=IF(COUNTIFS('SSP Loans'!$B:$B,'Bridge Asset'!$E5)>0,\"Y\",\"N\")",
        "CPP JV (Y/N)": '=IF($BR5="CPP JV","Y","N")',
        "Oaktree JV (Y/N)": '=IF($BR5="Oaktree JV","Y","N")',
        "Legacy (Y/N)": '=IF($BR5="Legacy","Y","N")',
        "Matured Loan (YN)": '=IF(_xlfn.MINIFS($CK:$CK,$E:$E,$E5)<0,"Y","N")',
        "DQ 45+ Loan (Y/N)": '=IF(_xlfn.MAXIFS($CL:$CL,$E:$E,$E5)>=45,"Y","N")',
        "SA Loan (Y/N)": "=IFERROR(VLOOKUP($AK5,'Strategy Groupings'!$F$4:$G$14,2,0),\"N\")",
        "3/31 NPL (Y/N)": '=IF(AND($D5<>"Sold",_xlfn.MINIFS($AC:$AC,$E:$E,$E5)<$CZ$3),"Y","N")',
        "Special Flag": '=IF(AND($D5<>"Sold",OR($CV5="Y",$CW5="Y",$CX5="Y",$CY5="Y")),"Y","N")',
    },
    "Bridge Loan": {
        "Days Past Due": '=+$V$3-$U5',
    },
    "Term Loan": {
        "Days Past Due": '=+$U$3-$S5',
        "DQ Status": '=IF($T5<>"N/A","REO",IF(AND($U5>0,$U5<30),"DQ 1-29",IF(AND($U5>=30,$U5<60),"DQ 30-59",IF(AND($U5>=60,$U5<90),"DQ 60-89",IF($U5>=90,"DQ 90+","Current")))))',
        "Special Loans List (Y/N)": '=IF(OR(AND(OR($J5="Active Term",$J5="DSCR"),$S5<$AD$3),$V5="REO",AND(OR($J5="Active Term",$J5="DSCR"),$U5>=45)),"Y","N")',
    },
    "Term Asset": {
        "__UPB__": "=($K5/SUMIFS($K:$K,$B:$B,$B5))*_xlfn.XLOOKUP($B5,'Term Loan'!$B:$B,'Term Loan'!$P:$P)",
        "Special (Y/N)": "=_xlfn.XLOOKUP($B5,'Term Loan'!$B:$B,'Term Loan'!$AD:$AD)",
    },
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

def strip_statebridge_display_id(servicer_id, servicer_name):
    sid = clean_text(servicer_id)
    if not sid:
        return pd.NA
    serv = clean_text(servicer_name).lower()
    if "statebridge" in serv and sid.startswith("0000"):
        sid = sid[4:]
    return sid or pd.NA

def normalize_servicer_id_for_report(servicer_ids: pd.Series, servicer_names: pd.Series) -> pd.Series:
    sid = pd.Series(servicer_ids, copy=False)
    serv = pd.Series(servicer_names, copy=False)
    out = []
    for i in sid.index:
        out.append(strip_statebridge_display_id(sid.loc[i], serv.loc[i] if i in serv.index else pd.NA))
    return pd.Series(out, index=sid.index, dtype="object")


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
        return "Id != NULL"
    return f"{field} IN ({', '.join(_soql_quote(v) for v in vals)})"


def _soql_not_equal_or_null(field: str, bad_value: str) -> str:
    q = _soql_quote(bad_value)
    return f"({field} = NULL OR {field} != {q})"


def _soql_false_or_null(field: str) -> str:
    return f"({field} = FALSE OR {field} = NULL)"


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
        "Step 3: Upload your files after login.\n\n"
        "Step 4: Click Build. This app uses Salesforce Bulk API 2.0 to pull the full datasets."
    )


def render_salesforce_login_gate() -> dict:
    st.markdown("### Step 1: Sign in to Salesforce")
    st.caption(
        "Please sign in before uploading files. The upload section stays hidden until login is complete so the Salesforce callback does not clear files you already selected."
    )
    show_salesforce_login_helper()
    sf_info = ensure_sf_session()

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
            st.query_params.clear()
            st.rerun()

    st.divider()
    return sf_info


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


def existing_field_names(sobject: str, candidates: Sequence[str]) -> List[str]:
    field_map = _field_map_by_name(sobject)
    return [name for name in candidates if name in field_map]


def picklist_values_for(sobject: str, field_api: str) -> List[str]:
    fld = _field_map_by_name(sobject).get(field_api) or {}
    out: List[str] = []
    seen = set()
    for item in fld.get("picklistValues", []) or []:
        val = clean_text(item.get("value") or item.get("label"))
        if val and val not in seen:
            seen.add(val)
            out.append(val)
    return out


def coalesce_columns(df: pd.DataFrame, columns: Sequence[str], index=None) -> pd.Series:
    if index is None:
        index = df.index
    out = pd.Series([pd.NA] * len(index), index=index, dtype="object")
    for col in reversed([c for c in columns if c in df.columns]):
        out = coalesce_keep_nonblank(pd.Series(df[col], index=index), out)
    return out


def _strict_active_rm_role_match(role) -> bool:
    s = re.sub(r"[^a-z0-9]+", " ", clean_text(role).lower()).strip()
    if not s:
        return False
    if s in {"active rm", "active relationship manager"}:
        return True
    if re.fullmatch(r"rm(?:\s*[12])?", s):
        return True
    return "relationship" in s and ("manager" in s or re.search(r"\brm\b", s) is not None)


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


def derive_term_portfolio_segment(loan_type, financing, loan_buyer, deal_number, template_maps: dict, sold_servicing_status=None):
    typ = clean_text(loan_type)
    fin = clean_text(financing)
    buyer = clean_text(loan_buyer)

    if typ in TERM_DSCR_TYPES:
        return "DSCR", "DSCR", "N"
    if fin.startswith("CPP JV"):
        return "Active Term", "CPP JV", "Y"
    if fin == "Sold" or buyer:
        retained = bool(_sold_servicing_retained_mask(pd.Series([sold_servicing_status])).iloc[0])
        seg = TERM_SOLD_SERVICING_RETAINED_SEGMENT if retained else "Sold Term"
        return "Sold Term", seg, "N"
    if deal_in_lookup(deal_number, template_maps.get("legacy_term_deals", set())):
        return "Active Term", "Legacy", "N"
    if re.match(r"^\d{4}[-A-Za-z0-9]+$", fin):
        return "Securitized Term", "Securitized Term", "N"
    return "Active Term", "Mortgage Banking", "N"


def _soql_text(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

def _recordtype_name_expr(alias_prefix: str = "") -> str:
    return f"{alias_prefix}RecordType.Name" if alias_prefix else "RecordType.Name"

def _lower_in_condition(field_expr: str, values: Sequence[str]) -> str:
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return "Id != NULL"
    return f"({field_expr} != NULL AND {field_expr} IN ({', '.join(_soql_text(str(x)) for x in vals)}))"

def _lower_contains_condition(field_expr: str, values: Sequence[str]) -> str:
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    contains_parts = [f"{field_expr} LIKE '%{str(x).replace(chr(39), chr(92)+chr(39))}%'" for x in vals]
    return "(" + " OR ".join(contains_parts) + ")" if contains_parts else "Id != NULL"

def _bridge_dealtype_condition(alias_prefix: str = "") -> str:
    expr = f"{alias_prefix}Type" if alias_prefix else "Type"
    exact_cond = _lower_in_condition(expr, BRIDGE_TYPES)
    contains_cond = _lower_contains_condition(expr, BRIDGE_TYPE_CONTAINS)
    return f"({exact_cond} OR {contains_cond})"

def _term_dealtype_condition(alias_prefix: str = "") -> str:
    expr = f"{alias_prefix}Type" if alias_prefix else "Type"
    return _lower_in_condition(expr, TERM_TYPES)

def _opportunity_status_active_condition(alias_prefix: str = "") -> Optional[str]:
    # Business logic now keys inclusion off StageName and property Status instead of the
    # custom Opportunity status fields. Leaving this filter enabled was dropping Sold /
    # REO rows upstream before the explicit retained-servicing and REO rules could run.
    return None

def _append_clause_if_present(parts: List[str], clause: Optional[str]) -> List[str]:
    if clause and str(clause).strip():
        parts.append(clause)
    return parts

def _bridge_recordtype_condition(alias_prefix: str = "") -> str:
    return _bridge_dealtype_condition(alias_prefix)

def _term_recordtype_condition(alias_prefix: str = "") -> str:
    return _term_dealtype_condition(alias_prefix)

def _coalesce_numeric_columns(df: pd.DataFrame, columns: Sequence[str], default=np.nan) -> pd.Series:
    out = pd.Series([default] * len(df), index=df.index, dtype="float64")
    for col in columns:
        if col in df.columns:
            cur = pd.to_numeric(df[col], errors="coerce")
            out = out.where(out.notna(), cur)
    return out

def _coalesce_datetime_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    out = pd.Series([pd.NaT] * len(df), index=df.index)
    for col in columns:
        if col in df.columns:
            cur = pd.to_datetime(df[col], errors="coerce")
            out = out.where(out.notna(), cur)
    return pd.to_datetime(out, errors="coerce")

def _coalesce_text_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    out = pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
    for col in columns:
        if col in df.columns:
            out = coalesce_keep_nonblank(out, pd.Series(df[col], index=df.index, dtype="object"))
    return out

def _build_bridge_spine_like() -> pd.DataFrame:
    opp_rel = property_opportunity_relationship_name()

    sold_pool_field = first_existing_field_name("Opportunity", ["Sold_Loan_Pool__c", "FK_Sold_Loan_Pool__c"])
    sold_pool_rel = relationship_name_for("Opportunity", sold_pool_field) if sold_pool_field else None

    contact_field = first_existing_field_name("Opportunity", ["Contact__c", "Primary_Contact__c"])
    contact_rel = relationship_name_for("Opportunity", contact_field) if contact_field else None

    special_asset_rel = relationship_name_for("Property__c", "Special_Asset__c")

    sold_to_expr = f"{opp_rel}.{sold_pool_rel}.Sold_To__r.Name" if sold_pool_rel else f"{opp_rel}.Account.Name"
    primary_contact_expr = f"{opp_rel}.{contact_rel}.Name" if contact_rel else f"{opp_rel}.Account.Name"

    updated_value_date_field = first_existing_field_name("Property__c", ["Updated_Valuation_Date__c", "Value_Date__c", "BPO_Appraisal_Date__c"])
    generic_value_date_field = first_existing_field_name("Property__c", ["Value_Date__c", "Updated_Valuation_Date__c", "BPO_Appraisal_Date__c"])
    generic_value_field = first_existing_field_name("Property__c", ["Value__c", "Appraised_Value_Amount__c"])

    select_pairs = [
        ("Sold To", sold_to_expr),
        ("Warehouse Line", f"{opp_rel}.Warehouse_Line__c"),
        ("Deal Loan Number", f"{opp_rel}.Deal_Loan_Number__c"),
        ("Servicer Loan Number", "Servicer_Loan_Number__c"),
        ("Servicer Commitment Id", f"{opp_rel}.Servicer_Commitment_Id__c"),
        ("Yardi ID", "Yardi_Id__c"),
        ("Asset ID", "Asset_ID__c"),
        ("Property ID", "Id"),
        ("Unique Active Id", "Unique_Active_Id__c"),
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
        ("Is Parent", "Is_Parent__c"),
        ("Is Sub Unit", "Is_Sub_Unit__c"),
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
        ("Most Recent Appraisal Order Date", "BPO_Appraisal_Order_Date__c"),
        ("BPO Appraisal Date", "BPO_Appraisal_Date__c"),
        ("Appraised Value Amount", "Appraised_Value_Amount__c"),
        ("After Repair Value", "After_Repair_Value__c"),
        ("Origination Valuation Date", "Origination_Date_Valuation_Date__c"),
        ("Origination As-Is Value", "Origination_Date_Value__c"),
        ("Origination After Repair Value", "Origination_After_Repair_Value__c"),
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
        ("CAF Originator: Full Name", f"{opp_rel}.Owner.Name"),
        ("CAF Originator: Active", f"{opp_rel}.Owner.IsActive"),
        ("Deal Intro Sub-Source", f"{opp_rel}.Deal_Intro_Sub_Source__c"),
        ("Referral Source Account: Account Name", f"{opp_rel}.Referral_Source__r.Name"),
        ("Referral Source Contact: Full Name", f"{opp_rel}.Referral_Source_Contact__r.Name"),
        ("Stage", f"{opp_rel}.StageName"),
        ("Status", "Status__c"),
        ("Current UPB", "Current_UPB__c"),
        ("Approved Advance Amount Funded", "Approved_Advance_Amount_Used__c"),
        ("Comments AM", f"{opp_rel}.Asset_Management_Comments__c"),
        ("Property Created Date", "CreatedDate"),
        ("Property Last Modified Date", "LastModifiedDate"),
    ]
    if updated_value_date_field and updated_value_date_field != "BPO_Appraisal_Date__c":
        select_pairs.append(("Updated Valuation Date Native", updated_value_date_field))
    if generic_value_date_field and generic_value_date_field not in {updated_value_date_field, "BPO_Appraisal_Date__c"}:
        select_pairs.append(("Generic Value Date", generic_value_date_field))
    if generic_value_field and generic_value_field != "Appraised_Value_Amount__c":
        select_pairs.append(("Generic Value", generic_value_field))

    rename_map = {expr: label for label, expr in select_pairs}

    where_parts = [
        f"{opp_rel}.Deal_Loan_Number__c != NULL",
        _soql_in(f"{opp_rel}.StageName", BRIDGE_ACTIVE_STAGES),
        _bridge_dealtype_condition(f"{opp_rel}."),
        _soql_in("Status__c", BRIDGE_ACTIVE_PROPERTY_STATUSES),
        _soql_not_equal_or_null(f"{opp_rel}.LOC_Loan_Type__c", BRIDGE_EXCLUDED_PRODUCT_TYPE),
        "Asset_ID__c != NULL",
    ]
    _append_clause_if_present(where_parts, _opportunity_status_active_condition(f"{opp_rel}."))

    soql = (
        "SELECT "
        + ", ".join(expr for _label, expr in select_pairs)
        + " FROM Property__c WHERE "
        + " AND ".join(where_parts)
    )

    df = run_bulk_query(soql, rename_map=rename_map)

    if df.empty:
        return df

    df["Current Appraisal Date"] = _coalesce_datetime_columns(df, ["Updated Valuation Date Native", "BPO Appraisal Date", "Generic Value Date"])
    df["Current Appraised As-Is Value"] = _coalesce_numeric_columns(df, ["Appraised Value Amount", "Generic Value"])
    if "After Repair Value" in df.columns:
        df["Current Appraised After Repair Value"] = pd.to_numeric(df["After Repair Value"], errors="coerce")

    for c in ["Servicer Loan Number", "Servicer Commitment Id", "Deal Loan Number", "Asset ID", "Property ID", "Unique Active Id"]:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip().replace({"": pd.NA})

    if {"Servicer Loan Number", "Servicer Commitment Id"}.issubset(df.columns):
        df["Servicer Loan Number"] = coalesce_keep_nonblank(df["Servicer Loan Number"], df["Servicer Commitment Id"])

    df["_asset_key"] = norm_id_series(df.get("Asset ID", pd.Series([None] * len(df), index=df.index)))
    df["_property_id_key"] = norm_id_series(df.get("Property ID", pd.Series([None] * len(df), index=df.index)))
    df["_is_sub_unit"] = _yn_from_bool_series(df.get("Is Sub Unit", pd.Series([pd.NA] * len(df), index=df.index))).eq("Y").astype("int8")
    df["_nonnull_score"] = 0
    for c in ["Address", "City", "State", "Zip", "CBSA", "Servicer Loan Number", "Origination Valuation Date", "Origination As-Is Value", "Current Appraisal Date", "Current Appraised As-Is Value"]:
        if c in df.columns:
            df["_nonnull_score"] = df["_nonnull_score"] + (~blankish_mask(df[c])).astype("int8")
    df["_mod_dt"] = pd.to_datetime(df.get("Property Last Modified Date"), errors="coerce")
    df["_created_dt"] = pd.to_datetime(df.get("Property Created Date"), errors="coerce")
    df = df[df["_asset_key"].notna()].copy()
    df = df.sort_values(["_asset_key", "_is_sub_unit", "_nonnull_score", "_mod_dt", "_created_dt", "_property_id_key"], ascending=[True, False, True, True, True, True])
    df = df.drop_duplicates(["_asset_key"], keep="last")
    df = df.drop(columns=["_asset_key", "_property_id_key", "_is_sub_unit", "_nonnull_score", "_mod_dt", "_created_dt"], errors="ignore")

    return downcast_numeric_frame(df)


def _build_bridge_loan_wide_like() -> pd.DataFrame:
    sold_pool_field = first_existing_field_name("Opportunity", ["Sold_Loan_Pool__c", "FK_Sold_Loan_Pool__c"])
    sold_pool_rel = relationship_name_for("Opportunity", sold_pool_field) if sold_pool_field else None

    contact_field = first_existing_field_name("Opportunity", ["Contact__c", "Primary_Contact__c"])
    contact_rel = relationship_name_for("Opportunity", contact_field) if contact_field else None

    servicer_primary_field = first_existing_field_name("Opportunity", ["Servicer_Loan_Number__c", "Servicer_Loan_Num__c", "Servicer_Loan_ID__c", "Servicer_Loan_Id__c"])
    servicer_commitment_field = first_existing_field_name("Opportunity", TERM_SERVICER_FALLBACK_FIELD_CANDIDATES)

    sold_to_expr = f"{sold_pool_rel}.Sold_To__r.Name" if sold_pool_rel else "Account.Name"
    primary_contact_expr = f"{contact_rel}.Name" if contact_rel else "Account.Name"

    funded_candidates = existing_field_names("Opportunity", [
        "Total_Funded_Active_Assets__c",
        "Aggregate_Funding__c",
        "Total_Amount_Advances__c",
        "Funded_Amount__c",
    ])
    asset_count_candidates = existing_field_names("Opportunity", ["Number_of_Properties__c", "Total_Properties__c"])
    unit_count_candidates = existing_field_names("Opportunity", ["Total_Units__c"])
    state_candidates = existing_field_names("Opportunity", ["Distinct_States__c", "Active_States__c"])
    last_funding_candidates = existing_field_names("Opportunity", ["Funds_Released_Date__c", "Last_Funding_Date__c"])
    valuation_date_candidates = existing_field_names("Opportunity", ["Updated_Value_Date__c", "Valuation_Date__c"])
    valuation_asis_candidates = existing_field_names("Opportunity", ["Updated_Value__c", "Total_Valuation_Amount__c"])

    select_pairs = [
        ("Loan Buyer", sold_to_expr),
        ("Financing", "Warehouse_Line__c"),
        ("Deal Number", "Deal_Loan_Number__c"),
        ("Deal Name", "Name"),
        ("Borrower Name", "Borrower_Entity__r.Name"),
        ("Account", "Account.Name"),
        ("Do Not Lend", "Account.Do_Not_Lend__c"),
        ("Primary Contact", primary_contact_expr),
        ("Origination Date", "CloseDate"),
        ("Next Payment Date", "Next_Payment_Date__c"),
        ("Original Maturity Date", "Stated_Maturity_Date__c"),
        ("Current Maturity Date", "Current_Line_Maturity_Date__c"),
        ("Loan Commitment", "LOC_Commitment__c"),
        ("Remaining Commitment", "Outstanding_Facility_Amount__c"),
        ("SF Suspense Balance", "Suspense_Balance__c"),
        ("SF Current UPB", "Current_UPB__c"),
        ("Loan Stage", "StageName"),
        ("Product Type", "LOC_Loan_Type__c"),
        ("Product Sub Type", "Product_Sub_Type__c"),
        ("Transaction Type", "Transaction_Type__c"),
        ("Project Strategy", "Project_Strategy__c"),
        ("CV Originator", "Owner.Name"),
        ("CV Originator: Active", "Owner.IsActive"),
        ("Deal Intro Sub-Source", "Deal_Intro_Sub_Source__c"),
        ("Referral Source Account", "Referral_Source__r.Name"),
        ("Referral Source Contact", "Referral_Source_Contact__r.Name"),
        ("AM Commentary", "Asset_Management_Comments__c"),
        ("Type", "Type"),
    ]
    if servicer_primary_field:
        select_pairs.insert(3, ("Opportunity Servicer Loan Number", servicer_primary_field))
    if servicer_commitment_field:
        select_pairs.insert(4 if servicer_primary_field else 3, ("Opportunity Servicer Commitment Id", servicer_commitment_field))
    for idx, field in enumerate(funded_candidates, start=1):
        select_pairs.append((f"Active Funded Amount Candidate {idx}", field))
    for idx, field in enumerate(asset_count_candidates, start=1):
        select_pairs.append((f"Number of Assets Candidate {idx}", field))
    for idx, field in enumerate(unit_count_candidates, start=1):
        select_pairs.append((f"Units Candidate {idx}", field))
    for idx, field in enumerate(state_candidates, start=1):
        select_pairs.append((f"States Candidate {idx}", field))
    for idx, field in enumerate(last_funding_candidates, start=1):
        select_pairs.append((f"Last Funding Date Candidate {idx}", field))
    for idx, field in enumerate(valuation_date_candidates, start=1):
        select_pairs.append((f"Most Recent Valuation Date Candidate {idx}", field))
    for idx, field in enumerate(valuation_asis_candidates, start=1):
        select_pairs.append((f"Most Recent As-Is Value Candidate {idx}", field))

    rename_map = {expr: label for label, expr in select_pairs}

    where_parts = [
        "Deal_Loan_Number__c != NULL",
        _soql_in("StageName", BRIDGE_LOAN_SOURCE_STAGES),
        _bridge_dealtype_condition(),
        _soql_not_equal_or_null("LOC_Loan_Type__c", BRIDGE_EXCLUDED_PRODUCT_TYPE),
    ]
    _append_clause_if_present(where_parts, _opportunity_status_active_condition())

    soql = "SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Opportunity WHERE " + " AND ".join(where_parts)
    df = run_bulk_query(soql, rename_map=rename_map)
    if df.empty:
        return df

    blank_obj = pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
    if "Opportunity Servicer Loan Number" in df.columns:
        df["Opportunity Servicer Loan Number"] = df["Opportunity Servicer Loan Number"].astype("string").str.strip().replace({"": pd.NA})
    if "Opportunity Servicer Commitment Id" in df.columns:
        df["Opportunity Servicer Commitment Id"] = df["Opportunity Servicer Commitment Id"].astype("string").str.strip().replace({"": pd.NA})

    df["Servicer ID"] = coalesce_keep_nonblank(df.get("Opportunity Servicer Loan Number", blank_obj), df.get("Opportunity Servicer Commitment Id", blank_obj))
    df["Active Funded Amount SF"] = _coalesce_numeric_columns(df, [c for c in df.columns if c.startswith("Active Funded Amount Candidate ")])
    df["Number of Assets SF"] = _coalesce_numeric_columns(df, [c for c in df.columns if c.startswith("Number of Assets Candidate ")])
    df["# of Units SF"] = _coalesce_numeric_columns(df, [c for c in df.columns if c.startswith("Units Candidate ")])
    df["State(s) SF"] = _coalesce_text_columns(df, [c for c in df.columns if c.startswith("States Candidate ")])
    df["Last Funding Date SF"] = _coalesce_datetime_columns(df, [c for c in df.columns if c.startswith("Last Funding Date Candidate ")])
    df["Most Recent Valuation Date SF"] = _coalesce_datetime_columns(df, [c for c in df.columns if c.startswith("Most Recent Valuation Date Candidate ")])
    df["Most Recent As-Is Value SF"] = _coalesce_numeric_columns(df, [c for c in df.columns if c.startswith("Most Recent As-Is Value Candidate ")])

    df["Deal Number"] = df.get("Deal Number", blank_obj).astype("string").str.strip().replace({"": pd.NA})
    return downcast_numeric_frame(df)


def _build_bridge_property_rollup_like() -> pd.DataFrame:
    opp_rel = property_opportunity_relationship_name()

    select_pairs = [
        ("Deal Number", f"{opp_rel}.Deal_Loan_Number__c"),
        ("Asset ID", "Asset_ID__c"),
        ("State", "State__c"),
        ("# of Units", "Number_of_Units__c"),
        ("Last Funding Date", "Funding_Date__c"),
        ("Property Status", "Status__c"),
        ("Property Current UPB", "Current_UPB__c"),
    ]
    rename_map = {expr: label for label, expr in select_pairs}

    where_parts = [
        f"{opp_rel}.Deal_Loan_Number__c != NULL",
        _soql_in(f"{opp_rel}.StageName", BRIDGE_LOAN_SOURCE_STAGES),
        _bridge_dealtype_condition(f"{opp_rel}."),
        _soql_not_equal_or_null(f"{opp_rel}.LOC_Loan_Type__c", BRIDGE_EXCLUDED_PRODUCT_TYPE),
    ]
    _append_clause_if_present(where_parts, _opportunity_status_active_condition(f"{opp_rel}."))

    soql = (
        "SELECT "
        + ", ".join(expr for _label, expr in select_pairs)
        + " FROM Property__c WHERE "
        + " AND ".join(where_parts)
    )

    df = run_bulk_query(soql, rename_map=rename_map)
    if df.empty:
        return pd.DataFrame(columns=[
            "_deal_key", "Number of Assets", "# of Units", "State(s)", "Last Funding Date",
            "Active Asset Count", "Active Asset UPB",
        ])

    df["_deal_key"] = norm_id_series(df.get("Deal Number", pd.Series([None] * len(df), index=df.index)))
    df["_asset_key"] = norm_id_series(df.get("Asset ID", pd.Series([None] * len(df), index=df.index)))
    status = df.get("Property Status", pd.Series([pd.NA] * len(df), index=df.index)).astype("string").str.strip()
    asset_upb = pd.to_numeric(df.get("Property Current UPB", pd.Series([np.nan] * len(df), index=df.index)), errors="coerce").fillna(0)
    is_active_asset = status.isin(BRIDGE_ACTIVE_PROPERTY_STATUSES)
    df["_active_asset_upb"] = asset_upb.where(is_active_asset, 0.0)
    df["_is_active_asset"] = is_active_asset.astype("int8")

    g = df.groupby("_deal_key", dropna=True)
    out = pd.DataFrame(
        {
            "Number of Assets": g["_asset_key"].nunique(),
            "# of Units": pd.to_numeric(g["# of Units"].sum(min_count=1), errors="coerce") if "# of Units" in df.columns else np.nan,
            "State(s)": g["State"].apply(lambda s: ", ".join(sorted({clean_text(x) for x in s if clean_text(x)}))) if "State" in df.columns else pd.Series(dtype="string"),
            "Last Funding Date": g["Last Funding Date"].apply(lambda s: pd.to_datetime(s, errors="coerce").dropna().max() if len(pd.to_datetime(s, errors="coerce").dropna()) else pd.NaT),
            "Active Asset Count": g["_is_active_asset"].sum(),
            "Active Asset UPB": pd.to_numeric(g["_active_asset_upb"].sum(min_count=1), errors="coerce"),
        }
    ).reset_index()
    return downcast_numeric_frame(out)


def _build_do_not_lend_like() -> pd.DataFrame:
    where_parts = [
        "Account.Do_Not_Lend__c = TRUE",
        _soql_in("StageName", DNL_STAGES),
        "Deal_Loan_Number__c != NULL",
    ]
    _append_clause_if_present(where_parts, _opportunity_status_active_condition())
    soql = (
        "SELECT Deal_Loan_Number__c, Account.Name, Account.Do_Not_Lend__c "
        "FROM Opportunity WHERE "
        + " AND ".join(where_parts)
    )
    df = run_bulk_query(soql)
    rename_map = {
        "Deal_Loan_Number__c": "Deal Loan Number",
        "Account.Name": "Account Name",
        "Account.Do_Not_Lend__c": "Do Not Lend",
    }
    return downcast_numeric_frame(_normalize_bulk_df(df.rename(columns=rename_map)))



def _build_appraisal_like(asset_ids=None) -> pd.DataFrame:
    asset_ids = _nonblank_unique(asset_ids or [])
    property_rel = appraisal_property_relationship_name()
    deal_rel = relationship_name_for("Appraisal__c", "Deal__c") if first_existing_field_name("Appraisal__c", ["Deal__c"]) else None

    effective_field = first_existing_field_name("Appraisal__c", ["Appraisal_Effective_Date__c", "Appraisal_Report_Date__c"])
    report_field = first_existing_field_name("Appraisal__c", ["Appraisal_Report_Date__c", "Appraisal_Effective_Date__c"])
    order_field = first_existing_field_name("Appraisal__c", ["Order_Received_Date__c"])
    as_is_field = first_existing_field_name("Appraisal__c", ["Reviewed_Appraisal_As_Is_Value__c", "Appraised_Value_Amount__c"])
    arv_field = first_existing_field_name("Appraisal__c", ["Reviewed_Appraisal_After_Repair_Value__c", "Appraised_After_Repair_Value__c", "Internal_as_Rehab_Value__c"])

    select_pairs = [
        ("Asset ID", f"{property_rel}.Asset_ID__c"),
        ("Property Asset Id", "Property_Asset_Id__c"),
        ("Property ID", f"{property_rel}.Id"),
        ("Appraisal Name", "Name"),
    ]
    if deal_rel:
        select_pairs.append(("Deal Loan Number", f"{deal_rel}.Deal_Loan_Number__c"))
    if order_field:
        select_pairs.append(("Most Recent Appraisal Order Date", order_field))
    if effective_field:
        select_pairs.append(("Appraisal Effective Date", effective_field))
    if report_field and report_field != effective_field:
        select_pairs.append(("Appraisal Report Date", report_field))
    if as_is_field:
        select_pairs.append(("Appraisal As-Is Value", as_is_field))
    if arv_field:
        select_pairs.append(("Appraisal ARV", arv_field))

    rename_map = {expr: label for label, expr in select_pairs}
    soqls = []
    if asset_ids:
        for chunk in _chunked(asset_ids, size=200):
            soqls.append("SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Appraisal__c WHERE " + _soql_in(f"{property_rel}.Asset_ID__c", chunk))
    else:
        soqls.append("SELECT " + ", ".join(expr for _label, expr in select_pairs) + f" FROM Appraisal__c WHERE {property_rel}.Asset_ID__c != NULL")

    df = _run_bulk_union(soqls, rename_map=rename_map)
    if df.empty:
        return df

    if "Asset ID" not in df.columns and "Property Asset Id" in df.columns:
        df["Asset ID"] = df["Property Asset Id"]
    df["_asset_key"] = norm_id_series(df.get("Asset ID", pd.Series([None] * len(df), index=df.index)))
    df["Current Appraisal Date"] = _coalesce_datetime_columns(df, ["Appraisal Effective Date", "Appraisal Report Date"])
    df["Current Appraised As-Is Value"] = _coalesce_numeric_columns(df, ["Appraisal As-Is Value"])
    df["Current Appraised After Repair Value"] = _coalesce_numeric_columns(df, ["Appraisal ARV"])
    df["_sort_dt"] = pd.to_datetime(df.get("Current Appraisal Date"), errors="coerce")
    df["_nonnull_score"] = 0
    for c in ["Current Appraisal Date", "Current Appraised As-Is Value", "Current Appraised After Repair Value", "Most Recent Appraisal Order Date"]:
        if c in df.columns:
            df["_nonnull_score"] = df["_nonnull_score"] + (~blankish_mask(df[c])).astype("int8")
    df = df[df["_asset_key"].notna()].copy()
    df = df.sort_values(["_asset_key", "_sort_dt", "_nonnull_score"], ascending=[True, True, True])
    df = df.drop_duplicates(["_asset_key"], keep="last")
    return downcast_numeric_frame(df.drop(columns=["_sort_dt", "_nonnull_score"], errors="ignore"))

def _build_valuation_like(asset_ids=None) -> pd.DataFrame:
    asset_ids = _nonblank_unique(asset_ids or [])
    soqls = []

    value_date_field = first_existing_field_name("Property__c", ["Updated_Valuation_Date__c", "Value_Date__c", "BPO_Appraisal_Date__c"])
    backup_value_date_field = first_existing_field_name("Property__c", ["Value_Date__c", "Updated_Valuation_Date__c", "BPO_Appraisal_Date__c"])
    generic_value_field = first_existing_field_name("Property__c", ["Value__c", "Appraised_Value_Amount__c"])

    select_pairs = [
        ("Asset ID", "Asset_ID__c"),
        ("Property ID", "Id"),
        ("Most Recent Appraisal Order Date", "BPO_Appraisal_Order_Date__c"),
        ("BPO Appraisal Date", "BPO_Appraisal_Date__c"),
        ("Appraised Value Amount", "Appraised_Value_Amount__c"),
        ("After Repair Value", "After_Repair_Value__c"),
        ("Origination Valuation Date", "Origination_Date_Valuation_Date__c"),
        ("Origination As-Is Value", "Origination_Date_Value__c"),
        ("Origination After Repair Value", "Origination_After_Repair_Value__c"),
        ("Is Sub Unit", "Is_Sub_Unit__c"),
        ("Property Created Date", "CreatedDate"),
        ("Property Last Modified Date", "LastModifiedDate"),
    ]
    if value_date_field and value_date_field != "BPO_Appraisal_Date__c":
        select_pairs.append(("Updated Value Date Native", value_date_field))
    if backup_value_date_field and backup_value_date_field not in {value_date_field, "BPO_Appraisal_Date__c"}:
        select_pairs.append(("Backup Value Date Native", backup_value_date_field))
    if generic_value_field and generic_value_field != "Appraised_Value_Amount__c":
        select_pairs.append(("Generic Value Native", generic_value_field))

    rename_map = {expr: label for label, expr in select_pairs}

    if asset_ids:
        for chunk in _chunked(asset_ids, size=200):
            soqls.append("SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Property__c WHERE " + _soql_in("Asset_ID__c", chunk))
    else:
        soqls.append("SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Property__c WHERE Asset_ID__c != NULL")

    df = _run_bulk_union(soqls, rename_map=rename_map)
    appraisal_df = _build_appraisal_like(asset_ids=asset_ids)
    if df.empty and appraisal_df.empty:
        return df

    if df.empty:
        df = appraisal_df.copy()
    else:
        df["Current Appraisal Date"] = _coalesce_datetime_columns(df, ["Updated Value Date Native", "BPO Appraisal Date", "Backup Value Date Native"])
        df["Current Appraised As-Is Value"] = _coalesce_numeric_columns(df, ["Appraised Value Amount", "Generic Value Native"])
        df["Current Appraised After Repair Value"] = pd.to_numeric(df.get("After Repair Value", pd.Series([np.nan] * len(df), index=df.index)), errors="coerce")
        if not appraisal_df.empty and "Asset ID" in appraisal_df.columns:
            app = appraisal_df.copy()
            app["_asset_key"] = norm_id_series(app["Asset ID"])
            keep = ["_asset_key"] + [c for c in ["Most Recent Appraisal Order Date", "Current Appraisal Date", "Current Appraised As-Is Value", "Current Appraised After Repair Value"] if c in app.columns]
            app = app[keep].drop_duplicates("_asset_key")
            df["_asset_key"] = norm_id_series(df.get("Asset ID", pd.Series([None] * len(df), index=df.index)))
            df = df.merge(app, on="_asset_key", how="left", suffixes=("", "_app"))
            for c in ["Most Recent Appraisal Order Date", "Current Appraisal Date", "Current Appraised As-Is Value", "Current Appraised After Repair Value"]:
                app_col = f"{c}_app"
                if app_col in df.columns:
                    df[c] = coalesce_keep_nonblank(df.get(c, pd.Series([pd.NA] * len(df), index=df.index)), df[app_col])
                    df = df.drop(columns=[app_col], errors="ignore")

    df["_asset_key"] = norm_id_series(df.get("Asset ID", pd.Series([None] * len(df), index=df.index)))
    df["_property_id_key"] = norm_id_series(df.get("Property ID", pd.Series([None] * len(df), index=df.index)))
    df["_is_sub_unit"] = _yn_from_bool_series(df.get("Is Sub Unit", pd.Series([pd.NA] * len(df), index=df.index))).eq("Y").astype("int8")
    df["_nonnull_score"] = 0
    for c in ["Most Recent Appraisal Order Date", "Current Appraisal Date", "Current Appraised As-Is Value", "Current Appraised After Repair Value", "Origination Valuation Date", "Origination As-Is Value", "Origination After Repair Value"]:
        if c in df.columns:
            df["_nonnull_score"] = df["_nonnull_score"] + (~blankish_mask(df[c])).astype("int8")
    df["_mod_dt"] = pd.to_datetime(df.get("Property Last Modified Date"), errors="coerce")
    df["_created_dt"] = pd.to_datetime(df.get("Property Created Date"), errors="coerce")
    df = df[df["_asset_key"].notna()].copy()
    df = df.sort_values(["_asset_key", "_is_sub_unit", "_nonnull_score", "_mod_dt", "_created_dt", "_property_id_key"], ascending=[True, False, True, True, True, True])
    df = df.drop_duplicates(["_asset_key"], keep="last")
    df = df.drop(columns=["_asset_key", "_property_id_key", "_is_sub_unit", "_nonnull_score", "_mod_dt", "_created_dt"], errors="ignore")
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
    frames: List[pd.DataFrame] = []

    direct_fields = existing_field_names("Opportunity", ACTIVE_RM_DIRECT_FIELD_CANDIDATES)
    if direct_fields:
        select_pairs = [("Deal Loan Number", "Deal_Loan_Number__c")]
        for idx, field_api in enumerate(direct_fields, start=1):
            select_pairs.append((f"Active RM Candidate {idx}", field_api))
        rename_map = {expr: label for label, expr in select_pairs}
        soql = (
            "SELECT "
            + ", ".join(expr for _label, expr in select_pairs)
            + " FROM Opportunity WHERE "
            + "Deal_Loan_Number__c != NULL AND "
            + _soql_in("StageName", ACTIVE_RM_STAGES)
        )
        direct_df = run_bulk_query(soql, rename_map=rename_map)
        if not direct_df.empty:
            candidate_cols = [c for c in direct_df.columns if c.startswith("Active RM Candidate ")]
            if candidate_cols:
                cand = pd.DataFrame(index=direct_df.index)
                for c in candidate_cols:
                    raw = pd.Series(direct_df[c], index=direct_df.index, dtype="object")
                    txt = raw.astype("string").str.strip().str.lower()
                    out_col = pd.Series([pd.NA] * len(raw), index=raw.index, dtype="object")
                    out_col = out_col.mask(~blankish_mask(raw), "Y")
                    out_col = out_col.mask(txt.isin(["false", "f", "n", "no", "0"]), "N")
                    out_col = out_col.mask(txt.isin(["true", "t", "y", "yes", "1"]), "Y")
                    cand[c] = out_col

                any_y = cand.eq("Y").any(axis=1)
                any_n = cand.eq("N").any(axis=1)
                active_rm = pd.Series([pd.NA] * len(cand), index=cand.index, dtype="object")
                active_rm = active_rm.mask(any_y, "Y")
                active_rm = active_rm.mask((~any_y) & any_n, "N")
                direct_df["Active RM"] = active_rm
                frames.append(direct_df[["Deal Loan Number", "Active RM"]])

    role_values = picklist_values_for("OpportunityTeamMember", "TeamMemberRole")
    rm_roles = [role for role in role_values if _strict_active_rm_role_match(role)]
    if rm_roles:
        soql = (
            "SELECT Opportunity.Deal_Loan_Number__c, TeamMemberRole "
            "FROM OpportunityTeamMember WHERE "
            "Opportunity.Deal_Loan_Number__c != NULL AND "
            + _soql_parent_name_not_equal_or_no_parent("Opportunity.AccountId", "Opportunity.Account.Name", EXCLUDED_TEST_ACCOUNT_NAME)
            + " AND Opportunity.StageName != NULL AND "
            + _soql_in("Opportunity.StageName", ACTIVE_RM_STAGES)
            + " AND "
            + _soql_in("TeamMemberRole", rm_roles)
        )
        role_df = run_bulk_query(soql)
        if not role_df.empty:
            role_df = role_df.rename(columns={"Opportunity.Deal_Loan_Number__c": "Deal Loan Number", "TeamMemberRole": "Team Role"})
            role_df["Active RM"] = "Y"
            frames.append(role_df[["Deal Loan Number", "Active RM"]])

    if not frames:
        return pd.DataFrame(columns=["Deal Loan Number", "Active RM"])

    out = pd.concat(frames, ignore_index=True, copy=False)
    out = out.dropna(subset=["Deal Loan Number"]).copy()
    out["_deal_key"] = norm_id_series(out["Deal Loan Number"])
    out["_rank"] = out["Active RM"].map({"Y": 2, "N": 1}).fillna(0)
    out = out.sort_values(["_deal_key", "_rank"]).drop_duplicates("_deal_key", keep="last")
    return out[["Deal Loan Number", "Active RM"]]



def _build_sold_term_like() -> pd.DataFrame:
    sold_pool_field = first_existing_field_name("Opportunity", ["FK_Sold_Loan_Pool__c", "Sold_Loan_Pool__c"])
    sold_pool_rel = relationship_name_for("Opportunity", sold_pool_field) if sold_pool_field else None
    servicer_commitment_field = first_existing_field_name("Opportunity", TERM_SERVICER_FALLBACK_FIELD_CANDIDATES)

    select_pairs = [
        ("Deal Loan Number", "Deal_Loan_Number__c"),
        ("Yardi ID", "Yardi_ID__c"),
        ("Deal Name", "Name"),
        ("Type", "Type"),
    ]
    if servicer_commitment_field:
        select_pairs.append(("Servicer Commitment Id", servicer_commitment_field))
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
        + _soql_in("Type", TERM_TYPES)
        + " AND Deal_Loan_Number__c != NULL AND Probability > 0"
    )
    return run_bulk_query(soql, rename_map=rename_map)




def _build_term_asset_deal_universe(deal_numbers: Optional[Sequence[str]] = None) -> List[str]:
    opp_rel = property_opportunity_relationship_name()
    deal_numbers = _nonblank_unique(deal_numbers or [])

    select_pairs = [("Deal Loan Number", f"{opp_rel}.Deal_Loan_Number__c")]
    rename_map = {expr: label for label, expr in select_pairs}

    base_where = [
        f"{opp_rel}.Deal_Loan_Number__c != NULL",
        f"{opp_rel}.Probability > 0",
        _term_dealtype_condition(f"{opp_rel}."),
        _soql_in(f"{opp_rel}.StageName", TERM_ACTIVE_STAGES),
        _soql_in("Status__c", TERM_ACTIVE_PROPERTY_STATUSES),
        "ALA__c > 0",
        "Asset_ID__c != NULL",
        _soql_false_or_null("Is_Sub_Unit__c"),
    ]

    soqls = []
    if deal_numbers:
        for chunk in _chunked(deal_numbers, size=200):
            where_parts = base_where + [_soql_in(f"{opp_rel}.Deal_Loan_Number__c", chunk)]
            soqls.append(
                "SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Property__c WHERE " + " AND ".join(where_parts)
            )
    else:
        soqls.append(
            "SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Property__c WHERE " + " AND ".join(base_where)
        )

    df = _run_bulk_union(soqls, rename_map=rename_map)
    if df.empty or "Deal Loan Number" not in df.columns:
        return []
    return _nonblank_unique(df["Deal Loan Number"].tolist())


def _build_term_wide_like() -> pd.DataFrame:
    contact_field = first_existing_field_name("Opportunity", ["Contact__c", "Primary_Contact__c"])
    contact_rel = relationship_name_for("Opportunity", contact_field) if contact_field else None
    primary_contact_expr = f"{contact_rel}.Name" if contact_rel else "Account.Name"
    funding_expr = opportunity_name_expr(["Current_Funding_Vehicle__c", "FK_Current_Funding_Vehicle__c"])

    servicer_name_field = first_existing_field_name("Opportunity", TERM_SERVICER_NAME_FIELD_CANDIDATES)
    servicer_commitment_field = first_existing_field_name("Opportunity", TERM_SERVICER_FALLBACK_FIELD_CANDIDATES)
    term_servicer_fields = existing_field_names("Opportunity", TERM_SERVICER_PRIMARY_FIELD_CANDIDATES)

    select_pairs = [
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
        ("Warehouse Line", "Warehouse_Line__c"),
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
    if servicer_name_field:
        select_pairs.insert(0, ("Servicer Name", servicer_name_field))
    if servicer_commitment_field:
        select_pairs.insert(1, ("Servicer Commitment Id", servicer_commitment_field))
    for idx, field_api in enumerate(term_servicer_fields, start=1):
        select_pairs.insert(2 + idx, (f"Term Servicer Key {idx}", field_api))

    rename_map = {expr: label for label, expr in select_pairs}

    where_parts = [
        "Deal_Loan_Number__c != NULL",
        "Probability > 0",
        _term_dealtype_condition(),
        _soql_in("StageName", TERM_ACTIVE_STAGES),
    ]
    _append_clause_if_present(where_parts, _opportunity_status_active_condition())

    soql = (
        "SELECT "
        + ", ".join(expr for _label, expr in select_pairs)
        + " FROM Opportunity WHERE "
        + " AND ".join(where_parts)
    )

    term_df = run_bulk_query(soql, rename_map=rename_map)
    sold_df = _build_sold_term_like()

    if term_df.empty:
        return term_df

    term_df["_deal_key"] = norm_id_series(term_df["Deal Loan Number"])

    if not sold_df.empty and "Deal Loan Number" in sold_df.columns:
        sold_keep = [
            c
            for c in [
                "Deal Loan Number",
                "Sold Loan: Sold To",
                "Sold Loan: Sold Date",
                "Sold Loan: Servicing Status",
            ]
            if c in sold_df.columns
        ]
        sold_df["_deal_key"] = norm_id_series(sold_df["Deal Loan Number"])
        sold_df = sold_df[["_deal_key"] + [c for c in sold_keep if c != "Deal Loan Number"]].drop_duplicates("_deal_key")
        term_df = term_df.merge(sold_df, on="_deal_key", how="left")

    term_df = term_df.drop_duplicates().copy()
    return downcast_numeric_frame(term_df)

def _build_term_asset_like(deal_numbers=None) -> pd.DataFrame:
    opp_rel = property_opportunity_relationship_name()
    deal_numbers = _nonblank_unique(deal_numbers or [])
    soqls = []

    generic_value_date_field = first_existing_field_name("Property__c", ["Value_Date__c", "Updated_Valuation_Date__c", "BPO_Appraisal_Date__c"])
    generic_value_field = first_existing_field_name("Property__c", ["Value__c", "Appraised_Value_Amount__c"])

    select_pairs = [
        ("Deal Loan Number", f"{opp_rel}.Deal_Loan_Number__c"),
        ("Property ID", "Id"),
        ("Asset ID", "Asset_ID__c"),
        ("Address", "Name"),
        ("City", "City__c"),
        ("State", "State__c"),
        ("Zip", "ZipCode__c"),
        ("CBSA", "MSA__c"),
        ("# of Units", "Number_of_Units__c"),
        ("Property Type", "Property_Type__c"),
        ("ALA", "ALA__c"),
        ("Updated Valuation Date", "Updated_Valuation_Date__c"),
        ("BPO Appraisal Date", "BPO_Appraisal_Date__c"),
        ("Appraised Value Amount", "Appraised_Value_Amount__c"),
        ("Property Special Asset", "Is_Special_Asset__c"),
        ("Is Parent", "Is_Parent__c"),
        ("Is Sub Unit", "Is_Sub_Unit__c"),
        ("Property Status", "Status__c"),
        ("Property Created Date", "CreatedDate"),
        ("Property Last Modified Date", "LastModifiedDate"),
    ]
    if generic_value_date_field and generic_value_date_field not in {"Updated_Valuation_Date__c", "BPO_Appraisal_Date__c"}:
        select_pairs.append(("Generic Value Date", generic_value_date_field))
    if generic_value_field and generic_value_field != "Appraised_Value_Amount__c":
        select_pairs.append(("Generic Value", generic_value_field))
    rename_map = {expr: label for label, expr in select_pairs}

    base_where = [
        f"{opp_rel}.Deal_Loan_Number__c != NULL",
        f"{opp_rel}.Probability > 0",
        _term_dealtype_condition(f"{opp_rel}."),
        _soql_in(f"{opp_rel}.StageName", TERM_ACTIVE_STAGES),
        _soql_in("Status__c", TERM_ACTIVE_PROPERTY_STATUSES),
        "ALA__c > 0",
        "Asset_ID__c != NULL",
        _soql_false_or_null("Is_Sub_Unit__c"),
    ]

    if deal_numbers:
        for chunk in _chunked(deal_numbers, size=200):
            where_parts = base_where + [_soql_in(f"{opp_rel}.Deal_Loan_Number__c", chunk)]
            soqls.append("SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Property__c WHERE " + " AND ".join(where_parts))
    else:
        soqls.append("SELECT " + ", ".join(expr for _label, expr in select_pairs) + " FROM Property__c WHERE " + " AND ".join(base_where))

    df = _run_bulk_union(soqls, rename_map=rename_map)
    if df.empty:
        return df

    df["Value Date"] = _coalesce_datetime_columns(df, ["Updated Valuation Date", "Generic Value Date", "BPO Appraisal Date"])
    df["As-Is Value"] = _coalesce_numeric_columns(df, ["Appraised Value Amount", "Generic Value"])

    df["_deal_key"] = norm_id_series(df.get("Deal Loan Number", pd.Series([None] * len(df), index=df.index)))
    df["_asset_key"] = norm_id_series(df.get("Asset ID", pd.Series([None] * len(df), index=df.index)))
    df["_property_id_key"] = norm_id_series(df.get("Property ID", pd.Series([None] * len(df), index=df.index)))
    df = df[df["_deal_key"].notna() & df["_asset_key"].notna()].copy()

    df["_is_sub_unit"] = _yn_from_bool_series(df.get("Is Sub Unit", pd.Series([pd.NA] * len(df), index=df.index))).eq("Y").astype("int8")
    df["_nonnull_score"] = 0
    for c in ["Address", "City", "State", "Zip", "CBSA", "ALA", "Value Date", "As-Is Value"]:
        if c in df.columns:
            df["_nonnull_score"] = df["_nonnull_score"] + (~blankish_mask(df[c])).astype("int8")
    df["_ala_sort"] = pd.to_numeric(df.get("ALA", np.nan), errors="coerce").fillna(0)
    df["_value_dt"] = pd.to_datetime(df.get("Value Date"), errors="coerce")
    df["_mod_dt"] = pd.to_datetime(df.get("Property Last Modified Date"), errors="coerce")
    df["_created_dt"] = pd.to_datetime(df.get("Property Created Date"), errors="coerce")
    df = df.sort_values(["_deal_key", "_asset_key", "_is_sub_unit", "_nonnull_score", "_ala_sort", "_value_dt", "_mod_dt", "_created_dt", "_property_id_key"], ascending=[True, True, False, True, True, True, True, True, True])
    df = df.drop_duplicates(["_deal_key", "_asset_key"], keep="last")
    return downcast_numeric_frame(df)


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


def _bridge_bucket_from_days(days_past_due) -> Optional[str]:
    try:
        if pd.isna(days_past_due):
            return None
    except Exception:
        pass
    try:
        days = int(float(days_past_due))
    except Exception:
        return None
    if days >= 90:
        return "90 + DAYS"
    if days >= 60:
        return "60 - 89 DAYS"
    if days >= 30:
        return "30 - 59 DAYS"
    return "CURRENT"


def _bridge_status_severity(status) -> int:
    s = clean_text(status).upper()
    order = {
        "CURRENT": 0,
        "30 - 59 DAYS": 1,
        "60 - 89 DAYS": 2,
        "90 + DAYS": 3,
        "BK": 4,
        "REO": 5,
    }
    return order.get(s, -1)


def _guess_days_past_due(next_payment_date, run_date: date) -> float:
    dt = pd.to_datetime(next_payment_date, errors="coerce")
    if pd.isna(dt):
        return np.nan
    delta = (pd.Timestamp(run_date) - dt.normalize()).days
    return float(max(delta, 0))


def _guess_days_from_bridge_bucket(status) -> float:
    s = clean_text(status).upper()
    mapping = {
        "CURRENT": 0.0,
        "30 - 59 DAYS": 45.0,
        "60 - 89 DAYS": 75.0,
        "90 + DAYS": 90.0,
        "BK": 90.0,
        "REO": 90.0,
    }
    return mapping.get(s, np.nan)


def normalize_bridge_servicer_status(raw_status, next_payment_date, run_date: date, loan_stage=None, property_status=None, reo_date=None) -> Optional[str]:
    if is_reo_stage(loan_stage) or is_reo_stage(property_status) or pd.notna(pd.to_datetime(reo_date, errors="coerce")):
        return "REO"

    txt = clean_text(raw_status).lower()
    txt_compact = re.sub(r"[^a-z0-9]+", " ", txt).strip()
    if txt_compact:
        if any(tok in txt_compact for tok in ["bankrupt", "bankruptcy", " bk "]) or txt_compact.startswith("bk") or txt_compact.endswith(" bk"):
            return "BK"
        if "reo" in txt_compact:
            return "REO"
        if any(tok in txt_compact for tok in ["90", "120", "180", "nonperform", "default", "foreclos", "serious delin"]):
            return "90 + DAYS"
        if any(tok in txt_compact for tok in ["60", "61", "89", "2 month"]):
            return "60 - 89 DAYS"
        if any(tok in txt_compact for tok in ["30", "31", "59", "1 month"]):
            return "30 - 59 DAYS"
        if any(tok in txt_compact for tok in ["current", "active", "perform", "paid to date", "p2d"]):
            return "CURRENT"

    return _bridge_bucket_from_days(_guess_days_past_due(next_payment_date, run_date))


def _worst_bridge_bucket(series: pd.Series):
    best = None
    best_rank = -1
    for val in series:
        rank = _bridge_status_severity(val)
        if rank > best_rank:
            best_rank = rank
            best = val
    return best if has_any_value(best) else pd.NA


def _fill_text_defaults(df: pd.DataFrame, columns: Sequence[str], default: str = "N/A") -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            continue
        s_raw = out[c]
        if pd.api.types.is_numeric_dtype(s_raw) or pd.api.types.is_datetime64_any_dtype(s_raw):
            continue
        s = pd.Series(s_raw, index=out.index, dtype="object")
        sample = s[~blankish_mask(s)].head(20)
        if len(sample) > 0 and sample.map(_looks_like_date_string).mean() >= 0.70:
            continue
        out[c] = s.where(~blankish_mask(s), default)
    return out



def _prev_term_keys(prev_maps: Optional[dict]) -> Set[str]:
    keys: Set[str] = set()
    if not prev_maps:
        return keys
    for bucket in ["term_loan_manual", "term_loan_reo", "term_loan_upb"]:
        df = prev_maps.get(bucket)
        if isinstance(df, pd.DataFrame) and not df.empty and "_deal_key" in df.columns:
            vals = df["_deal_key"].dropna().astype("string").tolist()
            keys.update([clean_text(v) for v in vals if clean_text(v)])
    return keys


def _prev_term_positive_upb_keys(prev_maps: Optional[dict]) -> Set[str]:
    keys: Set[str] = set()
    if not prev_maps:
        return keys
    df = prev_maps.get("term_loan_upb")
    if isinstance(df, pd.DataFrame) and not df.empty and {"_deal_key", "_prev_upb"}.issubset(df.columns):
        tmp = df.copy()
        tmp["_prev_upb"] = pd.to_numeric(tmp["_prev_upb"], errors="coerce").fillna(0)
        vals = tmp.loc[tmp["_prev_upb"] > 0, "_deal_key"].dropna().astype("string").tolist()
        keys.update([clean_text(v) for v in vals if clean_text(v)])
    return keys


def _prev_term_sold_retained_keys(prev_maps: Optional[dict]) -> Set[str]:
    keys: Set[str] = set()
    if not prev_maps:
        return keys
    df = prev_maps.get("term_loan_manual")
    if isinstance(df, pd.DataFrame) and not df.empty and {"_deal_key", "Segment"}.issubset(df.columns):
        seg = df["Segment"].astype("string").str.strip()
        vals = df.loc[seg.isin(list(TERM_SOLD_RETAINED_SEGMENT_VALUES)), "_deal_key"].dropna().astype("string").tolist()
        keys.update([clean_text(v) for v in vals if clean_text(v)])
    return keys


def _recent_close_mask(close_series: pd.Series, run_date: date, days: int = 45) -> pd.Series:
    close_dt = pd.to_datetime(close_series, errors="coerce")
    lower = pd.Timestamp(run_date) - pd.Timedelta(days=days)
    upper = pd.Timestamp(run_date) + pd.Timedelta(days=7)
    return close_dt.notna() & close_dt.ge(lower) & close_dt.le(upper)


def _sold_servicing_retained_mask(servicing_status: pd.Series) -> pd.Series:
    txt = pd.Series(servicing_status, copy=False).astype("string").str.lower().str.strip().fillna("")
    exact = txt.isin({
        "servicing retained",
        "sold servicing retained",
        "retain servicing",
        "retained servicing",
        "serv retained",
        "retained",
    })
    retained = txt.str.contains(r"servicing retained|retain servicing|retained servicing|serv retained|\bretained\b|\bretain\w*\b", regex=True, na=False)
    bad = txt.str.contains(r"release|released|transfer|transferred|xfer|sold away|none|no servicing|unserviced|servicing released|not retained", regex=True, na=False)
    return (exact | retained) & (~bad)


def _term_segment_is_sold_servicing_retained(segment_series: pd.Series) -> pd.Series:
    txt = pd.Series(segment_series, copy=False).astype("string").str.strip()
    return txt.isin(list(TERM_SOLD_RETAINED_SEGMENT_VALUES))


def _id_key_no_leading_zeros_scalar(val) -> str:
    s = clean_text(val)
    if not s:
        return ""
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"[^0-9A-Za-z]", "", s)
    s = s.lstrip("0")
    return s


def _first_nonblank_scalar(values: Sequence) -> object:
    for v in values:
        if has_any_value(v):
            return v
    return pd.NA


def _select_term_servicer_matches(sf_term: pd.DataFrame, serv_lookup: pd.DataFrame, sf_servicer: pd.Series) -> pd.DataFrame:
    result = pd.DataFrame(index=sf_term.index)
    result["selected_servicer_id"] = pd.NA
    result["selected_sid_key"] = pd.NA
    result["matched_servicer"] = pd.NA
    result["matched_upb"] = np.nan
    result["matched_next_payment_date"] = pd.NaT
    result["matched_maturity_date"] = pd.NaT
    result["matched_source"] = pd.NA

    candidate_cols = [c for c in sf_term.columns if c.startswith("Term Servicer Key ")]
    if "Servicer Commitment Id" in sf_term.columns:
        candidate_cols = candidate_cols + ["Servicer Commitment Id"]

    if not candidate_cols:
        return result

    preferred_raw = [
        _first_nonblank_scalar([sf_term.at[idx, c] for c in candidate_cols if c in sf_term.columns])
        for idx in sf_term.index
    ]
    result["selected_servicer_id"] = pd.Series(preferred_raw, index=sf_term.index, dtype="object")
    result["selected_sid_key"] = id_key_no_leading_zeros(pd.Series(preferred_raw, index=sf_term.index, dtype="object"))

    if serv_lookup is None or serv_lookup.empty or "_sid_key" not in serv_lookup.columns:
        return result

    base = serv_lookup.dropna(subset=["_sid_key"]).copy()
    if base.empty:
        return result

    keep_cols = [c for c in ["_sid_key", "servicer", "upb", "next_payment_date", "maturity_date"] if c in base.columns]
    info_map = {}
    for row in base[keep_cols].drop_duplicates("_sid_key", keep="last").itertuples(index=False):
        rec = {keep_cols[i]: row[i] for i in range(len(keep_cols))}
        info_map[clean_text(rec.get("_sid_key"))] = rec

    selected_raw = []
    selected_key = []
    selected_serv = []
    selected_upb = []
    selected_npd = []
    selected_mat = []
    selected_source = []

    for idx in sf_term.index:
        sf_serv = sf_servicer.loc[idx] if idx in sf_servicer.index else pd.NA
        best = None
        best_score = -10**9
        fallback = None
        fallback_score = -10**9

        for pos, col in enumerate(candidate_cols):
            if col not in sf_term.columns:
                continue
            raw = sf_term.at[idx, col]
            sid_key = _id_key_no_leading_zeros_scalar(raw)
            if not sid_key:
                continue
            info = info_map.get(sid_key)
            has_file = info is not None
            file_serv = info.get("servicer") if info else pd.NA
            file_upb = money_to_float(info.get("upb")) if info else np.nan
            checkpoint_ok = bool(has_file and _servicer_checkpoint_ok(sf_serv, file_serv))
            pref = max(0, 10 - pos) if col != "Servicer Commitment Id" else 1
            file_bonus = 20 if has_file else 0
            upb_bonus = 5 if (pd.notna(file_upb) and float(file_upb) > 0) else 0
            ok_score = 100 + file_bonus + upb_bonus + pref if checkpoint_ok else -10**9
            raw_score = file_bonus + upb_bonus + pref
            if ok_score > best_score:
                best_score = ok_score
                best = (raw, sid_key, info, col)
            if raw_score > fallback_score:
                fallback_score = raw_score
                fallback = (raw, sid_key, info, col)

        chosen = best if best is not None else None
        if chosen is None and not clean_text(sf_serv):
            chosen = fallback

        if chosen is not None:
            raw, sid_key, info, col = chosen
            selected_raw.append(raw)
            selected_key.append(sid_key or pd.NA)
            selected_serv.append(info.get("servicer") if info else pd.NA)
            selected_upb.append(money_to_float(info.get("upb")) if info else np.nan)
            selected_npd.append(pd.to_datetime(info.get("next_payment_date"), errors="coerce") if info else pd.NaT)
            selected_mat.append(pd.to_datetime(info.get("maturity_date"), errors="coerce") if info else pd.NaT)
            selected_source.append(col)
        else:
            raw = result.at[idx, "selected_servicer_id"]
            selected_raw.append(raw)
            selected_key.append(_id_key_no_leading_zeros_scalar(raw) or pd.NA)
            selected_serv.append(pd.NA)
            selected_upb.append(np.nan)
            selected_npd.append(pd.NaT)
            selected_mat.append(pd.NaT)
            selected_source.append(pd.NA)

    result["selected_servicer_id"] = pd.Series(selected_raw, index=sf_term.index, dtype="object")
    result["selected_sid_key"] = pd.Series(selected_key, index=sf_term.index, dtype="object")
    result["matched_servicer"] = pd.Series(selected_serv, index=sf_term.index, dtype="object")
    result["matched_upb"] = pd.to_numeric(pd.Series(selected_upb, index=sf_term.index), errors="coerce")
    result["matched_next_payment_date"] = pd.to_datetime(pd.Series(selected_npd, index=sf_term.index), errors="coerce")
    result["matched_maturity_date"] = pd.to_datetime(pd.Series(selected_mat, index=sf_term.index), errors="coerce")
    result["matched_source"] = pd.Series(selected_source, index=sf_term.index, dtype="object")
    return result

def _term_population_keep_mask(
    stage_series: pd.Series,
    current_upb_series: pd.Series,
    sold_servicing_status_series: pd.Series,
    deal_key_series: Optional[pd.Series] = None,
    prev_sold_retained_keys: Optional[Set[str]] = None,
    always_keep_keys: Optional[Set[str]] = None,
    reo_date_series: Optional[pd.Series] = None,
) -> pd.Series:
    stage = pd.Series(stage_series, copy=False).astype("string").str.strip()
    current_upb = pd.to_numeric(pd.Series(current_upb_series, copy=False), errors="coerce").fillna(0)
    sold_servicing_retained = _sold_servicing_retained_mask(sold_servicing_status_series)

    if deal_key_series is None:
        deal_key = pd.Series([pd.NA] * len(stage), index=stage.index, dtype="object")
    else:
        deal_key = norm_id_series(pd.Series(deal_key_series, copy=False))
        if not deal_key.index.equals(stage.index):
            deal_key.index = stage.index

    prev_sold_retained_keys = prev_sold_retained_keys or set()
    always_keep_keys = always_keep_keys or set()

    in_prev_sold_retained = deal_key.isin(prev_sold_retained_keys)
    always_keep = deal_key.isin(always_keep_keys)

    is_sold = stage.eq("Sold")
    is_reo = stage.isin(REO_FAMILY_STAGES)
    if reo_date_series is not None:
        is_reo = is_reo | pd.to_datetime(pd.Series(reo_date_series, copy=False), errors="coerce").notna()

    positive_upb = current_upb.gt(0)

    keep_mask = (
        always_keep
        | is_reo
        | ((~is_sold) & positive_upb)
        | (is_sold & (sold_servicing_retained | in_prev_sold_retained))
    )
    return keep_mask.fillna(False)


def _filter_term_population(
    sf_term: pd.DataFrame,
    prev_keys: Optional[Set[str]] = None,
    prev_positive_keys: Optional[Set[str]] = None,
    prev_sold_retained_keys: Optional[Set[str]] = None,
) -> pd.DataFrame:
    if sf_term is None or sf_term.empty:
        return sf_term

    out = sf_term.copy()
    out["_deal_key"] = norm_id_series(out.get("Deal Loan Number", pd.Series([None] * len(out), index=out.index)))

    keep_mask = _term_population_keep_mask(
        stage_series=out.get("Stage", pd.Series([""] * len(out), index=out.index)),
        current_upb_series=out.get("Current Servicer UPB", pd.Series([np.nan] * len(out), index=out.index)),
        sold_servicing_status_series=out.get("Sold Loan: Servicing Status", pd.Series([pd.NA] * len(out), index=out.index)),
        deal_key_series=out["_deal_key"],
        prev_sold_retained_keys=prev_sold_retained_keys,
    )
    return out.loc[keep_mask].copy()


def _score_header_row_values(row_values: Sequence[object], required_alias_groups: List[List[str]]) -> int:
    normalized = {normalize_header_name(v) for v in row_values if has_any_value(v)}
    normalized.discard("")
    if not normalized:
        return 0
    return sum(any(normalize_header_name(alias) in normalized for alias in aliases) for aliases in required_alias_groups)


def _best_header_read_excel(
    file_bytes: bytes,
    required_alias_groups: List[List[str]],
    preferred_sheets: Optional[List[str]] = None,
    max_header_scan: int = 8,
    progress_callback: Optional[Callable[[str], None]] = None,
):
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)
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

    best_sheet = None
    best_header_row = None
    best_score = -1
    probe_rows = max(SERVICER_HEADER_SCAN_ROWS, max_header_scan + 2)

    for sheet in ordered:
        if progress_callback:
            progress_callback(f"scanning header rows on sheet '{sheet}'")
        try:
            sample = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=probe_rows, dtype=object)
        except Exception:
            continue
        if sample is None or sample.empty:
            continue

        limit = min(max_header_scan, len(sample.index))
        for header_row in range(limit):
            score = _score_header_row_values(sample.iloc[header_row].tolist(), required_alias_groups)
            if score > best_score:
                best_score = score
                best_sheet = sheet
                best_header_row = header_row

        del sample
        gc.collect()

    if best_sheet is None or best_header_row is None or best_score <= 0:
        raise ValueError("Could not find a matching header row.")

    if progress_callback:
        progress_callback(f"reading matched sheet '{best_sheet}' using header row {best_header_row + 1}")

    df = pd.read_excel(xls, sheet_name=best_sheet, header=best_header_row)
    df = df.dropna(how="all")
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
    return df, best_sheet, best_header_row, best_score


def _best_header_read_csv(
    file_bytes: bytes,
    required_alias_groups: List[List[str]],
    max_header_scan: int = 3,
    progress_callback: Optional[Callable[[str], None]] = None,
):
    probe_rows = max(SERVICER_HEADER_SCAN_ROWS, max_header_scan + 2)
    if progress_callback:
        progress_callback(f"sampling first {probe_rows} CSV rows to find headers")

    sample = pd.read_csv(BytesIO(file_bytes), header=None, nrows=probe_rows, dtype=object)
    if sample is None or sample.empty:
        raise ValueError("Could not find a matching CSV header row.")

    best_header_row = None
    best_score = -1
    limit = min(max_header_scan, len(sample.index))
    for header_row in range(limit):
        score = _score_header_row_values(sample.iloc[header_row].tolist(), required_alias_groups)
        if score > best_score:
            best_score = score
            best_header_row = header_row

    del sample
    gc.collect()

    if best_header_row is None or best_score <= 0:
        raise ValueError("Could not find a matching CSV header row.")

    if progress_callback:
        progress_callback(f"reading CSV using header row {best_header_row + 1}")

    df = pd.read_csv(BytesIO(file_bytes), header=best_header_row)
    df = df.dropna(how="all")
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
    return df, best_header_row, best_score



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


def parse_servicer_bytes(filename: str, b: bytes, progress_callback: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    servicer_type = detect_servicer_type(filename)
    if progress_callback:
        progress_callback(f"detected {servicer_type}; matching layout")

    if servicer_type == "Shellpoint":
        df, _hdr, _score = _best_header_read_csv(
            b,
            [["LoanID", "Servicer Loan ID", "Loan Number"], ["PrincipalBalance", "UPB", "Current UPB"]],
            max_header_scan=2,
            progress_callback=progress_callback,
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
            progress_callback=progress_callback,
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
            progress_callback=progress_callback,
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
            progress_callback=progress_callback,
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
            progress_callback=progress_callback,
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
            progress_callback=progress_callback,
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


def build_servicer_lookup(
    servicer_uploads: List,
    progress_callback: Optional[Callable[[str], None]] = None,
    use_cache: bool = SERVICER_PARSE_CACHE_ENABLED,
) -> Tuple[pd.DataFrame, date, pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    file_dates: List[date] = []
    skipped_files: List[str] = []
    total_uploads = len(servicer_uploads or [])
    parse_started = time.perf_counter()

    for idx, upload in enumerate(servicer_uploads or [], start=1):
        blob = make_upload_blob(upload)

        def _emit(step: str) -> None:
            if progress_callback:
                elapsed = time.perf_counter() - parse_started
                progress_callback(f"servicer {idx}/{total_uploads} | {blob.filename} | {step} | elapsed {elapsed:0.1f}s")

        try:
            servicer_type = detect_servicer_type(blob.filename)
            _emit(f"detected {servicer_type}; starting parse")
        except Exception:
            servicer_type = None
            _emit("starting parse")

        try:
            if use_cache:
                parsed = parse_servicer_cached(blob)
            else:
                parsed = parse_servicer_bytes(blob.filename, blob.data, progress_callback=_emit)
        except Exception as e:
            skipped_files.append(f"{blob.filename}: {e}")
            del blob
            gc.collect()
            continue

        if parsed.empty:
            _emit("parsed 0 rows after cleanup")
            del parsed
            del blob
            gc.collect()
            continue

        frames.append(parsed)
        _emit(f"parsed {len(parsed):,} normalized rows")

        if "as_of" in parsed.columns and parsed["as_of"].notna().any():
            d = pd.to_datetime(parsed["as_of"].dropna().iloc[0]).date()
        else:
            d = date_from_filename(blob.filename)

        if d:
            file_dates.append(d)

        del blob
        gc.collect()

    if skipped_files:
        try:
            st.warning("Skipped servicer file(s): " + " | ".join(skipped_files))
        except Exception:
            pass

    if frames:
        if progress_callback:
            progress_callback("combining normalized servicer rows")
        full = pd.concat(frames, ignore_index=True, copy=False)
    else:
        full = pd.DataFrame(columns=["source_file", "servicer", "servicer_family", "servicer_id", "upb", "suspense", "next_payment_date", "maturity_date", "status", "as_of"])
    frames.clear()
    gc.collect()

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

        if progress_callback:
            progress_callback("ranking and deduplicating servicer rows")

        full = full.sort_values(
            ["_sid_key", "as_of", "_has_nonzero_upb", "_has_upb", "_has_suspense", "_has_npd", "_has_mat", "_label_rank", "upb"],
            ascending=[True, True, True, True, True, True, True, True, True],
        )

        join = full.drop_duplicates(["_sid_key"], keep="last").drop(
            columns=["_has_upb", "_has_nonzero_upb", "_has_suspense", "_has_npd", "_has_mat", "_label_rank"], errors="ignore"
        )
        preview = join.head(SERVICER_PREVIEW_ROWS).copy()
        del full
        gc.collect()
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
                    "Asset ID", "Portfolio", "Segment", "Strategy Grouping", "REO Date", "Active RM",
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

            upb_col_prev = _find_upb_col(ba.columns)
            if upb_col_prev:
                tmpu = ba[["Asset ID", upb_col_prev]].copy()
                tmpu["_asset_key"] = norm_id_series(tmpu["Asset ID"])
                tmpu["_prev_asset_upb"] = tmpu[upb_col_prev].apply(money_to_float)
                out["bridge_asset_upb"] = tmpu.dropna(subset=["_asset_key"]).drop_duplicates("_asset_key")[["_asset_key", "_prev_asset_upb"]]
    except Exception:
        pass

    try:
        bl = read_tab_df_from_active_loans(prev_bytes, "Bridge Loan")
        keep = [
            c for c in [
                "Deal Number", "Portfolio", "Segment", "Strategy Grouping", "Loan Level Delinquency",
                "Special Focus (Y/N)", "AM Commentary", "3/31 NPL", "Needs NPL Value", "Active RM",
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
                "Deal Number", "Servicer ID", "Servicer", "SF Yardi ID", "Deal Name", "Borrower Entity",
                "Account Name", "Do Not Lend (Y/N)", "Portfolio", "Segment", "Financing", "CPP JV",
                "Loan Buyer", "Loan Amount", "Origination Date", "Maturity Date", "Next Payment Date",
                "REO Date", "Asset Manager", "Originator", "Active RM", "Deal Intro Sub-Source",
                "Referral Source Account", "Referral Source Contact", "AM Commentary", "Special Loans List (Y/N)",
            ] if c in tl.columns
        ]
        if "Deal Number" in keep and len(keep) > 1:
            tmpm = tl[keep].copy()
            tmpm["_deal_key"] = norm_id_series(tmpm["Deal Number"])
            out["term_loan_manual"] = tmpm.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")

        if "Servicer ID" in tl.columns:
            keep_sid = [
                c for c in [
                    "Deal Number", "Servicer ID", "Servicer", "SF Yardi ID", "Deal Name", "Borrower Entity",
                    "Account Name", "Do Not Lend (Y/N)", "Portfolio", "Segment", "Financing", "CPP JV",
                    "Loan Buyer", "Loan Amount", "Origination Date", "Maturity Date", "Next Payment Date",
                    "REO Date", "Asset Manager", "Originator", "Active RM", "Deal Intro Sub-Source",
                    "Referral Source Account", "Referral Source Contact", "AM Commentary", "Special Loans List (Y/N)",
                ] if c in tl.columns
            ]
            if keep_sid:
                tmps = tl[keep_sid].copy()
                tmps["_sid_key"] = id_key_no_leading_zeros(tmps["Servicer ID"])
                tmps["_deal_key"] = norm_id_series(tmps["Deal Number"]) if "Deal Number" in tmps.columns else pd.NA
                out["term_loan_sid"] = tmps.dropna(subset=["_sid_key"]).drop_duplicates("_sid_key")

        upb_col_prev = _find_upb_col(tl.columns)
        if upb_col_prev and "Deal Number" in tl.columns:
            tmpu = tl[["Deal Number", upb_col_prev]].copy()
            tmpu["_deal_key"] = norm_id_series(tmpu["Deal Number"])
            tmpu["_prev_upb"] = tmpu[upb_col_prev].apply(money_to_float)
            out["term_loan_upb"] = tmpu.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")[["_deal_key", "_prev_upb"]]
    except Exception:
        pass

    try:
        ta = read_tab_df_from_active_loans(prev_bytes, "Term Asset")
        if "Deal Number" in ta.columns and "Asset ID" in ta.columns:
            keep = [c for c in ["Deal Number", "Asset ID", "Special (Y/N)"] if c in ta.columns]
            if len(keep) >= 2:
                tmpa = ta[keep].copy()
                tmpa["_deal_key"] = norm_id_series(tmpa["Deal Number"])
                tmpa["_asset_key"] = norm_id_series(tmpa["Asset ID"])
                out["term_asset_manual"] = tmpa.dropna(subset=["_deal_key", "_asset_key"]).drop_duplicates(["_deal_key", "_asset_key"])
    except Exception:
        pass

    gc.collect()
    return out


SHEET_BASELINE_KEY_CANDIDATES = {
    "Bridge Asset": [["Asset ID"]],
    "Bridge Loan": [["Deal Number"]],
    "Term Loan": [["Servicer ID"], ["Deal Number"]],
    "Term Asset": [["Deal Number", "Asset ID"]],
}


def _backfill_rule_candidates(sheet_name: str) -> List[List[str]]:
    return [list(x) for x in SHEET_BASELINE_KEY_CANDIDATES.get(sheet_name, [])]


def _available_backfill_keys(sheet_name: str, built_df: pd.DataFrame, baseline_df: pd.DataFrame) -> List[List[str]]:
    candidates = _backfill_rule_candidates(sheet_name)
    built_cols = set(built_df.columns)
    baseline_cols = set(baseline_df.columns)

    out: List[List[str]] = []
    for candidate in candidates:
        if not set(candidate).issubset(built_cols) or not set(candidate).issubset(baseline_cols):
            continue
        if sheet_name == "Term Loan" and candidate == ["Servicer ID"]:
            built_nonblank = id_key_no_leading_zeros(built_df["Servicer ID"]).notna().sum()
            baseline_nonblank = id_key_no_leading_zeros(baseline_df["Servicer ID"]).notna().sum()
            if built_nonblank == 0 or baseline_nonblank == 0:
                continue
        out.append(list(candidate))
    return out


def _choose_backfill_normalizer(sheet_name: str, key_cols: Sequence[str]):
    prefer_no_leading_zeros = sheet_name == "Term Loan" and list(key_cols) == ["Servicer ID"]

    def _one(s: pd.Series) -> pd.Series:
        return id_key_no_leading_zeros(s) if prefer_no_leading_zeros else norm_id_series(s)

    return _one


def _object_series_like(s: pd.Series) -> pd.Series:
    base = pd.Series(s, copy=False)
    return pd.Series(list(base), index=base.index, dtype="object")



def _safe_backfill_assign(sheet_name: str, out: pd.DataFrame, col: str, base_vals: pd.Series, mask: pd.Series) -> pd.DataFrame:
    if not bool(mask.any()):
        return out

    try:
        out.loc[mask, col] = base_vals.loc[mask]
        return out
    except Exception:
        pass

    current = out[col]
    source = base_vals

    if _is_date_header(sheet_name, col):
        parsed_current = _to_datetime_series_mixed(_object_series_like(current))
        parsed_source = _to_datetime_series_mixed(_object_series_like(source))
        parsed_mask = blankish_mask(parsed_current) & (~blankish_mask(parsed_source))
        if bool((mask & parsed_mask).any()):
            out[col] = parsed_current
            out.loc[mask & parsed_mask, col] = parsed_source.loc[mask & parsed_mask]
            remaining_mask = mask & (~parsed_mask)
            if bool(remaining_mask.any()):
                out[col] = _object_series_like(out[col])
                out.loc[remaining_mask, col] = _object_series_like(source).loc[remaining_mask]
            return out

    if pd.api.types.is_numeric_dtype(current):
        source_obj = _object_series_like(source)
        numeric_source = pd.to_numeric(source_obj, errors="coerce")
        numeric_mask = mask & numeric_source.notna()
        if bool(numeric_mask.any()):
            try:
                out.loc[numeric_mask, col] = numeric_source.loc[numeric_mask]
            except Exception:
                out[col] = _object_series_like(out[col])
                out.loc[numeric_mask, col] = numeric_source.loc[numeric_mask]
        remaining_mask = mask & (~numeric_mask)
        if bool(remaining_mask.any()):
            out[col] = _object_series_like(out[col])
            out.loc[remaining_mask, col] = source_obj.loc[remaining_mask]
        return out

    out[col] = _object_series_like(out[col])
    out.loc[mask, col] = _object_series_like(source).loc[mask]
    return out



def _backfill_df_from_baseline_once(
    sheet_name: str,
    built_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    key_cols: Sequence[str],
) -> Tuple[pd.DataFrame, int]:
    out = built_df.copy()
    base = baseline_df.copy()
    normer = _choose_backfill_normalizer(sheet_name, key_cols)
    helper_keys = [f"_baseline_k{i}" for i in range(len(key_cols))]

    for i, col in enumerate(key_cols):
        out[helper_keys[i]] = normer(out[col])
        base[helper_keys[i]] = normer(base[col])

    base = base.dropna(subset=helper_keys).copy()
    if base.empty:
        return out.drop(columns=helper_keys, errors="ignore"), 0

    common_cols = [
        c for c in base.columns
        if c in out.columns
        and c not in key_cols
        and c not in helper_keys
        and not c.startswith("_")
        and not UPB_HEADER_RE.search(str(c))
    ]
    if not common_cols:
        return out.drop(columns=helper_keys, errors="ignore"), 0

    score = pd.Series([0] * len(base), index=base.index, dtype="int64")
    for col in common_cols:
        score = score + (~blankish_mask(base[col])).astype("int64")
    base["_baseline_score"] = score
    base = base.sort_values(helper_keys + ["_baseline_score"], ascending=[True] * len(helper_keys) + [True])
    base = base.drop_duplicates(helper_keys, keep="last")

    base_lookup = base.set_index(helper_keys, drop=False)
    out = out.set_index(helper_keys, drop=False)

    fills = 0
    for col in common_cols:
        base_vals = base_lookup[col].reindex(out.index)
        mask = blankish_mask(out[col]) & (~blankish_mask(base_vals))
        if mask.any():
            fills += int(mask.sum())
            out = _safe_backfill_assign(sheet_name, out, col, base_vals, mask)

    out = out.reset_index(drop=True)
    out = out.drop(columns=helper_keys + ["_baseline_score"], errors="ignore")
    return downcast_numeric_frame(out), fills


def backfill_df_from_baseline(sheet_name: str, built_df: pd.DataFrame, baseline_bytes: Optional[bytes]) -> Tuple[pd.DataFrame, dict]:
    if baseline_bytes is None or built_df is None or built_df.empty:
        return built_df, {"sheet_name": sheet_name, "status": "skipped_empty", "keys": "", "fills": 0}

    try:
        baseline_df = read_tab_df_from_active_loans(baseline_bytes, sheet_name)
    except Exception as exc:
        return built_df, {"sheet_name": sheet_name, "status": f"baseline_read_failed: {exc}", "keys": "", "fills": 0}

    out = built_df.copy()
    key_candidates = _available_backfill_keys(sheet_name, out, baseline_df)
    if not key_candidates:
        return out, {"sheet_name": sheet_name, "status": "skipped_no_keys", "keys": "", "fills": 0}

    fills = 0
    used_keys: List[str] = []
    for key_cols in key_candidates:
        out, one_fill = _backfill_df_from_baseline_once(sheet_name, out, baseline_df, key_cols)
        if one_fill:
            fills += int(one_fill)
            used_keys.append(", ".join(key_cols))

    return downcast_numeric_frame(out), {
        "sheet_name": sheet_name,
        "status": "backfilled" if fills else "no_fills_needed",
        "keys": " | ".join(used_keys or [", ".join(x) for x in key_candidates]),
        "fills": fills,
    }


def _parse_npl_or_reo_sheet(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:

    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=4)
    df = df.dropna(how="all").copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def parse_npl_reo_bytes(file_bytes: bytes) -> dict:
    out = {
        "loan_flags": pd.DataFrame(columns=["_deal_key", "NPL Flag", "Needs NPL Value", "Special Focus (Y/N)"]),
        "asset_flags": pd.DataFrame(columns=["_deal_key", "_asset_key", "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]),
        "asset_deal_fallback": pd.DataFrame(columns=["_deal_key", "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]),
    }

    def _append_flags(df_src: pd.DataFrame, npl_flag: str, special_flag: str):
        nonlocal out
        deal_key = norm_id_series(df_src["Deal Number"])
        asset_key = norm_id_series(df_src["Asset ID"]) if "Asset ID" in df_src.columns else pd.Series([pd.NA] * len(df_src), index=df_src.index)

        loan_flags = pd.DataFrame(
            {
                "_deal_key": deal_key,
                "NPL Flag": npl_flag,
                "Needs NPL Value": "N",
                "Special Focus (Y/N)": special_flag,
            }
        ).dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")
        out["loan_flags"] = pd.concat([out["loan_flags"], loan_flags], ignore_index=True, copy=False)

        asset_specific = pd.DataFrame(
            {
                "_deal_key": deal_key,
                "_asset_key": asset_key,
                "3/31 NPL (Y/N)": npl_flag,
                "Needs NPL Value": "N",
                "Special Flag": special_flag,
            }
        )
        asset_specific = asset_specific.dropna(subset=["_deal_key"])
        out["asset_flags"] = pd.concat(
            [out["asset_flags"], asset_specific[asset_specific["_asset_key"].notna()]],
            ignore_index=True,
            copy=False,
        )
        out["asset_deal_fallback"] = pd.concat(
            [out["asset_deal_fallback"], asset_specific[asset_specific["_asset_key"].isna()].drop(columns=["_asset_key"])],
            ignore_index=True,
            copy=False,
        )

    try:
        npl = _parse_npl_or_reo_sheet(file_bytes, "NPL")
        if "Deal Number" in npl.columns:
            _append_flags(npl, "Y", "Y")
    except Exception:
        pass

    try:
        reo = _parse_npl_or_reo_sheet(file_bytes, "REO")
        if "Deal Number" in reo.columns:
            _append_flags(reo, "N", "Y")
    except Exception:
        pass

    if not out["loan_flags"].empty:
        out["loan_flags"] = out["loan_flags"].sort_values(["_deal_key", "Special Focus (Y/N)", "NPL Flag"]).drop_duplicates("_deal_key", keep="last")
    if not out["asset_flags"].empty:
        out["asset_flags"] = out["asset_flags"].drop_duplicates(["_deal_key", "_asset_key"], keep="last")
    if not out["asset_deal_fallback"].empty:
        specific_deals = set(out["asset_flags"]["_deal_key"].dropna().tolist())
        out["asset_deal_fallback"] = out["asset_deal_fallback"][~out["asset_deal_fallback"]["_deal_key"].isin(specific_deals)].copy()
        out["asset_deal_fallback"] = out["asset_deal_fallback"].drop_duplicates("_deal_key", keep="last")

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
    out["Active RM"] = pd.NA
    out["3/31 NPL (Y/N)"] = pd.NA
    out["Needs NPL Value"] = pd.NA
    out["Special Flag"] = pd.NA
    out["Servicer"] = pd.NA
    out["Servicer Status"] = pd.NA

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_sid_key"] = id_key_no_leading_zeros(out.get("Servicer ID", pd.Series([None] * len(out))))
    out["_asset_key"] = norm_id_series(out.get("Asset ID", pd.Series([None] * len(out))))

    if "bridge_asset_upb" in prev_maps:
        prev_asset_upb = prev_maps["bridge_asset_upb"].copy()
        out = out.merge(prev_asset_upb, on="_asset_key", how="left")
    else:
        out["_prev_asset_upb"] = np.nan

    if not sf_dnl.empty and "Deal Loan Number" in sf_dnl.columns:
        dnl = sf_dnl.copy()
        dnl["_deal_key"] = norm_id_series(dnl["Deal Loan Number"])
        if "Do Not Lend" in dnl.columns:
            dnl = dnl[["_deal_key", "Do Not Lend"]].drop_duplicates("_deal_key")
            out = out.merge(dnl, on="_deal_key", how="left")
            out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend"])
            out = out.drop(columns=["Do Not Lend"], errors="ignore")

    if not sf_active_rm.empty and "Deal Loan Number" in sf_active_rm.columns and "Active RM" in sf_active_rm.columns:
        arm = sf_active_rm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key", "Active RM"]].drop_duplicates("_deal_key")
        out = out.merge(arm, on="_deal_key", how="left", suffixes=("", "_sf"))
        out["Active RM"] = coalesce_keep_nonblank(out.get("Active RM_sf", pd.Series([pd.NA] * len(out), index=out.index)), out["Active RM"])
        out = out.drop(columns=["Active RM_sf"], errors="ignore")

    if not sf_val.empty and "Asset ID" in sf_val.columns:
        v = sf_val.copy()
        v["_asset_key"] = norm_id_series(v["Asset ID"])
        rename_map = {
            vlabel: f"__val__{tcol}"
            for tcol, vlabel in BRIDGE_ASSET_FROM_VALUATION.items()
            if vlabel in v.columns
        }
        keep = ["_asset_key"] + list(rename_map.keys())
        v = v[keep].rename(columns=rename_map).drop_duplicates("_asset_key")
        out = out.merge(v, on="_asset_key", how="left")
        for tcol in BRIDGE_ASSET_FROM_VALUATION.keys():
            tmpcol = f"__val__{tcol}"
            if tmpcol in out.columns:
                out[tcol] = coalesce_keep_nonblank(
                    out.get(tcol, pd.Series([pd.NA] * len(out), index=out.index)),
                    out[tmpcol],
                )
                out = out.drop(columns=[tmpcol], errors="ignore")

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
        af_asset = npl_maps["asset_flags"].copy()
        keep = ["_deal_key", "_asset_key", "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]
        af_asset = af_asset[keep].drop_duplicates(["_deal_key", "_asset_key"])
        out = out.merge(af_asset, on=["_deal_key", "_asset_key"], how="left", suffixes=("", "_nplasset"))
        for c in ["3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]:
            out[c] = coalesce_keep_nonblank(out.get(f"{c}_nplasset", pd.Series([pd.NA] * len(out), index=out.index)), out[c])
            out = out.drop(columns=[f"{c}_nplasset"], errors="ignore")

    if npl_maps and not npl_maps.get("asset_deal_fallback", pd.DataFrame()).empty:
        af_deal = npl_maps["asset_deal_fallback"].copy()
        af_deal = af_deal[["_deal_key", "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]].drop_duplicates("_deal_key")
        out = out.merge(af_deal, on="_deal_key", how="left", suffixes=("", "_npldeal"))
        for c in ["3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag"]:
            out[c] = coalesce_keep_nonblank(out.get(c, pd.Series([pd.NA] * len(out), index=out.index)), out.get(f"{c}_npldeal", pd.Series([pd.NA] * len(out), index=out.index)))
            out = out.drop(columns=[f"{c}_npldeal"], errors="ignore")

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

    base_stage_series = out.get("Loan Stage", pd.Series([pd.NA] * len(out), index=out.index)).astype("string").str.strip()
    out["Financing"] = pd.Series(out.get("Financing", pd.Series([pd.NA] * len(out), index=out.index)), index=out.index, dtype="object")
    out["Financing"] = out["Financing"].mask(blankish_mask(out["Financing"]) & base_stage_series.eq("Sold"), "Sold")

    prop_npd = pd.to_datetime(sf_spine.get("Property Next Payment Date", pd.Series([pd.NaT] * len(out))), errors="coerce")
    opp_npd = pd.to_datetime(sf_spine.get("Opportunity Next Payment Date", pd.Series([pd.NaT] * len(out))), errors="coerce")
    sf_next_payment = prop_npd.where(prop_npd.notna(), opp_npd)
    sf_current_upb = pd.to_numeric(sf_spine.get("Current UPB", pd.Series([np.nan] * len(out))), errors="coerce")

    blank_obj = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")

    if not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy()
        s = s.rename(
            columns={
                "servicer": "_servicer_file",
                "upb": "_loan_upb",
                "suspense": "_loan_suspense",
                "next_payment_date": "_serv_next_payment_date",
                "maturity_date": "_servicer_maturity_file",
                "status": "_servicer_status_file",
            }
        )

        out = out.merge(
            s[["_sid_key", "_servicer_file", "_loan_upb", "_loan_suspense", "_serv_next_payment_date", "_servicer_maturity_file", "_servicer_status_file", "source_file"]],
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

        late_stage_mask = stage_series.astype("string").str.strip().isin(EXPIRED_OR_MATURED_STAGES)
        prev_asset_upb_vals = pd.to_numeric(out.get("_prev_asset_upb", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
        current_upb_series = pd.to_numeric(out[upb_col], errors="coerce")
        out[upb_col] = current_upb_series.where(
            ~(late_stage_mask & (current_upb_series.isna() | current_upb_series.le(0))),
            prev_asset_upb_vals,
        )

        out["Next Payment Date"] = pd.to_datetime(out.get("_serv_next_payment_date"), errors="coerce")
        out["Next Payment Date"] = pd.to_datetime(out["Next Payment Date"], errors="coerce").where(
            pd.to_datetime(out["Next Payment Date"], errors="coerce").notna(),
            sf_next_payment,
        )

        out["Servicer"] = coalesce_keep_nonblank(out.get("_servicer_file", blank_obj), out.get("Servicer", blank_obj))
        out["Servicer Status"] = coalesce_keep_nonblank(out.get("_servicer_status_file", blank_obj), out.get("Servicer Status", blank_obj))
        out["Servicer Maturity Date"] = pd.to_datetime(out.get("_servicer_maturity_file"), errors="coerce")

        out = out.drop(columns=["_prev_upb"], errors="ignore")
    else:
        out[upb_col] = sf_current_upb
        late_stage_mask = out.get("Loan Stage", pd.Series([pd.NA] * len(out), index=out.index)).astype("string").str.strip().isin(EXPIRED_OR_MATURED_STAGES)
        prev_asset_upb_vals = pd.to_numeric(out.get("_prev_asset_upb", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
        current_upb_series = pd.to_numeric(out[upb_col], errors="coerce")
        out[upb_col] = current_upb_series.where(
            ~(late_stage_mask & (current_upb_series.isna() | current_upb_series.le(0))),
            prev_asset_upb_vals,
        )
        out["Next Payment Date"] = sf_next_payment
        out["Servicer Maturity Date"] = pd.NaT
        out["Suspense Balance"] = np.nan

    if "bridge_asset_manual" in prev_maps:
        man = prev_maps["bridge_asset_manual"].copy()
        keep_cols = ["_asset_key"] + [c for c in [
            "Portfolio", "Segment", "Strategy Grouping", "REO Date", "Active RM",
            "3/31 NPL (Y/N)", "Needs NPL Value", "Special Flag",
            "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
            "Construction Mgr.", "CM Assigned Date", "Servicer", "Servicer Status",
            "Remedy Plan", "Delinquency Notes", "Maturity Status", "Title Company",
            "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact",
        ] if c in man.columns]
        out = out.merge(man[keep_cols], on="_asset_key", how="left", suffixes=("", "_prev"))
        for c in [x for x in keep_cols if x != "_asset_key"]:
            if f"{c}_prev" in out.columns:
                out[c] = coalesce_keep_nonblank(out.get(c, blank_obj), out[f"{c}_prev"])
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    status_bucket = pd.Series(
        [
            normalize_bridge_servicer_status(raw_status, npd, run_dt, loan_stage, property_status, reo_date)
            for raw_status, npd, loan_stage, property_status, reo_date in zip(
                out.get("Servicer Status", blank_obj),
                out.get("Next Payment Date", pd.Series([pd.NaT] * len(out), index=out.index)),
                out.get("Loan Stage", blank_obj),
                out.get("Property Status", blank_obj),
                out.get("REO Date", pd.Series([pd.NaT] * len(out), index=out.index)),
            )
        ],
        index=out.index,
        dtype="object",
    )
    out["_bridge_dq_bucket"] = status_bucket
    out["_bridge_dpd_num"] = pd.Series(
        [
            min(_guess_days_past_due(npd, run_dt), 29.0) if has_any_value(bucket) and clean_text(bucket).upper() == "CURRENT" else _guess_days_from_bridge_bucket(bucket)
            for bucket, npd in zip(status_bucket, out.get("Next Payment Date", pd.Series([pd.NaT] * len(out), index=out.index)))
        ],
        index=out.index,
        dtype="float64",
    )
    out["Servicer Status"] = coalesce_keep_nonblank(status_bucket, out.get("Servicer Status", blank_obj))

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

    out["Servicer ID"] = normalize_servicer_id_for_report(out.get("Servicer ID", blank_obj), out.get("Servicer", blank_obj))
    out["Active RM"] = coalesce_keep_nonblank(out["Active RM"], pd.Series(["N"] * len(out), index=out.index))
    out["3/31 NPL (Y/N)"] = coalesce_keep_nonblank(out["3/31 NPL (Y/N)"], pd.Series(["N"] * len(out), index=out.index))
    out["Needs NPL Value"] = coalesce_keep_nonblank(out["Needs NPL Value"], pd.Series(["N"] * len(out), index=out.index))
    out["Special Flag"] = coalesce_keep_nonblank(out["Special Flag"], pd.Series(["N"] * len(out), index=out.index))

    out = _fill_text_defaults(
        out,
        [
            "Loan Buyer", "Servicer", "Primary Contact",
            "Remedy Plan", "Delinquency Notes", "Maturity Status",
            "Special Asset Status", "Special Asset Reason", "Special Asset: Special Asset Status",
            "Title Company", "Tax Frequency", "Tax Commentary",
            "Originator", "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact",
            "Servicer Status", "Asset Manager 1", "Asset Manager 2", "Construction Mgr.",
        ],
    )

    stage_series = out.get("Loan Stage", pd.Series([pd.NA] * len(out), index=out.index)).astype("string").str.strip()
    current_upb = pd.to_numeric(out.get(upb_col, pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").fillna(0)
    is_closed_won = stage_series.eq("Closed Won")
    is_sold = stage_series.eq("Sold")
    is_reo = stage_series.isin(REO_FAMILY_STAGES) | pd.to_datetime(out.get("REO Date", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").notna()
    is_expired_or_matured = stage_series.isin(EXPIRED_OR_MATURED_STAGES)

    keep_mask = is_closed_won | is_reo | is_sold | (is_expired_or_matured & current_upb.gt(0))
    out = out.loc[keep_mask].copy()
    out = out[out["_deal_key"].notna() & out["_asset_key"].notna()].copy()

    return downcast_numeric_frame(out)



def _build_term_loan_salesforce_fallback(
    sf_term: pd.DataFrame,
    sf_am: pd.DataFrame,
    sf_active_rm: pd.DataFrame,
    serv_lookup: pd.DataFrame,
    upb_col: str,
    prev_maps: dict,
    template_maps: dict,
    prev_sold_retained_keys: Optional[Set[str]] = None,
    always_keep_keys: Optional[Set[str]] = None,
) -> pd.DataFrame:
    if sf_term is None or sf_term.empty:
        return pd.DataFrame()

    out = pd.DataFrame(index=sf_term.index)

    for col, label in TERM_LOAN_FROM_TERM_WIDE.items():
        out[col] = sf_term[label] if label in sf_term.columns else pd.NA

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_stage_raw"] = sf_term.get("Stage", pd.Series([pd.NA] * len(out), index=out.index))
    out["_sold_servicing_status_raw"] = sf_term.get("Sold Loan: Servicing Status", pd.Series([pd.NA] * len(out), index=out.index))
    out["_sf_current_servicer_upb_raw"] = pd.to_numeric(
        sf_term.get("Current Servicer UPB", pd.Series([np.nan] * len(out), index=out.index)),
        errors="coerce",
    )

    if "Do Not Lend (Y/N)" in out.columns:
        out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend (Y/N)"])

    out["Loan Buyer"] = sf_term["Sold Loan: Sold To"] if "Sold Loan: Sold To" in sf_term.columns else pd.NA
    out["Active RM"] = pd.NA
    out["Servicer"] = sf_term["Servicer Name"] if "Servicer Name" in sf_term.columns else pd.NA
    out["Maturity Date"] = pd.to_datetime(sf_term["Original Loan Maturity Date"], errors="coerce") if "Original Loan Maturity Date" in sf_term.columns else pd.NaT
    out["Next Payment Date"] = pd.to_datetime(sf_term["Next Payment Date"], errors="coerce") if "Next Payment Date" in sf_term.columns else pd.NaT

    if not sf_active_rm.empty and "Deal Loan Number" in sf_active_rm.columns and "Active RM" in sf_active_rm.columns:
        arm = sf_active_rm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key", "Active RM"]].drop_duplicates("_deal_key")
        out = out.merge(arm, on="_deal_key", how="left", suffixes=("", "_sf"))
        out["Active RM"] = coalesce_keep_nonblank(out.get("Active RM_sf", pd.Series([pd.NA] * len(out), index=out.index)), out["Active RM"])
        out = out.drop(columns=["Active RM_sf"], errors="ignore")

    cls = sf_term.apply(
        lambda r: pd.Series(
            derive_term_portfolio_segment(
                r.get("Type"),
                r.get("Current Funding Vehicle"),
                r.get("Sold Loan: Sold To"),
                r.get("Deal Loan Number"),
                template_maps,
                sold_servicing_status=r.get("Sold Loan: Servicing Status"),
            ),
            index=["Portfolio", "Segment", "CPP JV"],
        ),
        axis=1,
    )
    out["Portfolio"] = cls["Portfolio"]
    out["Segment"] = cls["Segment"]
    out["CPP JV"] = cls["CPP JV"]

    sold_stage_series = sf_term.get("Stage", pd.Series([pd.NA] * len(out), index=out.index)).astype("string").str.strip()
    out["Financing"] = pd.Series(out.get("Financing", pd.Series([pd.NA] * len(out), index=out.index)), index=out.index, dtype="object")
    out["Financing"] = out["Financing"].mask(blankish_mask(out["Financing"]) & sold_stage_series.eq("Sold"), "Sold")

    blank_obj = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")

    if "term_loan_manual" in prev_maps:
        man = prev_maps["term_loan_manual"].copy()
        keep_cols = ["_deal_key"] + [c for c in [
            "Portfolio", "Segment", "CPP JV", "Special Loans List (Y/N)",
            "Asset Manager", "Deal Intro Sub-Source", "Referral Source Account",
            "Referral Source Contact", "AM Commentary", "Servicer", "Loan Buyer", "Servicer ID",
            "Active RM",
        ] if c in man.columns]
        out = out.merge(man[keep_cols], on="_deal_key", how="left", suffixes=("", "_prev"))
        for c in [x for x in keep_cols if x != "_deal_key"]:
            if f"{c}_prev" in out.columns:
                out[c] = coalesce_keep_nonblank(out.get(c, blank_obj), out[f"{c}_prev"])
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    if not sf_am.empty and "Deal Loan Number" in sf_am.columns:
        am = sf_am.copy()
        am["_deal_key"] = norm_id_series(am["Deal Loan Number"])
        am["_dt"] = pd.to_datetime(am.get("Date Assigned"), errors="coerce")
        am = am.sort_values(["_deal_key", "Team Role", "_dt"]).drop_duplicates(["_deal_key", "Team Role"], keep="last")

        am1 = am[am["Team Role"].astype("string").str.strip().eq("Asset Manager")][["_deal_key", "Team Member Name"]]
        am1 = am1.drop_duplicates("_deal_key")
        out = out.merge(am1, on="_deal_key", how="left")
        out["Asset Manager"] = coalesce_keep_nonblank(out.get("Asset Manager", blank_obj), out["Team Member Name"])
        out = out.drop(columns=["Team Member Name"], errors="ignore")
    else:
        out["Asset Manager"] = out.get("Asset Manager", blank_obj)

    base_sf_servicer = pd.Series(out.get("Servicer", blank_obj), index=out.index, dtype="object")
    match_df = _select_term_servicer_matches(sf_term, serv_lookup, base_sf_servicer)
    out["Servicer ID"] = coalesce_keep_nonblank(match_df["selected_servicer_id"], out.get("Servicer ID", blank_obj))

    sf_upb_fallback = pd.to_numeric(
        sf_term["Current Servicer UPB"] if "Current Servicer UPB" in sf_term.columns else pd.Series([np.nan] * len(out)),
        errors="coerce",
    )

    out["Servicer"] = coalesce_keep_nonblank(match_df["matched_servicer"], out["Servicer"])
    out["Maturity Date"] = pd.to_datetime(match_df["matched_maturity_date"], errors="coerce").where(
        pd.to_datetime(match_df["matched_maturity_date"], errors="coerce").notna(),
        pd.to_datetime(out["Maturity Date"], errors="coerce"),
    )
    out["Next Payment Date"] = pd.to_datetime(match_df["matched_next_payment_date"], errors="coerce").where(
        pd.to_datetime(match_df["matched_next_payment_date"], errors="coerce").notna(),
        pd.to_datetime(out["Next Payment Date"], errors="coerce"),
    )
    out[upb_col] = pd.to_numeric(match_df["matched_upb"], errors="coerce").where(
        pd.to_numeric(match_df["matched_upb"], errors="coerce").notna(),
        sf_upb_fallback,
    )

    if "term_loan_manual" in prev_maps and "Servicer ID" in prev_maps["term_loan_manual"].columns:
        prev_sid = prev_maps["term_loan_manual"][["_deal_key", "Servicer ID"]].copy()
        out = out.merge(prev_sid, on="_deal_key", how="left", suffixes=("", "_prev_sid"))
        if "Servicer ID_prev_sid" in out.columns:
            out["Servicer ID"] = coalesce_keep_nonblank(out.get("Servicer ID", blank_obj), out["Servicer ID_prev_sid"])
            out = out.drop(columns=["Servicer ID_prev_sid"], errors="ignore")

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

    keep_mask = _term_population_keep_mask(
        stage_series=out.get("_stage_raw", pd.Series([pd.NA] * len(out), index=out.index)),
        current_upb_series=out.get("_sf_current_servicer_upb_raw", pd.Series([np.nan] * len(out), index=out.index)),
        sold_servicing_status_series=out.get("_sold_servicing_status_raw", pd.Series([pd.NA] * len(out), index=out.index)),
        deal_key_series=out.get("_deal_key", pd.Series([pd.NA] * len(out), index=out.index)),
        prev_sold_retained_keys=prev_sold_retained_keys,
        always_keep_keys=always_keep_keys,
        reo_date_series=out.get("REO Date", pd.Series([pd.NaT] * len(out), index=out.index)),
    )
    out = out.loc[keep_mask].copy()
    out = out[(out.get("_sid_key", pd.Series([pd.NA] * len(out), index=out.index)).notna()) | (out["_deal_key"].notna())].copy()

    out["Active RM"] = coalesce_keep_nonblank(out.get("Active RM", pd.Series([pd.NA] * len(out), index=out.index)), pd.Series(["N"] * len(out), index=out.index))
    out["Special Loans List (Y/N)"] = coalesce_keep_nonblank(
        out.get("Special Loans List (Y/N)", pd.Series([pd.NA] * len(out), index=out.index)),
        pd.Series(["N"] * len(out), index=out.index),
    )

    out = _fill_text_defaults(
        out,
        [
            "Servicer ID", "Servicer", "Loan Buyer", "Asset Manager",
            "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact", "AM Commentary",
        ],
    )

    return downcast_numeric_frame(out)




def _build_term_servicer_spine(serv_lookup: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "_sid_key", "Servicer ID", "Servicer", "Servicer Family",
        "Current Servicer UPB", "Next Payment Date", "Maturity Date",
        "Servicer Status", "Source File",
    ]
    if serv_lookup is None or serv_lookup.empty:
        return pd.DataFrame(columns=cols)

    s = serv_lookup.copy()
    if "_sid_key" not in s.columns:
        s["_sid_key"] = id_key_no_leading_zeros(s.get("servicer_id", pd.Series([None] * len(s), index=s.index)))

    fam = s.get("servicer_family", pd.Series([""] * len(s), index=s.index)).astype("string").str.lower().str.strip()
    upb = pd.to_numeric(s.get("upb", pd.Series([np.nan] * len(s), index=s.index)), errors="coerce")
    maturity = pd.to_datetime(s.get("maturity_date", pd.Series([pd.NaT] * len(s), index=s.index)), errors="coerce")
    next_payment = pd.to_datetime(s.get("next_payment_date", pd.Series([pd.NaT] * len(s), index=s.index)), errors="coerce")
    as_of = pd.to_datetime(s.get("as_of", pd.Series([pd.NaT] * len(s), index=s.index)), errors="coerce")

    active_mask = fam.isin(TERM_SPINE_SERVICER_FAMILIES) & s["_sid_key"].notna() & upb.gt(0)
    s = s.loc[active_mask].copy()
    if s.empty:
        return pd.DataFrame(columns=cols)

    s["Servicer ID"] = coalesce_keep_nonblank(
        pd.Series(s.get("servicer_id", pd.Series([pd.NA] * len(s), index=s.index)), index=s.index, dtype="object"),
        pd.Series(s["_sid_key"], index=s.index, dtype="object"),
    )
    s["Servicer"] = pd.Series(s.get("servicer", pd.Series([pd.NA] * len(s), index=s.index)), index=s.index, dtype="object")
    s["Servicer Family"] = fam
    s["Current Servicer UPB"] = upb
    s["Next Payment Date"] = next_payment
    s["Maturity Date"] = maturity
    s["Servicer Status"] = pd.Series(s.get("status", pd.Series([pd.NA] * len(s), index=s.index)), index=s.index, dtype="object")
    s["Source File"] = pd.Series(s.get("source_file", pd.Series([pd.NA] * len(s), index=s.index)), index=s.index, dtype="object")
    s["_as_of_sort"] = as_of
    s["_mat_sort"] = s["Maturity Date"]
    s["_npd_sort"] = s["Next Payment Date"]

    s = s.sort_values(["_sid_key", "_as_of_sort", "Current Servicer UPB", "_mat_sort", "_npd_sort"]).drop_duplicates("_sid_key", keep="last")
    return downcast_numeric_frame(s[cols])


def _term_stage_rank(stage, sold_servicing_status=None) -> int:
    st = clean_text(stage)
    base = {
        "Closed Won": 80,
        "Approved by Committee": 70,
        "Sold": 60,
        "REO": 50,
        "REO-Sold": 45,
        "Paid Off": 20,
    }.get(st, 10 if st else 0)
    retained = bool(_sold_servicing_retained_mask(pd.Series([sold_servicing_status])).iloc[0])
    if retained:
        base += 15
    return base


def _build_term_sf_sid_lookup(sf_term: pd.DataFrame, prev_maps: Optional[dict] = None) -> pd.DataFrame:
    if sf_term is None or sf_term.empty:
        return pd.DataFrame()

    candidate_cols = [c for c in sf_term.columns if c.startswith("Term Servicer Key ")]
    if "Servicer Commitment Id" in sf_term.columns:
        candidate_cols.append("Servicer Commitment Id")
    if not candidate_cols:
        return pd.DataFrame()

    detail_fields = [
        "Deal Loan Number", "Yardi ID", "Deal Name", "Borrower Entity", "Account Name",
        "Current Funding Vehicle", "Loan Amount", "Close Date", "CAF Originator",
        "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact",
        "Comments AM", "Sold Loan: Sold To", "Servicer Name", "Current Servicer UPB",
        "Sold Loan: Servicing Status", "Stage",
    ]
    keep_cols = [c for c in dict.fromkeys(detail_fields) if c in sf_term.columns]

    frames = []
    for pos, col in enumerate(candidate_cols):
        sid_key = id_key_no_leading_zeros(sf_term.get(col, pd.Series([None] * len(sf_term), index=sf_term.index)))
        mask = sid_key.notna()
        if not mask.any():
            continue
        tmp = sf_term.loc[mask, keep_cols].copy()
        tmp["_sid_key"] = pd.Series(sid_key.loc[mask], index=tmp.index, dtype="object")
        tmp["_sid_source_col"] = col
        tmp["_sid_priority"] = 1 if col == "Servicer Commitment Id" else max(0, 50 - pos)
        frames.append(tmp)

    if not frames:
        return pd.DataFrame()

    key_df = pd.concat(frames, ignore_index=True, copy=False)
    key_df["_deal_key"] = norm_id_series(key_df.get("Deal Loan Number", pd.Series([None] * len(key_df), index=key_df.index)))
    key_df["_sf_upb_num"] = pd.to_numeric(key_df.get("Current Servicer UPB", pd.Series([np.nan] * len(key_df), index=key_df.index)), errors="coerce").fillna(0)
    key_df["_loan_amt_num"] = pd.to_numeric(key_df.get("Loan Amount", pd.Series([np.nan] * len(key_df), index=key_df.index)), errors="coerce").fillna(0)
    key_df["_close_dt"] = pd.to_datetime(key_df.get("Close Date", pd.Series([pd.NaT] * len(key_df), index=key_df.index)), errors="coerce")
    key_df["_sold_retained"] = _sold_servicing_retained_mask(
        key_df.get("Sold Loan: Servicing Status", pd.Series([pd.NA] * len(key_df), index=key_df.index))
    ).astype("int8")
    key_df["_stage_rank"] = [
        _term_stage_rank(stage, sold_status)
        for stage, sold_status in zip(
            key_df.get("Stage", pd.Series([pd.NA] * len(key_df), index=key_df.index)),
            key_df.get("Sold Loan: Servicing Status", pd.Series([pd.NA] * len(key_df), index=key_df.index)),
        )
    ]

    detail_count = pd.Series([0] * len(key_df), index=key_df.index, dtype="int64")
    for col in [c for c in detail_fields if c in key_df.columns]:
        detail_count = detail_count + (~blankish_mask(key_df[col])).astype("int64")
    key_df["_detail_count"] = detail_count

    key_df["_prev_sid_match"] = 0
    key_df["_prev_sid_present"] = 0
    if prev_maps and "term_loan_sid" in prev_maps:
        prev_sid = prev_maps["term_loan_sid"]
        if isinstance(prev_sid, pd.DataFrame) and not prev_sid.empty and {"_sid_key", "_deal_key"}.issubset(prev_sid.columns):
            sid_to_prev_deal = prev_sid.dropna(subset=["_sid_key"]).drop_duplicates("_sid_key").set_index("_sid_key")["_deal_key"].to_dict()
            prev_deal = key_df["_sid_key"].map(sid_to_prev_deal)
            prev_sid_present = prev_deal.notna()
            deal_key_present = key_df["_deal_key"].notna()
            prev_sid_match = prev_sid_present & deal_key_present & key_df["_deal_key"].eq(prev_deal)
            key_df["_prev_sid_present"] = prev_sid_present.astype("int8")
            key_df["_prev_sid_match"] = prev_sid_match.astype("int8")

    key_df = key_df.sort_values(
        [
            "_sid_key",
            "_prev_sid_match",
            "_prev_sid_present",
            "_sold_retained",
            "_stage_rank",
            "_sid_priority",
            "_detail_count",
            "_sf_upb_num",
            "_loan_amt_num",
            "_close_dt",
        ],
        ascending=[True, True, True, True, True, True, True, True, True, True],
    )
    return downcast_numeric_frame(key_df.drop_duplicates("_sid_key", keep="last"))


def build_term_loan(
    sf_term: pd.DataFrame,
    sf_am: pd.DataFrame,
    sf_active_rm: pd.DataFrame,
    serv_lookup: pd.DataFrame,
    upb_col: str,
    prev_maps: dict,
    template_maps: dict,
    asset_deal_numbers: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    prev_keys = _prev_term_keys(prev_maps)
    prev_positive_keys = _prev_term_positive_upb_keys(prev_maps)
    prev_sold_retained_keys = _prev_term_sold_retained_keys(prev_maps)
    sf_term_active = _filter_term_population(sf_term, prev_keys=prev_keys, prev_positive_keys=prev_positive_keys, prev_sold_retained_keys=prev_sold_retained_keys)

    asset_filter_provided = asset_deal_numbers is not None
    asset_deal_keys = set(norm_id_series(pd.Series(list(asset_deal_numbers or []), dtype="object")).dropna().tolist())
    always_keep_keys = set(norm_id_series(pd.Series(list(TERM_ALWAYS_INCLUDE_DEALS), dtype="object")).dropna().tolist())
    if asset_filter_provided and sf_term_active is not None and not sf_term_active.empty:
        sf_term_active = sf_term_active.copy()
        sf_term_active["_deal_key"] = norm_id_series(sf_term_active.get("Deal Loan Number", pd.Series([None] * len(sf_term_active), index=sf_term_active.index)))
        sf_term_active = sf_term_active[sf_term_active["_deal_key"].isin(asset_deal_keys | always_keep_keys)].copy()
        sf_term_active = sf_term_active.drop(columns=["_deal_key"], errors="ignore")

    out = _build_term_loan_salesforce_fallback(
        sf_term_active,
        sf_am,
        sf_active_rm,
        serv_lookup,
        upb_col,
        prev_maps,
        template_maps,
        prev_sold_retained_keys=prev_sold_retained_keys,
        always_keep_keys=always_keep_keys,
    )
    if out.empty:
        return out

    blank_obj = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")
    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out), index=out.index)))
    out["_sid_key"] = id_key_no_leading_zeros(out.get("Servicer ID", pd.Series([None] * len(out), index=out.index)))

    sf_sid = _build_term_sf_sid_lookup(sf_term_active, prev_maps=prev_maps)
    if not sf_sid.empty:
        sf_keep = [c for c in ["_sid_key", "Deal Loan Number", "Yardi ID", "Deal Name", "Borrower Entity", "Account Name", "Do Not Lend", "Current Funding Vehicle", "Loan Amount", "Close Date", "CAF Originator", "Deal Intro Sub-Source", "Referral Source Account", "Referral Source Contact", "Comments AM", "Sold Loan: Sold To", "Sold Loan: Servicing Status", "Type", "Servicer Name", "Stage", "Current Servicer UPB", "Original Loan Maturity Date", "Next Payment Date"] if c in sf_sid.columns]
        sf_pick = sf_sid[sf_keep].drop_duplicates("_sid_key")
        out = out.merge(sf_pick, on="_sid_key", how="left", suffixes=("", "_sid"))
        map_pairs = {
            "Deal Number": "Deal Loan Number_sid",
            "SF Yardi ID": "Yardi ID_sid",
            "Deal Name": "Deal Name_sid",
            "Borrower Entity": "Borrower Entity_sid",
            "Account Name": "Account Name_sid",
            "Do Not Lend (Y/N)": "Do Not Lend_sid",
            "Financing": "Current Funding Vehicle_sid",
            "Originator": "CAF Originator_sid",
            "Deal Intro Sub-Source": "Deal Intro Sub-Source_sid",
            "Referral Source Account": "Referral Source Account_sid",
            "Referral Source Contact": "Referral Source Contact_sid",
            "AM Commentary": "Comments AM_sid",
            "Loan Buyer": "Sold Loan: Sold To_sid",
            "Servicer": "Servicer Name_sid",
        }
        for target, source in map_pairs.items():
            if source in out.columns:
                if target == "Do Not Lend (Y/N)":
                    out[target] = coalesce_keep_nonblank(out.get(target, blank_obj), _yn_from_bool_series(out[source]))
                else:
                    out[target] = coalesce_keep_nonblank(out.get(target, blank_obj), out[source])
        if "Loan Amount_sid" in out.columns:
            out["Loan Amount"] = pd.to_numeric(out.get("Loan Amount", pd.Series([np.nan]*len(out), index=out.index)), errors="coerce").where(pd.to_numeric(out.get("Loan Amount", pd.Series([np.nan]*len(out), index=out.index)), errors="coerce").notna(), pd.to_numeric(out["Loan Amount_sid"], errors="coerce"))
        if "Close Date_sid" in out.columns:
            out["Origination Date"] = pd.to_datetime(out.get("Origination Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").where(pd.to_datetime(out.get("Origination Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").notna(), pd.to_datetime(out["Close Date_sid"], errors="coerce"))
        if "Original Loan Maturity Date_sid" in out.columns:
            out["Maturity Date"] = pd.to_datetime(out.get("Maturity Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").where(pd.to_datetime(out.get("Maturity Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").notna(), pd.to_datetime(out["Original Loan Maturity Date_sid"], errors="coerce"))
        if "Next Payment Date_sid" in out.columns:
            out["Next Payment Date"] = pd.to_datetime(out.get("Next Payment Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").where(pd.to_datetime(out.get("Next Payment Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").notna(), pd.to_datetime(out["Next Payment Date_sid"], errors="coerce"))
        if "Current Servicer UPB_sid" in out.columns:
            out[upb_col] = pd.to_numeric(out.get(upb_col, pd.Series([np.nan]*len(out), index=out.index)), errors="coerce").where(pd.to_numeric(out.get(upb_col, pd.Series([np.nan]*len(out), index=out.index)), errors="coerce").gt(0), pd.to_numeric(out["Current Servicer UPB_sid"], errors="coerce"))

    if serv_lookup is not None and not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy().rename(columns={"servicer": "_servicer_file", "upb": "_loan_upb", "next_payment_date": "_serv_next_payment_date", "maturity_date": "_serv_maturity_file", "status": "_serv_status_file"})
        out = out.merge(s[["_sid_key", "_servicer_file", "_loan_upb", "_serv_next_payment_date", "_serv_maturity_file", "_serv_status_file"]], on="_sid_key", how="left")
        out["Servicer"] = coalesce_keep_nonblank(out.get("Servicer", blank_obj), out.get("_servicer_file", blank_obj))
        out[upb_col] = pd.to_numeric(out.get(upb_col, pd.Series([np.nan]*len(out), index=out.index)), errors="coerce").where(pd.to_numeric(out.get(upb_col, pd.Series([np.nan]*len(out), index=out.index)), errors="coerce").gt(0), pd.to_numeric(out.get("_loan_upb", pd.Series([np.nan]*len(out), index=out.index)), errors="coerce"))
        out["Next Payment Date"] = pd.to_datetime(out.get("Next Payment Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").where(pd.to_datetime(out.get("Next Payment Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").notna(), pd.to_datetime(out.get("_serv_next_payment_date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce"))
        out["Maturity Date"] = pd.to_datetime(out.get("Maturity Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").where(pd.to_datetime(out.get("Maturity Date", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce").notna(), pd.to_datetime(out.get("_serv_maturity_file", pd.Series([pd.NaT]*len(out), index=out.index)), errors="coerce"))

    out["Servicer ID"] = normalize_servicer_id_for_report(out.get("Servicer ID", blank_obj), out.get("Servicer", blank_obj))
    out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out.get("Do Not Lend (Y/N)", pd.Series([pd.NA] * len(out), index=out.index)))
    out["CPP JV"] = coalesce_keep_nonblank(out.get("CPP JV", blank_obj), pd.Series(["N"] * len(out), index=out.index))
    out["Active RM"] = coalesce_keep_nonblank(out.get("Active RM", blank_obj), pd.Series(["N"] * len(out), index=out.index))
    out["Special Loans List (Y/N)"] = coalesce_keep_nonblank(out.get("Special Loans List (Y/N)", blank_obj), pd.Series(["N"] * len(out), index=out.index))

    if asset_filter_provided:
        out = out[out["_deal_key"].isin(asset_deal_keys | always_keep_keys)].copy()

    term_keep_mask = _term_population_keep_mask(
        stage_series=out.get("_stage_raw", pd.Series([pd.NA] * len(out), index=out.index)),
        current_upb_series=out.get("_sf_current_servicer_upb_raw", pd.Series([np.nan] * len(out), index=out.index)),
        sold_servicing_status_series=out.get("_sold_servicing_status_raw", pd.Series([pd.NA] * len(out), index=out.index)),
        deal_key_series=out.get("_deal_key", pd.Series([pd.NA] * len(out), index=out.index)),
        prev_sold_retained_keys=prev_sold_retained_keys,
        always_keep_keys=always_keep_keys,
        reo_date_series=out.get("REO Date", pd.Series([pd.NaT] * len(out), index=out.index)),
    )
    out = out.loc[term_keep_mask].copy()
    out = out[out["_deal_key"].notna()].copy()
    return downcast_numeric_frame(out.drop(columns=[c for c in out.columns if c.startswith("_") and c not in {"_deal_key", "_sid_key"}], errors="ignore"))

def build_term_asset(sf_term_asset: pd.DataFrame, term_loan: pd.DataFrame, upb_col: str, prev_maps: Optional[dict] = None) -> pd.DataFrame:
    out = pd.DataFrame(index=sf_term_asset.index)

    for col, label in TERM_ASSET_FROM_TERM_ASSET_REPORT.items():
        out[col] = sf_term_asset[label] if label in sf_term_asset.columns else pd.NA

    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out))))
    out["_asset_key"] = norm_id_series(out.get("Asset ID", pd.Series([None] * len(out))))
    out["CPP JV"] = pd.NA
    out["Special (Y/N)"] = _yn_from_bool_series(sf_term_asset.get("Property Special Asset", pd.Series([pd.NA] * len(out), index=out.index)))

    tl = term_loan.copy()
    tl["_deal_key"] = norm_id_series(tl.get("Deal Number", pd.Series([None] * len(tl))))

    valid_deals = set(tl["_deal_key"].dropna().tolist())
    out = out[out["_deal_key"].isin(valid_deals) & out["_asset_key"].notna()].copy()

    if "CPP JV" in tl.columns:
        tl_cpp = tl[["_deal_key", "CPP JV"]].drop_duplicates("_deal_key")
        out = out.merge(tl_cpp, on="_deal_key", how="left", suffixes=("", "_loan"))
        out["CPP JV"] = coalesce_keep_nonblank(out.get("CPP JV_loan", pd.Series([pd.NA] * len(out), index=out.index)), out["CPP JV"])
        out = out.drop(columns=["CPP JV_loan"], errors="ignore")

    if "Special Loans List (Y/N)" in tl.columns:
        tl_special = tl[["_deal_key", "Special Loans List (Y/N)"]].drop_duplicates("_deal_key")
        out = out.merge(tl_special, on="_deal_key", how="left")
        out["Special (Y/N)"] = coalesce_keep_nonblank(out.get("Special (Y/N)", pd.Series([pd.NA] * len(out), index=out.index)), out.get("Special Loans List (Y/N)", pd.Series([pd.NA] * len(out), index=out.index)))
        out = out.drop(columns=["Special Loans List (Y/N)"], errors="ignore")

    if upb_col in tl.columns:
        tl_upb = tl[["_deal_key", upb_col]].drop_duplicates("_deal_key")
        out = out.merge(tl_upb, on="_deal_key", how="left")

        ala = pd.to_numeric(out.get("Property ALA", np.nan), errors="coerce")
        ala_sum = ala.groupby(out["_deal_key"]).transform("sum")
        out[upb_col] = np.where(ala_sum > 0, out[upb_col] * (ala / ala_sum), out[upb_col])

    if prev_maps and "term_asset_manual" in prev_maps:
        prev = prev_maps["term_asset_manual"].copy()
        keep = ["_deal_key", "_asset_key"] + [c for c in ["Special (Y/N)"] if c in prev.columns]
        prev = prev[keep].drop_duplicates(["_deal_key", "_asset_key"])
        out = out.merge(prev, on=["_deal_key", "_asset_key"], how="left", suffixes=("", "_prev"))
        if "Special (Y/N)_prev" in out.columns:
            out["Special (Y/N)"] = coalesce_keep_nonblank(out.get("Special (Y/N)", pd.Series([pd.NA] * len(out), index=out.index)), out["Special (Y/N)_prev"])
            out = out.drop(columns=["Special (Y/N)_prev"], errors="ignore")

    out["Special (Y/N)"] = coalesce_keep_nonblank(out.get("Special (Y/N)", pd.Series([pd.NA] * len(out), index=out.index)), pd.Series(["N"] * len(out), index=out.index))

    for c in ["CPP JV"]:
        if c in out.columns:
            out[c] = out[c].replace({"": pd.NA})

    meaningful_mask = (
        out["_deal_key"].notna()
        & out["_asset_key"].notna()
        & (
            (~blankish_mask(out.get("Address", pd.Series([pd.NA] * len(out), index=out.index))))
            | pd.to_numeric(out.get("Property ALA", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").fillna(0).gt(0)
            | pd.to_numeric(out.get(upb_col, pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").fillna(0).ne(0)
            | pd.to_numeric(out.get("As-Is Value", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").notna()
        )
    )
    out = out.loc[meaningful_mask].copy()

    return downcast_numeric_frame(out.drop(columns=["_value_dt", "_mod_dt", "_created_dt", "_ala_sort"], errors="ignore"))


def build_bridge_loan(
    bridge_loan_wide: pd.DataFrame,
    bridge_asset: pd.DataFrame,
    bridge_property_rollup: pd.DataFrame,
    serv_lookup: pd.DataFrame,
    upb_col: str,
    prev_maps: dict,
    template_maps: dict,
    npl_maps: Optional[dict] = None,
) -> pd.DataFrame:
    out = bridge_loan_wide.copy()
    if out.empty:
        return out

    blank_obj = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")
    out["_deal_key"] = norm_id_series(out.get("Deal Number", pd.Series([None] * len(out), index=out.index)))
    out["Servicer ID"] = coalesce_keep_nonblank(out.get("Servicer ID", blank_obj), blank_obj)

    if "Do Not Lend" in out.columns:
        out["Do Not Lend (Y/N)"] = _yn_from_bool_series(out["Do Not Lend"])
        out = out.drop(columns=["Do Not Lend"], errors="ignore")
    elif "Do Not Lend (Y/N)" not in out.columns:
        out["Do Not Lend (Y/N)"] = "N"

    seg_guess = out.apply(
        lambda r: derive_bridge_segment(r.get("Deal Number"), r.get("Financing"), r.get("Loan Buyer"), template_maps),
        axis=1,
    )
    strat_guess = out.get("Project Strategy", pd.Series([pd.NA] * len(out), index=out.index)).map(
        lambda x: strategy_grouping_from_project_strategy(x, template_maps.get("strategy_map", {}))
    )
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
    out["Segment"] = coalesce_keep_nonblank(out.get("Segment", blank_obj), seg_guess)
    out["Strategy Grouping"] = coalesce_keep_nonblank(out.get("Strategy Grouping", blank_obj), strat_guess)
    out["Portfolio"] = coalesce_keep_nonblank(out.get("Portfolio", blank_obj), port_guess)

    sold_stage_series = out.get("Loan Stage", pd.Series([pd.NA] * len(out), index=out.index)).astype("string").str.strip()
    out["Financing"] = pd.Series(out.get("Financing", blank_obj), index=out.index, dtype="object")
    out["Financing"] = out["Financing"].mask(blankish_mask(out["Financing"]) & sold_stage_series.eq("Sold"), "Sold")

    if bridge_property_rollup is not None and not bridge_property_rollup.empty:
        out = out.merge(bridge_property_rollup, on="_deal_key", how="left")
    else:
        out["Number of Assets"] = np.nan
        out["# of Units"] = np.nan
        out["State(s)"] = pd.NA
        out["Active Asset Count"] = 0
        out["Active Asset UPB"] = np.nan

    if bridge_asset is not None and not bridge_asset.empty:
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

        active_roll = pd.DataFrame(
            {
                "Servicer ID_active": g["Servicer ID"].apply(first_or_various) if "Servicer ID" in ba.columns else pd.Series(dtype="string"),
                "Servicer_active": g["Servicer"].apply(first_or_various) if "Servicer" in ba.columns else pd.Series(dtype="string"),
                "Primary Contact_active": g["Primary Contact"].apply(_first) if "Primary Contact" in ba.columns else pd.Series(dtype="string"),
                "Last Funding Date_active": g["Last Funding Date"].apply(_max_dt) if "Last Funding Date" in ba.columns else pd.NaT,
                "Days Past Due_active": pd.to_numeric(g["_bridge_dpd_num"].max(), errors="coerce") if "_bridge_dpd_num" in ba.columns else pd.Series(dtype="float"),
                "Loan Level Delinquency_active": g["_bridge_dq_bucket"].apply(_worst_bridge_bucket) if "_bridge_dq_bucket" in ba.columns else pd.Series(dtype="string"),
                "Active Funded Amount": pd.to_numeric(g["SF Funded Amount"].sum(min_count=1), errors="coerce") if "SF Funded Amount" in ba.columns else np.nan,
                "Suspense Balance_active": pd.to_numeric(g["Suspense Balance"].sum(min_count=1), errors="coerce") if "Suspense Balance" in ba.columns else np.nan,
                "Most Recent Valuation Date": g["Updated Valuation Date"].apply(_max_dt) if "Updated Valuation Date" in ba.columns else pd.NaT,
                "Most Recent As-Is Value": pd.to_numeric(g["Updated As-Is Value"].sum(min_count=1), errors="coerce") if "Updated As-Is Value" in ba.columns else np.nan,
                "Most Recent ARV": pd.to_numeric(g["Updated ARV"].sum(min_count=1), errors="coerce") if "Updated ARV" in ba.columns else np.nan,
                "Initial Disbursement Funded": pd.to_numeric(g["Initial Disbursement Funded"].sum(min_count=1), errors="coerce") if "Initial Disbursement Funded" in ba.columns else np.nan,
                "Renovation Holdback": pd.to_numeric(g["Renovation Holdback"].sum(min_count=1), errors="coerce") if "Renovation Holdback" in ba.columns else np.nan,
                "Renovation HB Funded": pd.to_numeric(g["Renovation Holdback Funded"].sum(min_count=1), errors="coerce") if "Renovation Holdback Funded" in ba.columns else np.nan,
                "Renovation HB Remaining": pd.to_numeric(g["Renovation Holdback Remaining"].sum(min_count=1), errors="coerce") if "Renovation Holdback Remaining" in ba.columns else np.nan,
                "Interest Allocation": pd.to_numeric(g["Interest Allocation"].sum(min_count=1), errors="coerce") if "Interest Allocation" in ba.columns else np.nan,
                "Interest Allocation Funded": pd.to_numeric(g["Interest Allocation Funded"].sum(min_count=1), errors="coerce") if "Interest Allocation Funded" in ba.columns else np.nan,
                "3/31 NPL": g["3/31 NPL (Y/N)"].apply(_yn_any) if "3/31 NPL (Y/N)" in ba.columns else "N",
                "Needs NPL Value": g["Needs NPL Value"].apply(_yn_any) if "Needs NPL Value" in ba.columns else "N",
                "Special Focus (Y/N)": g["Special Flag"].apply(_yn_any) if "Special Flag" in ba.columns else "N",
                "Asset Manager 1": g["Asset Manager 1"].apply(_first) if "Asset Manager 1" in ba.columns else pd.Series(dtype="string"),
                "AM 1 Assigned Date": g["AM 1 Assigned Date"].apply(_first) if "AM 1 Assigned Date" in ba.columns else pd.NaT,
                "Asset Manager 2": g["Asset Manager 2"].apply(_first) if "Asset Manager 2" in ba.columns else pd.Series(dtype="string"),
                "AM 2 Assigned Date": g["AM 2 Assigned Date"].apply(_first) if "AM 2 Assigned Date" in ba.columns else pd.NaT,
                "Construction Mgr.": g["Construction Mgr."].apply(_first) if "Construction Mgr." in ba.columns else pd.Series(dtype="string"),
                "CM Assigned Date": g["CM Assigned Date"].apply(_first) if "CM Assigned Date" in ba.columns else pd.NaT,
                "Active RM": g["Active RM"].apply(_first) if "Active RM" in ba.columns else pd.Series(dtype="string"),
                "AM Commentary": g["Comments AM"].apply(_first) if "Comments AM" in ba.columns else pd.Series(dtype="string"),
            }
        ).reset_index()
        out = out.merge(active_roll, on="_deal_key", how="left")
    else:
        out["3/31 NPL"] = "N"
        out["Needs NPL Value"] = "N"
        out["Special Focus (Y/N)"] = "N"

    out["Primary Contact"] = coalesce_keep_nonblank(out.get("Primary Contact_active", blank_obj), out.get("Primary Contact", blank_obj))
    out["Last Funding Date"] = pd.to_datetime(out.get("Last Funding Date", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").where(
        pd.to_datetime(out.get("Last Funding Date", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").notna(),
        pd.to_datetime(out.get("Last Funding Date_active", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce"),
    )
    out["Servicer ID"] = coalesce_keep_nonblank(out.get("Servicer ID_active", blank_obj), out.get("Servicer ID", blank_obj))
    out["Servicer"] = coalesce_keep_nonblank(out.get("Servicer_active", blank_obj), out.get("Servicer", blank_obj))
    out["Number of Assets"] = pd.to_numeric(out.get("Number of Assets SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").where(pd.to_numeric(out.get("Number of Assets SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").notna(), pd.to_numeric(out.get("Number of Assets", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"))
    out["# of Units"] = pd.to_numeric(out.get("# of Units SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").where(pd.to_numeric(out.get("# of Units SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").notna(), pd.to_numeric(out.get("# of Units", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"))
    out["State(s)"] = coalesce_keep_nonblank(out.get("State(s) SF", blank_obj), out.get("State(s)", blank_obj))
    out["Last Funding Date"] = pd.to_datetime(out.get("Last Funding Date SF", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").where(pd.to_datetime(out.get("Last Funding Date SF", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").notna(), out["Last Funding Date"])
    out["Most Recent Valuation Date"] = pd.to_datetime(out.get("Most Recent Valuation Date SF", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").where(pd.to_datetime(out.get("Most Recent Valuation Date SF", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").notna(), pd.to_datetime(out.get("Most Recent Valuation Date", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce"))
    out["Most Recent As-Is Value"] = pd.to_numeric(out.get("Most Recent As-Is Value SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").where(pd.to_numeric(out.get("Most Recent As-Is Value SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").notna(), pd.to_numeric(out.get("Most Recent As-Is Value", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"))
    out["Active Funded Amount"] = pd.to_numeric(out.get("Active Funded Amount SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").where(pd.to_numeric(out.get("Active Funded Amount SF", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").notna(), pd.to_numeric(out.get("Active Funded Amount", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"))

    out["Active Asset Count"] = pd.to_numeric(out.get("Active Asset Count", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
    out["Active Asset UPB"] = pd.to_numeric(out.get("Active Asset UPB", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
    out["SF Current UPB"] = pd.to_numeric(out.get("SF Current UPB", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
    out["Suspense Balance"] = pd.to_numeric(out.get("Suspense Balance_active", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")

    out["_sid_key"] = id_key_no_leading_zeros(out.get("Servicer ID", pd.Series([None] * len(out), index=out.index)))
    if not serv_lookup.empty and "_sid_key" in serv_lookup.columns:
        s = serv_lookup.dropna(subset=["_sid_key"]).copy().rename(
            columns={
                "servicer": "_servicer_file",
                "upb": "_loan_upb",
                "suspense": "_loan_suspense",
                "next_payment_date": "_serv_next_payment_date",
                "maturity_date": "_servicer_maturity_file",
                "status": "_servicer_status_file",
            }
        )
        out = out.merge(
            s[["_sid_key", "_servicer_file", "_loan_upb", "_loan_suspense", "_serv_next_payment_date", "_servicer_maturity_file", "_servicer_status_file"]],
            on="_sid_key",
            how="left",
        )

    if "bridge_loan_upb" in prev_maps:
        prev_upb = prev_maps["bridge_loan_upb"].copy()
        out = out.merge(prev_upb, on="_deal_key", how="left")
    else:
        out["_prev_upb"] = np.nan

    stage_series = out.get("Loan Stage", pd.Series([pd.NA] * len(out), index=out.index)).astype("string").str.strip()
    final_upb = pd.to_numeric(out.get("_loan_upb", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
    final_upb = final_upb.where(final_upb.notna(), out["Active Asset UPB"])
    final_upb = final_upb.where(final_upb.notna(), out["SF Current UPB"])

    late_stage_mask = stage_series.isin(EXPIRED_OR_MATURED_STAGES)
    prev_upb_vals = pd.to_numeric(out.get("_prev_upb", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
    final_upb = final_upb.where(~(late_stage_mask & (final_upb.isna() | final_upb.le(0))), prev_upb_vals)
    out[upb_col] = pd.to_numeric(final_upb, errors="coerce")

    out["Suspense Balance"] = pd.to_numeric(out.get("_loan_suspense", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").where(
        pd.to_numeric(out.get("_loan_suspense", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").notna(),
        pd.to_numeric(out.get("Suspense Balance", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"),
    )
    out["Suspense Balance"] = pd.to_numeric(out["Suspense Balance"], errors="coerce").where(
        pd.to_numeric(out["Suspense Balance"], errors="coerce").notna(),
        pd.to_numeric(out.get("SF Suspense Balance", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"),
    )

    out["Next Payment Date"] = pd.to_datetime(out.get("_serv_next_payment_date", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").where(
        pd.to_datetime(out.get("_serv_next_payment_date", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce").notna(),
        pd.to_datetime(out.get("Next Payment Date", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce"),
    )
    out["Next Advance Maturity Date"] = pd.to_datetime(out.get("_servicer_maturity_file", pd.Series([pd.NaT] * len(out), index=out.index)), errors="coerce")
    out["Servicer"] = coalesce_keep_nonblank(out.get("_servicer_file", blank_obj), out.get("Servicer", blank_obj))

    loan_status_bucket = pd.Series(
        [
            normalize_bridge_servicer_status(raw_status, npd, run_dt, loan_stage, None, None)
            for raw_status, npd, loan_stage in zip(
                out.get("_servicer_status_file", blank_obj),
                out.get("Next Payment Date", pd.Series([pd.NaT] * len(out), index=out.index)),
                stage_series,
            )
        ],
        index=out.index,
        dtype="object",
    )
    out["Loan Level Delinquency"] = coalesce_keep_nonblank(out.get("Loan Level Delinquency_active", blank_obj), loan_status_bucket)
    out["Days Past Due"] = pd.to_numeric(out.get("Days Past Due_active", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
    derived_days = pd.Series(
        [
            min(_guess_days_past_due(npd, run_dt), 29.0) if has_any_value(bucket) and clean_text(bucket).upper() == "CURRENT" else _guess_days_from_bridge_bucket(bucket)
            for bucket, npd in zip(out["Loan Level Delinquency"], out.get("Next Payment Date", pd.Series([pd.NaT] * len(out), index=out.index)))
        ],
        index=out.index,
        dtype="float64",
    )
    out["Days Past Due"] = out["Days Past Due"].where(out["Days Past Due"].notna(), derived_days)

    if npl_maps and not npl_maps.get("loan_flags", pd.DataFrame()).empty:
        loan_flags = npl_maps["loan_flags"].copy().drop_duplicates("_deal_key")
        out = out.merge(loan_flags, on="_deal_key", how="left", suffixes=("", "_npl"))
        if "NPL Flag_npl" in out.columns:
            out["3/31 NPL"] = coalesce_keep_nonblank(out.get("3/31 NPL", blank_obj), out["NPL Flag_npl"])
            out = out.drop(columns=["NPL Flag_npl"], errors="ignore")
        for c in ["Needs NPL Value", "Special Focus (Y/N)"]:
            if f"{c}_npl" in out.columns:
                out[c] = coalesce_keep_nonblank(out.get(c, blank_obj), out[f"{c}_npl"])
                out = out.drop(columns=[f"{c}_npl"], errors="ignore")

    if "bridge_loan_manual" in prev_maps and not out.empty:
        man = prev_maps["bridge_loan_manual"].copy()
        out = out.merge(man, on="_deal_key", how="left", suffixes=("", "_prev"))
        for c in [
            "Portfolio", "Segment", "Strategy Grouping", "Loan Level Delinquency", "Special Focus (Y/N)",
            "AM Commentary", "3/31 NPL", "Needs NPL Value", "Active RM",
            "Asset Manager 1", "AM 1 Assigned Date", "Asset Manager 2", "AM 2 Assigned Date",
            "Construction Mgr.", "CM Assigned Date",
        ]:
            if f"{c}_prev" in out.columns:
                out[c] = coalesce_keep_nonblank(out.get(c, blank_obj), out[f"{c}_prev"])
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    out["Servicer ID"] = normalize_servicer_id_for_report(out.get("Servicer ID", blank_obj), out.get("Servicer", blank_obj))
    out["Active RM"] = coalesce_keep_nonblank(out.get("Active RM", blank_obj), pd.Series(["N"] * len(out), index=out.index))
    out["Special Focus (Y/N)"] = coalesce_keep_nonblank(out.get("Special Focus (Y/N)", blank_obj), pd.Series(["N"] * len(out), index=out.index))
    out["3/31 NPL"] = coalesce_keep_nonblank(out.get("3/31 NPL", blank_obj), pd.Series(["N"] * len(out), index=out.index))
    out["Needs NPL Value"] = coalesce_keep_nonblank(out.get("Needs NPL Value", blank_obj), pd.Series(["N"] * len(out), index=out.index))

    out["Number of Assets"] = pd.to_numeric(out.get("Number of Assets", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")
    out["# of Units"] = pd.to_numeric(out.get("# of Units", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce")

    bridge_asset_deal_keys = set()
    if bridge_asset is not None and not bridge_asset.empty and "_deal_key" in bridge_asset.columns:
        bridge_asset_deal_keys = set(pd.Series(bridge_asset["_deal_key"], copy=False).dropna().astype(str).tolist())
        out = out[out["_deal_key"].isin(bridge_asset_deal_keys)].copy()
        blank_obj = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")

    current_upb = pd.to_numeric(out.get(upb_col, pd.Series([np.nan] * len(out), index=out.index)), errors="coerce").fillna(0)
    active_asset_count = pd.to_numeric(out.get("Active Asset Count", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
    is_closed_won = stage_series.eq("Closed Won")
    is_sold = stage_series.eq("Sold")
    is_reo = stage_series.isin(REO_FAMILY_STAGES)
    is_late_stage = stage_series.isin(EXPIRED_OR_MATURED_STAGES)

    keep_mask = is_closed_won | is_reo | is_sold | (is_late_stage & (current_upb.gt(0) | active_asset_count.gt(0)))
    out = out.loc[keep_mask].copy()
    out = out[out["_deal_key"].notna()].copy()

    out = _fill_text_defaults(
        out,
        [
            "Loan Buyer", "Servicer ID", "Servicer", "Primary Contact", "Loan Level Delinquency",
            "Asset Manager 1", "Asset Manager 2", "Construction Mgr.", "Deal Intro Sub-Source",
            "Referral Source Account", "Referral Source Contact", "AM Commentary",
        ],
    )

    drop_cols = [
        c for c in out.columns
        if c.startswith("_") or c.endswith("_active") or c.startswith("SF ") or c.startswith("Opportunity Servicer")
    ]
    return downcast_numeric_frame(out.drop(columns=drop_cols, errors="ignore"))



def _set_scaffold_cell(ws, row_idx: int, col_idx: int, value):
    cell = ws.cell(row_idx, col_idx)
    cell.value = value
    if isinstance(value, (date, datetime)):
        cell.number_format = DATE_NUMBER_FORMAT


def refresh_summary_labels(wb, run_dt: date, upb_header: str):
    if "Summary" not in wb.sheetnames:
        return
    ws = wb["Summary"]
    current_md = f"{run_dt.month}/{run_dt.day}"
    q_end = quarter_end_for_run(run_dt)
    q_end_md = f"{q_end.month}/{q_end.day}"

    for row in ws.iter_rows():
        for cell in row:
            if not isinstance(cell.value, str):
                continue
            txt = cell.value
            new_txt = txt
            if "UPB" in txt.upper():
                new_txt = re.sub(r"\b\d{1,2}/\d{1,2}\s*UPB\b", upb_header, new_txt, flags=re.I)
            if "NPL" in txt.upper():
                new_txt = re.sub(r"\b\d{1,2}/\d{1,2}\b", q_end_md, new_txt)
            elif any(k in txt.upper() for k in ["DQ", "DELINQ", "PAST DUE", "CURRENT"]):
                new_txt = re.sub(r"\b\d{1,2}/\d{1,2}\b", current_md, new_txt)
            if new_txt != txt:
                cell.value = new_txt


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

    refresh_summary_labels(wb, run_dt, upb_header)


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
    keep_last = (start_row - 1) if row_count <= 0 else (start_row + row_count - 1)
    if ws.max_row > keep_last:
        ws.delete_rows(keep_last + 1, ws.max_row - keep_last)


def _drop_fully_blank_dataframe_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            mask = mask | pd.to_numeric(series, errors="coerce").notna()
        elif pd.api.types.is_datetime64_any_dtype(series):
            mask = mask | pd.to_datetime(series, errors="coerce").notna()
        else:
            mask = mask | (~blankish_mask(series))
    return df.loc[mask].copy()


def _drop_rows_missing_required_keys(sheet_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()

    key_map = {
        "Bridge Asset": ["Deal Number", "Asset ID"],
        "Bridge Loan": ["Deal Number"],
        "Term Loan": ["Deal Number"],
        "Term Asset": ["Deal Number", "Asset ID"],
    }
    required = [c for c in key_map.get(sheet_name, []) if c in df.columns]
    if not required:
        return df.copy()

    mask = pd.Series(True, index=df.index)
    for col in required:
        mask = mask & (~blankish_mask(df[col]))
    return df.loc[mask].copy()


def _reset_sheet_autofilter(ws, header_tuples: List[Tuple[int, str]], row_count: int, header_row: int = 4, start_row: int = 5):
    if not header_tuples:
        return
    first_col = min(col_idx for col_idx, _header in header_tuples)
    last_col = max(col_idx for col_idx, _header in header_tuples)
    end_row = header_row if row_count <= 0 else (start_row + row_count - 1)
    ws.auto_filter.ref = f"{get_column_letter(first_col)}{header_row}:{get_column_letter(last_col)}{end_row}"


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


def _copy_formula_columns_down(ws_formula, formula_seeds: dict, row_count: int, header_tuples: List[Tuple[int, str]], upb_header: str, start_row: int = 5):
    if row_count <= 0:
        return

    header_by_col = {c: h for c, h in header_tuples}
    overrides = DRAFT_FORMULA_OVERRIDES.get(ws_formula.title, {})

    for col_idx in sorted(formula_seeds):
        header = header_by_col.get(col_idx, "")
        override_key = "__UPB__" if header == upb_header else header
        origin_formula = overrides.get(override_key, formula_seeds[col_idx]["formula"])
        origin_row = start_row
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

    df = _drop_fully_blank_dataframe_rows(df)
    df = _drop_rows_missing_required_keys(sheet_name, df)

    ws = wb[sheet_name]
    hdr = header_tuples_from_ws(ws, header_row=4, wb=wb, upb_header=upb_col)
    fcols = formula_col_indices(ws, start_row=5, header_row=4)

    if sheet_name == "Term Asset":
        force_write_headers = {upb_col, "Special (Y/N)"}
        force_write_cols = {col_idx for col_idx, header in hdr if header in force_write_headers}
        fcols = {c for c in fcols if c not in force_write_cols}

    formula_seeds = _capture_formula_seeds(ws, fcols, start_row=5)

    used_cols = _used_output_columns(ws, wb=wb, upb_header=upb_col, header_row=4, start_row=5)
    _clear_sheet_body(ws, used_cols, start_row=5)

    write_df_to_sheet_preserve_formulas(ws, df, hdr, fcols, upb_col, start_row=5)
    _copy_formula_columns_down(ws, formula_seeds, row_count=len(df), header_tuples=hdr, upb_header=upb_col, start_row=5)
    _refresh_subtotal_formula(ws, row_count=len(df), subtotal_row=3, start_row=5)
    _trim_sheet_body_rows(ws, row_count=len(df), start_row=5)
    _reset_sheet_autofilter(ws, hdr, row_count=len(df), header_row=4, start_row=5)


def _normalize_sheet_key_value(header: str, value) -> str:
    txt = clean_text(value)
    if not txt:
        return ""
    txt = re.sub(r"\.0$", "", txt)
    if header == "Servicer ID":
        txt = re.sub(r"[^0-9A-Za-z]", "", txt).lstrip("0")
    elif header in {"Deal Number", "Asset ID"}:
        txt = re.sub(r"[^0-9A-Za-z]", "", txt)
    return txt



def _sheet_header_index(wb, ws, upb_header: str) -> Dict[str, int]:
    return {header: col_idx for col_idx, header in header_tuples_from_ws(ws, header_row=4, wb=wb, upb_header=upb_header)}



def _normalize_sheet_key_variants(header: str, value) -> List[str]:
    primary = _normalize_sheet_key_value(header, value)
    if not primary:
        return []
    out = [primary]
    if header == "Deal Number":
        for raw in deal_lookup_keys(value):
            normed = _normalize_sheet_key_value(header, raw)
            if normed and normed not in out:
                out.append(normed)
    return out


def _build_sheet_row_lookup(
    wb,
    ws,
    key_headers: Sequence[str],
    upb_header: str,
    include_key_variants: bool = False,
) -> Tuple[Dict[Tuple[str, ...], int], Dict[str, int]]:
    header_idx = _sheet_header_index(wb, ws, upb_header)
    if not all(h in header_idx for h in key_headers):
        return {}, header_idx

    out: Dict[Tuple[str, ...], int] = {}
    for r in range(5, ws.max_row + 1):
        parts: List[List[str]] = []
        valid = True
        for h in key_headers:
            raw = ws.cell(r, header_idx[h]).value
            vals = _normalize_sheet_key_variants(h, raw) if include_key_variants else [_normalize_sheet_key_value(h, raw)]
            vals = [v for v in vals if v]
            if not vals:
                valid = False
                break
            parts.append(vals)
        if not valid:
            continue
        if include_key_variants and len(parts) > 1:
            import itertools
            keys_to_add = list(itertools.product(*parts))
        else:
            keys_to_add = [tuple(part[0] for part in parts)]
        for key in keys_to_add:
            out.setdefault(key, r)
    return out, header_idx


def _available_sheet_key_matches(wb, out_ws, base_wb, base_ws, sheet_name: str, upb_header: str):
    for candidate in _backfill_rule_candidates(sheet_name):
        out_rows_try, out_header_try = _build_sheet_row_lookup(wb, out_ws, candidate, upb_header, include_key_variants=False)
        base_rows_try, base_header_try = _build_sheet_row_lookup(base_wb, base_ws, candidate, upb_header, include_key_variants=True)
        if sheet_name == "Term Loan" and candidate == ["Servicer ID"] and (not out_rows_try or not base_rows_try):
            continue
        if out_rows_try and base_rows_try:
            yield list(candidate), out_rows_try, base_rows_try, out_header_try, base_header_try


def repair_workbook_from_baseline(wb, baseline_bytes: Optional[bytes], upb_header: str, sheet_names: Optional[Sequence[str]] = None) -> List[dict]:
    if not baseline_bytes:
        return []

    summary: List[dict] = []
    base_wb = None
    try:
        base_wb = load_workbook(BytesIO(baseline_bytes), data_only=False, keep_links=True)
        targets = list(sheet_names) if sheet_names else list(SHEET_BASELINE_KEY_CANDIDATES.keys())

        for sheet_name in targets:
            if sheet_name not in wb.sheetnames or sheet_name not in base_wb.sheetnames:
                continue

            out_ws = wb[sheet_name]
            base_ws = base_wb[sheet_name]
            formula_cols = formula_col_indices(out_ws, start_row=5, header_row=4)

            fills = 0
            matched_rows: Set[int] = set()
            used_keys: List[str] = []
            any_keys = False

            for candidate, out_rows, base_rows, out_header_idx, base_header_idx in _available_sheet_key_matches(
                wb, out_ws, base_wb, base_ws, sheet_name, upb_header
            ):
                any_keys = True
                used_keys.append(", ".join(candidate))
                common_headers = [
                    h for h in out_header_idx
                    if h in base_header_idx
                    and h not in candidate
                    and not UPB_HEADER_RE.search(str(h))
                ]
                for key, out_row in out_rows.items():
                    base_row = base_rows.get(key)
                    if not base_row:
                        continue
                    matched_rows.add(out_row)
                    for header in common_headers:
                        out_col = out_header_idx[header]
                        if out_col in formula_cols:
                            continue
                        out_cell = out_ws.cell(out_row, out_col)
                        base_cell = base_ws.cell(base_row, base_header_idx[header])
                        if clean_text(out_cell.value) == "" and clean_text(base_cell.value) != "":
                            out_cell.value = base_cell.value
                            fills += 1

            if not any_keys:
                summary.append({"sheet_name": sheet_name, "status": "skipped_no_sheet_keys", "keys": "", "fills": 0, "matched_rows": 0})
                continue

            summary.append({
                "sheet_name": sheet_name,
                "status": "repaired" if fills else "no_fills_needed",
                "keys": " | ".join(used_keys),
                "fills": fills,
                "matched_rows": len(matched_rows),
            })
    finally:
        try:
            if base_wb is not None:
                base_wb.close()
        except Exception:
            pass

    return summary



EMBEDDED_AUDIT_NUMERIC_HEADERS = {
    "Square Feet", "# of Units", "Year Built", "Property Count", "Loan Amount",
    "Initial Disbursement Funded", "Renovation Holdback", "Renovation Holdback Funded",
    "Renovation Holdback Remaining", "Renovation HB Funded", "Renovation HB Remaining",
    "Interest Allocation", "Interest Allocation Funded", "Loan Commitment", "Active Funded Amount",
    "Suspense Balance", "Remaining Commitment", "Origination As-Is Value", "Origination ARV",
    "Updated As-Is Value", "Updated ARV", "Most Recent As-Is Value", "Most Recent ARV",
    "Property ALA", "As-Is Value", "Needs NPL Value", "Days Past Due",
}

EMBEDDED_AUDIT_PROTECTED_HEADERS = {
    "Bridge Asset": {
        "Deal Number", "Servicer ID", "SF Yardi ID", "Asset ID", "Deal Name", "Address",
        "City", "State", "Zip", "County", "CBSA", "APN", "# of Units", "Year Built",
        "Square Feet", "Origination Date", "Origination Value Dt", "Origination As-Is Value",
        "Origination ARV", "Most Recent Appraisal Order Date", "Updated Valuation Date",
        "Updated As-Is Value", "Updated ARV", "Initial Disbursement Funded", "Renovation Holdback",
        "Interest Allocation",
    },
    "Bridge Loan": {
        "Deal Number", "Servicer ID", "SF Yardi ID", "Loan Commitment", "Active Funded Amount",
        "Initial Disbursement Funded", "Renovation Holdback", "Interest Allocation",
        "Most Recent As-Is Value", "Most Recent ARV",
    },
    "Term Loan": {
        "Deal Number", "Servicer ID", "SF Yardi ID", "Loan Amount", "Origination Date", "Maturity Date",
    },
    "Term Asset": {
        "Deal Number", "Asset ID", "Address", "City", "State", "Zip", "Value Date",
        "Property ALA", "As-Is Value",
    },
}

EMBEDDED_AUDIT_HEADER_FONT = Font(name="Aptos Narrow", size=11, bold=True)
EMBEDDED_AUDIT_BODY_FONT = Font(name="Aptos Narrow", size=11)
EMBEDDED_AUDIT_WRAP_ALIGNMENT = Alignment(vertical="top", wrap_text=True)
EMBEDDED_AUDIT_ALL_DATE_HEADERS = set().union(*SHEET_DATE_HEADERS.values())


def _embedded_find_upb_header(wb, preferred_sheet: str = "Bridge Asset") -> str:
    sheets = [preferred_sheet] + [name for name in wb.sheetnames if name != preferred_sheet]
    for sheet_name in sheets:
        ws = wb[sheet_name]
        for cell in ws[4]:
            value = cell.value
            if isinstance(value, str) and UPB_HEADER_RE.search(value):
                return value.strip()
    return "UPB"



def _embedded_row_has_any_value(ws, row_idx: int, col_indices: Sequence[int]) -> bool:
    for col_idx in col_indices:
        if clean_text(ws.cell(row_idx, col_idx).value) != "":
            return True
    return False



def _embedded_normalize_compare_value(header: str, value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return ""
        if header in EMBEDDED_AUDIT_ALL_DATE_HEADERS or "date" in header.lower():
            return value.date().isoformat()
        return value.isoformat(sep=" ")
    if isinstance(value, datetime):
        if header in EMBEDDED_AUDIT_ALL_DATE_HEADERS or "date" in header.lower():
            return value.date().isoformat()
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, np.integer, np.floating)):
        try:
            val = float(value)
        except Exception:
            return clean_text(value)
        if np.isnan(val):
            return ""
        if header in EMBEDDED_AUDIT_ALL_DATE_HEADERS:
            return str(value)
        if float(val).is_integer():
            return str(int(val))
        return f"{val:.6f}".rstrip("0").rstrip(".")

    text = clean_text(value)
    if not text:
        return ""

    if header in EMBEDDED_AUDIT_ALL_DATE_HEADERS or "date" in header.lower():
        parsed = pd.to_datetime(text, errors="coerce")
        if not pd.isna(parsed):
            return parsed.date().isoformat()

    if header in EMBEDDED_AUDIT_NUMERIC_HEADERS:
        numeric = text.replace(",", "").replace("$", "")
        if numeric.startswith("(") and numeric.endswith(")"):
            numeric = "-" + numeric[1:-1]
        try:
            val = float(numeric)
            if float(val).is_integer():
                return str(int(val))
            return f"{val:.6f}".rstrip("0").rstrip(".")
        except Exception:
            pass

    return re.sub(r"\s+", " ", text).strip().upper()



def _embedded_build_row_lookup(
    wb,
    ws,
    key_headers: Sequence[str],
    upb_header: str,
    include_key_variants: bool = False,
) -> Tuple[Dict[Tuple[str, ...], int], Dict[str, int], int, int, List[dict]]:
    header_idx = _sheet_header_index(wb, ws, upb_header)
    if not all(h in header_idx for h in key_headers):
        return {}, header_idx, 0, 0, []

    rows: Dict[Tuple[str, ...], List[int]] = {}
    blank_key_rows = 0
    data_rows = 0
    duplicates: List[dict] = []
    scan_cols = list(header_idx.values())

    for row_idx in range(5, ws.max_row + 1):
        if not _embedded_row_has_any_value(ws, row_idx, scan_cols):
            continue
        data_rows += 1

        key_parts: List[List[str]] = []
        valid = True
        for key_header in key_headers:
            raw = ws.cell(row_idx, header_idx[key_header]).value
            vals = _normalize_sheet_key_variants(key_header, raw) if include_key_variants else [_normalize_sheet_key_value(key_header, raw)]
            vals = [v for v in vals if v]
            if not vals:
                blank_key_rows += 1
                valid = False
                break
            key_parts.append(vals)
        if not valid:
            continue

        if include_key_variants and len(key_parts) > 1:
            import itertools
            keys_to_add = list(itertools.product(*key_parts))
        else:
            keys_to_add = [tuple(part[0] for part in key_parts)]

        for key in keys_to_add:
            rows.setdefault(key, []).append(row_idx)
            if len(rows[key]) > 1:
                duplicates.append({
                    "issue_type": "duplicate_key",
                    "sheet_name": ws.title,
                    "row": row_idx,
                    "key": " | ".join(key),
                    "header": ", ".join(key_headers),
                    "built_value": "duplicate key row",
                    "baseline_value": "",
                    "detail": f"Duplicate key for {', '.join(key_headers)}",
                    "suspected_source_header": "",
                })

    chosen = {key: row_list[-1] for key, row_list in rows.items()}
    return chosen, header_idx, data_rows, blank_key_rows, duplicates



def _embedded_choose_keys(sheet_name: str, out_wb, out_ws, base_wb, base_ws, upb_header: str):
    for candidate in _backfill_rule_candidates(sheet_name):
        out_rows, out_headers, out_data_rows, out_blank_keys, dupes = _embedded_build_row_lookup(
            out_wb, out_ws, candidate, upb_header, include_key_variants=False
        )
        base_rows, base_headers, _base_data_rows, _base_blank_keys, _base_dupes = _embedded_build_row_lookup(
            base_wb, base_ws, candidate, upb_header, include_key_variants=True
        )
        if sheet_name == "Term Loan" and candidate == ["Servicer ID"] and (not out_rows or not base_rows):
            continue
        if out_rows or base_rows:
            return list(candidate), out_rows, base_rows, out_headers, base_headers, out_data_rows, out_blank_keys, dupes
    return [], {}, {}, {}, {}, 0, 0, []



def _embedded_choose_structural_keys(sheet_name: str, wb, ws, upb_header: str):
    for candidate in _backfill_rule_candidates(sheet_name):
        rows, headers, data_rows, blank_keys, dupes = _embedded_build_row_lookup(
            wb, ws, candidate, upb_header, include_key_variants=False
        )
        if sheet_name == "Term Loan" and candidate == ["Servicer ID"] and not rows:
            continue
        if headers:
            return list(candidate), rows, headers, data_rows, blank_keys, dupes
    return [], {}, {}, 0, 0, []



def _embedded_possible_shift_header(sheet_name: str, header: str, base_values: Dict[str, str], built_values: Dict[str, str]) -> str:
    protected = EMBEDDED_AUDIT_PROTECTED_HEADERS.get(sheet_name, set())
    if header not in protected:
        return ""
    target = base_values.get(header, "")
    if not target:
        return ""
    for other_header in protected:
        if other_header == header:
            continue
        if built_values.get(other_header, "") == target:
            return other_header
    return ""



def _embedded_audit_openpyxl_workbook(
    wb,
    baseline_bytes: Optional[bytes] = None,
    upb_header: Optional[str] = None,
    sheet_names: Optional[Sequence[str]] = None,
    max_examples_per_sheet: int = 250,
) -> Tuple[List[dict], List[dict]]:
    if upb_header is None:
        upb_header = _embedded_find_upb_header(wb)

    targets = list(sheet_names) if sheet_names else list(SHEET_BASELINE_KEY_CANDIDATES.keys())
    summaries: List[dict] = []
    exceptions: List[dict] = []

    base_wb = load_workbook(BytesIO(baseline_bytes), data_only=False, keep_links=True) if baseline_bytes else None
    try:
        for sheet_name in targets:
            if sheet_name not in wb.sheetnames:
                continue

            out_ws = wb[sheet_name]
            formula_cols = formula_col_indices(out_ws)

            if base_wb is None or sheet_name not in getattr(base_wb, "sheetnames", []):
                key_headers, out_rows, out_headers, out_data_rows, out_blank_keys, dupes = _embedded_choose_structural_keys(
                    sheet_name, wb, out_ws, upb_header
                )
                formula_gaps = 0
                sheet_example_count = 0
                if out_headers:
                    scan_cols = list(out_headers.values())
                    header_by_col = {col_idx: header for header, col_idx in out_headers.items()}
                    for row_idx in range(5, out_ws.max_row + 1):
                        if not _embedded_row_has_any_value(out_ws, row_idx, scan_cols):
                            continue
                        for col_idx in formula_cols:
                            value = out_ws.cell(row_idx, col_idx).value
                            if not (isinstance(value, str) and value.startswith("=")):
                                formula_gaps += 1
                                if sheet_example_count < max_examples_per_sheet:
                                    exceptions.append({
                                        "issue_type": "formula_gap",
                                        "sheet_name": sheet_name,
                                        "row": row_idx,
                                        "key": "",
                                        "header": header_by_col.get(col_idx, ""),
                                        "built_value": clean_text(value),
                                        "baseline_value": "formula expected",
                                        "detail": "Formula column is missing a formula on a populated data row.",
                                        "suspected_source_header": "",
                                    })
                                    sheet_example_count += 1
                for item in dupes[:max(0, max_examples_per_sheet - sheet_example_count)]:
                    exceptions.append(item)
                summaries.append({
                    "sheet_name": sheet_name,
                    "status": "structural_only",
                    "keys": ", ".join(key_headers),
                    "data_rows": out_data_rows,
                    "matched_rows": 0,
                    "built_only_rows": 0,
                    "baseline_only_rows": 0,
                    "duplicate_key_rows": len(dupes),
                    "blank_key_rows": out_blank_keys,
                    "formula_gap_cells": formula_gaps,
                    "critical_fillable_blanks": 0,
                    "review_fillable_blanks": 0,
                    "possible_shift_cells": 0,
                    "protected_headers_checked": len(EMBEDDED_AUDIT_PROTECTED_HEADERS.get(sheet_name, set())),
                    "top_fillable_blank_columns": [],
                })
                continue

            base_ws = base_wb[sheet_name]
            chosen_keys, out_rows, base_rows, out_headers, base_headers, out_data_rows, out_blank_keys, dupes = _embedded_choose_keys(
                sheet_name, wb, out_ws, base_wb, base_ws, upb_header
            )
            if not chosen_keys:
                summaries.append({
                    "sheet_name": sheet_name,
                    "status": "skipped_no_keys",
                    "keys": "",
                    "data_rows": 0,
                    "matched_rows": 0,
                    "built_only_rows": 0,
                    "baseline_only_rows": 0,
                    "duplicate_key_rows": 0,
                    "blank_key_rows": 0,
                    "formula_gap_cells": 0,
                    "critical_fillable_blanks": 0,
                    "review_fillable_blanks": 0,
                    "possible_shift_cells": 0,
                    "protected_headers_checked": 0,
                    "top_fillable_blank_columns": [],
                })
                continue

            built_only = set(out_rows) - set(base_rows)
            baseline_only = set(base_rows) - set(out_rows)
            common_headers = [
                header for header in out_headers
                if header in base_headers and header not in chosen_keys and not UPB_HEADER_RE.search(str(header))
            ]
            protected_headers = [header for header in common_headers if header in EMBEDDED_AUDIT_PROTECTED_HEADERS.get(sheet_name, set())]
            scan_cols = list(out_headers.values())
            header_by_col = {col_idx: header for header, col_idx in out_headers.items()}

            formula_gaps = 0
            sheet_example_count = 0
            for row_idx in range(5, out_ws.max_row + 1):
                if not _embedded_row_has_any_value(out_ws, row_idx, scan_cols):
                    continue
                for col_idx in formula_cols:
                    value = out_ws.cell(row_idx, col_idx).value
                    if not (isinstance(value, str) and value.startswith("=")):
                        formula_gaps += 1
                        if sheet_example_count < max_examples_per_sheet:
                            exceptions.append({
                                "issue_type": "formula_gap",
                                "sheet_name": sheet_name,
                                "row": row_idx,
                                "key": "",
                                "header": header_by_col.get(col_idx, ""),
                                "built_value": clean_text(value),
                                "baseline_value": "formula expected",
                                "detail": "Formula column is missing a formula on a populated data row.",
                                "suspected_source_header": "",
                            })
                            sheet_example_count += 1

            critical_fillable_blanks = 0
            review_fillable_blanks = 0
            possible_shifts = 0
            top_blank_counts: Dict[str, int] = {}

            for key, out_row in out_rows.items():
                base_row = base_rows.get(key)
                if not base_row:
                    continue
                key_text = " | ".join(key)

                built_norm = {
                    header: _embedded_normalize_compare_value(header, out_ws.cell(out_row, out_headers[header]).value)
                    for header in protected_headers
                    if out_headers[header] not in formula_cols
                }
                base_norm = {
                    header: _embedded_normalize_compare_value(header, base_ws.cell(base_row, base_headers[header]).value)
                    for header in protected_headers
                    if out_headers[header] not in formula_cols
                }

                for header in common_headers:
                    if out_headers[header] in formula_cols:
                        continue
                    built_raw = out_ws.cell(out_row, out_headers[header]).value
                    base_raw = base_ws.cell(base_row, base_headers[header]).value
                    built_value = _embedded_normalize_compare_value(header, built_raw)
                    base_value = _embedded_normalize_compare_value(header, base_raw)

                    if not built_value and base_value:
                        top_blank_counts[header] = top_blank_counts.get(header, 0) + 1
                        source_header = _embedded_possible_shift_header(sheet_name, header, base_norm, built_norm)
                        if source_header:
                            possible_shifts += 1
                        if header in EMBEDDED_AUDIT_PROTECTED_HEADERS.get(sheet_name, set()):
                            critical_fillable_blanks += 1
                            issue_type = "critical_blank"
                        else:
                            review_fillable_blanks += 1
                            issue_type = "review_blank"
                        if sheet_example_count < max_examples_per_sheet:
                            exceptions.append({
                                "issue_type": issue_type,
                                "sheet_name": sheet_name,
                                "row": out_row,
                                "key": key_text,
                                "header": header,
                                "built_value": clean_text(built_raw),
                                "baseline_value": clean_text(base_raw),
                                "detail": "Built workbook is blank where baseline has a value.",
                                "suspected_source_header": source_header,
                            })
                            sheet_example_count += 1
                    elif header in EMBEDDED_AUDIT_PROTECTED_HEADERS.get(sheet_name, set()) and base_value and built_value and base_value != built_value:
                        source_header = _embedded_possible_shift_header(sheet_name, header, base_norm, built_norm)
                        if source_header:
                            possible_shifts += 1
                            if sheet_example_count < max_examples_per_sheet:
                                exceptions.append({
                                    "issue_type": "possible_shift",
                                    "sheet_name": sheet_name,
                                    "row": out_row,
                                    "key": key_text,
                                    "header": header,
                                    "built_value": clean_text(built_raw),
                                    "baseline_value": clean_text(base_raw),
                                    "detail": "Baseline value appears to be present in another protected column on the same row.",
                                    "suspected_source_header": source_header,
                                })
                                sheet_example_count += 1

            for item in dupes[:max(0, max_examples_per_sheet - sheet_example_count)]:
                exceptions.append(item)

            status = "pass"
            if formula_gaps or critical_fillable_blanks or possible_shifts or dupes or out_blank_keys:
                status = "fail"
            elif review_fillable_blanks or built_only or baseline_only:
                status = "review"

            summaries.append({
                "sheet_name": sheet_name,
                "status": status,
                "keys": ", ".join(chosen_keys),
                "data_rows": out_data_rows,
                "matched_rows": len(set(out_rows) & set(base_rows)),
                "built_only_rows": len(built_only),
                "baseline_only_rows": len(baseline_only),
                "duplicate_key_rows": len(dupes),
                "blank_key_rows": out_blank_keys,
                "formula_gap_cells": formula_gaps,
                "critical_fillable_blanks": critical_fillable_blanks,
                "review_fillable_blanks": review_fillable_blanks,
                "possible_shift_cells": possible_shifts,
                "protected_headers_checked": len(protected_headers),
                "top_fillable_blank_columns": [
                    {"column": header, "count": count}
                    for header, count in sorted(top_blank_counts.items(), key=lambda item: (-item[1], item[0]))[:15]
                ],
            })
    finally:
        if base_wb is not None:
            try:
                base_wb.close()
            except Exception:
                pass

    return summaries, exceptions



def _embedded_audit_col_letter(index: int) -> str:
    out = ""
    n = index
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out



def _embedded_auto_width(ws, widths: Dict[int, int], max_width: int = 50):
    for col_idx, width in widths.items():
        ws.column_dimensions[_embedded_audit_col_letter(col_idx)].width = min(max_width, max(10, width + 2))



def _embedded_write_audit_sheets(wb, summary_rows: List[dict], exception_rows: List[dict]) -> None:
    for name in ["QA Summary", "QA Exceptions"]:
        if name in wb.sheetnames:
            del wb[name]

    ws = wb.create_sheet("QA Summary")
    ws.freeze_panes = "A2"
    summary_headers = [
        "sheet_name", "status", "keys", "data_rows", "matched_rows", "built_only_rows",
        "baseline_only_rows", "duplicate_key_rows", "blank_key_rows", "formula_gap_cells",
        "critical_fillable_blanks", "review_fillable_blanks", "possible_shift_cells",
        "protected_headers_checked", "top_fillable_blank_columns",
    ]
    widths = {i + 1: len(h) for i, h in enumerate(summary_headers)}
    for col_idx, header in enumerate(summary_headers, start=1):
        cell = ws.cell(1, col_idx, header)
        cell.font = copy(EMBEDDED_AUDIT_HEADER_FONT)
        cell.alignment = copy(EMBEDDED_AUDIT_WRAP_ALIGNMENT)
    for row_idx, item in enumerate(summary_rows, start=2):
        for col_idx, header in enumerate(summary_headers, start=1):
            value = item.get(header, "")
            if header == "top_fillable_blank_columns" and isinstance(value, list):
                value = "; ".join(f"{x.get('column')}: {x.get('count')}" for x in value)
            cell = ws.cell(row_idx, col_idx, value)
            cell.font = copy(EMBEDDED_AUDIT_BODY_FONT)
            cell.alignment = copy(EMBEDDED_AUDIT_WRAP_ALIGNMENT)
            widths[col_idx] = max(widths[col_idx], len(str(value)) if value is not None else 0)
    _embedded_auto_width(ws, widths)

    ex = wb.create_sheet("QA Exceptions")
    ex.freeze_panes = "A2"
    exception_headers = [
        "issue_type", "sheet_name", "row", "key", "header", "built_value",
        "baseline_value", "detail", "suspected_source_header",
    ]
    widths = {i + 1: len(h) for i, h in enumerate(exception_headers)}
    for col_idx, header in enumerate(exception_headers, start=1):
        cell = ex.cell(1, col_idx, header)
        cell.font = copy(EMBEDDED_AUDIT_HEADER_FONT)
        cell.alignment = copy(EMBEDDED_AUDIT_WRAP_ALIGNMENT)
    for row_idx, item in enumerate(exception_rows, start=2):
        for col_idx, header in enumerate(exception_headers, start=1):
            value = item.get(header, "")
            cell = ex.cell(row_idx, col_idx, value)
            cell.font = copy(EMBEDDED_AUDIT_BODY_FONT)
            cell.alignment = copy(EMBEDDED_AUDIT_WRAP_ALIGNMENT)
            widths[col_idx] = max(widths[col_idx], len(str(value)) if value is not None else 0)
    _embedded_auto_width(ex, widths, max_width=60)



def _embedded_audit_diagnostic_lines(summary_rows: List[dict]) -> List[str]:
    lines: List[str] = []
    for item in summary_rows:
        status = str(item.get("status", "")).upper()
        lines.append(
            f"QA {item.get('sheet_name')}: {status} | "
            f"critical blanks={int(item.get('critical_fillable_blanks', 0)):,}, "
            f"review blanks={int(item.get('review_fillable_blanks', 0)):,}, "
            f"possible shifts={int(item.get('possible_shift_cells', 0)):,}, "
            f"formula gaps={int(item.get('formula_gap_cells', 0)):,}, "
            f"duplicate keys={int(item.get('duplicate_key_rows', 0)):,}"
        )
    return lines



def _embedded_workbook_needs_attention(summary_rows: List[dict]) -> bool:
    for item in summary_rows:
        if str(item.get("status", "")).lower() in {"fail", "review"}:
            return True
    return False


if not POSTBUILD_AUDIT_AVAILABLE or audit_openpyxl_workbook is None or write_audit_sheets is None or audit_diagnostic_lines is None or workbook_needs_attention is None:
    audit_openpyxl_workbook = _embedded_audit_openpyxl_workbook
    write_audit_sheets = _embedded_write_audit_sheets
    audit_diagnostic_lines = _embedded_audit_diagnostic_lines
    workbook_needs_attention = _embedded_workbook_needs_attention
    POSTBUILD_AUDIT_AVAILABLE = True
    if POSTBUILD_AUDIT_IMPORT_ERROR:
        POSTBUILD_AUDIT_IMPORT_ERROR = f"{POSTBUILD_AUDIT_IMPORT_ERROR}; embedded fallback activated"
    else:
        POSTBUILD_AUDIT_IMPORT_ERROR = "embedded fallback activated"

def _audit_fillable_blank_totals(audit_summary: Sequence[dict]) -> Tuple[int, int, int]:
    critical = sum(int(item.get("critical_fillable_blanks", 0) or 0) for item in audit_summary)
    review = sum(int(item.get("review_fillable_blanks", 0) or 0) for item in audit_summary)
    possible_shift = sum(int(item.get("possible_shift_cells", 0) or 0) for item in audit_summary)
    return critical, review, possible_shift


def enforce_zero_fillable_blanks(
    wb,
    baseline_bytes: Optional[bytes],
    upb_header: str,
    sheet_names: Optional[Sequence[str]] = None,
    max_rounds: int = ZERO_BLANK_MAX_ROUNDS,
) -> Tuple[bool, List[str], List[dict], List[dict]]:
    diagnostics: List[str] = []
    audit_summary: List[dict] = []
    audit_exceptions: List[dict] = []

    if not baseline_bytes:
        diagnostics.append("Zero-blank enforcement skipped: no prior completed workbook was uploaded.")
        return False, diagnostics, audit_summary, audit_exceptions

    if not POSTBUILD_AUDIT_AVAILABLE or audit_openpyxl_workbook is None:
        diagnostics.append("Zero-blank enforcement skipped: post-build audit helper is not available in this runtime.")
        return False, diagnostics, audit_summary, audit_exceptions

    prev_blank_total: Optional[int] = None
    for round_no in range(1, max_rounds + 1):
        repair_summary = repair_workbook_from_baseline(wb, baseline_bytes, upb_header, sheet_names=sheet_names)
        fills = sum(int(item.get("fills", 0) or 0) for item in repair_summary)
        for item in repair_summary:
            if item.get("fills"):
                diagnostics.append(
                    f"Zero-blank round {round_no}: {item['sheet_name']} filled {int(item['fills']):,} cells using {item.get('keys', 'n/a')}"
                )

        audit_summary, audit_exceptions = audit_openpyxl_workbook(
            wb,
            baseline_bytes=baseline_bytes,
            upb_header=upb_header,
            sheet_names=sheet_names,
        )
        critical, review, possible_shift = _audit_fillable_blank_totals(audit_summary)
        blank_total = critical + review
        diagnostics.append(
            f"Zero-blank round {round_no}: remaining fillable blanks {blank_total:,} (critical {critical:,}, review {review:,}); possible shifts {possible_shift:,}."
        )

        if blank_total == 0:
            return True, diagnostics, audit_summary, audit_exceptions

        if prev_blank_total is not None and blank_total >= prev_blank_total and fills == 0:
            break
        prev_blank_total = blank_total

    return False, diagnostics, audit_summary, audit_exceptions


def mark_workbook_for_recalc(wb):
    try:
        wb.calculation.calcMode = "auto"
        wb.calculation.fullCalcOnLoad = True
        wb.calculation.forceFullCalc = True
    except Exception:
        pass


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
1) Log in to **Salesforce**
2) Upload the **current servicer files** or skip them
3) (Optional) Upload **last week’s / completed Active Loans report** for carry-forward
4) Choose **which sheet to build** or **All**

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
    "uses Midland / FCI / Berkadia as the term active-loan spine when available, "
    "resolves formula-linked UPB headers, fills formulas down, trims extra blank rows, keeps row-level Salesforce Servicer IDs intact, "
    "runs both dataframe-level and workbook-level blank repair against the uploaded known-good report when you provide one, "
    "and writes QA Summary / QA Exceptions tabs after the build when the post-build audit helper is available."
)

if POSTBUILD_AUDIT_AVAILABLE:
    st.caption("Post-build QA audit helper loaded. Completed workbooks will include QA Summary and QA Exceptions tabs.")
else:
    st.warning(f"Post-build QA audit helper was not loaded: {POSTBUILD_AUDIT_IMPORT_ERROR}")

sf_info = render_salesforce_login_gate()
sf_ready = bool(sf_info)
use_sf = True

st.markdown("### Step 2: Upload files")
col_a, col_b = st.columns([1.3, 1.0])
with col_a:
    prev_upload = st.file_uploader(
        "Upload LAST WEEK'S or COMPLETED Active Loans report (.xlsx) for carry-forward (required for blank-free weekly build)",
        type=["xlsx"],
    )
with col_b:
    servicer_uploads = st.file_uploader(
        "Upload current servicer files (csv/xlsx) (optional if skipped below)",
        type=["csv", "xlsx"],
        accept_multiple_files=True,
    )

st.markdown("### Step 3: Build options")
skip_servicer_files = st.checkbox(
    "Skip servicer files and build Salesforce-only version",
    value=False,
    help="Leaves servicer-driven columns blank or Salesforce-fallback where available.",
)

build_target = st.selectbox(
    "Which sheet do you want to build right now?",
    options=["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset", "All"],
    index=4,
)

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
    elif prev_upload is None:
        st.error("Upload the prior completed Active Loans workbook. The weekly build now enforces a zero-blank carry-forward repair pass and requires a known-good baseline.")
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
            prev_bytes = prev_upload.getvalue() if prev_upload else None
            npl_maps = {"loan_flags": pd.DataFrame(), "asset_flags": pd.DataFrame()}

            if prev_upload:
                status.update(label="Reading uploaded completed report for carry-forward...")
                prev_maps = build_prev_maps(prev_bytes)

            if skip_servicer_files:
                serv_join = pd.DataFrame(columns=["source_file", "servicer", "servicer_family", "servicer_id", "upb", "suspense", "next_payment_date", "maturity_date", "status", "as_of", "_sid_key"])
                detected_run_date = run_dt
                serv_preview = serv_join.copy()

                st.markdown("### Servicer lookup preview")
                st.caption("Servicer files were skipped. Servicer-driven columns will use Salesforce fallback where available.")
                st.caption(f"UPB header (always today): **{upb_col}**")
            else:
                servicer_phase_started = time.perf_counter()

                def _servicer_progress(message: str) -> None:
                    elapsed = time.perf_counter() - servicer_phase_started
                    status.update(label=f"Servicer processing | {message} | total {elapsed:0.1f}s")

                _servicer_progress(f"starting {len(servicer_uploads)} uploaded file(s)")
                serv_join, detected_run_date, serv_preview = build_servicer_lookup(servicer_uploads, progress_callback=_servicer_progress, use_cache=False)

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
            mark_workbook_for_recalc(wb)
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
                bridge_loan_wide = _build_bridge_loan_wide_like()
                bridge_property_rollup = _build_bridge_property_rollup_like()
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
                bridge_asset_df, bridge_asset_backfill = backfill_df_from_baseline("Bridge Asset", bridge_asset_df, prev_bytes)
                if bridge_asset_backfill and bridge_asset_backfill.get("fills"):
                    diagnostics.append(f"Bridge Asset baseline backfill cells: {int(bridge_asset_backfill['fills']):,} using {bridge_asset_backfill.get('keys', 'n/a')}")

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
                    bridge_loan_df = build_bridge_loan(
                        bridge_loan_wide,
                        bridge_asset_df,
                        bridge_property_rollup,
                        serv_join,
                        upb_col,
                        prev_maps,
                        template_maps,
                        npl_maps=npl_maps,
                    )
                    bridge_loan_df, bridge_loan_backfill = backfill_df_from_baseline("Bridge Loan", bridge_loan_df, prev_bytes)
                    if bridge_loan_backfill and bridge_loan_backfill.get("fills"):
                        diagnostics.append(f"Bridge Loan baseline backfill cells: {int(bridge_loan_backfill['fills']):,} using {bridge_loan_backfill.get('keys', 'n/a')}")

                    diagnostics.append(f"Bridge Loan rows: {len(bridge_loan_df):,}")
                    diagnostics.append(
                        f"Bridge Loan nonblank {upb_col}: {bridge_loan_df[upb_col].notna().mean():.1%}"
                        if upb_col in bridge_loan_df.columns
                        else f"Bridge Loan nonblank {upb_col}: n/a"
                    )

                    status.update(label="Writing Bridge Loan sheet...")
                    write_output_sheet(wb, "Bridge Loan", bridge_loan_df, upb_col)
                    del bridge_loan_df

                del bridge_spine, bridge_loan_wide, bridge_property_rollup, bridge_dnl, bridge_asset_ids, bridge_val, bridge_asset_df
                gc.collect()

            if need_term:
                status.update(label="Pulling term data from Salesforce...")
                term_wide = _build_term_wide_like()

                candidate_term_deals = _nonblank_unique(term_wide["Deal Loan Number"].tolist()) if not term_wide.empty and "Deal Loan Number" in term_wide.columns else []

                status.update(label="Pulling term asset deal universe from Salesforce...")
                term_asset_filter_deals = _build_term_asset_deal_universe(candidate_term_deals)
                diagnostics.append(f"Term asset deal universe: {len(term_asset_filter_deals):,} deals")

                status.update(label="Building Term Loan...")
                term_loan_df = build_term_loan(
                    term_wide,
                    sf_am,
                    sf_active_rm,
                    serv_join,
                    upb_col,
                    prev_maps,
                    template_maps,
                    asset_deal_numbers=term_asset_filter_deals,
                )
                term_loan_df, term_loan_backfill = backfill_df_from_baseline("Term Loan", term_loan_df, prev_bytes)
                if term_loan_backfill and term_loan_backfill.get("fills"):
                    diagnostics.append(f"Term Loan baseline backfill cells: {int(term_loan_backfill['fills']):,} using {term_loan_backfill.get('keys', 'n/a')}")

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
                    term_deal_numbers = [d for d in _nonblank_unique(term_loan_df["Deal Number"].tolist()) if clean_text(d).upper() != "N/A"] if "Deal Number" in term_loan_df.columns else []

                    status.update(label="Pulling term asset rows from Salesforce...")
                    term_asset_source = _build_term_asset_like(deal_numbers=term_deal_numbers)

                    status.update(label="Building Term Asset...")
                    term_asset_df = build_term_asset(term_asset_source, term_loan_df, upb_col, prev_maps=prev_maps)
                    term_asset_df, term_asset_backfill = backfill_df_from_baseline("Term Asset", term_asset_df, prev_bytes)
                    if term_asset_backfill and term_asset_backfill.get("fills"):
                        diagnostics.append(f"Term Asset baseline backfill cells: {int(term_asset_backfill['fills']):,} using {term_asset_backfill.get('keys', 'n/a')}")

                    status.update(label="Writing Term Asset sheet...")
                    write_output_sheet(wb, "Term Asset", term_asset_df, upb_col)
                    del term_deal_numbers, term_asset_source, term_asset_df

                del term_wide, term_loan_df, term_asset_filter_deals, candidate_term_deals
                gc.collect()

            del sf_am, sf_active_rm, serv_join, serv_preview
            gc.collect()

            selected_sheet_names = [
                sheet_name
                for sheet_name in ["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset"]
                if build_target in (sheet_name, "All")
            ]
            audit_summary = []
            audit_exceptions = []
            if ENFORCE_ZERO_FILLABLE_BLANKS:
                status.update(label="Running zero-blank enforcement...")
                zero_blank_ok, zero_blank_diags, audit_summary, audit_exceptions = enforce_zero_fillable_blanks(
                    wb,
                    baseline_bytes=prev_bytes,
                    upb_header=upb_col,
                    sheet_names=selected_sheet_names,
                )
                diagnostics.extend(zero_blank_diags)
                if write_audit_sheets is not None and audit_summary is not None:
                    write_audit_sheets(wb, audit_summary, audit_exceptions)
                if audit_diagnostic_lines is not None and audit_summary:
                    diagnostics.extend(audit_diagnostic_lines(audit_summary))
                if not zero_blank_ok:
                    diagnostics.append(
                        "Zero-blank enforcement could not eliminate all fillable blanks. Review the QA Summary / QA Exceptions tabs and use the immediately prior completed workbook as baseline on the next run."
                    )
                    st.warning(
                        "Zero-blank enforcement did not clear every fillable blank. The workbook was still built; review QA Summary / QA Exceptions before use."
                    )
                if any(int(item.get("possible_shift_cells", 0) or 0) for item in audit_summary):
                    diagnostics.append("Blank enforcement passed, but possible shifted values were still detected. Review the QA Exceptions tab before weekly use.")
            elif POSTBUILD_AUDIT_AVAILABLE and audit_openpyxl_workbook is not None and write_audit_sheets is not None:
                status.update(label="Running post-build QA audit...")
                audit_summary, audit_exceptions = audit_openpyxl_workbook(
                    wb,
                    baseline_bytes=prev_bytes,
                    upb_header=upb_col,
                    sheet_names=selected_sheet_names,
                )
                write_audit_sheets(wb, audit_summary, audit_exceptions)
                if audit_diagnostic_lines is not None:
                    diagnostics.extend(audit_diagnostic_lines(audit_summary))
                if workbook_needs_attention is not None and workbook_needs_attention(audit_summary):
                    diagnostics.append("QA attention needed: review the QA Summary and QA Exceptions tabs in the completed workbook before weekly use.")
            else:
                diagnostics.append("Post-build QA audit helper not available in this runtime; workbook-level QA tabs were not added.")

            status.update(label="Saving workbook...")
            out_bytes = BytesIO()
            mark_workbook_for_recalc(wb)
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
