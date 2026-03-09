"""Active Loans Report Builder (Streamlit)

What you upload:
  1) Active Loans TEMPLATE workbook (.xlsx)
  2) Servicer files (csv/xlsx) for UPB/Next Pay/Maturity/Suspense/Servicer Status
  3) Salesforce report exports (.xlsx) (detail rows)

What it outputs:
  - One Excel file per requested sheet (fast) OR one workbook containing all requested sheets

Key behaviors (requested):
  - The UPB column header is dynamic: "M/D UPB" where M/D comes from the *servicer file date in the filename*.
    (Default = latest date found across uploaded servicer filenames; you can override.)
  - Servicer files are only used to populate servicer-related columns and to validate SF data.
  - Template formulas are preserved (formula columns are not overwritten).
  - Template “as-of” date cells (row 3) are updated so formula columns recalc correctly.

No Salesforce API / OAuth is used in this version (uses exports only).
"""

import re
from datetime import date, datetime
from io import BytesIO
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from openpyxl import load_workbook


# =============================================================================
# SALESFORCE REPORT EXPORT MAPPINGS (Label-based)
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


def dq_bucket(days_past_due: float) -> str:
    if pd.isna(days_past_due):
        return ""
    d = int(max(0, float(days_past_due)))
    if d == 0:
        return "Current"
    if d < 30:
        return "DQ 1-29"
    if d < 60:
        return "DQ 30-59"
    if d < 90:
        return "DQ 60-89"
    return "DQ 90+"


def make_upb_header(as_of: date) -> str:
    return f"{as_of.month}/{as_of.day} UPB"


def date_from_filename(name: str) -> Optional[date]:
    # YYYYMMDD
    m = re.search(r"(20\d{2})(\d{2})(\d{2})", name)
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
    m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.search(r"(\d{1,2})/(\d{1,2})/(20\d{2})", s)
    if m:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    return None


# =============================================================================
# SALESFORCE EXPORT LOADER
# =============================================================================

def _sniff_export_header_row(file_bytes: bytes, required_any: Sequence[str], max_scan_rows: int = 30) -> int:
    """Return 0-indexed header row for pandas.read_excel."""
    wb = load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    try:
        best_row = 0
        best_score = -1
        req = {str(r).strip() for r in required_any if r and str(r).strip()}

        for r in range(1, max_scan_rows + 1):
            row = next(ws.iter_rows(min_row=r, max_row=r, values_only=True))
            vals = {str(v).strip() for v in row if v is not None and str(v).strip()}
            score = len(vals & req)
            if score > best_score:
                best_score = score
                best_row = r
            if score >= max(4, min(10, len(req) // 2)):
                return r - 1

        return best_row - 1
    finally:
        wb.close()


def load_sf_export(upload, expected_labels: Sequence[str]) -> pd.DataFrame:
    if upload is None:
        return pd.DataFrame()
    b = upload.getvalue()
    header = _sniff_export_header_row(b, expected_labels)
    df = pd.read_excel(BytesIO(b), header=header)
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df


# =============================================================================
# SERVICER FILE PARSING (STREAMING)
# =============================================================================

def _sniff_header_row_openpyxl(
    file_bytes: bytes,
    required_cols: Set[str],
    sheet_name: Optional[str] = None,
    max_scan_rows: int = 35,
) -> Tuple[Optional[str], Optional[int], Dict[str, int]]:
    wb = load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        sheet_names = [sheet_name] if sheet_name and sheet_name in wb.sheetnames else list(wb.sheetnames)
        req = {str(x).strip() for x in required_cols}

        for sn in sheet_names:
            ws = wb[sn]
            for r in range(1, max_scan_rows + 1):
                row = next(ws.iter_rows(min_row=r, max_row=r, values_only=True))
                headers = [str(v).strip() if v is not None else None for v in row]
                hset = {h for h in headers if h}
                if req.issubset(hset):
                    col_map = {h: i for i, h in enumerate(headers) if h}
                    return sn, r, col_map
        return None, None, {}
    finally:
        wb.close()


def _stream_xlsx_columns(
    file_bytes: bytes,
    sheet_name: str,
    header_row: int,
    col_map: Dict[str, int],
    wanted: Sequence[str],
) -> pd.DataFrame:
    wb = load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        ws = wb[sheet_name]
        idxs = [(c, col_map[c]) for c in wanted if c in col_map]
        cols_present = [c for c, _i in idxs]

        rows_out: List[List[object]] = []
        for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
            rows_out.append([row[i] if i < len(row) else None for _c, i in idxs])

        return pd.DataFrame(rows_out, columns=cols_present)
    finally:
        wb.close()


def parse_servicer_upload(upload) -> Tuple[pd.DataFrame, Optional[date], Optional[date]]:
    name = upload.name
    b = upload.getvalue()

    file_dt = date_from_filename(name)
    embedded_dt: Optional[date] = None

    # CHL CSV
    if name.lower().endswith(".csv"):
        df = pd.read_csv(BytesIO(b))
        req = {"Servicer Loan ID", "UPB"}
        if not req.issubset(set(df.columns)):
            raise ValueError(f"CSV doesn't look like CHL Streamline (missing {req - set(df.columns)}).")

        out = pd.DataFrame(
            {
                "servicer": "CHL",
                "servicer_id": norm_id_series(df["Servicer Loan ID"]),
                "upb": df["UPB"].apply(money_to_float),
                "suspense": pd.to_numeric(df.get("Suspense Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Current Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Performing Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": pd.to_datetime(file_dt) if file_dt else pd.NaT,
                "source_file": name,
            }
        )
        return out.dropna(subset=["servicer_id"]), file_dt, embedded_dt

    # CHL Streamline XLSX (tiny; parse via pandas with header sniff)
    if name.lower().endswith(".xlsx") and "streamline" in name.lower():
        raw = pd.read_excel(BytesIO(b), header=None)
        hdr_row = None
        for i in range(min(15, len(raw))):
            row = raw.iloc[i].astype("string").fillna("").str.strip().tolist()
            if "Servicer Loan ID" in row and "UPB" in row:
                hdr_row = i
                break
        if hdr_row is None:
            raise ValueError("Could not detect CHL Streamline header row.")

        df = pd.read_excel(BytesIO(b), header=hdr_row)
        df.columns = [str(c).strip() for c in df.columns]

        # embedded run date from title cell if present
        try:
            title = str(raw.iloc[0, 0]) if pd.notna(raw.iloc[0, 0]) else ""
            embedded_dt = _extract_date_from_text(title)
        except Exception:
            embedded_dt = None

        out = pd.DataFrame(
            {
                "servicer": "CHL",
                "servicer_id": norm_id_series(df["Servicer Loan ID"]),
                "upb": df["UPB"].apply(money_to_float),
                "suspense": pd.to_numeric(df.get("Suspense Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Current Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Performing Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": pd.to_datetime(file_dt) if file_dt else pd.NaT,
                "source_file": name,
            }
        )
        return out.dropna(subset=["servicer_id"]), file_dt, embedded_dt

    # XLSX types detected by required columns
    patterns = [
        ("Statebridge", {"Loan Number", "Current UPB", "Due Date", "Maturity Date", "Loan Status"}, None),
        ("Berkadia", {"BCM Loan#", "Principal Balance", "Next Payment Due Date", "Maturity Date"}, "Loan"),
        ("FCI", {"Account", "Current Balance", "Next Due Date", "Maturity Date", "Status"}, None),
        ("Midland", {"ServicerLoanNumber", "UPB$", "NextPaymentDate", "MaturityDate", "ServicerLoanStatus"}, None),
    ]

    detected = None
    sheet = None
    header_row = None
    col_map: Dict[str, int] = {}

    for serv, req, sn in patterns:
        s, hr, cmap = _sniff_header_row_openpyxl(b, req, sheet_name=sn)
        if hr is not None:
            detected, sheet, header_row, col_map = serv, s, hr, cmap
            break

    if detected is None or sheet is None or header_row is None:
        raise ValueError("Could not detect servicer file type from columns.")

    if detected == "Statebridge":
        df = _stream_xlsx_columns(
            b, sheet, header_row, col_map,
            ["Loan Number", "Current UPB", "Unapplied Balance", "Due Date", "Maturity Date", "Loan Status", "Date"]
        )
        if "Date" in df.columns:
            dmax = pd.to_datetime(df["Date"], errors="coerce").max()
            embedded_dt = dmax.date() if pd.notna(dmax) else None

        out = pd.DataFrame(
            {
                "servicer": "Statebridge",
                "servicer_id": norm_id_series(df["Loan Number"]),
                "upb": pd.to_numeric(df["Current UPB"], errors="coerce"),
                "suspense": pd.to_numeric(df.get("Unapplied Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Loan Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": pd.to_datetime(file_dt) if file_dt else pd.NaT,
                "source_file": name,
            }
        )
        return out.dropna(subset=["servicer_id"]), file_dt, embedded_dt

    if detected == "Berkadia":
        df = _stream_xlsx_columns(
            b, sheet, header_row, col_map,
            ["BCM Loan#", "Principal Balance", "Suspense Balance", "Next Payment Due Date", "Maturity Date", "Run Date"]
        )
        if "Run Date" in df.columns:
            dmax = pd.to_datetime(df["Run Date"], errors="coerce").max()
            embedded_dt = dmax.date() if pd.notna(dmax) else None

        # Matches your Completed workbook behavior: Berkadia Servicer Status = "Active"
        status = pd.Series(["Active"] * len(df))

        out = pd.DataFrame(
            {
                "servicer": "Berkadia",
                "servicer_id": norm_id_series(df["BCM Loan#"]),
                "upb": pd.to_numeric(df["Principal Balance"], errors="coerce"),
                "suspense": pd.to_numeric(df.get("Suspense Balance", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Payment Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": status.astype("string"),
                "as_of": pd.to_datetime(file_dt) if file_dt else pd.NaT,
                "source_file": name,
            }
        )
        return out.dropna(subset=["servicer_id"]), file_dt, embedded_dt

    if detected == "FCI":
        df = _stream_xlsx_columns(
            b, sheet, header_row, col_map,
            ["Account", "Current Balance", "Suspense Pmt.", "Next Due Date", "Maturity Date", "Status"]
        )
        out = pd.DataFrame(
            {
                "servicer": "FCI",
                "servicer_id": norm_id_series(df["Account"]),
                "upb": pd.to_numeric(df["Current Balance"], errors="coerce"),
                "suspense": pd.to_numeric(df.get("Suspense Pmt.", np.nan), errors="coerce"),
                "next_payment_date": df.get("Next Due Date", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("Maturity Date", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("Status", pd.Series([None] * len(df))).astype("string"),
                "as_of": pd.to_datetime(file_dt) if file_dt else pd.NaT,
                "source_file": name,
            }
        )
        return out.dropna(subset=["servicer_id"]), file_dt, embedded_dt

    if detected == "Midland":
        df = _stream_xlsx_columns(
            b, sheet, header_row, col_map,
            ["ServicerLoanNumber", "UPB$", "NextPaymentDate", "MaturityDate", "ServicerLoanStatus", "ReportDate"]
        )
        # normalize Midland IDs: strip COM, non-alnum, leading zeros
        raw = df["ServicerLoanNumber"].astype("string").str.strip()
        raw = raw.str.replace(r"COM$", "", regex=True)
        raw = raw.str.replace(r"[^0-9A-Za-z]", "", regex=True).str.lstrip("0")

        if "ReportDate" in df.columns:
            dmax = pd.to_datetime(df["ReportDate"], errors="coerce").max()
            embedded_dt = dmax.date() if pd.notna(dmax) else None

        out = pd.DataFrame(
            {
                "servicer": "Midland",
                "servicer_id": raw.replace({"": pd.NA}),
                "upb": df["UPB$"].apply(money_to_float),
                "suspense": np.nan,
                "next_payment_date": df.get("NextPaymentDate", pd.Series([None] * len(df))).apply(to_dt),
                "maturity_date": df.get("MaturityDate", pd.Series([None] * len(df))).apply(to_dt),
                "status": df.get("ServicerLoanStatus", pd.Series([None] * len(df))).astype("string"),
                "as_of": pd.to_datetime(file_dt) if file_dt else pd.NaT,
                "source_file": name,
            }
        )
        return out.dropna(subset=["servicer_id"]), file_dt, embedded_dt

    raise ValueError("Unhandled servicer type.")


def build_servicer_lookup(servicer_uploads: List) -> Tuple[pd.DataFrame, Optional[date], Optional[date]]:
    frames: List[pd.DataFrame] = []
    file_dates: List[date] = []
    embedded_dates: List[date] = []

    for f in servicer_uploads:
        df, fdt, edt = parse_servicer_upload(f)
        frames.append(df)
        if fdt:
            file_dates.append(fdt)
        if edt:
            embedded_dates.append(edt)

    lookup = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["servicer", "servicer_id", "upb", "suspense", "next_payment_date", "maturity_date", "status", "as_of", "source_file"]
    )

    if not lookup.empty:
        lookup = lookup.dropna(subset=["servicer_id"]).copy()
        lookup["servicer_id"] = norm_id_series(lookup["servicer_id"])
        lookup = lookup.sort_values(["as_of"]).drop_duplicates(["servicer", "servicer_id"], keep="last")

    as_of_file = max(file_dates) if file_dates else None
    as_of_embedded = max(embedded_dates) if embedded_dates else None

    return lookup, as_of_file, as_of_embedded


# =============================================================================
# LAST WEEK CARRY-FORWARD
# =============================================================================

def read_tab_df_from_active_loans(file_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=3)
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_prev_maps(prev_bytes: bytes) -> dict:
    out: Dict[str, pd.DataFrame] = {}

    # Term Loan REO Date
    try:
        tl = read_tab_df_from_active_loans(prev_bytes, "Term Loan")
        if "Deal Number" in tl.columns and "REO Date" in tl.columns:
            tmp = tl[["Deal Number", "REO Date"]].copy()
            tmp["_deal_key"] = norm_id_series(tmp["Deal Number"])
            out["term_loan_reo"] = tmp.dropna(subset=["_deal_key"]).drop_duplicates("_deal_key")
    except Exception:
        pass

    # Bridge Loan manual carry-forward
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
# BUILD DATAFRAMES
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

    out["_deal_key"] = norm_id_series(out["Deal Number"])
    out["_serv_id_key"] = norm_id_series(out["Servicer ID"])
    out["_asset_key"] = norm_id_series(out["Asset ID"])

    # Do Not Lend
    if not sf_dnl.empty and "Deal Loan Number" in sf_dnl.columns and "Do Not Lend" in sf_dnl.columns:
        dnl = sf_dnl.copy()
        dnl["_deal_key"] = norm_id_series(dnl["Deal Loan Number"])
        dnl_flag = dnl.groupby("_deal_key")["Do Not Lend"].max().reset_index()
        out = out.merge(dnl_flag, on="_deal_key", how="left")
        out["Do Not Lend (Y/N)"] = out["Do Not Lend"].fillna(False).map(lambda x: "Y" if bool(x) else "N")
        out = out.drop(columns=["Do Not Lend"], errors="ignore")

    # Valuation by Asset
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

        out = out.merge(piv_name.rename(columns=role_to_namecol).reset_index(), on="_deal_key", how="left")
        out = out.merge(piv_date.rename(columns=role_to_datecol).reset_index(), on="_deal_key", how="left")

    # Active RM (CAF Originator)
    if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns and "CAF Originator" in sf_arm.columns:
        arm = sf_arm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key", "CAF Originator"]].drop_duplicates("_deal_key")
        out = out.merge(arm, on="_deal_key", how="left")
        out["Active RM"] = out["CAF Originator"]
        out = out.drop(columns=["CAF Originator"], errors="ignore")

    # Servicer join
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
            )[["_serv_id_key", "Servicer", "_loan_upb", "_loan_suspense", "Next Payment Date", "Servicer Maturity Date", "Servicer Status"]],
            on="_serv_id_key",
            how="left",
        )

        # Allocate loan UPB across assets using SF Current UPB weights if present
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

    # Defaults
    for c in ["Portfolio", "Segment", "Strategy Grouping"]:
        if c not in out.columns:
            out[c] = ""

    # Y/N
    if "Is Special Asset (Y/N)" in out.columns:
        out["Is Special Asset (Y/N)"] = out["Is Special Asset (Y/N)"].fillna(False).map(lambda x: "Y" if bool(x) else "N")

    return out


def build_bridge_loan(
    bridge_asset: pd.DataFrame,
    sf_spine: pd.DataFrame,
    upb_col: str,
    as_of: date,
    prev_maps: dict,
) -> pd.DataFrame:
    ba = bridge_asset.copy()

    ba["_sf_funded_calc"] = (
        pd.to_numeric(ba.get("Initial Disbursement Funded", 0), errors="coerce").fillna(0)
        + pd.to_numeric(ba.get("Renovation Holdback Funded", 0), errors="coerce").fillna(0)
        + pd.to_numeric(ba.get("Interest Allocation Funded", 0), errors="coerce").fillna(0)
    )

    if "_deal_key" not in ba.columns:
        ba["_deal_key"] = norm_id_series(ba.get("Deal Number", pd.Series([None] * len(ba))))

    g = ba.groupby("_deal_key", dropna=True)

    out = pd.DataFrame(
        {
            "Deal Number": g["Deal Number"].first(),
            "Portfolio": g["Portfolio"].first() if "Portfolio" in ba.columns else "",
            "Loan Buyer": g["Loan Buyer"].first() if "Loan Buyer" in ba.columns else "",
            "Financing": g["Financing"].first() if "Financing" in ba.columns else "",
            "Servicer ID": g["Servicer ID"].first() if "Servicer ID" in ba.columns else "",
            "Servicer": g["Servicer"].first() if "Servicer" in ba.columns else "",
            "Deal Name": g["Deal Name"].first() if "Deal Name" in ba.columns else "",
            "Borrower Name": g["Borrower Entity"].first() if "Borrower Entity" in ba.columns else "",
            "Account ": g["Account Name"].first() if "Account Name" in ba.columns else "",
            "Do Not Lend (Y/N)": g["Do Not Lend (Y/N)"].max() if "Do Not Lend (Y/N)" in ba.columns else "N",
            "Primary Contact": g["Primary Contact"].first() if "Primary Contact" in ba.columns else "",
            "Number of Assets": g["Asset ID"].nunique() if "Asset ID" in ba.columns else 0,
            "# of Units": pd.to_numeric(g["# of Units"].sum(min_count=1), errors="coerce") if "# of Units" in ba.columns else np.nan,
            "State(s)": g["State"].apply(lambda s: ", ".join(sorted({str(x).strip() for x in s.dropna() if str(x).strip()}))) if "State" in ba.columns else "",
            "Origination Date": g["Origination Date"].min() if "Origination Date" in ba.columns else "",
            "Last Funding Date": g["Last Funding Date"].max() if "Last Funding Date" in ba.columns else "",
            "Original Maturity Date": g["Original Loan Maturity date"].first() if "Original Loan Maturity date" in ba.columns else "",
            "Current Maturity Date": g["Current Loan Maturity date"].first() if "Current Loan Maturity date" in ba.columns else "",
            # matches Completed workbook: Bridge Loan Next Advance Maturity Date = Bridge Asset Servicer Maturity Date
            "Next Advance Maturity Date": g["Servicer Maturity Date"].first() if "Servicer Maturity Date" in ba.columns else "",
            "Next Payment Date": g["Next Payment Date"].min() if "Next Payment Date" in ba.columns else "",
            "Active Funded Amount": pd.to_numeric(g["_sf_funded_calc"].sum(min_count=1), errors="coerce"),
            upb_col: pd.to_numeric(g[upb_col].sum(min_count=1), errors="coerce") if upb_col in ba.columns else np.nan,
            "Suspense Balance": pd.to_numeric(g["Suspense Balance"].sum(min_count=1), errors="coerce") if "Suspense Balance" in ba.columns else np.nan,
            "Most Recent Valuation Date": pd.to_datetime(g["Updated Valuation Date"].max(), errors="coerce") if "Updated Valuation Date" in ba.columns else "",
            "Most Recent As-Is Value": pd.to_numeric(g["Updated As-Is Value"].sum(min_count=1), errors="coerce") if "Updated As-Is Value" in ba.columns else np.nan,
            "Most Recent ARV": pd.to_numeric(g["Updated ARV"].sum(min_count=1), errors="coerce") if "Updated ARV" in ba.columns else np.nan,
            "Initial Disbursement Funded": pd.to_numeric(g["Initial Disbursement Funded"].sum(min_count=1), errors="coerce") if "Initial Disbursement Funded" in ba.columns else np.nan,
            "Renovation Holdback": pd.to_numeric(g["Renovation Holdback"].sum(min_count=1), errors="coerce") if "Renovation Holdback" in ba.columns else np.nan,
            "Renovation HB Funded": pd.to_numeric(g["Renovation Holdback Funded"].sum(min_count=1), errors="coerce") if "Renovation Holdback Funded" in ba.columns else np.nan,
            "Renovation HB Remaining": pd.to_numeric(g["Renovation Holdback Remaining"].sum(min_count=1), errors="coerce") if "Renovation Holdback Remaining" in ba.columns else np.nan,
            "Interest Allocation": pd.to_numeric(g["Interest Allocation"].sum(min_count=1), errors="coerce") if "Interest Allocation" in ba.columns else np.nan,
            "Interest Allocation Funded": pd.to_numeric(g["Interest Allocation Funded"].sum(min_count=1), errors="coerce") if "Interest Allocation Funded" in ba.columns else np.nan,
            "Loan Stage": g["Loan Stage"].first() if "Loan Stage" in ba.columns else "",
            "Segment": g["Segment"].first() if "Segment" in ba.columns else "",
            "Product Type": g["Product Type"].first() if "Product Type" in ba.columns else "",
            "Product Sub Type": g["Product Sub-Type"].first() if "Product Sub-Type" in ba.columns else "",
            "Transaction Type": g["Transaction Type"].first() if "Transaction Type" in ba.columns else "",
            "Project Strategy": g["Project Strategy"].first() if "Project Strategy" in ba.columns else "",
            "Strategy Grouping": g["Strategy Grouping"].first() if "Strategy Grouping" in ba.columns else "",
            "CV Originator": "",
            "Active RM": g["Active RM"].first() if "Active RM" in ba.columns else "",
            "Deal Intro Sub-Source": g["Deal Intro Sub-Source"].first() if "Deal Intro Sub-Source" in ba.columns else "",
            "Referral Source Account": g["Referral Source Account"].first() if "Referral Source Account" in ba.columns else "",
            "Referral Source Contact": g["Referral Source Contact"].first() if "Referral Source Contact" in ba.columns else "",
            "3/31 NPL": "",
            "Needs NPL Value": "",
            "Special Focus (Y/N)": (g["Is Special Asset (Y/N)"] .max().astype(str).eq("Y")).map(lambda b: "Y" if bool(b) else "N") if "Is Special Asset (Y/N)" in ba.columns else "N",
            "Asset Manager 1": g["Asset Manager 1"].first() if "Asset Manager 1" in ba.columns else "",
            "AM 1 Assigned Date": g["AM 1 Assigned Date"].first() if "AM 1 Assigned Date" in ba.columns else "",
            "Asset Manager 2": g["Asset Manager 2"].first() if "Asset Manager 2" in ba.columns else "",
            "AM 2 Assigned Date": g["AM 2 Assigned Date"].first() if "AM 2 Assigned Date" in ba.columns else "",
            "Construction Mgr.": g["Construction Mgr."].first() if "Construction Mgr." in ba.columns else "",
            "CM Assigned Date": g["CM Assigned Date"].first() if "CM Assigned Date" in ba.columns else "",
        }
    ).reset_index()

    # compute Loan Level Delinquency if missing (then override from last week if provided)
    if "Loan Level Delinquency" not in out.columns:
        out["Loan Level Delinquency"] = ""

    if "Next Payment Date" in out.columns:
        npd = pd.to_datetime(out["Next Payment Date"], errors="coerce")
        dpd = (pd.to_datetime(as_of) - npd).dt.days.clip(lower=0)
        out.loc[out["Loan Level Delinquency"].astype(str).str.strip().eq(""), "Loan Level Delinquency"] = dpd.apply(dq_bucket)

    # deal-level fields from Bridge Maturity export
    if "Deal Loan Number" in sf_spine.columns:
        deal = sf_spine.copy()
        deal["_deal_key"] = norm_id_series(deal["Deal Loan Number"])
        keep = ["_deal_key"]
        for c in ["Loan Commitment", "Total Remaining Commitment Amount", "Comments AM"]:
            if c in deal.columns:
                keep.append(c)
        if len(keep) > 1:
            deal = deal[keep].drop_duplicates("_deal_key")
            out = out.merge(deal, left_on="_deal_key", right_on="_deal_key", how="left")
            if "Total Remaining Commitment Amount" in out.columns and "Remaining Commitment" not in out.columns:
                out["Remaining Commitment"] = out["Total Remaining Commitment Amount"]
            if "Comments AM" in out.columns and "AM Commentary" not in out.columns:
                out["AM Commentary"] = out["Comments AM"]
                out = out.drop(columns=["Comments AM"], errors="ignore")

    # carry-forward manual columns
    if "bridge_loan_manual" in prev_maps:
        man = prev_maps["bridge_loan_manual"].copy()
        out = out.merge(man, on="_deal_key", how="left", suffixes=("", "_prev"))
        for c in ["State(s)", "Loan Level Delinquency", "Special Focus (Y/N)"]:
            if f"{c}_prev" in out.columns:
                out[c] = out[f"{c}_prev"].fillna(out.get(c, ""))
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    return out.drop(columns=["_deal_key"], errors="ignore")


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

    out["_deal_key"] = norm_id_series(out["Deal Number"])

    # Do Not Lend -> Y/N
    if "Do Not Lend (Y/N)" in out.columns:
        out["Do Not Lend (Y/N)"] = out["Do Not Lend (Y/N)"].fillna(False).map(lambda x: "Y" if bool(x) else "N")

    # Sold Term
    if not sf_sold.empty and "Deal Loan Number" in sf_sold.columns and "Sold Loan: Sold To" in sf_sold.columns:
        sold = sf_sold.copy()
        sold["_deal_key"] = norm_id_series(sold["Deal Loan Number"])
        sold = sold[["_deal_key", "Sold Loan: Sold To"]].drop_duplicates("_deal_key")
        out = out.merge(sold, on="_deal_key", how="left")
        out["Loan Buyer"] = out["Sold Loan: Sold To"]
        out = out.drop(columns=["Sold Loan: Sold To"], errors="ignore")

    # Servicer ID + Servicer from Term Export (if present)
    if "Servicer Commitment Id" in sf_term.columns:
        out["Servicer ID"] = sf_term["Servicer Commitment Id"]
    if "Servicer Name" in sf_term.columns:
        out["Servicer"] = sf_term["Servicer Name"]

    out["_serv_id_key"] = norm_id_series(out.get("Servicer ID", pd.Series([None] * len(out))))

    # Active RM from Term Export if present, else from Active RM export
    if "Active RM" in sf_term.columns:
        out["Active RM"] = sf_term["Active RM"]
    elif not sf_arm.empty and "Deal Loan Number" in sf_arm.columns and "CAF Originator" in sf_arm.columns:
        arm = sf_arm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        arm = arm[["_deal_key", "CAF Originator"]].drop_duplicates("_deal_key")
        out = out.merge(arm, on="_deal_key", how="left")
        out["Active RM"] = out["CAF Originator"].fillna("")
        out = out.drop(columns=["CAF Originator"], errors="ignore")
    else:
        out["Active RM"] = ""

    # Asset Manager from AM Assignments
    if not sf_am.empty and "Deal Loan Number" in sf_am.columns and "Team Role" in sf_am.columns and "Team Member Name" in sf_am.columns:
        am = sf_am.copy()
        am["_deal_key"] = norm_id_series(am["Deal Loan Number"])
        am["_dt"] = pd.to_datetime(am.get("Date Assigned"), errors="coerce")
        am = am.sort_values(["_deal_key", "Team Role", "_dt"]).drop_duplicates(["_deal_key", "Team Role"], keep="last")
        am1 = am[am["Team Role"].astype("string").str.strip().eq("Asset Manager")][["_deal_key", "Team Member Name"]].drop_duplicates("_deal_key")
        out = out.merge(am1, on="_deal_key", how="left")
        out["Asset Manager"] = out["Team Member Name"].fillna("")
        out = out.drop(columns=["Team Member Name"], errors="ignore")
    else:
        out["Asset Manager"] = ""

    # Join servicer lookup for UPB + dates
    if not serv_lookup.empty:
        s = serv_lookup.dropna(subset=["servicer_id"]).copy()
        out["_serv_id_key_mid"] = out["_serv_id_key"].astype("string").str.lstrip("0")

        s2 = s.rename(
            columns={
                "servicer_id": "_sid",
                "servicer": "_servicer_file",
                "upb": upb_col,
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Maturity Date",
            }
        )[["_sid", "_servicer_file", upb_col, "Next Payment Date", "Maturity Date"]]

        out = out.merge(s2, left_on=out["_serv_id_key_mid"], right_on="_sid", how="left").drop(columns=["_sid", "key_0"], errors="ignore")

        if "Servicer" not in out.columns:
            out["Servicer"] = out["_servicer_file"]
        else:
            out["Servicer"] = out["Servicer"].fillna(out["_servicer_file"])
        out = out.drop(columns=["_servicer_file"], errors="ignore")

    # REO carry-forward
    out["REO Date"] = ""
    if "term_loan_reo" in prev_maps:
        reo = prev_maps["term_loan_reo"][["_deal_key", "REO Date"]].copy()
        out = out.merge(reo, on="_deal_key", how="left", suffixes=("", "_prev"))
        out["REO Date"] = out["REO Date_prev"].fillna("")
        out = out.drop(columns=["REO Date_prev"], errors="ignore")

    for c in ["Portfolio", "Segment"]:
        if c not in out.columns:
            out[c] = ""

    return out


def build_term_asset(sf_term_asset: pd.DataFrame, term_loan: pd.DataFrame, upb_col: str) -> pd.DataFrame:
    out = pd.DataFrame()

    for col, label in TERM_ASSET_FROM_TERM_ASSET_REPORT.items():
        out[col] = sf_term_asset[label] if label in sf_term_asset.columns else None

    out["_deal_key"] = norm_id_series(out["Deal Number"])

    tl = term_loan.copy()
    tl["_deal_key"] = norm_id_series(tl["Deal Number"])

    if upb_col in tl.columns:
        tl = tl[["_deal_key", upb_col]].drop_duplicates("_deal_key")
        out = out.merge(tl, on="_deal_key", how="left")

        ala = pd.to_numeric(out.get("Property ALA", np.nan), errors="coerce")
        ala_sum = ala.groupby(out["_deal_key"]).transform("sum")
        out[upb_col] = np.where(ala_sum > 0, out[upb_col] * (ala / ala_sum), out[upb_col])

    out["CPP JV"] = ""
    return out


# =============================================================================
# EXCEL OUTPUT — preserve formulas
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


def normalize_header_name(h: str, upb_col: str) -> str:
    if isinstance(h, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", h):
        return upb_col
    if h.strip() == "2/28 UPB":
        return upb_col
    return h.strip()


def set_upb_header_cell(ws_formula, ws_values, upb_col: str, header_row: int = 4):
    for col_idx, cell in enumerate(ws_values[header_row], start=1):
        v = cell.value
        if isinstance(v, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", v):
            ws_formula.cell(header_row, col_idx).value = upb_col
            return


def update_as_of_cells(ws_formula, ws_values, as_of: date, header_row: int = 4, date_row: int = 3):
    md = None
    for cell in ws_values[header_row]:
        v = cell.value
        if isinstance(v, str):
            m = re.search(r"\b(\d{1,2})/(\d{1,2})\s*UPB\b", v)
            if m:
                md = (int(m.group(1)), int(m.group(2)))
                break
    if md is None:
        md = (as_of.month, as_of.day)

    for cell in ws_values[date_row]:
        v = cell.value
        if isinstance(v, (date, datetime)):
            if v.month == md[0] and v.day == md[1]:
                ws_formula.cell(date_row, cell.column).value = as_of
        elif isinstance(v, str):
            d = _extract_date_from_text(v)
            if d and d.month == md[0] and d.day == md[1]:
                ws_formula.cell(date_row, cell.column).value = as_of


def clear_columns(ws, col_indices: Sequence[int], start_row: int = 5):
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
    force_overwrite_headers: Optional[Set[str]] = None,
):
    force_overwrite_headers = force_overwrite_headers or set()

    write_cols: List[Tuple[int, str]] = []
    for col_idx, header in header_tuples:
        if col_idx in formula_cols and header not in force_overwrite_headers:
            continue
        write_cols.append((col_idx, header))

    col_indices = [c for c, _ in write_cols]
    headers = [h for _, h in write_cols]

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


def build_single_sheet_workbook(template_bytes: bytes, sheet_name: str):
    wb_f = load_workbook(BytesIO(template_bytes), data_only=False)
    wb_v = load_workbook(BytesIO(template_bytes), data_only=True)

    for sn in list(wb_f.sheetnames):
        if sn != sheet_name:
            del wb_f[sn]
    for sn in list(wb_v.sheetnames):
        if sn != sheet_name:
            del wb_v[sn]

    return wb_f, wb_v


# =============================================================================
# STREAMLIT UI
# =============================================================================

st.set_page_config(page_title="Active Loans Builder", layout="wide")
st.title("Active Loans Report Builder")

st.markdown(
    """
Upload the template + servicer files + Salesforce exports, then build the sheet(s) you want.

- **Fast mode**: output one file per sheet.
- UPB column header defaults to the **latest date in your servicer filenames**.
"""
)

# Inputs

template_upload = st.file_uploader("Upload Active Loans TEMPLATE (.xlsx)", type=["xlsx"], key="tmpl")
prev_upload = st.file_uploader("Upload LAST WEEK'S Active Loans report (.xlsx) for carry-forward", type=["xlsx"], key="prev")
servicer_uploads = st.file_uploader(
    "Upload current servicer files (csv/xlsx) — any names", type=["csv", "xlsx"], accept_multiple_files=True, key="serv"
)

st.subheader("Salesforce report exports")
col1, col2 = st.columns(2)
with col1:
    exp_bridge_maturity = st.file_uploader("Bridge Maturity Report v3 export (.xlsx)", type=["xlsx"], key="exp_bridge")
    exp_valuation = st.file_uploader("Valuation v4 export (.xlsx) (optional)", type=["xlsx"], key="exp_val")
    exp_am = st.file_uploader("AM Assignments export (.xlsx) (optional)", type=["xlsx"], key="exp_am")
    exp_active_rm = st.file_uploader("Active RM export (.xlsx) (optional)", type=["xlsx"], key="exp_arm")

with col2:
    exp_dnl = st.file_uploader("Do Not Lend export (.xlsx) (optional)", type=["xlsx"], key="exp_dnl")
    exp_term = st.file_uploader("Term Data Export (.xlsx)", type=["xlsx"], key="exp_term")
    exp_sold = st.file_uploader("Sold Term Loans (.xlsx) (optional)", type=["xlsx"], key="exp_sold")
    exp_term_asset = st.file_uploader("Term Asset Level - By Deal (.xlsx) (optional unless building Term Asset)", type=["xlsx"], key="exp_ta")

st.subheader("Build options")

sheets_requested = st.multiselect(
    "Sheets to build",
    options=["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset"],
    default=["Bridge Asset"],
)

single_sheet_files = st.checkbox("Output separate file per sheet (faster)", value=True)

build_btn = st.button("Build", type="primary")

if build_btn:
    if not template_upload:
        st.error("Upload the template workbook first.")
        st.stop()

    if not servicer_uploads:
        st.error("Upload the servicer files (UPB/Next Payment/Maturity/Status come from them).")
        st.stop()

    need_bridge_asset = ("Bridge Asset" in sheets_requested) or ("Bridge Loan" in sheets_requested)
    need_bridge_loan = "Bridge Loan" in sheets_requested
    need_term_loan = ("Term Loan" in sheets_requested) or ("Term Asset" in sheets_requested)
    need_term_asset = "Term Asset" in sheets_requested

    if need_bridge_asset and exp_bridge_maturity is None:
        st.error("Bridge Maturity export is required for Bridge Asset / Bridge Loan.")
        st.stop()

    if need_term_loan and exp_term is None:
        st.error("Term Data Export is required for Term Loan / Term Asset.")
        st.stop()

    if need_term_asset and exp_term_asset is None:
        st.error("Term Asset export is required for Term Asset.")
        st.stop()

    prev_maps = {}
    if prev_upload:
        prev_maps = build_prev_maps(prev_upload.getvalue())

    with st.spinner("Parsing servicer files..."):
        serv_lookup, as_of_file_dt, as_of_embedded_dt = build_servicer_lookup(servicer_uploads)

    if serv_lookup.empty:
        st.error("Could not parse any servicer rows. Check file formats.")
        st.stop()

    if as_of_file_dt is None:
        as_of_file_dt = date.today()

    st.write("Detected UPB as-of date from **servicer filenames**:", as_of_file_dt)
    if as_of_embedded_dt:
        st.caption(f"(For reference: latest embedded run date found inside files = {as_of_embedded_dt})")

    as_of = st.date_input("UPB 'as-of' date (controls the UPB column header)", value=as_of_file_dt)
    upb_col = make_upb_header(as_of)

    st.write("Servicer lookup preview (standardized):")
    st.dataframe(serv_lookup.head(50), use_container_width=True)

    with st.spinner("Loading Salesforce exports..."):
        sf_bridge = load_sf_export(exp_bridge_maturity, list(BRIDGE_ASSET_FROM_BRIDGE_MATURITY.values())) if exp_bridge_maturity else pd.DataFrame()
        sf_dnl = load_sf_export(exp_dnl, ["Deal Loan Number", "Do Not Lend"]) if exp_dnl else pd.DataFrame()
        sf_val = load_sf_export(exp_valuation, ["Asset ID"] + list(BRIDGE_ASSET_FROM_VALUATION.values())) if exp_valuation else pd.DataFrame()
        sf_am = load_sf_export(exp_am, ["Deal Loan Number", "Team Role", "Team Member Name"]) if exp_am else pd.DataFrame()
        sf_arm = load_sf_export(exp_active_rm, ["Deal Loan Number", "CAF Originator"]) if exp_active_rm else pd.DataFrame()
        sf_term = load_sf_export(exp_term, list(TERM_LOAN_FROM_TERM_EXPORT.values()) + ["Deal Loan Number"]) if exp_term else pd.DataFrame()
        sf_sold = load_sf_export(exp_sold, ["Deal Loan Number", "Sold Loan: Sold To"]) if exp_sold else pd.DataFrame()
        sf_term_asset = load_sf_export(exp_term_asset, list(TERM_ASSET_FROM_TERM_ASSET_REPORT.values()) + ["Deal Loan Number"]) if exp_term_asset else pd.DataFrame()

    bridge_asset = pd.DataFrame()
    bridge_loan = pd.DataFrame()
    term_loan = pd.DataFrame()
    term_asset = pd.DataFrame()

    if need_bridge_asset:
        with st.spinner("Building Bridge Asset..."):
            bridge_asset = build_bridge_asset(sf_bridge, sf_dnl, sf_val, sf_am, sf_arm, serv_lookup, upb_col)

    if need_bridge_loan:
        with st.spinner("Building Bridge Loan..."):
            bridge_loan = build_bridge_loan(bridge_asset, sf_bridge, upb_col, as_of, prev_maps)

    if need_term_loan:
        with st.spinner("Building Term Loan..."):
            term_loan = build_term_loan(sf_term, sf_sold, sf_am, sf_arm, serv_lookup, upb_col, prev_maps)

    if need_term_asset:
        with st.spinner("Building Term Asset..."):
            term_asset = build_term_asset(sf_term_asset, term_loan, upb_col)

    st.subheader("Diagnostics")
    if not bridge_asset.empty and "_loan_upb" in bridge_asset.columns:
        st.write(f"Bridge Asset servicer-join match rate: {bridge_asset['_loan_upb'].notna().mean():.1%}")
    if not term_loan.empty and upb_col in term_loan.columns:
        st.write(f"Term Loan servicer-join match rate: {term_loan[upb_col].notna().mean():.1%}")

    tmpl_bytes = template_upload.getvalue()

    def _sheet_df(sheet_name: str) -> pd.DataFrame:
        return {
            "Bridge Asset": bridge_asset,
            "Bridge Loan": bridge_loan,
            "Term Loan": term_loan,
            "Term Asset": term_asset,
        }.get(sheet_name, pd.DataFrame())

    outputs: List[Tuple[str, bytes]] = []

    if single_sheet_files:
        for sheet_name in sheets_requested:
            df_sheet = _sheet_df(sheet_name)
            if df_sheet.empty:
                st.warning(f"No rows for {sheet_name}.")
                continue

            wb_f, wb_v = build_single_sheet_workbook(tmpl_bytes, sheet_name)
            ws = wb_f[sheet_name]
            ws_v = wb_v[sheet_name]

            hdr = header_tuples_from_ws(ws_v, header_row=4)
            hdr = [(c, normalize_header_name(h, upb_col)) for (c, h) in hdr]

            # Update row-3 as-of cells and UPB header
            update_as_of_cells(ws, ws_v, as_of, header_row=4, date_row=3)
            set_upb_header_cell(ws, ws_v, upb_col, header_row=4)

            # Term Asset sheet has cross-sheet formulas; overwrite those columns when exporting as a single sheet
            force_headers: Set[str] = set()
            if sheet_name == "Term Asset":
                force_headers = {upb_col, "CPP JV"}

            fcols = formula_col_indices(ws, start_row=5, header_row=4)
            write_df_to_sheet_preserve_formulas(ws, df_sheet, hdr, fcols, start_row=5, force_overwrite_headers=force_headers)

            out_buf = BytesIO()
            wb_f.save(out_buf)
            out_buf.seek(0)
            outputs.append((sheet_name, out_buf.getvalue()))

        st.success("Built requested sheet file(s).")
        for sheet_name, bts in outputs:
            st.download_button(
                f"Download {sheet_name}",
                data=bts,
                file_name=f"Active Loans - {sheet_name} - {as_of.isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    else:
        wb_f = load_workbook(BytesIO(tmpl_bytes), data_only=False)
        wb_v = load_workbook(BytesIO(tmpl_bytes), data_only=True)

        for sheet_name in sheets_requested:
            if sheet_name not in wb_f.sheetnames:
                continue
            df_sheet = _sheet_df(sheet_name)
            if df_sheet.empty:
                continue

            ws = wb_f[sheet_name]
            ws_v = wb_v[sheet_name]

            hdr = header_tuples_from_ws(ws_v, header_row=4)
            hdr = [(c, normalize_header_name(h, upb_col)) for (c, h) in hdr]

            update_as_of_cells(ws, ws_v, as_of, header_row=4, date_row=3)

            # Update UPB header on the "source" sheets (others reference via formulas)
            if sheet_name in ("Bridge Asset", "Term Loan"):
                set_upb_header_cell(ws, ws_v, upb_col, header_row=4)

            fcols = formula_col_indices(ws, start_row=5, header_row=4)
            write_df_to_sheet_preserve_formulas(ws, df_sheet, hdr, fcols, start_row=5)

        out_buf = BytesIO()
        wb_f.save(out_buf)
        out_buf.seek(0)

        st.success("Built workbook.")
        st.download_button(
            "Download Active Loans Output",
            data=out_buf.getvalue(),
            file_name=f"Active Loans_{as_of.isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
