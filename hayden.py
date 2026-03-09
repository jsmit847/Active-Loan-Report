import re
from io import BytesIO
from datetime import date, datetime

import numpy as np
import pandas as pd
import streamlit as st
from openpyxl import load_workbook


# =============================================================================
# SALESFORCE REPORTS (IDs you gave)
# =============================================================================
REPORTS = {
    "bridge_maturity": ("Bridge Maturity Report v3", "00O5b000005s0aFEAQ"),
    "do_not_lend":     ("Do Not Lend",              "00OPK000005tu3V2AQ"),
    "valuation":       ("Valuation v4 Report",      "00OPK000003PXS52AO"),
    "am_assignments":  ("AM Assignments Report",    "00OPK00000257Kf2AI"),
    "active_rm":       ("Active RM Report",         "00OPK000005QLAn2AO"),
    "term_export":     ("Term Data Export",         "00OPK000004p7Uz2AI"),
    "sold_term":       ("Sold Term Loans",          "00OPK0000030QFJ2A2"),
    "term_asset":      ("Term Asset Level - By Deal","00OPK00000DRwy52AD"),
}


# =============================================================================
# YOUR PROVIDED MAPPINGS (Label-based; SF report outputs are label columns)
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
    "Originator": "CAF Originator",  # User.Name label in your Term Data Export series
    "Deal Intro Sub-Source": "Deal Intro Sub-Source",
    "Referral Source Account": "Referral Source Account",
    "Referral Source Contact": "Referral Source Contact",
    "AM Commentary": "Comments AM",
    # Active RM: you said SF-Deal Term Data Export; if missing we fall back to Active RM report
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
    return (s.astype("string")
              .str.strip()
              .str.replace(r"\.0$", "", regex=True)
              .str.replace(r"[^0-9A-Za-z]", "", regex=True)
              .replace({"": pd.NA}))


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


def find_upb_header(cols):
    # finds "2/28 UPB" / "3/2 UPB" etc
    for c in cols:
        if isinstance(c, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", c):
            return c
    # fallback: any column that ends with UPB
    for c in cols:
        if isinstance(c, str) and c.strip().upper().endswith("UPB"):
            return c
    return None


def make_upb_header(as_of: date) -> str:
    return f"{as_of.month}/{as_of.day} UPB"


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
# SALESFORCE (YOU PLUG THIS IN)
# =============================================================================
def get_sf_session():
    """
    Return a simple_salesforce.Salesforce session object as `sf`.
    Must support: sf.restful(path, method="GET")
    """
    raise RuntimeError("Implement get_sf_session() with your Salesforce auth (OAuth/session).")


def get_report_metadata(sf, report_id: str) -> dict:
    return sf.restful(f"analytics/reports/{report_id}", method="GET")


def run_report_page(sf, report_id: str, page: int, page_size: int) -> dict:
    return sf.restful(
        f"analytics/reports/{report_id}?includeDetails=true&pageSize={page_size}&page={page}",
        method="GET"
    )


def report_json_to_df(report_json: dict) -> pd.DataFrame:
    rm = report_json.get("reportMetadata") or {}
    em = report_json.get("reportExtendedMetadata") or {}
    colinfo = em.get("detailColumnInfo") or {}
    detail_cols = rm.get("detailColumns") or []

    labels = []
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
        data_rows.append(vals[:len(labels)])

    df = pd.DataFrame(data_rows, columns=labels)

    # avoid duplicate-column headaches: if duplicates exist, keep first and suffix the rest
    if df.columns.duplicated().any():
        seen = {}
        new_cols = []
        for c in df.columns:
            if c not in seen:
                seen[c] = 1
                new_cols.append(c)
            else:
                seen[c] += 1
                new_cols.append(f"{c} ({seen[c]})")
        df.columns = new_cols

    return df


def run_report_all_rows(sf, report_id: str, page_size: int = 2000, max_pages: int = 5000) -> pd.DataFrame:
    meta = get_report_metadata(sf, report_id)
    total_rows = (meta.get("attributes") or {}).get("reportTotalRows") or (meta.get("reportMetadata") or {}).get("reportTotalRows")

    chunks = []
    page = 0
    total_seen = 0

    while page < max_pages:
        js = run_report_page(sf, report_id, page=page, page_size=page_size)
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
def sniff_excel_header_row(file_bytes: bytes, required_cols: set[str], max_scan_rows: int = 25) -> int | None:
    # scan first max_scan_rows rows; find row that contains all required columns
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


def date_from_filename(name: str) -> date | None:
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


def parse_servicer_upload(upload) -> pd.DataFrame:
    name = upload.name
    b = upload.getvalue()

    # CHL CSV
    if name.lower().endswith(".csv"):
        df = pd.read_csv(BytesIO(b))
        req = {"Servicer Loan ID", "UPB"}
        if not req.issubset(set(df.columns)):
            raise ValueError(f"CSV doesn't look like CHL Streamline (missing {req - set(df.columns)}).")
        out = pd.DataFrame({
            "servicer": "CHL",
            "servicer_id": norm_id_series(df["Servicer Loan ID"]),
            "upb": df["UPB"].apply(money_to_float),
            "suspense": np.nan,
            "next_payment_date": df.get("Next Due Date", pd.Series([None]*len(df))).apply(to_dt),
            "maturity_date": df.get("Current Maturity Date", pd.Series([None]*len(df))).apply(to_dt),
            "status": df.get("Performing Status", pd.Series([None]*len(df))).astype("string"),
        })
        d = date_from_filename(name)
        out["as_of"] = pd.to_datetime(d) if d else pd.NaT
        return out.dropna(subset=["servicer_id"])

    # Excel types detected by required columns
    checks = [
        ("Statebridge", {"Loan Number", "Current UPB", "Due Date", "Maturity Date", "Loan Status"}),
        ("Berkadia",    {"BCM Loan#", "Principal Balance", "Next Payment Due Date", "Maturity Date"}),
        ("FCI",         {"Account", "Current Balance", "Next Due Date", "Maturity Date", "Status"}),
        ("Midland",     {"ServicerLoanNumber", "UPB$", "NextPaymentDate", "MaturityDate", "ServicerLoanStatus"}),
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
        raise ValueError("Could not detect servicer file type from columns (Statebridge/Berkadia/FCI/Midland/CHL).")

    # read the sheet using that header row
    # pandas header is 0-indexed
    df = pd.read_excel(BytesIO(b), header=header_row - 1)

    d = date_from_filename(name)
    as_of = pd.to_datetime(d) if d else pd.NaT

    if detected == "Statebridge":
        out = pd.DataFrame({
            "servicer": "Statebridge",
            "servicer_id": norm_id_series(df["Loan Number"]),
            "upb": pd.to_numeric(df["Current UPB"], errors="coerce"),
            "suspense": pd.to_numeric(df.get("Unapplied Balance", np.nan), errors="coerce"),
            "next_payment_date": df["Due Date"].apply(to_dt),
            "maturity_date": df["Maturity Date"].apply(to_dt),
            "status": df["Loan Status"].astype("string"),
            "as_of": df.get("Date", pd.Series([as_of]*len(df))).apply(to_dt).fillna(as_of),
        })
        return out.dropna(subset=["servicer_id"])

    if detected == "Berkadia":
        out = pd.DataFrame({
            "servicer": "Berkadia",
            "servicer_id": norm_id_series(df["BCM Loan#"]),
            "upb": pd.to_numeric(df["Principal Balance"], errors="coerce"),
            "suspense": pd.to_numeric(df.get("Suspense Balance", np.nan), errors="coerce"),
            "next_payment_date": df.get("Next Payment Due Date", pd.Series([None]*len(df))).apply(to_dt),
            "maturity_date": df.get("Maturity Date", pd.Series([None]*len(df))).apply(to_dt),
            "status": df.get("B/T", pd.Series([None]*len(df))).astype("string"),
            "as_of": as_of,
        })
        return out.dropna(subset=["servicer_id"])

    if detected == "FCI":
        out = pd.DataFrame({
            "servicer": "FCI",
            "servicer_id": norm_id_series(df["Account"]),
            "upb": pd.to_numeric(df["Current Balance"], errors="coerce"),
            "suspense": pd.to_numeric(df.get("Suspense Pmt.", np.nan), errors="coerce"),
            "next_payment_date": df.get("Next Due Date", pd.Series([None]*len(df))).apply(to_dt),
            "maturity_date": df.get("Maturity Date", pd.Series([None]*len(df))).apply(to_dt),
            "status": df.get("Status", pd.Series([None]*len(df))).astype("string"),
            "as_of": as_of,
        })
        return out.dropna(subset=["servicer_id"])

    if detected == "Midland":
        raw = df["ServicerLoanNumber"].astype("string").str.strip()
        raw = raw.str.replace(r"COM$", "", regex=True)
        raw = raw.str.replace(r"[^0-9A-Za-z]", "", regex=True).str.lstrip("0")
        out = pd.DataFrame({
            "servicer": "Midland",
            "servicer_id": raw.replace({"": pd.NA}),
            "upb": df["UPB$"].apply(money_to_float),
            "suspense": np.nan,
            "next_payment_date": df["NextPaymentDate"].apply(to_dt),
            "maturity_date": df["MaturityDate"].apply(to_dt),
            "status": df["ServicerLoanStatus"].astype("string"),
            "as_of": df.get("ReportDate", pd.Series([as_of]*len(df))).apply(to_dt).fillna(as_of),
        })
        return out.dropna(subset=["servicer_id"])

    raise ValueError("Unhandled servicer type.")


def build_servicer_lookup(servicer_uploads: list) -> tuple[pd.DataFrame, date]:
    frames = []
    asof_candidates = []

    for f in servicer_uploads:
        df = parse_servicer_upload(f)
        frames.append(df)
        dmax = pd.to_datetime(df["as_of"], errors="coerce").max()
        if pd.notna(dmax):
            asof_candidates.append(dmax.date())

    lookup = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["servicer","servicer_id","upb","suspense","next_payment_date","maturity_date","status","as_of"]
    )

    # latest row per servicer_id within each servicer
    if not lookup.empty:
        lookup = lookup.sort_values("as_of").drop_duplicates(["servicer","servicer_id"], keep="last")

    # choose "current date for UPB" default = max as_of found; fallback today
    as_of = max(asof_candidates) if asof_candidates else date.today()
    return lookup, as_of


# =============================================================================
# LAST WEEK REPORT CARRY-FORWARD (REO DATE + optional manual columns)
# =============================================================================
def read_tab_df_from_active_loans(file_bytes: bytes, sheet: str) -> pd.DataFrame:
    # header is row 4 => pandas header=3
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=3)
    # drop fully empty rows
    df = df.dropna(how="all")
    # normalize column names spacing
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_prev_maps(prev_bytes: bytes) -> dict:
    out = {}

    # Term Loan REO carry-forward
    try:
        tl = read_tab_df_from_active_loans(prev_bytes, "Term Loan")
        if "Deal Number" in tl.columns and "REO Date" in tl.columns:
            tmp = tl[["Deal Number","REO Date"]].copy()
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
def build_bridge_asset(sf_spine: pd.DataFrame,
                       sf_dnl: pd.DataFrame,
                       sf_val: pd.DataFrame,
                       sf_am: pd.DataFrame,
                       sf_arm: pd.DataFrame,
                       serv_lookup: pd.DataFrame,
                       upb_col: str,
                       as_of: date) -> pd.DataFrame:
    out = pd.DataFrame()

    # map from Bridge Maturity
    for col, label in BRIDGE_ASSET_FROM_BRIDGE_MATURITY.items():
        out[col] = sf_spine[label] if label in sf_spine.columns else None

    # keys
    out["_deal_key"] = norm_id_series(out["Deal Number"])
    out["_serv_id_key"] = norm_id_series(out["Servicer ID"])
    out["_asset_key"] = norm_id_series(out["Asset ID"])

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
        # rename valuation labels -> template columns
        for tcol, vlabel in BRIDGE_ASSET_FROM_VALUATION.items():
            if vlabel in out.columns:
                out[tcol] = out[vlabel]
                out = out.drop(columns=[vlabel], errors="ignore")

    # AM Assignments pivot by Deal
    if not sf_am.empty and "Deal Loan Number" in sf_am.columns:
        am = sf_am.copy()
        am["_deal_key"] = norm_id_series(am["Deal Loan Number"])
        am["_dt"] = pd.to_datetime(am.get("Date Assigned"), errors="coerce")
        am = am.sort_values(["_deal_key", "Team Role", "_dt"]).drop_duplicates(["_deal_key","Team Role"], keep="last")

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

    # Active RM (CAF Originator) by Deal
    if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns:
        arm = sf_arm.copy()
        arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
        if "CAF Originator" in arm.columns:
            arm = arm[["_deal_key","CAF Originator"]].drop_duplicates("_deal_key")
            out = out.merge(arm, on="_deal_key", how="left")
            out["Active RM"] = out["CAF Originator"]
            out = out.drop(columns=["CAF Originator"], errors="ignore")

    # Servicer lookup join (by Servicer ID)
    if not serv_lookup.empty:
        s = serv_lookup.copy()
        s = s.dropna(subset=["servicer_id"]).copy()
        out = out.merge(
            s.rename(columns={
                "servicer_id": "_serv_id_key",
                "servicer": "Servicer",
                "upb": "_loan_upb",
                "suspense": "_loan_suspense",
                "next_payment_date": "Next Payment Date",
                "maturity_date": "Servicer Maturity Date",
                "status": "Servicer Status",
            })[["_serv_id_key","Servicer","_loan_upb","_loan_suspense","Next Payment Date","Servicer Maturity Date","Servicer Status"]],
            on="_serv_id_key",
            how="left"
        )

        # Allocate loan-level UPB across assets.
        # Best guess from your current workbook behavior: allocate proportional to SF "Current UPB" (Property__c.Current_UPB__c) if present; else equal split.
        w = pd.to_numeric(sf_spine.get("Current UPB", pd.Series([np.nan]*len(out))), errors="coerce")
        out["_w"] = w
        out["_w_sum"] = out.groupby("_serv_id_key")["_w"].transform("sum")
        out["_n_in_loan"] = out.groupby("_serv_id_key")["_serv_id_key"].transform("size").replace({0: np.nan})

        out[upb_col] = np.where(
            out["_w_sum"].fillna(0) > 0,
            out["_loan_upb"] * (out["_w"] / out["_w_sum"]),
            out["_loan_upb"] / out["_n_in_loan"]
        )

        # Allocate suspense similarly (keep totals consistent)
        out["Suspense Balance"] = np.where(
            out["_w_sum"].fillna(0) > 0,
            out["_loan_suspense"] * (out["_w"] / out["_w_sum"]),
            out["_loan_suspense"] / out["_n_in_loan"]
        )

    # SF Funded Amount (best guess): prefer "Approved Advance Amount Funded" if present; else sum of known funded buckets
    if "Approved Advance Amount Funded" in sf_spine.columns:
        out["SF Funded Amount"] = pd.to_numeric(sf_spine["Approved Advance Amount Funded"], errors="coerce")
    else:
        out["SF Funded Amount"] = (
            pd.to_numeric(out.get("Initial Disbursement Funded", 0), errors="coerce").fillna(0) +
            pd.to_numeric(out.get("Renovation Holdback Funded", 0), errors="coerce").fillna(0) +
            pd.to_numeric(out.get("Interest Allocation Funded", 0), errors="coerce").fillna(0)
        )

    # Portfolio / Segment / Strategy Grouping are calc fields (keep blank unless you want heuristics)
    if "Portfolio" not in out.columns:
        out["Portfolio"] = ""
    if "Segment" not in out.columns:
        out["Segment"] = ""
    if "Strategy Grouping" not in out.columns:
        out["Strategy Grouping"] = ""

    # normalize Y/N for Is Special Asset
    if "Is Special Asset (Y/N)" in out.columns:
        out["Is Special Asset (Y/N)"] = out["Is Special Asset (Y/N)"].fillna(False).map(lambda x: "Y" if bool(x) else "")

    return out


# =============================================================================
# BUILD: TERM LOAN
# =============================================================================
def build_term_loan(sf_term: pd.DataFrame,
                    sf_sold: pd.DataFrame,
                    sf_am: pd.DataFrame,
                    sf_arm: pd.DataFrame,
                    serv_lookup: pd.DataFrame,
                    upb_col: str,
                    as_of: date,
                    prev_maps: dict) -> pd.DataFrame:
    out = pd.DataFrame()

    # base from Term Data Export
    for col, label in TERM_LOAN_FROM_TERM_EXPORT.items():
        out[col] = sf_term[label] if label in sf_term.columns else None

    out["_deal_key"] = norm_id_series(out["Deal Number"])

    # Do Not Lend -> Y/N
    if "Do Not Lend (Y/N)" in out.columns:
        out["Do Not Lend (Y/N)"] = out["Do Not Lend (Y/N)"].fillna(False).map(lambda x: "Y" if bool(x) else "")

    # Loan Buyer from Sold Term Loans
    if not sf_sold.empty and "Deal Loan Number" in sf_sold.columns:
        sold = sf_sold.copy()
        sold["_deal_key"] = norm_id_series(sold["Deal Loan Number"])
        if "Sold Loan: Sold To" in sold.columns:
            sold = sold[["_deal_key","Sold Loan: Sold To"]].drop_duplicates("_deal_key")
            out = out.merge(sold, on="_deal_key", how="left")
            out["Loan Buyer"] = out["Sold Loan: Sold To"]
            out = out.drop(columns=["Sold Loan: Sold To"], errors="ignore")

    # Servicer ID + Servicer: best guess for term = use Term Export fields if present, else fill from servicer lookup
    if "Servicer Commitment Id" in sf_term.columns:
        out["Servicer ID"] = sf_term["Servicer Commitment Id"]
    if "Servicer Name" in sf_term.columns:
        out["Servicer"] = sf_term["Servicer Name"]

    out["_serv_id_key"] = norm_id_series(out.get("Servicer ID", pd.Series([None]*len(out))))

    # Active RM: if Term Data Export has it, use it; else fallback to Active RM report CAF Originator
    if "Active RM" in sf_term.columns:
        out["Active RM"] = sf_term["Active RM"]
    else:
        out["Active RM"] = ""

        if not sf_arm.empty and "Deal Loan Number" in sf_arm.columns and "CAF Originator" in sf_arm.columns:
            arm = sf_arm.copy()
            arm["_deal_key"] = norm_id_series(arm["Deal Loan Number"])
            arm = arm[["_deal_key","CAF Originator"]].drop_duplicates("_deal_key")
            out = out.merge(arm, on="_deal_key", how="left")
            out["Active RM"] = out["CAF Originator"].fillna("")
            out = out.drop(columns=["CAF Originator"], errors="ignore")

    # Asset Manager from AM Assignments (role = Asset Manager; take Team Member Name)
    if not sf_am.empty and "Deal Loan Number" in sf_am.columns:
        am = sf_am.copy()
        am["_deal_key"] = norm_id_series(am["Deal Loan Number"])
        am["_dt"] = pd.to_datetime(am.get("Date Assigned"), errors="coerce")
        am = am.sort_values(["_deal_key","Team Role","_dt"]).drop_duplicates(["_deal_key","Team Role"], keep="last")
        # pick Asset Manager only
        if "Team Role" in am.columns and "Team Member Name" in am.columns:
            am1 = am[am["Team Role"].astype("string").str.strip().eq("Asset Manager")][["_deal_key","Team Member Name"]]
            am1 = am1.drop_duplicates("_deal_key")
            out = out.merge(am1, on="_deal_key", how="left")
            out["Asset Manager"] = out["Team Member Name"].fillna("")
            out = out.drop(columns=["Team Member Name"], errors="ignore")
        else:
            out["Asset Manager"] = ""
    else:
        out["Asset Manager"] = ""

    # Servicer join for UPB + dates/status
    if not serv_lookup.empty:
        s = serv_lookup.dropna(subset=["servicer_id"]).copy()
        # Midland normalization: compare also lstrip zeros
        out["_serv_id_key_mid"] = out["_serv_id_key"].astype("string").str.lstrip("0")

        s2 = s.rename(columns={
            "servicer_id": "_sid",
            "servicer": "_servicer_file",
            "upb": upb_col,
            "next_payment_date": "Next Payment Date",
            "maturity_date": "Maturity Date",
            "status": "Servicer Status",
        })[["_sid","_servicer_file",upb_col,"Next Payment Date","Maturity Date","Servicer Status"]]

        out = out.merge(s2, left_on=out["_serv_id_key_mid"], right_on="_sid", how="left").drop(columns=["_sid","key_0"], errors="ignore")

        # fill Servicer from file if missing
        if "Servicer" not in out.columns:
            out["Servicer"] = out["_servicer_file"]
        else:
            out["Servicer"] = out["Servicer"].fillna(out["_servicer_file"])
        out = out.drop(columns=["_servicer_file"], errors="ignore")

    # REO Date carry-forward from last week's report (or user fills manually)
    out["REO Date"] = ""
    if "term_loan_reo" in prev_maps:
        reo = prev_maps["term_loan_reo"][["_deal_key","REO Date"]].copy()
        out = out.merge(reo, on="_deal_key", how="left", suffixes=("","_prev"))
        out["REO Date"] = out["REO Date_prev"].fillna("")
        out = out.drop(columns=["REO Date_prev"], errors="ignore")

    # Days Past Due + DQ Status from Next Payment Date vs as_of
    out["Days Past Due"] = np.nan
    out["DQ Status"] = ""
    if "Next Payment Date" in out.columns:
        npd = pd.to_datetime(out["Next Payment Date"], errors="coerce")
        asof_dt = pd.to_datetime(as_of)
        out["Days Past Due"] = (asof_dt - npd).dt.days
        out["Days Past Due"] = out["Days Past Due"].clip(lower=0)
        out["DQ Status"] = out["Days Past Due"].apply(dq_bucket)

    # Portfolio/Segment are N/A per your notes (leave blank)
    if "Portfolio" not in out.columns:
        out["Portfolio"] = ""
    if "Segment" not in out.columns:
        out["Segment"] = ""

    return out


# =============================================================================
# BUILD: TERM ASSET (ALA-weight UPB from Term Loan)
# =============================================================================
def build_term_asset(sf_term_asset: pd.DataFrame,
                     term_loan: pd.DataFrame,
                     upb_col: str) -> pd.DataFrame:
    out = pd.DataFrame()

    for col, label in TERM_ASSET_FROM_TERM_ASSET_REPORT.items():
        out[col] = sf_term_asset[label] if label in sf_term_asset.columns else None

    out["_deal_key"] = norm_id_series(out["Deal Number"])
    out["CPP JV"] = ""  # N/A per your note

    # allocate UPB from Term Loan across assets by ALA
    tl = term_loan.copy()
    tl["_deal_key"] = norm_id_series(tl["Deal Number"])
    if upb_col in tl.columns:
        tl = tl[["_deal_key", upb_col]].drop_duplicates("_deal_key")
        out = out.merge(tl, on="_deal_key", how="left")

        ala = pd.to_numeric(out.get("Property ALA", np.nan), errors="coerce")
        ala_sum = ala.groupby(out["_deal_key"]).transform("sum")
        out[upb_col] = np.where(ala_sum > 0, out[upb_col] * (ala / ala_sum), out[upb_col])

    return out


# =============================================================================
# BUILD: BRIDGE LOAN (GUESS LOGIC: roll-up Bridge Asset)
# =============================================================================
def build_bridge_loan(bridge_asset: pd.DataFrame,
                      sf_spine: pd.DataFrame,
                      upb_col: str,
                      as_of: date,
                      prev_maps: dict) -> pd.DataFrame:
    ba = bridge_asset.copy()

    # roll-up per Deal
    g = ba.groupby("_deal_key", dropna=True)

    out = pd.DataFrame({
        "Deal Number": g["Deal Number"].first(),
        "Portfolio": g.get_group(next(iter(g.groups))).get("Portfolio", pd.Series([""])).iloc[0] if len(g.groups) else "",
        "Loan Buyer": g["Loan Buyer"].first(),
        "Financing": g["Financing"].first(),
        "Servicer ID": g["Servicer ID"].first(),
        "Servicer": g.get_group(next(iter(g.groups))).get("Servicer", pd.Series([""])).iloc[0] if len(g.groups) else "",
        "Deal Name": g["Deal Name"].first(),
        "Borrower Name": g["Borrower Entity"].first(),
        "Account ": g["Account Name"].first(),  # note the trailing space in template
        "Do Not Lend (Y/N)": g["Do Not Lend (Y/N)"].max(),
        "Primary Contact": g["Primary Contact"].first(),
        "Number of Assets": g["Asset ID"].nunique(),
        "# of Units": pd.to_numeric(g["# of Units"].sum(min_count=1), errors="coerce"),
        "State(s)": g["State"].apply(lambda s: ", ".join(sorted({str(x).strip() for x in s.dropna() if str(x).strip() != ""}))),
        "Origination Date": g["Origination Date"].min(),
        "Last Funding Date": g["Last Funding Date"].max(),
        "Original Maturity Date": g["Original Loan Maturity date"].first(),
        "Current Maturity Date": g["Current Loan Maturity date"].first(),
        "Next Payment Date": g["Next Payment Date"].min() if "Next Payment Date" in ba.columns else "",
        "Suspense Balance": pd.to_numeric(g["Suspense Balance"].sum(min_count=1), errors="coerce") if "Suspense Balance" in ba.columns else np.nan,
        "Active Funded Amount": pd.to_numeric(g["SF Funded Amount"].sum(min_count=1), errors="coerce"),
        upb_col: pd.to_numeric(g[upb_col].sum(min_count=1), errors="coerce") if upb_col in ba.columns else np.nan,
        "Most Recent Valuation Date": pd.to_datetime(g.get("Updated Valuation Date", pd.Series(dtype="datetime64[ns]")).max(), errors="coerce") if "Updated Valuation Date" in ba.columns else "",
        "Most Recent As-Is Value": pd.to_numeric(g.get("Updated As-Is Value", pd.Series(dtype="float")).sum(min_count=1), errors="coerce") if "Updated As-Is Value" in ba.columns else np.nan,
        "Most Recent ARV": pd.to_numeric(g.get("Updated ARV", pd.Series(dtype="float")).sum(min_count=1), errors="coerce") if "Updated ARV" in ba.columns else np.nan,
        "Initial Disbursement Funded": pd.to_numeric(g.get("Initial Disbursement Funded", pd.Series(dtype="float")).sum(min_count=1), errors="coerce"),
        "Renovation Holdback": pd.to_numeric(g.get("Renovation Holdback", pd.Series(dtype="float")).sum(min_count=1), errors="coerce"),
        "Renovation HB Funded": pd.to_numeric(g.get("Renovation Holdback Funded", pd.Series(dtype="float")).sum(min_count=1), errors="coerce"),
        "Renovation HB Remaining": pd.to_numeric(g.get("Renovation Holdback Remaining", pd.Series(dtype="float")).sum(min_count=1), errors="coerce"),
        "Interest Allocation": pd.to_numeric(g.get("Interest Allocation", pd.Series(dtype="float")).sum(min_count=1), errors="coerce"),
        "Interest Allocation Funded": pd.to_numeric(g.get("Interest Allocation Funded", pd.Series(dtype="float")).sum(min_count=1), errors="coerce"),
        "Loan Stage": g["Loan Stage"].first(),
        "Segment": g.get_group(next(iter(g.groups))).get("Segment", pd.Series([""])).iloc[0] if len(g.groups) else "",
        "Product Type": g["Product Type"].first() if "Product Type" in ba.columns else "",
        "Product Sub Type": g["Product Sub-Type"].first() if "Product Sub-Type" in ba.columns else "",
        "Transaction Type": g["Transaction Type"].first() if "Transaction Type" in ba.columns else "",
        "Project Strategy": g["Project Strategy"].first() if "Project Strategy" in ba.columns else "",
        "Strategy Grouping": g.get_group(next(iter(g.groups))).get("Strategy Grouping", pd.Series([""])).iloc[0] if len(g.groups) else "",
        "CV Originator": "",   # can be filled if you add it from SF
        "Active RM": g.get_group(next(iter(g.groups))).get("Active RM", pd.Series([""])).iloc[0] if len(g.groups) else "",
        "Deal Intro Sub-Source": g["Deal Intro Sub-Source"].first() if "Deal Intro Sub-Source" in ba.columns else "",
        "Referral Source Account": g["Referral Source Account"].first() if "Referral Source Account" in ba.columns else "",
        "Referral Source Contact": g["Referral Source Contact"].first() if "Referral Source Contact" in ba.columns else "",
        "3/31 NPL": "",
        "Needs NPL Value": "",
        "Special Focus (Y/N)": np.where(g.get("Is Special Asset (Y/N)", pd.Series([""])).max().astype(str).eq("Y"), "Y", "") if "Is Special Asset (Y/N)" in ba.columns else "",
        "Asset Manager 1": g.get("Asset Manager 1", pd.Series([""])).first() if "Asset Manager 1" in ba.columns else "",
        "AM 1 Assigned Date": g.get("AM 1 Assigned Date", pd.Series([""])).first() if "AM 1 Assigned Date" in ba.columns else "",
        "Asset Manager 2": g.get("Asset Manager 2", pd.Series([""])).first() if "Asset Manager 2" in ba.columns else "",
        "AM 2 Assigned Date": g.get("AM 2 Assigned Date", pd.Series([""])).first() if "AM 2 Assigned Date" in ba.columns else "",
        "Construction Mgr.": g.get("Construction Mgr.", pd.Series([""])).first() if "Construction Mgr." in ba.columns else "",
        "CM Assigned Date": g.get("CM Assigned Date", pd.Series([""])).first() if "CM Assigned Date" in ba.columns else "",
    }).reset_index(drop=True)

    # Days Past Due & Loan Level Delinquency guessed from Next Payment Date
    out["Days Past Due"] = np.nan
    out["Loan Level Delinquency"] = ""
    if "Next Payment Date" in out.columns:
        npd = pd.to_datetime(out["Next Payment Date"], errors="coerce")
        asof_dt = pd.to_datetime(as_of)
        out["Days Past Due"] = (asof_dt - npd).dt.days.clip(lower=0)
        out["Loan Level Delinquency"] = out["Days Past Due"].apply(dq_bucket)

    # Loan Commitment + Remaining Commitment from SF (Bridge Maturity report has these labels)
    # Best-effort: if present in sf_spine, join by Deal Loan Number
    if "Deal Loan Number" in sf_spine.columns:
        deal = sf_spine.copy()
        deal["_deal_key"] = norm_id_series(deal["Deal Loan Number"])
        keep = ["_deal_key"]
        if "Loan Commitment" in deal.columns:
            keep.append("Loan Commitment")
        if "Total Remaining Commitment Amount" in deal.columns:
            keep.append("Total Remaining Commitment Amount")
        if len(keep) > 1:
            deal = deal[keep].drop_duplicates("_deal_key")
            out = out.merge(deal, on="_deal_key", how="left")
            # map to template names if template uses different header
            if "Remaining Commitment" not in out.columns and "Total Remaining Commitment Amount" in out.columns:
                out["Remaining Commitment"] = out["Total Remaining Commitment Amount"]

    # carry-forward optional manual columns from last week (if present)
    if "bridge_loan_manual" in prev_maps:
        man = prev_maps["bridge_loan_manual"].copy()
        out = out.merge(man, on="_deal_key", how="left", suffixes=("","_prev"))
        for c in ["State(s)", "Loan Level Delinquency", "Special Focus (Y/N)"]:
            if f"{c}_prev" in out.columns:
                out[c] = out[f"{c}_prev"].fillna(out.get(c, ""))
                out = out.drop(columns=[f"{c}_prev"], errors="ignore")

    return out.drop(columns=["_deal_key"], errors="ignore")


# =============================================================================
# EXCEL OUTPUT (template-based; updates dynamic UPB header)
# =============================================================================
def get_headers(ws, header_row: int = 4) -> list[str]:
    vals = [c.value for c in next(ws.iter_rows(min_row=header_row, max_row=header_row))]
    return [str(v).strip() if v is not None else "" for v in vals]


def replace_upb_header(ws, new_upb_header: str, header_row: int = 4):
    cols = [c.value for c in next(ws.iter_rows(min_row=header_row, max_row=header_row))]
    for i, v in enumerate(cols, start=1):
        if isinstance(v, str) and re.search(r"\b\d{1,2}/\d{1,2}\s*UPB\b", v):
            ws.cell(header_row, i).value = new_upb_header
            return
        if isinstance(v, str) and v.strip() == "2/28 UPB":  # fallback
            ws.cell(header_row, i).value = new_upb_header
            return


def clear_sheet_data(ws, start_row: int = 5):
    # keep formatting, only clear values in existing rows
    max_r = ws.max_row
    max_c = ws.max_column
    if max_r < start_row:
        return
    for r in range(start_row, max_r + 1):
        for c in range(1, max_c + 1):
            ws.cell(r, c).value = None


def write_df_to_sheet(ws, df: pd.DataFrame, header_row: int = 4, start_row: int = 5):
    headers = get_headers(ws, header_row=header_row)
    headers = [h for h in headers if h != ""]
    df_out = df.copy()
    for h in headers:
        if h not in df_out.columns:
            df_out[h] = None
    df_out = df_out[headers]

    clear_sheet_data(ws, start_row=start_row)

    for r_idx, row in enumerate(df_out.itertuples(index=False, name=None), start=start_row):
        for c_idx, val in enumerate(row, start=1):
            ws.cell(r_idx, c_idx).value = val


# =============================================================================
# STREAMLIT UI
# =============================================================================
st.set_page_config(page_title="Active Loans Builder", layout="wide")
st.title("Active Loans Report Builder")

template_upload = st.file_uploader("Upload Active Loans TEMPLATE (.xlsx)", type=["xlsx"])
prev_upload = st.file_uploader("Upload LAST WEEK'S Active Loans report (.xlsx) for REO Date carry-forward", type=["xlsx"])
servicer_uploads = st.file_uploader("Upload current servicer files (csv/xlsx) - any names; formats must match", type=["csv","xlsx"], accept_multiple_files=True)

use_sf = st.checkbox("Pull Salesforce via API (required for full automation)", value=True)

if st.button("Build Active Loans"):
    if not template_upload:
        st.error("Upload the template workbook first.")
        st.stop()

    # Parse last week maps
    prev_maps = {}
    if prev_upload:
        prev_maps = build_prev_maps(prev_upload.getvalue())

    # Parse servicer files
    if not servicer_uploads:
        st.error("Upload the servicer files. UPB/Next Payment/Maturity/Status come from them.")
        st.stop()

    with st.spinner("Parsing servicer files..."):
        serv_lookup, detected_asof = build_servicer_lookup(servicer_uploads)

    # user can override date label
    as_of = st.date_input("UPB 'as-of' date (controls the UPB column header)", value=detected_asof)
    upb_col = make_upb_header(as_of)

    st.write("Servicer lookup preview (standardized):")
    st.dataframe(serv_lookup.head(20), use_container_width=True)

    if not use_sf:
        st.error("This version expects Salesforce API pulls. (We can add export-upload fallback if you want.)")
        st.stop()

    # Pull Salesforce
    with st.spinner("Connecting to Salesforce..."):
        sf = get_sf_session()

    dfs = {}
    for key, (nm, rid) in REPORTS.items():
        with st.spinner(f"Pulling Salesforce report: {nm} ({rid})"):
            dfs[key] = run_report_all_rows(sf, rid, page_size=2000)

    # Build sheets
    with st.spinner("Building Bridge Asset..."):
        bridge_asset = build_bridge_asset(
            dfs["bridge_maturity"],
            dfs["do_not_lend"],
            dfs["valuation"],
            dfs["am_assignments"],
            dfs["active_rm"],
            serv_lookup,
            upb_col,
            as_of
        )

    with st.spinner("Building Term Loan..."):
        term_loan = build_term_loan(
            dfs["term_export"],
            dfs["sold_term"],
            dfs["am_assignments"],
            dfs["active_rm"],
            serv_lookup,
            upb_col,
            as_of,
            prev_maps
        )

    with st.spinner("Building Term Asset..."):
        term_asset = build_term_asset(dfs["term_asset"], term_loan, upb_col)

    with st.spinner("Building Bridge Loan (roll-up guess)..."):
        bridge_loan = build_bridge_loan(bridge_asset, dfs["bridge_maturity"], upb_col, as_of, prev_maps)

    # Diagnostics: servicer match coverage + UPB reconciliation
    st.subheader("Diagnostics")

    def match_rate(df, key_col):
        if key_col not in df.columns:
            return 0.0
        k = norm_id_series(df[key_col])
        return float(k.notna().mean())

    if "Servicer ID" in bridge_asset.columns:
        matched = bridge_asset["_loan_upb"].notna().mean() if "_loan_upb" in bridge_asset.columns else np.nan
        st.write(f"Bridge Asset servicer-join match rate: {matched:.1%}")

    if "Servicer ID" in term_loan.columns:
        matched = term_loan[upb_col].notna().mean() if upb_col in term_loan.columns else np.nan
        st.write(f"Term Loan servicer-join match rate: {matched:.1%}")

    # Bridge Asset UPB reconciliation by Servicer ID (sum asset UPB vs loan UPB)
    if "_loan_upb" in bridge_asset.columns and upb_col in bridge_asset.columns:
        rec = (bridge_asset
               .dropna(subset=["_serv_id_key"])
               .groupby("_serv_id_key")
               .agg(loan_upb=("_loan_upb","max"),
                    sum_asset_upb=(upb_col,"sum"),
                    n_assets=("Asset ID","nunique"))
               .reset_index())
        rec["diff"] = rec["sum_asset_upb"] - rec["loan_upb"]
        st.write("Bridge Asset UPB reconciliation (top diffs):")
        st.dataframe(rec.sort_values("diff", key=lambda s: s.abs(), ascending=False).head(20), use_container_width=True)

    # Write output workbook
    tmpl_bytes = template_upload.getvalue()
    wb = load_workbook(BytesIO(tmpl_bytes))

    for sheet in ["Bridge Asset", "Bridge Loan", "Term Loan", "Term Asset"]:
        if sheet in wb.sheetnames:
            replace_upb_header(wb[sheet], upb_col, header_row=4)

    # Write data
    if "Bridge Asset" in wb.sheetnames:
        write_df_to_sheet(wb["Bridge Asset"], bridge_asset, header_row=4, start_row=5)
    if "Bridge Loan" in wb.sheetnames:
        write_df_to_sheet(wb["Bridge Loan"], bridge_loan, header_row=4, start_row=5)
    if "Term Loan" in wb.sheetnames:
        write_df_to_sheet(wb["Term Loan"], term_loan, header_row=4, start_row=5)
    if "Term Asset" in wb.sheetnames:
        write_df_to_sheet(wb["Term Asset"], term_asset, header_row=4, start_row=5)

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    st.success("Built Active Loans workbook.")
    st.download_button(
        "Download Active Loans Output",
        data=out.getvalue(),
        file_name=f"Active Loans_{as_of.isoformat()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )