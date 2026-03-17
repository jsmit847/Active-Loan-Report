# =========================
# BULK API 2.0 + mapping-driven Salesforce layer
# Place this AFTER helper functions like:
# today_et, norm_id_series, id_key_no_leading_zeros, money_to_float,
# to_dt, is_reo_stage, has_any_value, _yn_from_bool_series, ensure_sf_session
# =========================

API_VERSION = "v66.0"
REFERENCE_WORKBOOK_FILENAME = "20260302 Active Loans_Bridge Asset Column Mapping.xlsx"

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

# Active RM intentionally removed here. It comes from separate dataset.
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


def show_salesforce_login_helper():
    st.info(
        "Step 1: Log in to Salesforce.\n\n"
        "Step 2: Approve access.\n\n"
        "Step 3: Click Build. This app uses the Salesforce API and Bulk API 2.0 to pull larger datasets."
    )


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


@st.cache_data(show_spinner=False)
def load_reference_workbook_tables() -> dict:
    base_dir = Path(__file__).resolve().parent
    candidates = [
        base_dir / REFERENCE_WORKBOOK_FILENAME,
        base_dir / "assets" / REFERENCE_WORKBOOK_FILENAME,
        base_dir / "templates" / REFERENCE_WORKBOOK_FILENAME,
        Path.cwd() / REFERENCE_WORKBOOK_FILENAME,
        Path("/mnt/data") / REFERENCE_WORKBOOK_FILENAME,
    ]

    path = None
    for p in candidates:
        if p.exists() and p.is_file():
            path = p
            break

    if path is None:
        return {
            "source_path": None,
            "strategy_grouping_map": {},
            "special_asset_officers": set(),
            "ssp_deal_keys": set(),
            "legacy_bridge_keys": set(),
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
