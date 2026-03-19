# Active Loan Builder patch block
# Paste these replacements into hayden.py and remove the duplicated/truncated block.

# 1) Replace downcast_numeric_frame

def downcast_numeric_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        s = out[c]
        try:
            if pd.api.types.is_integer_dtype(s):
                out.loc[:, c] = pd.to_numeric(s, errors='coerce', downcast='integer')
            elif pd.api.types.is_float_dtype(s):
                out.loc[:, c] = pd.to_numeric(s, errors='coerce', downcast='float')
        except Exception:
            pass
    return out


# 2) Add these helper functions near the other parsing helpers

def normalize_header_name(x) -> str:
    return re.sub(r'[^0-9a-z]+', '', str(x).strip().lower())


def header_lookup(columns: Sequence[str]) -> Dict[str, str]:
    return {normalize_header_name(c): c for c in columns}


def first_matching_col(df: pd.DataFrame, aliases: Sequence[str]) -> Optional[str]:
    lookup = header_lookup(df.columns)
    for alias in aliases:
        k = normalize_header_name(alias)
        if k in lookup:
            return lookup[k]
    return None


def normalize_servicer_family(val) -> str:
    s = clean_text(val).lower()
    if not s:
        return ''
    if 'berkadia' in s:
        return 'berkadia'
    if 'midland' in s:
        return 'midland'
    if 'statebridge' in s:
        return 'statebridge'
    if 'shellpoint' in s:
        return 'shellpoint'
    if 'selene' in s:
        return 'selene'
    if s == 'sps' or 'specialized' in s or 'select portfolio' in s:
        return 'sps'
    if 'fci' in s:
        return 'fci'
    return s


def fci_servicer_label_from_filename(filename: str) -> str:
    n = filename.lower()
    if '2012632' in n:
        return 'FCI 2012632'
    if '18105510' in n or '1805510' in n:
        return 'FCI v1805510'
    return 'FCI'


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
                df = df.dropna(how='all')
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
        raise ValueError('Could not find a matching header row.')

    return best


def _best_header_read_csv(file_bytes: bytes, required_alias_groups: List[List[str]], max_header_scan: int = 3):
    best = None
    best_score = -1

    for header_row in range(max_header_scan):
        try:
            df = pd.read_csv(BytesIO(file_bytes), header=header_row)
            df = df.dropna(how='all')
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
        raise ValueError('Could not find a matching CSV header row.')

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
        return pd.Series([pd.NA] * len(df), index=df.index, dtype='object')
    return df[col].astype('string')


def _series_to_id(df: pd.DataFrame, aliases: Sequence[str], transform=None) -> pd.Series:
    col = first_matching_col(df, aliases)
    if not col:
        raise ValueError('Required ID column not found.')
    s = df[col]
    return transform(s) if transform else norm_id_series(s)


def _as_of_for_df(df: pd.DataFrame, filename: str, aliases: Sequence[str]) -> date:
    col = first_matching_col(df, aliases)
    if col and df[col].notna().any():
        d = report_date_from_scalar(df[col].dropna().iloc[0])
        if d:
            return d
    return date_from_filename(filename) or today_et()


# 3) Replace detect_servicer_type + parse_servicer_bytes + build_servicer_lookup

def detect_servicer_type(filename: str) -> str:
    n = filename.lower()
    if n.endswith('.csv'):
        return 'CHL'
    if 'corevest_data_tape' in n:
        return 'CoreVest_Data_Tape'
    if 'corevestloandata' in n:
        return 'CoreVestLoanData'
    if 'midland' in n:
        return 'Midland'
    if 'fci' in n:
        return 'FCI'
    raise ValueError(
        'Could not detect servicer file type from the filename. '
        'Use one of these naming patterns: CHL, CoreVest_Data_Tape, CoreVestLoanData, FCI, Midland.'
    )


def parse_servicer_bytes(filename: str, b: bytes) -> pd.DataFrame:
    servicer_type = detect_servicer_type(filename)

    if servicer_type == 'CHL':
        df, _hdr, _score = _best_header_read_csv(
            b,
            [['Servicer Loan ID', 'Loan ID', 'Loan Number'], ['UPB', 'Principal Balance', 'Current UPB']],
        )
        servicer_col = first_matching_col(df, ['Servicing Company', 'Servicer', 'Servicer Name'])
        servicer = df[servicer_col].astype('string') if servicer_col else pd.Series(['CHL Streamline'] * len(df))
        servicer = servicer.fillna('CHL Streamline')
        servicer = servicer.where(~servicer.astype('string').str.upper().eq('FCI'), 'FCI CHL Streamline')
        out = pd.DataFrame(
            {
                'source_file': filename,
                'servicer': servicer,
                'servicer_family': servicer.map(normalize_servicer_family),
                'servicer_id': _series_to_id(df, ['Servicer Loan ID', 'Loan ID', 'Loan Number']),
                'upb': _series_to_num(df, ['UPB', 'Principal Balance', 'Current UPB']),
                'suspense': np.nan,
                'next_payment_date': _series_to_dt(df, ['Next Due Date', 'Due Date', 'Next Payment Date']),
                'maturity_date': _series_to_dt(df, ['Current Maturity Date', 'Maturity Date']),
                'status': _series_to_text(df, ['Performing Status', 'Status', 'Loan Status']),
                'as_of': pd.to_datetime(_as_of_for_df(df, filename, ['Report Date', 'As Of Date', 'Run Date'])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=['servicer_id']))

    if servicer_type == 'CoreVestLoanData':
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [['Loan Number', 'Loan No', 'BCM Loan#', 'Servicer Loan Number'], ['Current UPB', 'Principal Balance', 'UPB']],
            preferred_sheets=['loan'],
        )

        def _idfix(s: pd.Series) -> pd.Series:
            sid = norm_id_series(s).astype('string')
            return sid.apply(lambda x: x if pd.isna(x) else (x if x.startswith('0000') else f'0000{x}'))

        out = pd.DataFrame(
            {
                'source_file': filename,
                'servicer': 'Statebridge',
                'servicer_family': 'statebridge',
                'servicer_id': _idfix(df[first_matching_col(df, ['Loan Number', 'Loan No', 'Servicer Loan Number'])]),
                'upb': _series_to_num(df, ['Current UPB', 'Principal Balance', 'UPB']),
                'suspense': _series_to_num(df, ['Unapplied Balance', 'Suspense Balance', 'Suspense']),
                'next_payment_date': _series_to_dt(df, ['Due Date', 'Next Due Date', 'Next Payment Date']),
                'maturity_date': _series_to_dt(df, ['Maturity Date', 'Current Maturity Date']),
                'status': _series_to_text(df, ['Loan Status', 'Status']),
                'as_of': pd.to_datetime(_as_of_for_df(df, filename, ['Date', 'Run Date', 'Report Date', 'As Of Date'])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=['servicer_id']))

    if servicer_type == 'CoreVest_Data_Tape':
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [['BCM Loan#', 'Loan Number', 'Loan No'], ['Principal Balance', 'Current UPB', 'UPB']],
            preferred_sheets=['loan'],
        )
        out = pd.DataFrame(
            {
                'source_file': filename,
                'servicer': 'Berkadia',
                'servicer_family': 'berkadia',
                'servicer_id': _series_to_id(df, ['BCM Loan#', 'Loan Number', 'Loan No']),
                'upb': _series_to_num(df, ['Principal Balance', 'Current UPB', 'UPB']),
                'suspense': _series_to_num(df, ['Suspense Balance', 'Unapplied Balance', 'Suspense']),
                'next_payment_date': _series_to_dt(df, ['Next Payment Due Date', 'Next Due Date', 'Due Date']),
                'maturity_date': _series_to_dt(df, ['Maturity Date', 'Current Maturity Date']),
                'status': _series_to_text(df, ['Loan Status', 'Status']),
                'as_of': pd.to_datetime(_as_of_for_df(df, filename, ['Run Date', 'Date', 'Report Date', 'As Of Date'])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=['servicer_id']))

    if servicer_type == 'FCI':
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [['Account', 'Loan Number', 'Loan No'], ['Current Balance', 'Current UPB', 'UPB', 'Principal Balance']],
            preferred_sheets=['fci', 'cvmaster', 'v1805510', 'report'],
        )
        servicer = fci_servicer_label_from_filename(filename)
        out = pd.DataFrame(
            {
                'source_file': filename,
                'servicer': servicer,
                'servicer_family': 'fci',
                'servicer_id': _series_to_id(df, ['Account', 'Loan Number', 'Loan No']),
                'upb': _series_to_num(df, ['Current Balance', 'Current UPB', 'UPB', 'Principal Balance']),
                'suspense': _series_to_num(df, ['Suspense Pmt.', 'Suspense Payment', 'Suspense Balance', 'Unapplied Balance']),
                'next_payment_date': _series_to_dt(df, ['Next Due Date', 'Due Date', 'Next Payment Date']),
                'maturity_date': _series_to_dt(df, ['Maturity Date', 'Current Maturity Date']),
                'status': _series_to_text(df, ['Status', 'Loan Status']),
                'as_of': pd.to_datetime(_as_of_for_df(df, filename, ['Report Date', 'As Of Date', 'Date', 'Run Date'])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=['servicer_id']))

    if servicer_type == 'Midland':
        df, _sheet, _hdr, _score = _best_header_read_excel(
            b,
            [['ServicerLoanNumber', 'Servicer Loan Number', 'Loan Number'], ['UPB$', 'UPB', 'Current UPB', 'Principal Balance']],
            preferred_sheets=['export', 'midland', 'loan'],
        )

        def _idfix(s: pd.Series) -> pd.Series:
            raw = s.astype('string').str.strip()
            raw = raw.str.replace(r'COM$', '', regex=True)
            raw = raw.str.replace(r'[^0-9A-Za-z]', '', regex=True).str.lstrip('0')
            return raw.replace({'': pd.NA})

        out = pd.DataFrame(
            {
                'source_file': filename,
                'servicer': 'Midland',
                'servicer_family': 'midland',
                'servicer_id': _idfix(df[first_matching_col(df, ['ServicerLoanNumber', 'Servicer Loan Number', 'Loan Number'])]),
                'upb': _series_to_num(df, ['UPB$', 'UPB', 'Current UPB', 'Principal Balance']),
                'suspense': np.nan,
                'next_payment_date': _series_to_dt(df, ['NextPaymentDate', 'Next Payment Date', 'Next Due Date']),
                'maturity_date': _series_to_dt(df, ['MaturityDate', 'Maturity Date']),
                'status': _series_to_text(df, ['ServicerLoanStatus', 'Loan Status', 'Status']),
                'as_of': pd.to_datetime(_as_of_for_df(df, filename, ['ReportDate', 'Report Date', 'As Of Date', 'Run Date'])),
            }
        )
        return downcast_numeric_frame(out.dropna(subset=['servicer_id']))

    raise ValueError('Unhandled servicer type.')


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60, max_entries=128, hash_funcs={UploadBlob: lambda b: f'{b.filename}:{b.file_hash}'})
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

        if 'as_of' in parsed.columns and parsed['as_of'].notna().any():
            d = pd.to_datetime(parsed['as_of'].dropna().iloc[0]).date()
        else:
            d = date_from_filename(blob.filename)

        if d:
            file_dates.append(d)

    if frames:
        full = pd.concat(frames, ignore_index=True, copy=False)
    else:
        full = pd.DataFrame(
            columns=['source_file', 'servicer', 'servicer_family', 'servicer_id', 'upb', 'suspense', 'next_payment_date', 'maturity_date', 'status', 'as_of']
        )

    if not full.empty:
        full = full.dropna(subset=['servicer_id']).copy()
        full['_sid_key'] = id_key_no_leading_zeros(full['servicer_id'])
        full = full.dropna(subset=['_sid_key']).copy()

        full['_has_upb'] = full['upb'].notna().astype('int8')
        full['_has_nonzero_upb'] = (pd.to_numeric(full['upb'], errors='coerce').fillna(0) > 0).astype('int8')
        full['_has_suspense'] = full['suspense'].notna().astype('int8')
        full['_has_npd'] = full['next_payment_date'].notna().astype('int8')
        full['_has_mat'] = full['maturity_date'].notna().astype('int8')

        full = full.sort_values(
            ['_sid_key', 'as_of', '_has_nonzero_upb', '_has_upb', '_has_suspense', '_has_npd', '_has_mat', 'upb'],
            ascending=[True, True, True, True, True, True, True, True],
        )

        join = full.drop_duplicates(['_sid_key'], keep='last').drop(
            columns=['_has_upb', '_has_nonzero_upb', '_has_suspense', '_has_npd', '_has_mat'], errors='ignore'
        )
        preview = full.head(200).copy()
    else:
        full['_sid_key'] = pd.Series(dtype='string')
        join = full.copy()
        preview = full.copy()

    run_date = max(file_dates) if file_dates else today_et()
    return downcast_numeric_frame(join), run_date, downcast_numeric_frame(preview)


# 4) Add NPL / REO parser helpers

def _parse_npl_or_reo_sheet(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=4)
    df = df.dropna(how='all').copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def parse_npl_reo_bytes(file_bytes: bytes) -> dict:
    out = {
        'loan_flags': pd.DataFrame(columns=['_deal_key', 'NPL Flag', 'Needs NPL Value', 'Special Focus (Y/N)']),
        'asset_flags': pd.DataFrame(columns=['_deal_key', '_asset_key', '3/31 NPL (Y/N)', 'Needs NPL Value', 'Special Flag']),
    }

    try:
        npl = _parse_npl_or_reo_sheet(file_bytes, 'NPL')
        if 'Deal Number' in npl.columns:
            loan_flags = pd.DataFrame(
                {
                    '_deal_key': norm_id_series(npl['Deal Number']),
                    'NPL Flag': 'Y',
                    'Needs NPL Value': 'N',
                    'Special Focus (Y/N)': 'Y',
                }
            ).dropna(subset=['_deal_key']).drop_duplicates('_deal_key')

            asset_flags = pd.DataFrame(
                {
                    '_deal_key': norm_id_series(npl['Deal Number']),
                    '_asset_key': pd.NA,
                    '3/31 NPL (Y/N)': 'Y',
                    'Needs NPL Value': 'N',
                    'Special Flag': 'Y',
                }
            ).dropna(subset=['_deal_key'])

            out['loan_flags'] = pd.concat([out['loan_flags'], loan_flags], ignore_index=True, copy=False)
            out['asset_flags'] = pd.concat([out['asset_flags'], asset_flags], ignore_index=True, copy=False)
    except Exception:
        pass

    try:
        reo = _parse_npl_or_reo_sheet(file_bytes, 'REO')
        if 'Deal Number' in reo.columns:
            loan_flags = pd.DataFrame(
                {
                    '_deal_key': norm_id_series(reo['Deal Number']),
                    'NPL Flag': 'N',
                    'Needs NPL Value': 'N',
                    'Special Focus (Y/N)': 'Y',
                }
            ).dropna(subset=['_deal_key']).drop_duplicates('_deal_key')

            asset_flags = pd.DataFrame(
                {
                    '_deal_key': norm_id_series(reo['Deal Number']),
                    '_asset_key': norm_id_series(reo['Asset ID']) if 'Asset ID' in reo.columns else pd.Series([pd.NA] * len(reo)),
                    '3/31 NPL (Y/N)': 'N',
                    'Needs NPL Value': 'N',
                    'Special Flag': 'Y',
                }
            ).dropna(subset=['_deal_key'])

            out['loan_flags'] = pd.concat([out['loan_flags'], loan_flags], ignore_index=True, copy=False)
            out['asset_flags'] = pd.concat([out['asset_flags'], asset_flags], ignore_index=True, copy=False)
    except Exception:
        pass

    if not out['loan_flags'].empty:
        out['loan_flags'] = out['loan_flags'].sort_values(['_deal_key', 'Special Focus (Y/N)', 'NPL Flag']).drop_duplicates('_deal_key', keep='last')
    if not out['asset_flags'].empty:
        out['asset_flags'] = out['asset_flags'].drop_duplicates(['_deal_key', '_asset_key'], keep='last')

    return out


# 5) In _build_bridge_spine_like(), replace the old deal-level servicer overwrite block with ONLY this:
#
# if {'Servicer Loan Number', 'Servicer Commitment Id'}.issubset(df.columns):
#     df['Servicer Loan Number'] = coalesce_keep_nonblank(
#         df['Servicer Loan Number'],
#         df['Servicer Commitment Id'],
#     )
#
# Do not keep the old groupby/authoritative overwrite logic.


# 6) Replace read_tab_df_from_active_loans + build_prev_maps

def read_tab_df_from_active_loans(file_bytes: bytes, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=3)
    df = df.dropna(how='all').copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_prev_maps(prev_bytes: bytes) -> dict:
    out: dict = {}

    try:
        ba = read_tab_df_from_active_loans(prev_bytes, 'Bridge Asset')
        if 'Asset ID' in ba.columns:
            keep = [c for c in ['Asset ID', 'Portfolio', 'Segment', 'Strategy Grouping', '3/31 NPL (Y/N)', 'Needs NPL Value', 'Special Flag'] if c in ba.columns]
            tmp = ba[keep].copy()
            tmp['_asset_key'] = norm_id_series(tmp['Asset ID'])
            out['bridge_asset_manual'] = tmp.dropna(subset=['_asset_key']).drop_duplicates('_asset_key')
    except Exception:
        pass

    try:
        bl = read_tab_df_from_active_loans(prev_bytes, 'Bridge Loan')
        keep = [c for c in ['Deal Number', 'Portfolio', 'Segment', 'Strategy Grouping', 'Loan Level Delinquency', 'Special Focus (Y/N)', 'AM Commentary', '3/31 NPL', 'Needs NPL Value'] if c in bl.columns]
        if 'Deal Number' in keep and len(keep) > 1:
            tmp = bl[keep].copy()
            tmp['_deal_key'] = norm_id_series(tmp['Deal Number'])
            out['bridge_loan_manual'] = tmp.dropna(subset=['_deal_key']).drop_duplicates('_deal_key')

        upb_col_prev = _find_upb_col(bl.columns)
        if upb_col_prev and 'Deal Number' in bl.columns:
            tmpu = bl[['Deal Number', upb_col_prev]].copy()
            tmpu['_deal_key'] = norm_id_series(tmpu['Deal Number'])
            tmpu['_prev_upb'] = tmpu[upb_col_prev].apply(money_to_float)
            out['bridge_loan_upb'] = tmpu.dropna(subset=['_deal_key']).drop_duplicates('_deal_key')[['_deal_key', '_prev_upb']]
    except Exception:
        pass

    try:
        tl = read_tab_df_from_active_loans(prev_bytes, 'Term Loan')
        if 'Deal Number' in tl.columns and 'REO Date' in tl.columns:
            tmp = tl[['Deal Number', 'REO Date']].copy()
            tmp['_deal_key'] = norm_id_series(tmp['Deal Number'])
            out['term_loan_reo'] = tmp.dropna(subset=['_deal_key']).drop_duplicates('_deal_key')

        keep = [c for c in ['Deal Number', 'Portfolio', 'Segment', 'CPP JV', 'Special Loans List (Y/N)'] if c in tl.columns]
        if 'Deal Number' in keep and len(keep) > 1:
            tmpm = tl[keep].copy()
            tmpm['_deal_key'] = norm_id_series(tmpm['Deal Number'])
            out['term_loan_manual'] = tmpm.dropna(subset=['_deal_key']).drop_duplicates('_deal_key')

        upb_col_prev = _find_upb_col(tl.columns)
        if upb_col_prev and 'Deal Number' in tl.columns:
            tmpu = tl[['Deal Number', upb_col_prev]].copy()
            tmpu['_deal_key'] = norm_id_series(tmpu['Deal Number'])
            tmpu['_prev_upb'] = tmpu[upb_col_prev].apply(money_to_float)
            out['term_loan_upb'] = tmpu.dropna(subset=['_deal_key']).drop_duplicates('_deal_key')[['_deal_key', '_prev_upb']]
    except Exception:
        pass

    gc.collect()
    return out


# 7) Replace build_bridge_asset / build_bridge_loan / build_term_loan / build_term_asset
#    with the versions from the message that accompanied this patch file.
#    They are long, so I kept this file focused on the parsers + previous-report carry-forward + syntax fix.
#    Key required signature changes:
#
# build_bridge_asset(..., template_maps, npl_maps=None)
# build_bridge_loan(bridge_asset, upb_col, prev_maps, npl_maps=None)
# build_term_loan(...)
# build_term_asset(...)
#
# And in build_term_loan(), keep this filter near the end:
#
# prev_keys = set(prev_maps.get('term_loan_manual', pd.DataFrame()).get('_deal_key', pd.Series(dtype='string')).dropna().tolist())
# keep_mask = pd.to_numeric(out[upb_col], errors='coerce').fillna(0).gt(0)
# if prev_keys:
#     keep_mask = keep_mask | out['_deal_key'].isin(prev_keys)
# out = out.loc[keep_mask].copy()
#
# That removes the blank / zero-UPB term rows creating the visual gaps.


# 8) Replace _clear_sheet_body + add _trim_sheet_body_rows + update write_output_sheet

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
def build_bridge_asset(sf_spine, sf_dnl, sf_val, sf_am, sf_active_rm, serv_lookup, upb_col, prev_maps, template_maps, npl_maps=None):
    out = pd.DataFrame(index=sf_spine.index)
    for col, label in BRIDGE_ASSET_FROM_BRIDGE_SPINE.items():
        out[col] = sf_spine[label] if label in sf_spine.columns else pd.NA
    for extra in ['Loan Commitment','Remaining Commitment','Current UPB','Comments AM']:
        if extra in sf_spine.columns:
            out[extra] = sf_spine[extra]
    out['Portfolio']=pd.NA; out['Segment']=pd.NA; out['Strategy Grouping']=pd.NA; out['Do Not Lend (Y/N)']='N'; out['Active RM']='N'
    out['3/31 NPL (Y/N)']=pd.NA; out['Needs NPL Value']=pd.NA; out['Special Flag']=pd.NA
    out['_deal_key'] = norm_id_series(out.get('Deal Number', pd.Series([None]*len(out))))
    out['_sid_key'] = id_key_no_leading_zeros(out.get('Servicer ID', pd.Series([None]*len(out))))
    out['_asset_key'] = norm_id_series(out.get('Asset ID', pd.Series([None]*len(out))))
    if not sf_dnl.empty and 'Deal Loan Number' in sf_dnl.columns:
        dnl = sf_dnl.copy(); dnl['_deal_key'] = norm_id_series(dnl['Deal Loan Number'])
        if 'Do Not Lend' in dnl.columns:
            dnl = dnl[['_deal_key','Do Not Lend']].drop_duplicates('_deal_key')
            out = out.merge(dnl, on='_deal_key', how='left')
            out['Do Not Lend (Y/N)'] = _yn_from_bool_series(out['Do Not Lend'])
            out = out.drop(columns=['Do Not Lend'], errors='ignore')
    if not sf_active_rm.empty and 'Deal Loan Number' in sf_active_rm.columns:
        arm = sf_active_rm.copy(); arm['_deal_key']=norm_id_series(arm['Deal Loan Number']); arm=arm[['_deal_key']].drop_duplicates('_deal_key'); arm['Active RM']='Y'
        out=out.merge(arm,on='_deal_key',how='left',suffixes=('','_active'))
        out['Active RM']=coalesce_keep_nonblank(out.get('Active RM_active', pd.Series([pd.NA]*len(out))), out['Active RM'])
        out=out.drop(columns=['Active RM_active'], errors='ignore')
    if not sf_val.empty and 'Asset ID' in sf_val.columns:
        v=sf_val.copy(); v['_asset_key']=norm_id_series(v['Asset ID']); keep=['_asset_key']+[lbl for lbl in BRIDGE_ASSET_FROM_VALUATION.values() if lbl in v.columns]; v=v[keep].drop_duplicates('_asset_key'); out=out.merge(v,on='_asset_key',how='left')
        for tcol, vlabel in BRIDGE_ASSET_FROM_VALUATION.items():
            if vlabel in out.columns:
                out[tcol]=out[vlabel]; out=out.drop(columns=[vlabel], errors='ignore')
    if npl_maps and not npl_maps.get('asset_flags', pd.DataFrame()).empty:
        af = npl_maps['asset_flags'].copy()
        if '_asset_key' in af.columns:
            af_asset = af[af['_asset_key'].notna()].copy()
            af_deal = af[af['_asset_key'].isna()].copy()
            if not af_deal.empty:
                keep = ['_deal_key','3/31 NPL (Y/N)','Needs NPL Value','Special Flag']
                af_deal = af_deal[keep].drop_duplicates('_deal_key')
                out = out.merge(af_deal, on='_deal_key', how='left', suffixes=('','_npldeal'))
                for c in ['3/31 NPL (Y/N)','Needs NPL Value','Special Flag']:
                    out[c] = coalesce_keep_nonblank(out.get(f'{c}_npldeal', pd.Series([pd.NA]*len(out))), out[c])
                    out = out.drop(columns=[f'{c}_npldeal'], errors='ignore')
            if not af_asset.empty:
                keep = ['_deal_key','_asset_key','3/31 NPL (Y/N)','Needs NPL Value','Special Flag']
                af_asset = af_asset[keep].drop_duplicates(['_deal_key','_asset_key'])
                out = out.merge(af_asset, on=['_deal_key','_asset_key'], how='left', suffixes=('','_nplasset'))
                for c in ['3/31 NPL (Y/N)','Needs NPL Value','Special Flag']:
                    out[c] = coalesce_keep_nonblank(out.get(f'{c}_nplasset', pd.Series([pd.NA]*len(out))), out[c])
                    out = out.drop(columns=[f'{c}_nplasset'], errors='ignore')
    if 'bridge_asset_manual' in prev_maps:
        man = prev_maps['bridge_asset_manual'].copy()
        keep_cols = ['_asset_key'] + [c for c in ['Portfolio','Segment','Strategy Grouping','3/31 NPL (Y/N)','Needs NPL Value','Special Flag'] if c in man.columns]
        out = out.merge(man[keep_cols], on='_asset_key', how='left', suffixes=('','_prev'))
        for c in ['Portfolio','Segment','Strategy Grouping','3/31 NPL (Y/N)','Needs NPL Value','Special Flag']:
            if f'{c}_prev' in out.columns:
                out[c] = coalesce_keep_nonblank(out[f'{c}_prev'], out[c])
                out = out.drop(columns=[f'{c}_prev'], errors='ignore')
    seg_guess = out.apply(lambda r: derive_bridge_segment(r.get('Deal Number'), r.get('Financing'), r.get('Loan Buyer'), template_maps), axis=1)
    strat_guess = out['Project Strategy'].map(lambda x: strategy_grouping_from_project_strategy(x, template_maps.get('strategy_map', {}))) if 'Project Strategy' in out.columns else pd.Series([pd.NA]*len(out), index=out.index)
    port_guess = out.apply(lambda r: derive_bridge_portfolio(r.get('Product Type'), r.get('Segment') if has_any_value(r.get('Segment')) else derive_bridge_segment(r.get('Deal Number'), r.get('Financing'), r.get('Loan Buyer'), template_maps), r.get('Financing'), r.get('Deal Intro Sub-Source'), r.get('Deal Number')), axis=1)
    out['Segment'] = coalesce_keep_nonblank(out['Segment'], seg_guess)
    out['Strategy Grouping'] = coalesce_keep_nonblank(out['Strategy Grouping'], strat_guess)
    out['Portfolio'] = coalesce_keep_nonblank(out['Portfolio'], port_guess)
    sf_next_payment = pd.to_datetime(sf_spine.get('Opportunity Next Payment Date', pd.Series([pd.NaT]*len(out))), errors='coerce')
    sf_current_upb = pd.to_numeric(sf_spine.get('Current UPB', pd.Series([np.nan]*len(out))), errors='coerce')
    if not serv_lookup.empty and '_sid_key' in serv_lookup.columns:
        s = serv_lookup.dropna(subset=['_sid_key']).copy().rename(columns={'servicer':'Servicer','upb':'_loan_upb','suspense':'_loan_suspense','next_payment_date':'_serv_next_payment_date','maturity_date':'Servicer Maturity Date','status':'Servicer Status'})
        out = out.merge(s[['_sid_key','Servicer','_loan_upb','_loan_suspense','_serv_next_payment_date','Servicer Maturity Date','Servicer Status','source_file']], on='_sid_key', how='left')
        if 'bridge_loan_upb' in prev_maps:
            out = out.merge(prev_maps['bridge_loan_upb'].copy(), on='_deal_key', how='left')
        else:
            out['_prev_upb']=np.nan
        reo_mask = out.get('Loan Stage', pd.Series([None]*len(out))).apply(is_reo_stage)
        loan_upb = pd.to_numeric(out.get('_loan_upb', pd.Series([np.nan]*len(out))), errors='coerce')
        prev_upb_vals = pd.to_numeric(out.get('_prev_upb', pd.Series([np.nan]*len(out))), errors='coerce')
        fill_val = prev_upb_vals.fillna(0.0)
        out['_loan_upb'] = np.where(reo_mask & ((loan_upb.isna()) | (loan_upb <= 0)), fill_val, loan_upb)
        out['_w'] = sf_current_upb
        out['_w_sum'] = out.groupby('_sid_key')['_w'].transform('sum')
        out['_n_in_loan'] = out.groupby('_sid_key')['_sid_key'].transform('size').replace({0: np.nan})
        out[upb_col] = np.where(out['_w_sum'].fillna(0) > 0, out['_loan_upb'] * (out['_w'] / out['_w_sum']), out['_loan_upb'] / out['_n_in_loan'])
        out['Suspense Balance'] = np.where(out['_w_sum'].fillna(0) > 0, out['_loan_suspense'] * (out['_w'] / out['_w_sum']), out['_loan_suspense'] / out['_n_in_loan'])
        cur = pd.to_numeric(out[upb_col], errors='coerce')
        out[upb_col] = cur.where(cur.notna(), sf_current_upb)
        out['Next Payment Date'] = pd.to_datetime(out.get('_serv_next_payment_date'), errors='coerce')
        out['Next Payment Date'] = pd.to_datetime(out['Next Payment Date'], errors='coerce').where(pd.to_datetime(out['Next Payment Date'], errors='coerce').notna(), sf_next_payment)
        out = out.drop(columns=['_prev_upb'], errors='ignore')
    else:
        out[upb_col] = sf_current_upb; out['Servicer']=pd.NA; out['Next Payment Date']=sf_next_payment; out['Servicer Maturity Date']=pd.NaT; out['Servicer Status']=pd.NA; out['Suspense Balance']=np.nan
    out['SF Funded Amount'] = pd.to_numeric(sf_spine.get('Approved Advance Amount Funded', pd.Series([0]*len(out))), errors='coerce').fillna(0)
    out['3/31 NPL (Y/N)'] = coalesce_keep_nonblank(out['3/31 NPL (Y/N)'], pd.Series(['N']*len(out), index=out.index))
    out['Needs NPL Value'] = coalesce_keep_nonblank(out['Needs NPL Value'], pd.Series(['N']*len(out), index=out.index))
    out['Special Flag'] = coalesce_keep_nonblank(out['Special Flag'], pd.Series(['N']*len(out), index=out.index))
    return out

def build_bridge_loan(bridge_asset, upb_col, prev_maps, npl_maps=None):
    ba = bridge_asset.copy(); g = ba.groupby('_deal_key', dropna=True)
    def _first(series): return first_nonblank(series)
    def _max_dt(series):
        s = pd.to_datetime(series, errors='coerce').dropna(); return s.max() if len(s) else pd.NaT
    def _min_dt(series):
        s = pd.to_datetime(series, errors='coerce').dropna(); return s.min() if len(s) else pd.NaT
    def _yn_any(series):
        vals = pd.Series(series).astype('string').str.strip().str.upper()
        return 'Y' if vals.eq('Y').any() else 'N'
    out = pd.DataFrame({
        'Deal Number': g['Deal Number'].first() if 'Deal Number' in ba.columns else pd.Series(dtype='string'),
        'Portfolio': g['Portfolio'].apply(_first) if 'Portfolio' in ba.columns else pd.Series(dtype='string'),
        'Loan Buyer': g['Loan Buyer'].apply(_first) if 'Loan Buyer' in ba.columns else pd.Series(dtype='string'),
        'Financing': g['Financing'].apply(_first) if 'Financing' in ba.columns else pd.Series(dtype='string'),
        'Servicer ID': g['Servicer ID'].apply(first_or_various) if 'Servicer ID' in ba.columns else pd.Series(dtype='string'),
        'Servicer': g['Servicer'].apply(first_or_various) if 'Servicer' in ba.columns else pd.Series(dtype='string'),
        'Deal Name': g['Deal Name'].apply(_first) if 'Deal Name' in ba.columns else pd.Series(dtype='string'),
        'Borrower Name': g['Borrower Entity'].apply(_first) if 'Borrower Entity' in ba.columns else pd.Series(dtype='string'),
        'Account': g['Account Name'].apply(_first) if 'Account Name' in ba.columns else pd.Series(dtype='string'),
        'Do Not Lend (Y/N)': g['Do Not Lend (Y/N)'].max() if 'Do Not Lend (Y/N)' in ba.columns else pd.Series(dtype='string'),
        'Primary Contact': g['Primary Contact'].apply(_first) if 'Primary Contact' in ba.columns else pd.Series(dtype='string'),
        'Number of Assets': g['Asset ID'].nunique() if 'Asset ID' in ba.columns else pd.Series(dtype='float'),
        '# of Units': pd.to_numeric(g['# of Units'].sum(min_count=1), errors='coerce') if '# of Units' in ba.columns else np.nan,
        'State(s)': g['State'].apply(lambda s: ', '.join(sorted({clean_text(x) for x in s if clean_text(x)}))) if 'State' in ba.columns else pd.Series(dtype='string'),
        'Origination Date': g['Origination Date'].apply(_min_dt) if 'Origination Date' in ba.columns else pd.NaT,
        'Last Funding Date': g['Last Funding Date'].apply(_max_dt) if 'Last Funding Date' in ba.columns else pd.NaT,
        'Original Maturity Date': g['Original Loan Maturity date'].apply(_first) if 'Original Loan Maturity date' in ba.columns else pd.NaT,
        'Current Maturity Date': g['Current Loan Maturity date'].apply(_first) if 'Current Loan Maturity date' in ba.columns else pd.NaT,
        'Next Advance Maturity Date': g['Servicer Maturity Date'].apply(_first) if 'Servicer Maturity Date' in ba.columns else pd.NaT,
        'Next Payment Date': g['Next Payment Date'].apply(_min_dt) if 'Next Payment Date' in ba.columns else pd.NaT,
        'Days Past Due': pd.NA,
        'Loan Level Delinquency': pd.NA,
        'Loan Commitment': g['Loan Commitment'].apply(_first) if 'Loan Commitment' in ba.columns else np.nan,
        'Active Funded Amount': pd.to_numeric(g['SF Funded Amount'].sum(min_count=1), errors='coerce') if 'SF Funded Amount' in ba.columns else np.nan,
        upb_col: pd.to_numeric(g[upb_col].sum(min_count=1), errors='coerce') if upb_col in ba.columns else np.nan,
        'Suspense Balance': pd.to_numeric(g['Suspense Balance'].sum(min_count=1), errors='coerce') if 'Suspense Balance' in ba.columns else np.nan,
        'Remaining Commitment': g['Remaining Commitment'].apply(_first) if 'Remaining Commitment' in ba.columns else np.nan,
        'Most Recent Valuation Date': g['Updated Valuation Date'].apply(_max_dt) if 'Updated Valuation Date' in ba.columns else pd.NaT,
        'Most Recent As-Is Value': pd.to_numeric(g['Updated As-Is Value'].sum(min_count=1), errors='coerce') if 'Updated As-Is Value' in ba.columns else np.nan,
        'Most Recent ARV': pd.to_numeric(g['Updated ARV'].sum(min_count=1), errors='coerce') if 'Updated ARV' in ba.columns else np.nan,
        'Initial Disbursement Funded': pd.to_numeric(g['Initial Disbursement Funded'].sum(min_count=1), errors='coerce') if 'Initial Disbursement Funded' in ba.columns else np.nan,
        'Renovation Holdback': pd.to_numeric(g['Renovation Holdback'].sum(min_count=1), errors='coerce') if 'Renovation Holdback' in ba.columns else np.nan,
        'Renovation HB Funded': pd.to_numeric(g['Renovation Holdback Funded'].sum(min_count=1), errors='coerce') if 'Renovation Holdback Funded' in ba.columns else np.nan,
        'Renovation HB Remaining': pd.to_numeric(g['Renovation Holdback Remaining'].sum(min_count=1), errors='coerce') if 'Renovation Holdback Remaining' in ba.columns else np.nan,
        'Interest Allocation': pd.to_numeric(g['Interest Allocation'].sum(min_count=1), errors='coerce') if 'Interest Allocation' in ba.columns else np.nan,
        'Interest Allocation Funded': pd.to_numeric(g['Interest Allocation Funded'].sum(min_count=1), errors='coerce') if 'Interest Allocation Funded' in ba.columns else np.nan,
        'Loan Stage': g['Loan Stage'].apply(_first) if 'Loan Stage' in ba.columns else pd.Series(dtype='string'),
        'Segment': g['Segment'].apply(_first) if 'Segment' in ba.columns else pd.Series(dtype='string'),
        'Product Type': g['Product Type'].apply(_first) if 'Product Type' in ba.columns else pd.Series(dtype='string'),
        'Product Sub Type': g['Product Sub-Type'].apply(_first) if 'Product Sub-Type' in ba.columns else pd.Series(dtype='string'),
        'Transaction Type': g['Transaction Type'].apply(_first) if 'Transaction Type' in ba.columns else pd.Series(dtype='string'),
        'Project Strategy': g['Project Strategy'].apply(_first) if 'Project Strategy' in ba.columns else pd.Series(dtype='string'),
        'Strategy Grouping': g['Strategy Grouping'].apply(_first) if 'Strategy Grouping' in ba.columns else pd.Series(dtype='string'),
        'CV Originator': g['Originator'].apply(_first) if 'Originator' in ba.columns else pd.Series(dtype='string'),
        'Active RM': g['Active RM'].apply(_first) if 'Active RM' in ba.columns else pd.Series(dtype='string'),
        'Deal Intro Sub-Source': g['Deal Intro Sub-Source'].apply(_first) if 'Deal Intro Sub-Source' in ba.columns else pd.Series(dtype='string'),
        'Referral Source Account': g['Referral Source Account'].apply(_first) if 'Referral Source Account' in ba.columns else pd.Series(dtype='string'),
        'Referral Source Contact': g['Referral Source Contact'].apply(_first) if 'Referral Source Contact' in ba.columns else pd.Series(dtype='string'),
        '3/31 NPL': g['3/31 NPL (Y/N)'].apply(_yn_any) if '3/31 NPL (Y/N)' in ba.columns else 'N',
        'Needs NPL Value': g['Needs NPL Value'].apply(_yn_any) if 'Needs NPL Value' in ba.columns else 'N',
        'Special Focus (Y/N)': g['Special Flag'].apply(_yn_any) if 'Special Flag' in ba.columns else 'N',
        'Asset Manager 1': g['Asset Manager 1'].apply(_first) if 'Asset Manager 1' in ba.columns else pd.Series(dtype='string'),
        'AM 1 Assigned Date': g['AM 1 Assigned Date'].apply(_first) if 'AM 1 Assigned Date' in ba.columns else pd.NaT,
        'Asset Manager 2': g['Asset Manager 2'].apply(_first) if 'Asset Manager 2' in ba.columns else pd.Series(dtype='string'),
        'AM 2 Assigned Date': g['AM 2 Assigned Date'].apply(_first) if 'AM 2 Assigned Date' in ba.columns else pd.NaT,
        'Construction Mgr.': g['Construction Mgr.'].apply(_first) if 'Construction Mgr.' in ba.columns else pd.Series(dtype='string'),
        'CM Assigned Date': g['CM Assigned Date'].apply(_first) if 'CM Assigned Date' in ba.columns else pd.NaT,
        'AM Commentary': g['Comments AM'].apply(_first) if 'Comments AM' in ba.columns else pd.Series(dtype='string'),
    }).reset_index(drop=True)
    out['_deal_key'] = norm_id_series(out['Deal Number'])
    if npl_maps and not npl_maps.get('loan_flags', pd.DataFrame()).empty:
        loan_flags = npl_maps['loan_flags'].copy().drop_duplicates('_deal_key')
        out = out.merge(loan_flags, on='_deal_key', how='left', suffixes=('','_npl'))
        if 'NPL Flag_npl' in out.columns:
            out['3/31 NPL'] = coalesce_keep_nonblank(out['NPL Flag_npl'], out['3/31 NPL'])
            out = out.drop(columns=['NPL Flag_npl'], errors='ignore')
        for c in ['Needs NPL Value','Special Focus (Y/N)']:
            if f'{c}_npl' in out.columns:
                out[c] = coalesce_keep_nonblank(out[f'{c}_npl'], out[c]); out = out.drop(columns=[f'{c}_npl'], errors='ignore')
    if 'bridge_loan_manual' in prev_maps and not out.empty:
        man = prev_maps['bridge_loan_manual'].copy()
        out = out.merge(man, on='_deal_key', how='left', suffixes=('','_prev'))
        for c in ['Portfolio','Segment','Strategy Grouping','Loan Level Delinquency','Special Focus (Y/N)','AM Commentary','3/31 NPL','Needs NPL Value']:
            if f'{c}_prev' in out.columns:
                out[c] = coalesce_keep_nonblank(out[f'{c}_prev'], out[c]); out = out.drop(columns=[f'{c}_prev'], errors='ignore')
    out['Special Focus (Y/N)'] = coalesce_keep_nonblank(out['Special Focus (Y/N)'], pd.Series(['N']*len(out), index=out.index))
    out['3/31 NPL'] = coalesce_keep_nonblank(out['3/31 NPL'], pd.Series(['N']*len(out), index=out.index))
    out['Needs NPL Value'] = coalesce_keep_nonblank(out['Needs NPL Value'], pd.Series(['N']*len(out), index=out.index))
    return out.drop(columns=['_deal_key'], errors='ignore')

def build_term_loan(sf_term, sf_am, sf_active_rm, serv_lookup, upb_col, prev_maps, template_maps):
    out = pd.DataFrame(index=sf_term.index)
    for col, label in TERM_LOAN_FROM_TERM_WIDE.items():
        out[col] = sf_term[label] if label in sf_term.columns else pd.NA
    out['_deal_key'] = norm_id_series(out.get('Deal Number', pd.Series([None]*len(out))))
    if 'Do Not Lend (Y/N)' in out.columns:
        out['Do Not Lend (Y/N)'] = _yn_from_bool_series(out['Do Not Lend (Y/N)'])
    out['Loan Buyer'] = sf_term['Sold Loan: Sold To'] if 'Sold Loan: Sold To' in sf_term.columns else pd.NA
    out['Active RM'] = 'N'
    out['Servicer'] = sf_term['Servicer Name'] if 'Servicer Name' in sf_term.columns else pd.NA
    out['Maturity Date'] = pd.to_datetime(sf_term['Original Loan Maturity Date'], errors='coerce') if 'Original Loan Maturity Date' in sf_term.columns else pd.NaT
    out['Next Payment Date'] = pd.to_datetime(sf_term['Next Payment Date'], errors='coerce') if 'Next Payment Date' in sf_term.columns else pd.NaT
    cls = sf_term.apply(lambda r: pd.Series(derive_term_portfolio_segment(r.get('Type'), r.get('Current Funding Vehicle'), r.get('Sold Loan: Sold To'), r.get('Deal Loan Number'), template_maps), index=['Portfolio','Segment','CPP JV']), axis=1)
    out['Portfolio']=cls['Portfolio']; out['Segment']=cls['Segment']; out['CPP JV']=cls['CPP JV']
    if 'term_loan_manual' in prev_maps:
        man = prev_maps['term_loan_manual'].copy(); out = out.merge(man, on='_deal_key', how='left', suffixes=('','_prev'))
        for c in ['Portfolio','Segment','CPP JV','Special Loans List (Y/N)']:
            if f'{c}_prev' in out.columns:
                out[c] = coalesce_keep_nonblank(out[f'{c}_prev'], out.get(c, pd.Series([pd.NA]*len(out)))); out = out.drop(columns=[f'{c}_prev'], errors='ignore')
    if not sf_am.empty and 'Deal Loan Number' in sf_am.columns:
        am = sf_am.copy(); am['_deal_key']=norm_id_series(am['Deal Loan Number']); am['_dt']=pd.to_datetime(am.get('Date Assigned'), errors='coerce'); am=am.sort_values(['_deal_key','Team Role','_dt']).drop_duplicates(['_deal_key','Team Role'], keep='last')
        am1 = am[am['Team Role'].astype('string').str.strip().eq('Asset Manager')][['_deal_key','Team Member Name']].drop_duplicates('_deal_key')
        out = out.merge(am1, on='_deal_key', how='left'); out['Asset Manager'] = out['Team Member Name'].replace({'':pd.NA}); out = out.drop(columns=['Team Member Name'], errors='ignore')
    else:
        out['Asset Manager'] = pd.NA
    out['Servicer ID'] = sf_term['Servicer Commitment Id'] if 'Servicer Commitment Id' in sf_term.columns else pd.NA
    out['_sid_key'] = id_key_no_leading_zeros(out['Servicer ID'].astype('string'))
    sf_upb_fallback = pd.to_numeric(sf_term['Current Servicer UPB'] if 'Current Servicer UPB' in sf_term.columns else pd.Series([np.nan]*len(out)), errors='coerce')
    if not serv_lookup.empty and '_sid_key' in serv_lookup.columns:
        s2 = serv_lookup.dropna(subset=['_sid_key']).copy().rename(columns={'servicer':'_servicer_file','upb':'_servicer_upb','next_payment_date':'_servicer_next_payment_date','maturity_date':'_servicer_maturity_date'})[['_sid_key','_servicer_file','_servicer_upb','_servicer_next_payment_date','_servicer_maturity_date']]
        out = out.merge(s2, on='_sid_key', how='left')
        out['Servicer'] = coalesce_keep_nonblank(out['_servicer_file'], out['Servicer'])
        out['Maturity Date'] = pd.to_datetime(out['_servicer_maturity_date'], errors='coerce').where(pd.to_datetime(out['_servicer_maturity_date'], errors='coerce').notna(), pd.to_datetime(out['Maturity Date'], errors='coerce'))
        out['Next Payment Date'] = pd.to_datetime(out['_servicer_next_payment_date'], errors='coerce').where(pd.to_datetime(out['_servicer_next_payment_date'], errors='coerce').notna(), pd.to_datetime(out['Next Payment Date'], errors='coerce'))
        out[upb_col] = pd.to_numeric(out['_servicer_upb'], errors='coerce').where(pd.to_numeric(out['_servicer_upb'], errors='coerce').notna(), sf_upb_fallback)
        out = out.drop(columns=['_servicer_file','_servicer_upb','_servicer_next_payment_date','_servicer_maturity_date'], errors='ignore')
    else:
        out[upb_col] = sf_upb_fallback
    out['REO Date'] = pd.NaT
    if 'term_loan_reo' in prev_maps:
        reo = prev_maps['term_loan_reo'][['_deal_key','REO Date']].copy(); out = out.merge(reo, on='_deal_key', how='left', suffixes=('','_prev')); out['REO Date'] = pd.to_datetime(out['REO Date_prev'], errors='coerce').where(pd.to_datetime(out['REO Date_prev'], errors='coerce').notna(), pd.to_datetime(out['REO Date'], errors='coerce')); out = out.drop(columns=['REO Date_prev'], errors='ignore')
    if 'term_loan_upb' in prev_maps and upb_col in out.columns:
        prevu = prev_maps['term_loan_upb'].copy(); out = out.merge(prevu, on='_deal_key', how='left')
        reo_mask = pd.to_datetime(out['REO Date'], errors='coerce').notna(); cur_upb = pd.to_numeric(out[upb_col], errors='coerce'); prev_upb = pd.to_numeric(out.get('_prev_upb', np.nan), errors='coerce'); fill_val = prev_upb.fillna(0.0)
        out[upb_col] = np.where(reo_mask & ((cur_upb.isna()) | (cur_upb <= 0)), fill_val, cur_upb); out = out.drop(columns=['_prev_upb'], errors='ignore')
    prev_keys = set(prev_maps.get('term_loan_manual', pd.DataFrame()).get('_deal_key', pd.Series(dtype='string')).dropna().tolist())
    keep_mask = pd.to_numeric(out[upb_col], errors='coerce').fillna(0).gt(0)
    if prev_keys:
        keep_mask = keep_mask | out['_deal_key'].isin(prev_keys)
    out = out.loc[keep_mask].copy()
    out['Special Loans List (Y/N)'] = coalesce_keep_nonblank(out.get('Special Loans List (Y/N)', pd.Series([pd.NA]*len(out), index=out.index)), pd.Series(['N']*len(out), index=out.index))
    return out

def build_term_asset(sf_term_asset, term_loan, upb_col):
    out = pd.DataFrame(index=sf_term_asset.index)
    for col, label in TERM_ASSET_FROM_TERM_ASSET_REPORT.items():
        out[col] = sf_term_asset[label] if label in sf_term_asset.columns else pd.NA
    out['_deal_key'] = norm_id_series(out.get('Deal Number', pd.Series([None]*len(out))))
    tl = term_loan.copy(); tl['_deal_key'] = norm_id_series(tl.get('Deal Number', pd.Series([None]*len(tl))))
    valid_deals = set(tl['_deal_key'].dropna().tolist())
    out = out[out['_deal_key'].isin(valid_deals)].copy()
    if 'CPP JV' in tl.columns:
        tl_cpp = tl[['_deal_key','CPP JV']].drop_duplicates('_deal_key'); out = out.merge(tl_cpp, on='_deal_key', how='left', suffixes=('','_loan')); out['CPP JV'] = coalesce_keep_nonblank(out.get('CPP JV_loan', pd.Series([pd.NA]*len(out))), out.get('CPP JV', pd.Series([pd.NA]*len(out)))); out = out.drop(columns=['CPP JV_loan'], errors='ignore')
    if upb_col in tl.columns:
        tl_upb = tl[['_deal_key', upb_col]].drop_duplicates('_deal_key'); out = out.merge(tl_upb, on='_deal_key', how='left'); ala = pd.to_numeric(out.get('Property ALA', np.nan), errors='coerce'); ala_sum = ala.groupby(out['_deal_key']).transform('sum'); out[upb_col] = np.where(ala_sum > 0, out[upb_col] * (ala / ala_sum), out[upb_col])
    return out

def _parse_direct_ref_formula(formula_text: str):
    if not isinstance(formula_text, str): return None
    txt = formula_text.strip()
    if not txt.startswith('='): return None
    txt = txt[1:].lstrip('+').strip()
    m = re.fullmatch(r"'([^']+)'!\$?([A-Z]{1,3})\$?(\d+)", txt)
    if m: return m.group(1), f"{m.group(2)}{m.group(3)}"
    m = re.fullmatch(r'([A-Za-z0-9_ ]+)!\$?([A-Z]{1,3})\$?(\d+)', txt)
    if m: return m.group(1), f"{m.group(2)}{m.group(3)}"
    return None

def _resolve_header_value(wb, ws, cell, upb_header: str, max_depth: int = 6) -> str:
    cur_val = cell.value
    for _ in range(max_depth):
        if cur_val is None: return ''
        if not isinstance(cur_val, str): return str(cur_val).strip()
        txt = cur_val.strip()
        if UPB_HEADER_RE.search(txt): return upb_header
        ref = _parse_direct_ref_formula(txt)
        if not ref: return txt
        ref_sheet, ref_cell = ref
        if ref_sheet not in wb.sheetnames: return txt
        cur_val = wb[ref_sheet][ref_cell].value
    if cur_val is None: return ''
    if isinstance(cur_val, str) and UPB_HEADER_RE.search(cur_val.strip()): return upb_header
    return str(cur_val).strip()

def header_tuples_from_ws(ws, header_row: int = 4, wb=None, upb_header: Optional[str] = None) -> List[Tuple[int, str]]:
    out=[]; row=list(ws.iter_rows(min_row=header_row, max_row=header_row, values_only=False))[0]
    for col_idx, cell in enumerate(row, start=1):
        header = _resolve_header_value(wb, ws, cell, upb_header) if wb is not None and upb_header is not None else ('' if cell.value is None else str(cell.value).strip())
        if header: out.append((col_idx, header.strip()))
    return out

def formula_col_indices(ws_formula, start_row: int = 5, header_row: int = 4, scan_rows: int = 50) -> Set[int]:
    fcols=set(); max_scan_row=min(ws_formula.max_row, start_row + scan_rows - 1)
    for r in range(start_row, max_scan_row + 1):
        for col_idx in range(1, ws_formula.max_column + 1):
            v = ws_formula.cell(r, col_idx).value
            if isinstance(v, str) and v.startswith('='): fcols.add(col_idx)
    return fcols

def _capture_formula_seeds(ws_formula, formula_cols: Set[int], start_row: int = 5, scan_rows: int = 50):
    seeds={}; max_scan_row=min(ws_formula.max_row, start_row + scan_rows - 1)
    for col_idx in sorted(formula_cols):
        for r in range(start_row, max_scan_row + 1):
            v = ws_formula.cell(r, col_idx).value
            if isinstance(v, str) and v.startswith('='):
                seeds[col_idx]={'origin_row': r, 'formula': v}; break
    return seeds

def _used_output_columns(ws, wb, upb_header: str, header_row: int = 4, start_row: int = 5) -> Set[int]:
    hdr = header_tuples_from_ws(ws, header_row=header_row, wb=wb, upb_header=upb_header)
    cols={c for c,_ in hdr}; cols |= formula_col_indices(ws, start_row=start_row, header_row=header_row); return cols

def _clear_sheet_body(ws, used_cols: Set[int], start_row: int = 5):
    if not used_cols: return
    max_r = ws.max_row
    for r in range(start_row, max_r + 1):
        for c in used_cols:
            ws.cell(r, c).value = None

def _trim_sheet_body_rows(ws, row_count: int, start_row: int = 5):
    keep_last = max(start_row, start_row + row_count - 1)
    if ws.max_row > keep_last:
        ws.delete_rows(keep_last + 1, ws.max_row - keep_last)

def _excel_safe_value(val):
    if val is None or val is pd.NA: return None
    if isinstance(val, pd.Timestamp): return None if pd.isna(val) else val.to_pydatetime()
    if isinstance(val, np.generic): val = val.item()
    try:
        if pd.isna(val): return None
    except Exception:
        pass
    return val

def _coerce_excel_date_value(val):
    if val is None: return None
    if isinstance(val, pd.Timestamp): return None if pd.isna(val) else val.to_pydatetime().date()
    if isinstance(val, datetime): return val.date()
    if isinstance(val, date): return val
    try:
        parsed = pd.to_datetime(val, errors='coerce')
        if pd.isna(parsed): return val
        return parsed.to_pydatetime().date()
    except Exception:
        return val

def _money_format_for_header(sheet_name: str, header: str, upb_header: str) -> Optional[str]:
    if header == upb_header:
        return MONEY2_FORMAT if sheet_name in {'Bridge Asset','Term Asset'} else MONEY0_FORMAT
    if header in SHEET_MONEY2_HEADERS.get(sheet_name, set()): return MONEY2_FORMAT
    if header in SHEET_MONEY0_HEADERS.get(sheet_name, set()): return MONEY0_FORMAT
    return None

def _is_date_header(sheet_name: str, header: str) -> bool:
    return header in SHEET_DATE_HEADERS.get(sheet_name, set())

def _copy_reference_row_style(ws_formula, col_idx: int, target_cell):
    ref_cell = ws_formula.cell(5, col_idx)
    if ref_cell.has_style: target_cell._style = copy(ref_cell._style)

def _apply_display_style(ws_formula, row_idx: int, col_idx: int, header: str, upb_header: str):
    cell = ws_formula.cell(row_idx, col_idx)
    _copy_reference_row_style(ws_formula, col_idx, cell)
    if _is_date_header(ws_formula.title, header):
        cell.number_format = DATE_NUMBER_FORMAT
    else:
        money_fmt = _money_format_for_header(ws_formula.title, header, upb_header)
        if money_fmt: cell.number_format = money_fmt

def _copy_formula_columns_down(ws_formula, formula_seeds: dict, row_count: int, start_row: int = 5):
    if row_count <= 0: return
    for col_idx, seed in formula_seeds.items():
        origin_row = seed['origin_row']; origin_formula = seed['formula']; origin_ref = f"{get_column_letter(col_idx)}{origin_row}"
        for r in range(start_row, start_row + row_count):
            target = ws_formula.cell(r, col_idx)
            if r == origin_row: target.value = origin_formula
            else: target.value = Translator(origin_formula, origin=origin_ref).translate_formula(f"{get_column_letter(col_idx)}{r}")
            _copy_reference_row_style(ws_formula, col_idx, target)

def _refresh_subtotal_formula(ws_formula, row_count: int, subtotal_row: int = 3, start_row: int = 5):
    blueprint = SHEET_BLUEPRINTS.get(ws_formula.title, {}); subtotal_col = blueprint.get('subtotal_col')
    if not subtotal_col: return
    col_letter = get_column_letter(subtotal_col); end_row = max(start_row, start_row + row_count - 1)
    ws_formula.cell(subtotal_row, subtotal_col).value = f"=SUBTOTAL(9,{col_letter}{start_row}:{col_letter}{end_row})"

def write_df_to_sheet_preserve_formulas(ws_formula, df: pd.DataFrame, header_tuples: List[Tuple[int, str]], formula_cols: Set[int], upb_header: str, start_row: int = 5):
    write_cols=[(c,h) for (c,h) in header_tuples if c not in formula_cols]; headers=[h for _,h in write_cols]
    missing={h: pd.NA for h in headers if h not in df.columns}; df_out = df.assign(**missing) if missing else df; df_out = df_out[headers]
    for r_offset, row in enumerate(df_out.itertuples(index=False, name=None), start=0):
        r = start_row + r_offset
        for (c,h), val in zip(write_cols, row):
            safe_val = _excel_safe_value(val)
            if _is_date_header(ws_formula.title, h): safe_val = _coerce_excel_date_value(safe_val)
            ws_formula.cell(r, c).value = safe_val
            _apply_display_style(ws_formula, r, c, h, upb_header)

def write_output_sheet(wb, sheet_name: str, df: pd.DataFrame, upb_col: str):
    if sheet_name not in wb.sheetnames: return
    ws = wb[sheet_name]; hdr = header_tuples_from_ws(ws, header_row=4, wb=wb, upb_header=upb_col); fcols = formula_col_indices(ws, start_row=5, header_row=4); formula_seeds = _capture_formula_seeds(ws, fcols, start_row=5)
    used_cols = _used_output_columns(ws, wb=wb, upb_header=upb_col, header_row=4, start_row=5)
    _clear_sheet_body(ws, used_cols, start_row=5)
    write_df_to_sheet_preserve_formulas(ws, df, hdr, fcols, upb_col, start_row=5)
    _copy_formula_columns_down(ws, formula_seeds, row_count=len(df), start_row=5)
    _refresh_subtotal_formula(ws, row_count=len(df), subtotal_row=3, start_row=5)
    _trim_sheet_body_rows(ws, row_count=max(len(df),1), start_row=5)


# 9) UI / build-flow integration snippets
# Add this uploader next to the servicer uploader:
#
# npl_reo_upload = st.file_uploader(
#     'Upload CV NPL / REO workbook (.xlsx) (optional)',
#     type=['xlsx'],
# )
#
# Inside the build button block, after prev_maps:
#
# npl_maps = {'loan_flags': pd.DataFrame(), 'asset_flags': pd.DataFrame()}
# if npl_reo_upload is not None:
#     status.update(label='Reading CV NPL / REO workbook...')
#     npl_maps = parse_npl_reo_bytes(npl_reo_upload.getvalue())
#
# Pass npl_maps into bridge build calls:
#
# bridge_asset_df = build_bridge_asset(
#     bridge_spine,
#     bridge_dnl,
#     bridge_val,
#     sf_am,
#     sf_active_rm,
#     serv_join,
#     upb_col,
#     prev_maps,
#     template_maps,
#     npl_maps=npl_maps,
# )
#
# bridge_loan_df = build_bridge_loan(
#     bridge_asset_df,
#     upb_col,
#     prev_maps,
#     npl_maps=npl_maps,
# )
#
# Also keep using the previous completed Active Loans workbook as the base template when uploaded:
#
# tmpl_bytes, tmpl_path_used = resolve_template_bytes(prev_upload)
