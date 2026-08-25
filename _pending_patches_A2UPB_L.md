# Pending patches (UNAPPLIED) — A.2-UPB and L

Translated from the row-dict pseudocode into the actual **vectorized** `hayden.py`
architecture. Hold until the V41 rebuild diff confirms behavior, then apply.

Key architecture facts these patches respect:
- `_build_term_loan_salesforce_fallback` and `build_bridge_asset` operate on whole
  DataFrames (`out`), not per-row dicts.
- Inside `build_bridge_asset`, after the servicer merge, `out`-indexed columns
  (`_serv_next_payment_date`, `Next Payment Date`, `Current Servicer UPB`,
  `_prev_asset_upb`, `SF Funded Amount`) are mutually aligned. `sf_next_payment` /
  `sf_current_upb` carry the *spine* index and can silently reindex to NaN — so both
  patches use **only `out`-indexed columns** for masks.
- The servicer files parsed by `parse_servicer_bytes` expose `upb` but **no
  last-paid-date**, so the L "payment activity" test reduces to `matched_upb > 0`.

---

## A.2 — UPB half (FCI rolled-forward → SF UPB basis)

The A.2 **Status** half already shipped in V41. This adds the UPB half using the SAME
`_fci_rolled_mask` already computed in that block (index-safe, `out`-only).

### Anchor
In `build_bridge_asset`, immediately AFTER the existing V41 block that ends with:

```python
    if bool(_fci_rolled_mask.any()):
        out.loc[_fci_rolled_mask, "Servicer Status"] = "N/A"
```

(and BEFORE `out["Servicer Maturity Date"] = ...`), insert:

```python
    # Fix A.2 (UPB half): for the same FCI rolled-forward rows, the FCI servicer-file UPB
    # is a post-modification balance the official report does not honor. Replace it with
    # the per-asset SF basis (prior completed asset UPB -> SF loan UPB -> funded amount),
    # all of which are out-indexed so this stays index-safe. upb_col was already set above
    # from the servicer-file value; this overrides only the masked rows.
    if bool(_fci_rolled_mask.any()):
        _a2_sf_basis = _coalesce_positive_then_any_numeric(
            pd.to_numeric(out.get("_prev_asset_upb", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"),
            pd.to_numeric(out.get("Current Servicer UPB", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"),
            pd.to_numeric(out.get("SF Funded Amount", pd.Series([np.nan] * len(out), index=out.index)), errors="coerce"),
            index=out.index,
        )
        out.loc[_fci_rolled_mask, upb_col] = pd.to_numeric(_a2_sf_basis, errors="coerce").loc[_fci_rolled_mask]
```

### Caveat to verify on the diff
The basis ordering (`prev asset UPB -> SF loan UPB -> funded`) is the judgment call.
If the rebuild diff shows the masked rows landing on the wrong number, the fix is to
re-order these three or add `prev_loan_upb`. The *set of masked rows* is correct (it is
exactly A.1's gate); only the replacement value is open.

### Verification after rebuild
```python
# FCI rolled-forward rows should no longer carry the FCI-file UPB:
ba = pd.read_excel(test_path, sheet_name="Bridge Asset", header=4, keep_default_na=False, dtype=object)
# (cross-check the ~329 '6/19 UPB' cascade cells against same-day real -> expect ~0)
```

---

## L — Term servicer payment-activity gate

Real reports `Servicer = N/A` on freshly-boarded term deals (SF_Term has a
`Servicer Name`, but no servicer file row with a positive UPB yet). Confirmed on the
38 same-day mismatches (e.g. 47576/59864/61372 → real N/A, SF_Term says Berkadia/FCI).

The real `_select_term_servicer_matches` is a per-row scorer; rather than rewrite it,
gate the **projection** in `_build_term_loan_salesforce_fallback` where the servicer
columns are written from `match_df`. The activity signal is `match_df["matched_upb"]`
(the servicer-file UPB) being > 0.

### Anchor
In `_build_term_loan_salesforce_fallback`, find this existing block:

```python
    out["Servicer ID"] = coalesce_keep_nonblank(sf_commitment_display, match_df["selected_servicer_id"])
    out["Servicer ID"] = coalesce_keep_nonblank(out["Servicer ID"], out.get("Servicer ID", blank_obj))

    sf_upb_fallback = pd.to_numeric(
        sf_term["Current Servicer UPB"] if "Current Servicer UPB" in sf_term.columns else pd.Series([np.nan] * len(out)),
        errors="coerce",
    )

    # Servicer file/match name may enrich reporting but should not overwrite the visible
    # Term Loan Servicer label from prior/SF. N/A is a valid display value.
    out["Servicer"] = coalesce_keep_nonblank(out["Servicer"], match_df["matched_servicer"])
    matched_mat = pd.to_datetime(match_df["matched_maturity_date"], errors="coerce")
    cur_mat = pd.to_datetime(out["Maturity Date"], errors="coerce")
    out["Maturity Date"] = cur_mat.where(cur_mat.notna(), matched_mat)
    matched_npd = pd.to_datetime(match_df["matched_next_payment_date"], errors="coerce")
    cur_npd = pd.to_datetime(out["Next Payment Date"], errors="coerce")
    out["Next Payment Date"] = cur_npd.where(cur_npd.notna(), matched_npd)
    out[upb_col] = pd.to_numeric(match_df["matched_upb"], errors="coerce").where(
        pd.to_numeric(match_df["matched_upb"], errors="coerce").notna(),
        sf_upb_fallback,
    )
```

Immediately AFTER that block (before `out = _apply_term_preboarding_upb_fallback(...)`),
insert the gate:

```python
    # Fix L: a Term deal that is BOARDED to a servicer but not yet serviced (no servicer
    # file row with a positive UPB) is shown as N/A by the official report, even though
    # SF_Term carries a Servicer Name and Commitment Id. Detect "no payment activity"
    # (matched servicer-file UPB is not > 0) and blank the servicer-derived columns for
    # those rows so they read N/A. Verified vs same-day real on 38 deals (47576, 59864,
    # 61372, 61683, 62486, ...). Preboarding (Approved/Purchased) deals are exempt -- the
    # preboarding fallback below intentionally supplies their Loan-Amount UPB.
    _l_matched_upb = pd.to_numeric(match_df["matched_upb"], errors="coerce")
    _l_has_activity = _l_matched_upb.gt(0).fillna(False)
    _l_stage = pd.Series(sf_term.get("Stage", pd.Series([pd.NA] * len(out), index=out.index)).to_numpy(), index=out.index).astype("string").str.strip()
    _l_preboarding = _l_stage.isin(TERM_PREBOARDING_STAGES).fillna(False)
    # A deal "claims a servicer" if SF or the match produced any servicer id/name.
    _l_claims_servicer = (~blankish_mask(out.get("Servicer ID", blank_obj))) | (~blankish_mask(out.get("Servicer", blank_obj)))
    _l_boarding_only = _l_claims_servicer & (~_l_has_activity) & (~_l_preboarding)
    if bool(_l_boarding_only.any()):
        out.loc[_l_boarding_only, "Servicer"] = pd.NA
        out.loc[_l_boarding_only, "Servicer ID"] = pd.NA
        out.loc[_l_boarding_only, upb_col] = np.nan
        out.loc[_l_boarding_only, "Next Payment Date"] = pd.NaT
        # Maturity Date is left as-is: it can come from SF and is shown even pre-service.
```

### Caveats to verify on the diff
1. **Over-blank risk:** a genuinely-serviced loan that happens to have a servicer-file
   UPB of exactly 0 (paid to zero but still active) would be blanked. If the diff shows
   *new* N/A regressions on established deals, tighten `_l_boarding_only` to also require
   the deal be absent from the prior workbook (`~out["_deal_key"].isin(prev_positive_keys)`).
2. **UPB cascade:** blanking `upb_col` here means Term Asset UPB for these deals will not
   allocate — which matches real (TL UPB 32 + TA cascade). Confirm `_guard_term_loan_upb_vs_amount`
   and `_allocate_term_asset_upb_from_loan` tolerate the N/A (they already treat blank as
   no-allocation).
3. **Special Loans List (201 cascade):** once NPD is N/A, the materialized special-list
   formula returns N/A for these deals — confirm that lands.

### Verification after rebuild
```python
tl = pd.read_excel(test_path, sheet_name="Term Loan", header=4, keep_default_na=False, dtype=object)
for d in ["47576", "59864", "61372", "61683", "62486"]:
    row = tl[tl["Deal Number"].astype(str) == d]
    assert row["Servicer"].iloc[0] in ("N/A", ""), f"deal {d} Servicer={row['Servicer'].iloc[0]}"
```

---

## Apply order once the V41 diff is in hand
1. Confirm V41 cleared A/K/H.1/G/N/E.2/F/A.2-Status as expected.
2. Apply A.2-UPB (low risk, index-safe; only value choice to verify).
3. Apply L (medium risk; watch the over-blank caveat). Re-diff.
4. Then revisit B / C.2 / E.1 / I / M with the post-L numbers.
