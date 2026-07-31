"""
Temporary merge of split mismatch rows (ETOF + Cost type) for selected shippers.

Currently enabled for Aptiv only. Merges complementary pairs only:
  - row with Pre-calc. cost but no Carrier's cost
  - row with Carrier's cost but no Pre-calc. cost

Must run BEFORE discrepancy filtering so Pre-calc-only rows are not dropped first.
"""

from __future__ import annotations

import pandas as pd

# Temporary: remove shipper from this set or delete this module when merge is no longer needed
SHIPPERS_WITH_ETOF_ROW_MERGE = {"aptiv"}


def is_etof_merge_enabled_for_shipper(shipper_id: str | None) -> bool:
    """Return True if row merge is enabled for the given shipper."""
    if not shipper_id:
        return False
    return shipper_id.strip().lower() in SHIPPERS_WITH_ETOF_ROW_MERGE


def _column_rank(col_name: str, preferred_terms: list[str]) -> int:
    """Lower rank is better. Unmatched columns get a large rank."""
    col_lower = str(col_name).lower()
    for idx, term in enumerate(preferred_terms):
        if term in col_lower:
            return idx
    return len(preferred_terms) + 1


def _pick_best_column(df: pd.DataFrame, preferred_terms: list[str]) -> str | None:
    """Pick the best matching column using ordered preference terms."""
    candidates = []
    for col in df.columns:
        col_lower = str(col).lower()
        if any(term in col_lower for term in preferred_terms):
            candidates.append(col)
    if not candidates:
        return None
    return min(candidates, key=lambda col: _column_rank(col, preferred_terms))


def _find_etof_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        col_lower = str(col).lower().replace(" ", "").replace("_", "")
        if col_lower in {"etof", "etofnumber", "etof#"} or (
            "etof" in col_lower and ("#" in col_lower or "number" in col_lower)
        ):
            return col
    return None


def _find_cost_type_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        col_lower = str(col).lower()
        if col_lower == "cost type" or ("cost" in col_lower and "type" in col_lower):
            return col
    return None


def _resolve_amount_columns(df: pd.DataFrame) -> dict:
    """
    Resolve amount columns, preferring invoice-currency fields used in mismatch reports.

    Raw ISD files have both `Pre-calc. cost value` and `Pre-calc. cost (in inv curr)`.
    The pipeline must use invoice-currency columns for merge and discrepancy recompute.
    """
    precalc_col = _pick_best_column(
        df,
        [
            "pre-calc. cost (in inv curr)",
            "pre-calc. cost",
            "pre-calc",
            "precalc",
        ],
    )
    carrier_col = _pick_best_column(
        df,
        [
            "invoice statement cost  (in inv curr)",
            "invoice statement cost (in inv curr)",
            "carrier's cost",
            "invoice statement cost",
            "carrier cost",
        ],
    )
    discrepancy_col = _pick_best_column(
        df,
        [
            "discrepancy in inv currency  (in inv curr)",
            "discrepancy in inv currency (in inv curr)",
            "discrepancy in inv currency",
            "discrepancy",
        ],
    )
    agreement_col = _pick_best_column(df, ["carrier agreement"])

    return {
        "etof": _find_etof_column(df),
        "cost_type": _find_cost_type_column(df),
        "precalc": precalc_col,
        "carrier": carrier_col,
        "discrepancy": discrepancy_col,
        "agreement": agreement_col,
    }


def _is_present(value) -> bool:
    """Return True if value is a non-empty, non-null cell value."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _to_float(value):
    try:
        if _is_present(value):
            return float(value)
    except (TypeError, ValueError):
        return None
    return None


def recompute_discrepancy(df: pd.DataFrame, columns: dict | None = None) -> pd.DataFrame:
    """Recompute discrepancy as Carrier's cost minus Pre-calc. cost when both values exist."""
    work = df.copy()
    columns = columns or _resolve_amount_columns(work)
    precalc_col = columns.get("precalc")
    carrier_col = columns.get("carrier")
    discrepancy_col = columns.get("discrepancy")

    if not precalc_col or not carrier_col or not discrepancy_col:
        return work

    for idx, row in work.iterrows():
        precalc = _to_float(row.get(precalc_col))
        carrier = _to_float(row.get(carrier_col))
        if precalc is not None and carrier is not None:
            work.at[idx, discrepancy_col] = carrier - precalc

    return work


def _merge_two_rows(row_a: pd.Series, row_b: pd.Series, columns: dict) -> pd.Series:
    """Combine two complementary rows into one, filling Pre-calc / Carrier from each side."""
    merged = row_a.copy()
    precalc_col = columns.get("precalc")
    carrier_col = columns.get("carrier")

    for col in (precalc_col, carrier_col):
        if not col:
            continue
        for row in (row_a, row_b):
            val = row.get(col)
            if _is_present(val) and not _is_present(merged.get(col)):
                merged[col] = val

    return merged


def merge_split_etof_cost_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Merge complementary split rows within ETOF + Cost type (+ Carrier Agreement #).

    Returns:
        Tuple of (merged DataFrame, stats dict).
    """
    if df is None or df.empty:
        return df, {"enabled": True, "input_rows": 0, "output_rows": 0, "merged_pairs": 0}

    columns = _resolve_amount_columns(df)
    etof_col = columns["etof"]
    cost_type_col = columns["cost_type"]
    precalc_col = columns["precalc"]
    carrier_col = columns["carrier"]
    agreement_col = columns["agreement"]

    if not etof_col or not cost_type_col:
        print("   [ETOF merge] Skip: ETOF or Cost type column not found")
        return df.copy(), {"enabled": True, "skipped": True, "reason": "missing columns"}

    if not precalc_col or not carrier_col:
        print(
            f"   [ETOF merge] Skip: amount columns not found "
            f"(precalc={precalc_col}, carrier={carrier_col})"
        )
        return df.copy(), {"enabled": True, "skipped": True, "reason": "missing amount columns"}

    print(
        f"   [ETOF merge] Using columns: precalc='{precalc_col}', "
        f"carrier='{carrier_col}', discrepancy='{columns.get('discrepancy')}'"
    )

    work = df.copy()
    work["_cost_type_key"] = work[cost_type_col].astype(str).str.strip()
    work.loc[work["_cost_type_key"].isin(["", "nan", "NaN", "None"]), "_cost_type_key"] = pd.NA
    work["_cost_type_key"] = work["_cost_type_key"].ffill()

    group_cols = [etof_col, "_cost_type_key"]
    if agreement_col:
        group_cols.append(agreement_col)

    merged_rows: list[pd.Series] = []
    merged_pairs = 0

    for _, group in work.groupby(group_cols, sort=False, dropna=False):
        both_rows = []
        precalc_only = []
        carrier_only = []
        other_rows = []

        for _, row in group.iterrows():
            has_precalc = _is_present(row.get(precalc_col))
            has_carrier = _is_present(row.get(carrier_col))

            if has_precalc and has_carrier:
                both_rows.append(row)
            elif has_precalc:
                precalc_only.append(row)
            elif has_carrier:
                carrier_only.append(row)
            else:
                other_rows.append(row)

        merged_rows.extend(both_rows)
        merged_rows.extend(other_rows)

        while precalc_only and carrier_only:
            merged = _merge_two_rows(precalc_only.pop(0), carrier_only.pop(0), columns)
            merged_rows.append(merged)
            merged_pairs += 1

        merged_rows.extend(precalc_only)
        merged_rows.extend(carrier_only)

    result = pd.DataFrame(merged_rows).drop(columns=["_cost_type_key"], errors="ignore")
    result = result.reset_index(drop=True)
    result = recompute_discrepancy(result, columns)

    stats = {
        "enabled": True,
        "input_rows": len(df),
        "output_rows": len(result),
        "merged_pairs": merged_pairs,
        "rows_removed": len(df) - len(result),
        "precalc_col": precalc_col,
        "carrier_col": carrier_col,
        "discrepancy_col": columns.get("discrepancy"),
    }
    return result, stats


def maybe_merge_mismatch_rows(df: pd.DataFrame, shipper_id: str | None) -> pd.DataFrame:
    """Apply row merge only when shipper is in the whitelist (currently: aptiv)."""
    if not is_etof_merge_enabled_for_shipper(shipper_id):
        return df

    print(f"\n   [ETOF merge] Enabled for shipper '{shipper_id}' (Aptiv temporary logic)")
    merged_df, stats = merge_split_etof_cost_rows(df)
    print(
        f"   [ETOF merge] {stats['input_rows']} -> {stats['output_rows']} rows "
        f"({stats['merged_pairs']} complementary pairs merged)"
    )
    return merged_df
