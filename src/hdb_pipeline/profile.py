"""Profiling: a small set of dataset-specific checks, reusable at every pipeline stage.

`profile_dataset(df, reference)` returns a dict of small DataFrames. Each one answers one question:

  overview        How many rows, from where, over which months? Any months missing?
  columns         Per column: type, nulls, distinct values, min/max.
  domains         Which category values are NOT in the Jan 2012 reference set, and how often?
  numeric         How are price, floor area and price per sqm spread, by flat type?
  duplicates      How many exact duplicate records are there (business columns only)?
  consistency     Do values agree with each other (e.g. lease starting after the sale)?

The same function runs on the master, cleaned and quarantined datasets, so the report can show
how the data changes through the pipeline.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

BUSINESS_COLS = ["month", "town", "flat_type", "block", "street_name", "storey_range",
                 "floor_area_sqm", "flat_model", "lease_commence_date", "resale_price"]


def overview(df: pd.DataFrame, scope: dict[str, str]) -> pd.DataFrame:
    expected = pd.period_range(scope["start_month"], scope["end_month"], freq="M").strftime("%Y-%m")
    present = set(df["month"].dropna())
    missing = [m for m in expected if m not in present]
    by_month = df.groupby("month").size()
    return pd.DataFrame([{
        "rows": len(df),
        "sources": df["_source_dataset_id"].nunique(),
        "first_month": df["month"].min(),
        "last_month": df["month"].max(),
        "months_expected": len(expected),
        "months_missing": len(missing),
        "missing_list": missing[:12],
        "rows_per_month_min": int(by_month.min()) if len(by_month) else 0,
        "rows_per_month_median": int(by_month.median()) if len(by_month) else 0,
        "rows_per_month_max": int(by_month.max()) if len(by_month) else 0,
    }])


def column_profile(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for c in BUSINESS_COLS:
        s = df[c]
        numeric = pd.api.types.is_numeric_dtype(s)
        rows.append({
            "column": c,
            "dtype": str(s.dtype),
            "nulls": int(s.isna().sum()),
            "distinct": int(s.nunique()),
            "min": s.min() if numeric else None,
            "max": s.max() if numeric else None,
            "top_value": s.mode().iloc[0] if s.notna().any() else None,
        })
    return pd.DataFrame(rows)


def domain_drift(df: pd.DataFrame, reference: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Values in `df` that the reference set doesn't contain, with counts and when they appear.
    Compared case-insensitively, so a casing difference alone is not reported as drift."""
    out = []
    for c in columns:
        allowed = set(reference[c].dropna().str.upper())
        unseen = df[~df[c].str.upper().isin(allowed)]
        for value, g in unseen.groupby(c):
            out.append({"column": c, "value": value, "rows": len(g),
                        "first_month": g["month"].min(), "last_month": g["month"].max()})
    cols = ["column", "value", "rows", "first_month", "last_month"]
    return pd.DataFrame(out, columns=cols).sort_values(["column", "rows"], ascending=[True, False],
                                                       ignore_index=True)


def numeric_profile(df: pd.DataFrame) -> pd.DataFrame:
    d = df.assign(price_per_sqm=df["resale_price"] / df["floor_area_sqm"])
    stats = (d.groupby("flat_type")[["resale_price", "floor_area_sqm", "price_per_sqm"]]
               .describe(percentiles=[0.01, 0.5, 0.99]))
    keep = ["count", "min", "1%", "50%", "99%", "max"]
    stats = stats.loc[:, (slice(None), keep)].round(0)
    stats.columns = [f"{col}|{stat}" for col, stat in stats.columns]
    return stats


def duplicate_profile(df: pd.DataFrame) -> pd.DataFrame:
    dup_mask = df.duplicated(subset=BUSINESS_COLS, keep="first")
    by_src = df[dup_mask].groupby("_source_dataset_id").size().rename("duplicate_rows")
    total = pd.Series({"ALL": int(dup_mask.sum())}, name="duplicate_rows")
    return pd.concat([by_src, total]).to_frame()


def consistency_checks(df: pd.DataFrame) -> pd.DataFrame:
    sale_year = pd.to_numeric(df["month"].str[:4], errors="coerce")
    checks = {
        "lease_commence_after_sale_year": df["lease_commence_date"] > sale_year,
        "non_positive_price": df["resale_price"] <= 0,
        "non_positive_floor_area": df["floor_area_sqm"] <= 0,
        "block_without_digits": ~df["block"].str.contains(r"\d", na=False),
        "storey_range_bad_format": ~df["storey_range"].str.fullmatch(r"\d{2} TO \d{2}", na=False),
    }
    return pd.DataFrame([{"check": k, "rows_flagged": int(v.fillna(False).sum())} for k, v in checks.items()])


def profile_dataset(df: pd.DataFrame, reference: pd.DataFrame, cfg: dict[str, Any]) -> dict[str, pd.DataFrame]:
    """Run every check and return the results keyed by name."""
    return {
        "overview": overview(df, cfg["scope"]),
        "columns": column_profile(df),
        "domains": domain_drift(df, reference, cfg["validation"]["domain_columns"]),
        "numeric": numeric_profile(df),
        "duplicates": duplicate_profile(df),
        "consistency": consistency_checks(df),
    }


def reference_set(master: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    """The Jan 2012 authoritative rows that domain checks are compared against."""
    ref = master[master["month"] == cfg["validation"]["reference_month"]]
    if ref.empty:
        raise ValueError(f"No rows for reference month {cfg['validation']['reference_month']}")
    return ref
