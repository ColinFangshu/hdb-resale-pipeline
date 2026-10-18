"""Validation stage: split the master dataset into Cleaned and Quarantined.

Each rule returns a True/False mask (True = the row fails) and has a reason code. A row can fail
several rules: every failure is recorded in `_dq_reasons` (codes joined with "|"), so the
quarantine shows *all* the reasons, not just the first one found.

Rules
-----
  CAST_ERROR            A numeric column couldn't be cast (recorded by the standardise stage)
  INVALID_MONTH         month isn't in YYYY-MM format (kept by standardise so it can't vanish)
  MISSING_VALUE         A business column is null or empty
  INVALID_<COLUMN>      town / flat_type / flat_model / storey_range isn't in the Jan 2012 set
                        (compared case-insensitively; passing values are rewritten to the
                        reference spelling)
  NON_POSITIVE_PRICE    resale_price <= 0
  NON_POSITIVE_AREA     floor_area_sqm <= 0
  LEASE_AFTER_SALE      lease_commence_date is later than the sale year
  DUPLICATE             Exact copy of an earlier record across the 10 business columns. The
                        first copy is kept, ordered by source then source row, so reruns
                        always pick the same one

Guarantee: len(cleaned) + len(quarantined) == len(master), with no row in both.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import pandas as pd

from hdb_pipeline.profile import BUSINESS_COLS

log = logging.getLogger(__name__)

Rule = Callable[[pd.DataFrame], pd.Series]


def _domain_rule(column: str, reference: pd.DataFrame) -> Rule:
    allowed = set(reference[column].dropna().str.upper())
    return lambda df: df[column].notna() & ~df[column].str.upper().isin(allowed)


def build_rules(reference: pd.DataFrame, cfg: dict[str, Any]) -> dict[str, Rule]:
    """Every validation rule, keyed by reason code."""
    sale_year = lambda df: pd.to_numeric(df["month"].str[:4], errors="coerce")
    rules: dict[str, Rule] = {
        "CAST_ERROR": lambda df: df["_cast_errors"].str.len() > 0,
        "INVALID_MONTH": lambda df: ~df["month"].str.fullmatch(r"\d{4}-\d{2}", na=False),
        "MISSING_VALUE": lambda df: df[BUSINESS_COLS].isna().any(axis=1)
                                    | df[BUSINESS_COLS].astype("string").eq("").any(axis=1),
    }
    for col in cfg["validation"]["domain_columns"]:
        rules[f"INVALID_{col.upper()}"] = _domain_rule(col, reference)
    rules.update({
        "NON_POSITIVE_PRICE": lambda df: df["resale_price"] <= 0,
        "NON_POSITIVE_AREA": lambda df: df["floor_area_sqm"] <= 0,
        "LEASE_AFTER_SALE": lambda df: df["lease_commence_date"] > sale_year(df),
        "DUPLICATE": lambda df: df.duplicated(subset=BUSINESS_COLS, keep="first"),
    })
    return rules


def canonicalise(df: pd.DataFrame, reference: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Rewrite each passing domain value to the reference spelling (e.g. 'IMPROVED' -> 'Improved')."""
    out = df.copy()
    for c in columns:
        # if the reference itself had two spellings of one value, use the more frequent one
        canon = (reference[c].dropna().to_frame().assign(key=lambda x: x[c].str.upper())
                 .groupby("key")[c].agg(lambda s: s.mode().iloc[0]))
        out[c] = out[c].str.upper().map(canon).fillna(out[c]).astype("string")
    return out


def validate(master: pd.DataFrame, reference: pd.DataFrame, cfg: dict[str, Any]
             ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Returns (cleaned, quarantined, rule_summary)."""
    df = master.sort_values(["_source_dataset_id", "_source_row"], kind="stable")
    rules = build_rules(reference, cfg)

    fails = pd.DataFrame({code: rule(df).fillna(False).astype(bool) for code, rule in rules.items()})
    reasons = fails.apply(lambda row: "|".join(fails.columns[row.to_numpy()]), axis=1)
    df = df.assign(_dq_reasons=reasons)

    is_bad = reasons.ne("")
    cleaned = canonicalise(df[~is_bad].drop(columns="_dq_reasons"), reference,
                           cfg["validation"]["domain_columns"])
    quarantined = df[is_bad]

    summary = pd.DataFrame({
        "rows_failed": fails.sum(),
        "only_reason": fails[fails.sum(axis=1) == 1].sum(),   # rows quarantined by this rule alone
    }).rename_axis("rule").reset_index()
    summary["pct_of_master"] = (100 * summary["rows_failed"] / len(df)).round(2)

    assert len(cleaned) + len(quarantined) == len(master), "Validation reconciliation failed"
    assert not set(cleaned.index) & set(quarantined.index), "A row is in both Cleaned and Quarantined"
    log.info("Validation: %d cleaned, %d quarantined (%.1f%%)",
             len(cleaned), len(quarantined), 100 * len(quarantined) / len(master))
    return cleaned, quarantined, summary
