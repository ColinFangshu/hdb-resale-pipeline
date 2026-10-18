"""Standardise stage: combine the raw files into one master dataset shaped like the Jan 2012 file.

What happens here
-----------------
1. Pick the authoritative dataset: the raw file whose coverage contains the scope start month
   (2012-01). This is worked out from the manifest, not hardcoded.
2. Read every raw file with all columns as strings, so nothing is coerced on read.
3. Keep only rows inside the scope window. Rows whose `month` can't be parsed are kept, not
   silently dropped: they can't be scoped, so validation will quarantine them.
4. Conform each file to the authoritative schema (same columns, same order). Extra source columns
   are kept with a `src_` prefix (e.g. `src_remaining_lease`). Columns the authoritative file has
   but a source lacks are added as nulls.
5. Tidy text (trim and collapse whitespace) and cast numeric columns. If a value can't be cast,
   the column name goes into `_cast_errors` and validation decides what to do with it. This stage
   never drops or fixes a bad value.
6. Add lineage columns: `_source_dataset_id`, `_source_row` (1-based data row in the raw file).

Reconciliation guarantee: len(master) == sum of in-scope rows across raw files.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

LINEAGE_COLS = ["_source_dataset_id", "_source_row"]
EXTRA_PREFIX = "src_"


def pick_authoritative(manifest: pd.DataFrame, scope_start: str) -> pd.Series:
    """Return the manifest row whose coverage window contains `scope_start` (YYYY-MM)."""
    hits = manifest[(manifest["coverage_start"] <= scope_start) & (manifest["coverage_end"] >= scope_start)]
    if len(hits) != 1:
        raise ValueError(f"Expected exactly one dataset covering {scope_start}, found {len(hits)}")
    return hits.iloc[0]


def _tidy_text(s: pd.Series) -> pd.Series:
    """Trim leading/trailing whitespace and collapse internal runs of whitespace to one space."""
    return s.str.strip().str.replace(r"\s+", " ", regex=True)


def _read_raw(path: Path) -> pd.DataFrame:
    # dtype=str + keep_default_na=False: read exactly what is in the file ("NA" stays "NA")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def standardise(cfg: dict[str, Any], manifest: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build the master dataset. Returns (master, per-source summary)."""
    raw_dir: Path = cfg["paths"]["raw"]
    std = cfg["standardise"]
    scope_start, scope_end = cfg["scope"]["start_month"], cfg["scope"]["end_month"]

    auth = pick_authoritative(manifest, scope_start)
    auth_cols: list[str] = list(auth["columns"])
    log.info("Authoritative schema from %s (%s): %s", auth["dataset_id"], auth["name"], auth_cols)

    frames, summary = [], []
    for _, entry in manifest.iterrows():
        df = _read_raw(raw_dir / entry["file"])
        df["_source_row"] = range(1, len(df) + 1)

        month_ok = df["month"].str.fullmatch(r"\d{4}-\d{2}")
        in_scope = df["month"].between(scope_start, scope_end) | ~month_ok
        df = df[in_scope].copy()

        missing = [c for c in auth_cols if c not in df.columns]
        extra = [c for c in df.columns if c not in auth_cols and c != "_source_row"]
        for c in missing:
            df[c] = pd.NA
        df = df.rename(columns={c: EXTRA_PREFIX + c for c in extra})
        df["_source_dataset_id"] = entry["dataset_id"]

        summary.append({
            "dataset_id": entry["dataset_id"], "name": entry["name"],
            "rows_raw": int(entry["row_count"]), "rows_in_scope": len(df),
            "unparseable_month": int((~month_ok[in_scope]).sum()),
            "missing_cols": missing, "extra_cols": extra,
        })
        frames.append(df)

    master = pd.concat(frames, ignore_index=True)
    extra_cols = sorted({c for c in master.columns if c.startswith(EXTRA_PREFIX)})
    master = master[auth_cols + extra_cols + LINEAGE_COLS]

    # --- tidy text: count how many values actually change, for the profiling report
    text_cols = [c for c in auth_cols + extra_cols if c not in std["numeric_columns"]]
    tidied = {}
    for c in text_cols:
        before = master[c]
        master[c] = _tidy_text(before.astype("string"))
        tidied[c] = int((before.notna() & (before.astype("string") != master[c])).sum())

    # --- cast numerics; record failures instead of dropping or fixing anything
    errors = pd.Series([[] for _ in range(len(master))], index=master.index, dtype=object)
    for c in std["numeric_columns"]:
        raw_vals = master[c].astype("string").str.strip()
        cast = pd.to_numeric(raw_vals, errors="coerce")
        if std["numeric_columns"][c].lower().startswith("int"):
            cast = cast.where(cast.isna() | (cast % 1 == 0))   # 1986.5 is not a valid year
        failed = raw_vals.notna() & (raw_vals != "") & cast.isna()
        for i in master.index[failed]:
            errors[i] = errors[i] + [c]
        master[c] = cast.astype(std["numeric_columns"][c])
    master.insert(len(master.columns) - len(LINEAGE_COLS), "_cast_errors", errors)

    summary_df = pd.DataFrame(summary)
    expected = int(summary_df["rows_in_scope"].sum())
    assert len(master) == expected, f"Reconciliation failed: {len(master)} != {expected}"
    log.info("Master dataset: %d rows from %d sources; whitespace fixes: %s; cast failures: %d",
             len(master), len(summary_df), {k: v for k, v in tidied.items() if v} or "none",
             int(master["_cast_errors"].str.len().gt(0).sum()))
    summary_df.attrs["tidied_values"] = tidied
    return master, summary_df


def write_staging(master: pd.DataFrame, cfg: dict[str, Any]) -> Path:
    """Persist the master dataset to the staging zone as Parquet (keeps dtypes, unlike CSV)."""
    out_dir: Path = cfg["paths"]["staging"]
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "master.parquet"
    master.to_parquet(path, index=False)
    return path
