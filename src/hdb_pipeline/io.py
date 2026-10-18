"""Write an output group (cleaned, quarantined, ...) as Parquet (for pipelines) and CSV (for people)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _flatten_lists(df: pd.DataFrame) -> pd.DataFrame:
    """List columns (e.g. _cast_errors) become 'a|b' strings so CSV and Parquet match."""
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == object and out[c].map(lambda v: isinstance(v, list)).any():
            out[c] = out[c].map(lambda v: "|".join(v) if isinstance(v, list) else v).astype("string")
    return out


def write_output(df: pd.DataFrame, group: str, cfg: dict[str, Any]) -> dict[str, Path]:
    out_dir: Path = cfg["paths"][group]
    out_dir.mkdir(parents=True, exist_ok=True)
    flat = _flatten_lists(df).reset_index(drop=True)
    paths = {"parquet": out_dir / f"{group}.parquet", "csv": out_dir / f"{group}.csv"}
    flat.to_parquet(paths["parquet"], index=False)
    flat.to_csv(paths["csv"], index=False)
    return paths
