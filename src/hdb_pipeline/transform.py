"""Transform stage: build the Resale Identifier (requirement 8) on the Cleaned dataset.

Base identifier (12 characters), per the spec:

    S | BBB | PPP | YYMM | T
    S     literal "S"
    BBB   first 3 digits of `block` after removing non-digits, left-padded with zeros (19 -> 019)
    PPP   first 3 digits of the average resale_price for (month, town, flat_type). The average is
          truncated to whole dollars before taking digits, e.g. 230,512.50 -> "230"
    YYMM  year-month of the sale, e.g. 2012-01 -> 1201
    T     first character of town, e.g. ANG MO KIO -> A

Making it unique (no records dropped)
-------------------------------------
The base ID is not unique: rows in the same block digits, month and town-letter whose group
averages share the same 3-digit prefix collide. Every row gets a zero-padded sequence suffix:

    resale_identifier = base_id + NNN        (e.g. S01923012011A001)

The sequence is a row number within each base_id, in a fixed, data-based order (storey_range,
floor_area_sqm, resale_price, lease_commence_date, flat_model, street_name, then source lineage).
So:
  * IDs are deterministic: rerunning on the same data gives the same IDs;
  * every ID has the same length, whether or not that row collided;
  * the base ID stays recoverable as the first 12 characters.
The stage fails loudly if any base group outgrows the configured suffix width or if the final
IDs aren't unique.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


def block_digits(block: pd.Series) -> pd.Series:
    digits = block.str.replace(r"\D", "", regex=True)
    return digits.str[:3].str.zfill(3)          # a block with no digits becomes "000"


def avg_price_prefix(df: pd.DataFrame, group_keys: list[str]) -> tuple[pd.Series, pd.Series]:
    avg = df.groupby(group_keys)["resale_price"].transform("mean")
    prefix = avg.astype("int64").astype("string").str[:3].str.zfill(3)
    return avg, prefix


def base_identifier(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    t = cfg["transform"]
    avg, prefix = avg_price_prefix(df, t["avg_price_group"])
    out = df.assign(
        rid_block=block_digits(df["block"]),
        rid_avg_price=avg.round(2),
        rid_price_prefix=prefix,
        rid_yymm=df["month"].str[2:4] + df["month"].str[5:7],
        rid_town=df["town"].str[0],
    )
    out["rid_base"] = (t["prefix"] + out["rid_block"] + out["rid_price_prefix"]
                       + out["rid_yymm"] + out["rid_town"]).astype("string")
    return out


def make_unique(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    t = cfg["transform"]
    width = t["sequence_width"]
    ordered = df.sort_values(["rid_base"] + t["sequence_order"], kind="stable")
    seq = ordered.groupby("rid_base").cumcount() + 1
    if seq.max() >= 10 ** width:
        raise ValueError(f"A base ID has {seq.max()} rows; sequence_width={width} is too small")
    ordered["rid_sequence"] = seq.astype("int64")
    ordered["resale_identifier"] = (ordered["rid_base"] + seq.astype("string").str.zfill(width)).astype("string")
    out = ordered.loc[df.index]                              # restore the input row order
    if out["resale_identifier"].duplicated().any():
        raise AssertionError("resale_identifier is not unique")
    return out


def transform(cleaned: pd.DataFrame, cfg: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (transformed, collision_summary)."""
    out = make_unique(base_identifier(cleaned, cfg), cfg)
    sizes = out.groupby("rid_base").size()
    summary = pd.DataFrame([{
        "rows": len(out),
        "distinct_base_ids": len(sizes),
        "rows_with_colliding_base_id": int(sizes[sizes > 1].sum()),
        "pct_rows_colliding": round(100 * sizes[sizes > 1].sum() / len(out), 2),
        "largest_collision_group": int(sizes.max()),
        "distinct_final_ids": int(out["resale_identifier"].nunique()),
    }])
    assert len(out) == len(cleaned), "Transform dropped or added rows"
    log.info("Resale Identifier: %d rows, %d base IDs, %d final IDs (unique)",
             len(out), len(sizes), out["resale_identifier"].nunique())
    return out, summary
