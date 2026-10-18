"""Enrich stage: add remaining lease (requirement 4) and price-anomaly flags (requirement 5) to Cleaned.

Remaining lease as of 1 Jan 2027
--------------------------------
Assumption: the data gives the lease start as a year only, so every lease is taken to start on
1 Jan of `lease_commence_date` and run for 99 years. Then:
    months_left = (lease_commence_date + 99) * 12 - 2027 * 12      (never below 0)
    years, months = divmod(months_left, 12)                         (rounded down by construction)
Because the year-only start always falls on 1 Jan, `months` is always 0. The cross-check against
HDB's own `remaining_lease` (2015 onwards) measures how far this assumption is off.

Price anomalies (flag, don't quarantine)
----------------------------------------
  PEER_OUTLIER      Price per sqm is far from its peers by robust z-score:
                      z = 0.6745 * (x - median) / MAD,  flagged when |z| > threshold (3.5, the
                      Iglewicz & Hoaglin cut-off). Median and MAD aren't dragged around by the
                      outliers being looked for, unlike mean and standard deviation.
                    Peers are the most specific group in `peer_levels` with at least
                    `min_peer_size` sales. Price depends on location, flat type, flat model,
                    lease age and time, so the first level groups on all five. Every fallback
                    keeps flat_model, so rare but genuine homes (e.g. Whampoa terrace houses)
                    are compared with each other, not with ordinary flats.
  PRICE_OUT_OF_RANGE  Price or price per sqm outside hard plausibility bounds from config.
"""

from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------------------------
# Remaining lease
# ---------------------------------------------------------------------------------------------
def remaining_lease(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    e = cfg["enrich"]
    as_of = pd.Timestamp(e["lease_as_of"])
    end_months = (df["lease_commence_date"] + e["lease_years"]) * 12          # lease ends 1 Jan
    left = (end_months - (as_of.year * 12 + as_of.month - 1)).clip(lower=0)
    years, months = left // 12, left % 12
    return df.assign(
        remaining_lease_years=years.astype("Int64"),
        remaining_lease_months=months.astype("Int64"),
        remaining_lease_2027=(years.astype("string") + " years " +
                              months.astype("string").str.zfill(2) + " months"),
    )


def parse_src_remaining_lease(s: pd.Series) -> pd.Series:
    """HDB's own remaining lease in months. Handles '70', '61 years 04 months', '54 years 01 month'
    and '60 years'. Returns <NA> where it's missing or doesn't parse."""
    def one(v):
        if pd.isna(v):
            return pd.NA
        if re.fullmatch(r"\d+", v):
            return int(v) * 12
        m = re.fullmatch(r"(\d+) years?(?: (\d+) months?)?", v)
        return int(m[1]) * 12 + int(m[2] or 0) if m else pd.NA
    return s.map(one).astype("Int64")


def lease_cross_check(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    """Compare our method, applied at each sale date, with HDB's published remaining lease.
    Differences are in months (ours minus HDB's). Negative means ours is shorter."""
    sale_months = df["month"].str[:4].astype(int) * 12 + df["month"].str[5:7].astype(int) - 1
    ours = (df["lease_commence_date"] + cfg["enrich"]["lease_years"]) * 12 - sale_months
    src = parse_src_remaining_lease(df["src_remaining_lease"])
    d = pd.DataFrame({"era": np.where(df["month"] >= "2017", "2017 (years+months)", "2015-16 (years)"),
                      "diff": (ours - src).astype("Float64")})[src.notna()]
    return (d.groupby("era")["diff"]
             .describe(percentiles=[0.05, 0.5, 0.95]).round(1)
             .rename(columns=lambda c: f"diff_months_{c}" if c != "count" else "rows"))


# ---------------------------------------------------------------------------------------------
# Price anomalies
# ---------------------------------------------------------------------------------------------
def price_anomalies(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    a = cfg["enrich"]["anomaly"]
    out = df.assign(sale_year=df["month"].str[:4],
                    lease_decade=(df["lease_commence_date"] // 10 * 10).astype("string") + "s",
                    price_per_sqm=(df["resale_price"] / df["floor_area_sqm"]).round(2))

    # choose the most specific peer level that has enough sales, row by row
    out["peer_level"] = pd.NA
    out["peer_median_ppsm"] = np.nan
    out["robust_z"] = np.nan
    for level, keys in enumerate(a["peer_levels"], start=1):
        g = out.groupby(keys)["price_per_sqm"]
        size = g.transform("size")
        take = out["peer_level"].isna() & (size >= a["min_peer_size"])
        if not take.any():
            continue
        med = g.transform("median")
        mad = (out["price_per_sqm"] - med).abs().groupby([out[k] for k in keys]).transform("median")
        z = 0.6745 * (out["price_per_sqm"] - med) / mad.replace(0, np.nan)   # MAD 0 -> can't score
        out.loc[take, "peer_level"] = "×".join(keys)
        out.loc[take, "peer_median_ppsm"] = med[take]
        out.loc[take, "robust_z"] = z[take].round(2)

    b = a["bounds"]
    flags = pd.DataFrame({
        "PEER_OUTLIER": out["robust_z"].abs() > a["z_threshold"],
        "PRICE_OUT_OF_RANGE": ~out["resale_price"].between(b["price_min"], b["price_max"])
                              | ~out["price_per_sqm"].between(b["ppsm_min"], b["ppsm_max"]),
    }).fillna(False).astype(bool)
    out["anomaly_reasons"] = flags.apply(lambda r: "|".join(flags.columns[r.to_numpy()]), axis=1).astype("string")
    out["is_price_anomaly"] = out["anomaly_reasons"].ne("")
    out["peer_level"] = out["peer_level"].astype("string")
    return out.drop(columns=["sale_year", "lease_decade"])


def enrich(cleaned: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    return price_anomalies(remaining_lease(cleaned, cfg), cfg)
