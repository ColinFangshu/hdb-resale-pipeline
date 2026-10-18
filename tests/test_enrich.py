"""Unit tests for the enrich stage."""

import numpy as np
import pandas as pd
import pytest

from hdb_pipeline.enrich import enrich, parse_src_remaining_lease, remaining_lease


@pytest.fixture
def cfg():
    return {"enrich": {
        "lease_years": 99, "lease_as_of": "2027-01-01",
        "anomaly": {"z_threshold": 3.5, "min_peer_size": 20,
                    "peer_levels": [["town", "flat_type", "flat_model", "sale_year"], ["flat_type"]],
                    "bounds": {"price_min": 50000, "price_max": 3000000, "ppsm_min": 1000, "ppsm_max": 20000}},
    }}


def _peers(n=30, town="BEDOK", model="Improved", seed=0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "month": ["2015-06"] * n, "town": [town] * n, "flat_type": ["4 ROOM"] * n,
        "flat_model": [model] * n, "lease_commence_date": pd.array([1985] * n, dtype="Int64"),
        "floor_area_sqm": pd.array([100.0] * n, dtype="Float64"),
        "resale_price": pd.array(rng.normal(450000, 10000, n).round(), dtype="Float64"),
        "src_remaining_lease": pd.array([pd.NA] * n, dtype="string"),
    })


def test_remaining_lease_rounds_down_and_never_negative(cfg):
    df = pd.DataFrame({"lease_commence_date": pd.array([1966, 1990, 1920], dtype="Int64")})
    out = remaining_lease(df, cfg)
    assert out["remaining_lease_years"].tolist() == [38, 62, 0]       # 1966+99=2065 -> 38y
    assert out["remaining_lease_months"].tolist() == [0, 0, 0]
    assert out["remaining_lease_2027"].iloc[0] == "38 years 00 months"


def test_parse_all_published_lease_formats():
    s = pd.Series(["70", "61 years 04 months", "54 years 01 month", "60 years", None, "junk"], dtype="string")
    assert parse_src_remaining_lease(s).tolist() == [840, 736, 649, 720, pd.NA, pd.NA]


def test_injected_anomalies_are_flagged(cfg):
    df = _peers()
    df.loc[0, "resale_price"] = 900000.0      # double the peer price
    df.loc[1, "resale_price"] = 40000.0       # below the hard floor and far below peers
    out = enrich(df, cfg)
    assert out.loc[0, "anomaly_reasons"] == "PEER_OUTLIER"
    assert out.loc[1, "anomaly_reasons"] == "PEER_OUTLIER|PRICE_OUT_OF_RANGE"
    assert out["is_price_anomaly"].sum() == 2


def test_small_group_falls_back_to_wider_peers(cfg):
    df = pd.concat([_peers(30), _peers(5, town="BISHAN", seed=1)], ignore_index=True)
    out = enrich(df, cfg)
    assert out.loc[out["town"] == "BISHAN", "peer_level"].unique().tolist() == ["flat_type"]
    assert out.loc[out["town"] == "BEDOK", "peer_level"].unique().tolist() == ["town×flat_type×flat_model×sale_year"]
