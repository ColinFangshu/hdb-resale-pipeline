"""Unit tests for the Resale Identifier."""

import pandas as pd
import pytest

from hdb_pipeline.transform import block_digits, transform


@pytest.fixture
def cfg():
    return {"transform": {
        "prefix": "S", "avg_price_group": ["month", "town", "flat_type"], "sequence_width": 3,
        "sequence_order": ["storey_range", "floor_area_sqm", "resale_price", "_source_row"],
    }}


def _df(rows):
    df = pd.DataFrame(rows, columns=["month", "town", "flat_type", "block", "storey_range",
                                     "floor_area_sqm", "resale_price", "_source_row"])
    for c in ["month", "town", "flat_type", "block", "storey_range"]:
        df[c] = df[c].astype("string")
    df["resale_price"] = df["resale_price"].astype("Float64")
    return df


def test_block_digits_rules():
    s = pd.Series(["19", "436B", "1009", "7", "C1"], dtype="string")
    assert block_digits(s).tolist() == ["019", "436", "100", "007", "001"]


def test_spec_example_ang_mo_kio(cfg):
    df = _df([["2012-01", "ANG MO KIO", "2 ROOM", "19", "01 TO 03", 45.0, 230000.0, 1],
              ["2012-01", "ANG MO KIO", "2 ROOM", "200", "04 TO 06", 45.0, 230999.0, 2]])
    out, _ = transform(df, cfg)
    # average 230,499.50 -> "230"; block 19 -> 019; 2012-01 -> 1201; Ang Mo Kio -> A
    assert out["rid_base"].iloc[0] == "S0192301201A"
    assert out["resale_identifier"].iloc[0] == "S0192301201A001"


def test_collisions_get_deterministic_unique_suffix(cfg):
    rows = [["2013-04", "SENGKANG", "4 ROOM", "436B", "07 TO 09", 96.0, 505000.0, 2],
            ["2013-04", "SENGKANG", "4 ROOM", "436A", "04 TO 06", 96.0, 519000.0, 1]]
    out1, summary = transform(_df(rows), cfg)
    out2, _ = transform(_df(rows[::-1]), cfg)                 # same data, opposite input order
    assert out1["rid_base"].nunique() == 1
    assert sorted(out1["resale_identifier"]) == ["S4365121304S001", "S4365121304S002"]
    by_row = lambda o: dict(zip(o["_source_row"], o["resale_identifier"]))
    assert by_row(out1) == by_row(out2)
    assert summary["largest_collision_group"].iloc[0] == 2


def test_overflowing_sequence_width_fails_loudly(cfg):
    cfg["transform"]["sequence_width"] = 1
    rows = [["2013-04", "SENGKANG", "4 ROOM", "436", "07 TO 09", 96.0, 500000.0, i] for i in range(10)]
    with pytest.raises(ValueError, match="too small"):
        transform(_df(rows), cfg)
