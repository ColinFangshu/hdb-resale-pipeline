"""Unit tests for the validation stage."""

import pandas as pd
import pytest

from hdb_pipeline.validate import validate

COLS = ["month", "town", "flat_type", "block", "street_name", "storey_range", "floor_area_sqm",
        "flat_model", "lease_commence_date", "resale_price", "_cast_errors", "_source_dataset_id", "_source_row"]
GOOD = ["2012-01", "BEDOK", "3 ROOM", "10", "BEDOK RD", "01 TO 03", 67.0, "Improved", 1980, 300000.0, [], "a", 1]


def _df(rows):
    df = pd.DataFrame(rows, columns=COLS)
    for c in ["floor_area_sqm", "resale_price"]:
        df[c] = df[c].astype("Float64")
    df["lease_commence_date"] = df["lease_commence_date"].astype("Int64")
    for c in ["month", "town", "flat_type", "block", "street_name", "storey_range", "flat_model"]:
        df[c] = df[c].astype("string")
    return df


def _row(**changes):
    r = dict(zip(COLS, GOOD))
    r.update(changes)
    return [r[c] for c in COLS]


@pytest.fixture
def cfg():
    return {"validation": {"domain_columns": ["town", "flat_type", "flat_model", "storey_range"]}}


def test_good_row_passes_and_bad_rows_carry_all_reasons(cfg):
    master = _df([
        _row(),
        _row(flat_model="DBSS", storey_range="01 TO 05", _source_row=2),   # two failures
        _row(resale_price=0.0, _source_row=3),
        _row(lease_commence_date=2013, _source_row=4),
        _row(month="Jan-12", _source_row=5),
        _row(_cast_errors=["resale_price"], resale_price=None, _source_row=6),
    ])
    cleaned, quarantined, _ = validate(master, _df([_row()]), cfg)
    assert len(cleaned) == 1
    reasons = dict(zip(quarantined["_source_row"], quarantined["_dq_reasons"]))
    assert reasons[2] == "INVALID_FLAT_MODEL|INVALID_STOREY_RANGE"
    assert reasons[3] == "NON_POSITIVE_PRICE"
    assert reasons[4] == "LEASE_AFTER_SALE"
    assert "INVALID_MONTH" in reasons[5]
    assert reasons[6] == "CAST_ERROR|MISSING_VALUE"


def test_duplicates_keep_first_by_lineage_order(cfg):
    master = _df([_row(_source_row=9), _row(_source_row=1)])   # input order deliberately reversed
    cleaned, quarantined, _ = validate(master, _df([_row()]), cfg)
    assert cleaned["_source_row"].tolist() == [1]
    assert quarantined["_dq_reasons"].tolist() == ["DUPLICATE"]


def test_domain_check_is_case_insensitive_and_canonicalises(cfg):
    master = _df([_row(flat_model="IMPROVED")])
    cleaned, quarantined, _ = validate(master, _df([_row()]), cfg)
    assert quarantined.empty and cleaned["flat_model"].tolist() == ["Improved"]


def test_reconciles_to_master(cfg):
    master = _df([_row(), _row(town="NOWHERE", _source_row=2), _row(_source_row=3)])
    cleaned, quarantined, summary = validate(master, _df([_row()]), cfg)
    assert len(cleaned) + len(quarantined) == len(master)
    assert summary.set_index("rule").loc["INVALID_TOWN", "rows_failed"] == 1
