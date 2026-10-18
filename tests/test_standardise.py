"""Unit tests for the standardise stage, using small synthetic raw files."""

import pandas as pd
import pytest

from hdb_pipeline.standardise import pick_authoritative, standardise

BASE = "month,town,flat_type,resale_price,lease_commence_date\n"


@pytest.fixture
def setup(tmp_path):
    (tmp_path / "a.csv").write_text(
        BASE + "2011-12,BEDOK,3 ROOM,200000,1980\n"            # out of scope
               "2012-01,  ANG  MO KIO ,3 ROOM,250000,1986\n"   # messy whitespace
               "2012-02,BEDOK,4 ROOM,abc,1986.5\n", encoding="utf-8")   # cast failures
    (tmp_path / "b.csv").write_text(
        "month,town,flat_type,lease_commence_date,remaining_lease,resale_price\n"
        "2017-12,YISHUN,5 ROOM,1990,72,500000.88\n"
        "2018-01,YISHUN,5 ROOM,1990,71,510000\n"                # out of scope
        "Jan-17,YISHUN,5 ROOM,1990,72,500000\n", encoding="utf-8")       # bad month, kept
    manifest = pd.DataFrame([
        {"dataset_id": "a", "name": "A", "file": "a.csv", "coverage_start": "2000-01",
         "coverage_end": "2012-02", "row_count": 3, "columns": BASE.strip().split(",")},
        {"dataset_id": "b", "name": "B", "file": "b.csv", "coverage_start": "2017-01",
         "coverage_end": "2026-10", "row_count": 3,
         "columns": ["month", "town", "flat_type", "lease_commence_date", "remaining_lease", "resale_price"]},
    ])
    cfg = {
        "scope": {"start_month": "2012-01", "end_month": "2017-12"},
        "standardise": {"numeric_columns": {"resale_price": "Float64", "lease_commence_date": "Int64"}},
        "paths": {"raw": tmp_path, "staging": tmp_path / "staging"},
    }
    return cfg, manifest


def test_authoritative_is_file_covering_scope_start(setup):
    cfg, manifest = setup
    assert pick_authoritative(manifest, "2012-01")["dataset_id"] == "a"


def test_schema_conformed_to_authoritative_order(setup):
    master, _ = standardise(*setup)
    assert list(master.columns) == ["month", "town", "flat_type", "resale_price", "lease_commence_date",
                                    "src_remaining_lease", "_cast_errors", "_source_dataset_id", "_source_row"]
    assert master.loc[master["_source_dataset_id"] == "a", "src_remaining_lease"].isna().all()


def test_scope_filter_keeps_unparseable_months(setup):
    master, summary = standardise(*setup)
    assert sorted(master["month"]) == ["2012-01", "2012-02", "2017-12", "Jan-17"]
    assert summary["rows_in_scope"].sum() == len(master)
    assert summary.set_index("dataset_id").loc["b", "unparseable_month"] == 1


def test_whitespace_tidied_and_cast_errors_recorded_not_dropped(setup):
    master, _ = standardise(*setup)
    assert "ANG MO KIO" in master["town"].tolist()
    bad = master[master["month"] == "2012-02"].iloc[0]
    assert bad["_cast_errors"] == ["resale_price", "lease_commence_date"]
    assert master.loc[master["month"] == "2017-12", "resale_price"].iloc[0] == 500000.88


def test_lineage_points_back_to_raw_row(setup):
    master, _ = standardise(*setup)
    row = master[master["month"] == "2012-01"].iloc[0]
    assert (row["_source_dataset_id"], row["_source_row"]) == ("a", 2)
