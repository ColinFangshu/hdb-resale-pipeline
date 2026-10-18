"""Unit tests for the profiling checks."""

import pandas as pd

from hdb_pipeline.profile import consistency_checks, domain_drift, duplicate_profile, overview


def _df(rows):
    cols = ["month", "town", "flat_type", "block", "street_name", "storey_range", "floor_area_sqm",
            "flat_model", "lease_commence_date", "resale_price", "_source_dataset_id"]
    df = pd.DataFrame(rows, columns=cols)
    for c in ["floor_area_sqm", "resale_price"]:
        df[c] = df[c].astype("Float64")
    df["lease_commence_date"] = df["lease_commence_date"].astype("Int64")
    for c in ["month", "town", "flat_type", "block", "street_name", "storey_range", "flat_model"]:
        df[c] = df[c].astype("string")
    return df


ROW = ["2012-01", "BEDOK", "3 ROOM", "10A", "BEDOK RD", "01 TO 03", 67.0, "Improved", 1980, 300000.0, "a"]


def test_overview_reports_missing_months():
    df = _df([ROW, ROW[:0] + ["2012-03"] + ROW[1:]])
    out = overview(df, {"start_month": "2012-01", "end_month": "2012-03"}).iloc[0]
    assert out["months_missing"] == 1 and out["missing_list"] == ["2012-02"]


def test_domain_drift_is_case_insensitive_and_counts_unseen():
    ref = _df([ROW])
    df = _df([ROW, ROW[:7] + ["IMPROVED"] + ROW[8:], ROW[:7] + ["DBSS"] + ROW[8:]])
    out = domain_drift(df, ref, ["flat_model"])
    assert out["value"].tolist() == ["DBSS"] and out["rows"].tolist() == [1]


def test_duplicates_ignore_lineage_columns():
    df = _df([ROW, ROW[:-1] + ["b"]])   # same record, different source
    assert duplicate_profile(df).loc["ALL", "duplicate_rows"] == 1


def test_consistency_flags_lease_after_sale():
    df = _df([ROW[:8] + [2013] + ROW[9:]])
    out = consistency_checks(df).set_index("check")["rows_flagged"]
    assert out["lease_commence_after_sale_year"] == 1
