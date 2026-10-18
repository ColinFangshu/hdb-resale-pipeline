"""Unit tests for the extract stage, using a fake API client (no network needed)."""

from pathlib import Path

import pytest

from hdb_pipeline.extract import overlaps, run_extract, sha256_of

FAKE_DATASETS = {
    "d_1990": ("1990 - 1999", "1990-01-01T08:00:00+08:00", "1999-12-01T08:00:00+08:00"),
    "d_2000": ("2000 - Feb 2012", "2000-01-01T08:00:00+08:00", "2012-02-01T08:00:00+08:00"),
    "d_2012": ("Mar 2012 - Dec 2014", "2012-03-01T08:00:00+08:00", "2014-12-01T08:00:00+08:00"),
    "d_2017": ("Jan 2017 onwards", "2017-01-01T08:00:00+08:00", "2026-10-01T08:00:00+08:00"),
}
CSV_BODY = "month,town,resale_price\n2012-01,ANG MO KIO,250000\n2012-01,BEDOK,300000\n"


class FakeClient:
    def __init__(self, last_updated="2024-01-01"):
        self.last_updated = last_updated
        self.downloads: list[str] = []

    def list_collection_datasets(self, _):
        return list(FAKE_DATASETS)

    def get_dataset_metadata(self, ds_id):
        name, start, end = FAKE_DATASETS[ds_id]
        return {"name": name, "coverageStart": start, "coverageEnd": end,
                "lastUpdatedAt": self.last_updated}

    def get_download_url(self, ds_id):
        return ds_id

    def stream_to_file(self, url, dest: Path):
        self.downloads.append(url)
        dest.write_text(CSV_BODY, encoding="utf-8")
        return sha256_of(dest), dest.stat().st_size


@pytest.fixture
def cfg(tmp_path):
    return {
        "source": {"collection_id": "189"},
        "scope": {"start_month": "2012-01", "end_month": "2017-12"},
        "paths": {"raw": tmp_path, "manifest": tmp_path / "_manifest.json"},
    }


def test_overlaps():
    assert overlaps("2000-01", "2012-02", "2012-01", "2017-12")      # tail end in scope
    assert overlaps("2017-01", "2026-10", "2012-01", "2017-12")      # head end in scope
    assert not overlaps("1990-01", "1999-12", "2012-01", "2017-12")


def test_selects_only_in_scope_datasets(cfg):
    client = FakeClient()
    manifest = run_extract(cfg, client=client)
    assert client.downloads == ["d_2000", "d_2012", "d_2017"]
    assert manifest["row_count"].tolist() == [2, 2, 2]
    assert manifest["columns"].iloc[0] == ["month", "town", "resale_price"]


def test_second_run_uses_cache_unless_source_updated(cfg):
    run_extract(cfg, client=FakeClient())
    again = FakeClient()
    run_extract(cfg, client=again)
    assert again.downloads == []                         # nothing changed -> no re-download
    updated = FakeClient(last_updated="2025-06-01")
    run_extract(cfg, client=updated)
    assert len(updated.downloads) == 3                   # source refreshed -> re-pull


def test_offline_detects_tampered_raw_file(cfg):
    run_extract(cfg, client=FakeClient())
    run_extract(cfg, offline=True)                       # clean files pass
    (cfg["paths"]["raw"] / "d_2012.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="altered"):
        run_extract(cfg, offline=True)
