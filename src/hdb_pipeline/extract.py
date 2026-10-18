"""Extract stage: discover in-scope datasets on data.gov.sg and land them as-is in the raw zone.

Design notes
------------
* Discovery, not hardcoding: dataset IDs come from the collection's metadata, and the files to
  pull are chosen by checking each dataset's coverage window against the configured scope.
* As-is landing: bytes are streamed straight to disk and never parsed or rewritten. Filtering to
  the 2012-2017 window happens later, in the standardise stage.
* Lineage: every file gets a manifest entry (source ID, title, coverage, source last-updated
  timestamp, download time, SHA-256, byte size, header, row count).
* Idempotent: a file is downloaded again only if the source has been updated since the last pull,
  the local copy is missing, or its checksum no longer matches. `force=True` overrides this.
* Offline mode: re-verifies the committed raw files against the manifest without touching the
  network, so reviewers can run the pipeline even if the API is down or rate-limited.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------------
# API client
# --------------------------------------------------------------------------------------------
class DataGovClient:
    """Thin wrapper over the data.gov.sg v1/v2 public APIs, with retry and backoff."""

    RETRYABLE_STATUS = {429, 500, 502, 503, 504}

    def __init__(self, cfg: dict[str, Any], session: requests.Session | None = None):
        self.meta_base = cfg["source"]["metadata_base_url"].rstrip("/")
        self.dl_base = cfg["source"]["download_base_url"].rstrip("/")
        self.http = cfg["http"]
        self.session = session or requests.Session()
        api_key = os.getenv(cfg["source"]["api_key_env_var"])
        if api_key:
            self.session.headers["x-api-key"] = api_key

    def _get_json(self, url: str) -> dict[str, Any]:
        """GET a JSON endpoint, retrying on rate-limit or server errors with exponential backoff."""
        for attempt in range(1, self.http["max_retries"] + 1):
            try:
                resp = self.session.get(url, timeout=self.http["timeout_seconds"])
            except requests.RequestException as exc:
                if attempt == self.http["max_retries"]:
                    raise
                log.warning("Request error on %s (%s); retry %d", url, exc, attempt)
            else:
                if resp.status_code not in self.RETRYABLE_STATUS:
                    resp.raise_for_status()
                    body = resp.json()
                    if body.get("code") not in (0, None):
                        raise RuntimeError(f"API error from {url}: {body.get('errorMsg')}")
                    return body["data"]
                if attempt == self.http["max_retries"]:
                    resp.raise_for_status()
                log.warning("HTTP %s on %s; retry %d", resp.status_code, url, attempt)
            time.sleep(self.http["backoff_seconds"] * 2 ** (attempt - 1))
        raise RuntimeError(f"Exhausted retries for {url}")  # pragma: no cover

    def list_collection_datasets(self, collection_id: str) -> list[str]:
        data = self._get_json(f"{self.meta_base}/collections/{collection_id}/metadata")
        return list(data["collectionMetadata"]["childDatasets"])

    def get_dataset_metadata(self, dataset_id: str) -> dict[str, Any]:
        return self._get_json(f"{self.meta_base}/datasets/{dataset_id}/metadata")

    def get_download_url(self, dataset_id: str) -> str:
        """Ask the API to prepare a download, then poll until it returns a pre-signed S3 URL."""
        self._get_json(f"{self.dl_base}/datasets/{dataset_id}/initiate-download")
        for _ in range(self.http["poll_max_attempts"]):
            data = self._get_json(f"{self.dl_base}/datasets/{dataset_id}/poll-download")
            if data.get("status") == "DOWNLOAD_SUCCESS" and data.get("url"):
                return data["url"]
            time.sleep(self.http["poll_interval_seconds"])
        raise TimeoutError(f"Download for {dataset_id} not ready after polling")

    def stream_to_file(self, url: str, dest: Path) -> tuple[str, int]:
        """Stream a URL to `dest` atomically, hashing while writing. Returns (sha256, bytes)."""
        tmp = dest.with_suffix(dest.suffix + ".part")
        sha, size = hashlib.sha256(), 0
        with self.session.get(url, stream=True, timeout=self.http["timeout_seconds"]) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=self.http["chunk_size_bytes"]):
                    fh.write(chunk)
                    sha.update(chunk)
                    size += len(chunk)
        os.replace(tmp, dest)  # atomic: never leaves a half-written file under the final name
        return sha.hexdigest(), size


# --------------------------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------------------------
@dataclass
class ManifestEntry:
    dataset_id: str
    name: str
    coverage_start: str          # YYYY-MM
    coverage_end: str            # YYYY-MM
    source_last_updated: str
    downloaded_at_utc: str
    file: str                    # path relative to the raw folder
    bytes: int
    sha256: str
    row_count: int               # data rows, excluding header
    columns: list[str]


def load_manifest(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as fh:
        return {e["dataset_id"]: e for e in json.load(fh)}


def save_manifest(path: Path, entries: dict[str, dict[str, Any]]) -> None:
    ordered = sorted(entries.values(), key=lambda e: e["coverage_start"])
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(ordered, fh, indent=2)


# --------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------
def to_month(ts: str) -> str:
    """'2012-03-01T08:00:00+08:00' -> '2012-03' (the API reports coverage in SGT)."""
    return ts[:7]


def overlaps(cov_start: str, cov_end: str, scope_start: str, scope_end: str) -> bool:
    """True if [cov_start, cov_end] intersects [scope_start, scope_end]. Inputs are YYYY-MM strings."""
    return cov_start <= scope_end and cov_end >= scope_start


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def inspect_csv(path: Path) -> tuple[list[str], int]:
    """Read only the header and count data rows. The file is never modified."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = sum(1 for _ in reader)
    return header, rows


def _is_current(entry: dict[str, Any] | None, raw_dir: Path, source_last_updated: str) -> bool:
    if not entry or entry["source_last_updated"] != source_last_updated:
        return False
    path = raw_dir / entry["file"]
    return path.exists() and sha256_of(path) == entry["sha256"]


# --------------------------------------------------------------------------------------------
# Stage entry point
# --------------------------------------------------------------------------------------------
def run_extract(cfg: dict[str, Any], force: bool = False, offline: bool = False,
                client: DataGovClient | None = None) -> pd.DataFrame:
    """Land every in-scope dataset in the raw zone and return the manifest as a DataFrame."""
    raw_dir: Path = cfg["paths"]["raw"]
    manifest_path: Path = cfg["paths"]["manifest"]
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(manifest_path)
    scope_start, scope_end = cfg["scope"]["start_month"], cfg["scope"]["end_month"]

    if offline:
        log.info("Offline mode: verifying %d raw files against the manifest", len(manifest))
        for e in manifest.values():
            actual = sha256_of(raw_dir / e["file"])
            if actual != e["sha256"]:
                raise ValueError(f"Checksum mismatch for {e['file']}: raw file has been altered")
        return pd.DataFrame(manifest.values())

    client = client or DataGovClient(cfg)
    dataset_ids = client.list_collection_datasets(cfg["source"]["collection_id"])
    log.info("Collection %s has %d datasets", cfg["source"]["collection_id"], len(dataset_ids))

    for ds_id in dataset_ids:
        meta = client.get_dataset_metadata(ds_id)
        cov_start, cov_end = to_month(meta["coverageStart"]), to_month(meta["coverageEnd"])
        if not overlaps(cov_start, cov_end, scope_start, scope_end):
            log.info("SKIP  %s  %s [%s..%s] out of scope", ds_id, meta["name"], cov_start, cov_end)
            continue

        if not force and _is_current(manifest.get(ds_id), raw_dir, meta["lastUpdatedAt"]):
            log.info("CACHE %s  %s unchanged since last pull", ds_id, meta["name"])
            continue

        log.info("PULL  %s  %s [%s..%s]", ds_id, meta["name"], cov_start, cov_end)
        dest = raw_dir / f"{ds_id}.csv"
        sha, size = client.stream_to_file(client.get_download_url(ds_id), dest)
        header, rows = inspect_csv(dest)
        manifest[ds_id] = asdict(ManifestEntry(
            dataset_id=ds_id, name=meta["name"], coverage_start=cov_start, coverage_end=cov_end,
            source_last_updated=meta["lastUpdatedAt"],
            downloaded_at_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            file=dest.name, bytes=size, sha256=sha, row_count=rows, columns=header,
        ))
        save_manifest(manifest_path, manifest)  # persist after each file so a failure keeps progress

    return pd.DataFrame(sorted(manifest.values(), key=lambda e: e["coverage_start"]))
