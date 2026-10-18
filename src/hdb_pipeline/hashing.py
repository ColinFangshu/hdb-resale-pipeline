"""De-identify stage: replace the Resale Identifier with a keyed hash (requirement 9).

Why HMAC-SHA256 with a secret key
---------------------------------
  * Irreversible: SHA-256 is a one-way function, so the identifier can't be recovered from the hash.
  * Keeps uniqueness: distinct identifiers give distinct 256-bit hashes (a collision among about
    100k values has a probability of roughly 1e-67).
  * Resists dictionary attacks: the identifier space is small (block x price prefix x month x
    town x sequence), so a *plain* SHA-256 could be broken by hashing every candidate and
    matching. HMAC mixes in a secret key, so without the key an attacker can't build that table.
  * Deterministic: the same identifier and key always give the same hash, so hashed datasets can
    still be joined and reruns are reproducible. A random per-row salt would lose this.

The key comes from an environment variable (default `HDB_HASH_KEY`). If it's missing, a clearly
labelled demo key is used and a warning is logged, so reviewers can run the notebook. In
production the key would come from a secrets manager (e.g. AWS Secrets Manager) and the demo
fallback would be switched off.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


def get_key(cfg: dict[str, Any]) -> tuple[bytes, str]:
    """Return (key, source), where source is 'env' or 'demo'."""
    h = cfg["hashing"]
    key = os.getenv(h["key_env_var"])
    if key:
        if len(key) < h["min_key_length"]:
            raise ValueError(f"{h['key_env_var']} must be at least {h['min_key_length']} characters")
        return key.encode("utf-8"), "env"
    if not h["allow_demo_key"]:
        raise RuntimeError(f"Set {h['key_env_var']} to a secret key before running this stage")
    log.warning("%s not set: using the DEMO key. Output hashes are NOT protected.", h["key_env_var"])
    return h["demo_key"].encode("utf-8"), "demo"


def hmac_sha256(values: pd.Series, key: bytes) -> pd.Series:
    return values.map(lambda v: hmac.new(key, v.encode("utf-8"), hashlib.sha256).hexdigest()).astype("string")


def hash_identifier(transformed: pd.DataFrame, cfg: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    """Hashed output = Cleaned columns + the hashed identifier. The plain identifier and its
    building blocks (rid_*) are removed, because the parts would reveal the identifier."""
    key, source = get_key(cfg)
    hashed = transformed.assign(resale_identifier_hash=hmac_sha256(transformed["resale_identifier"], key))
    hashed = hashed.drop(columns=[c for c in hashed.columns
                                  if c == "resale_identifier" or c.startswith("rid_")])
    assert hashed["resale_identifier_hash"].is_unique, "Hash collision: identifiers no longer unique"
    assert len(hashed) == len(transformed)
    return hashed, source
