"""Unit tests for the de-identify stage."""

import hashlib

import pandas as pd
import pytest

from hdb_pipeline.hashing import get_key, hash_identifier

KEY = "k" * 32


@pytest.fixture
def cfg():
    return {"hashing": {"key_env_var": "HDB_HASH_KEY", "min_key_length": 32,
                        "allow_demo_key": True, "demo_key": "DEMO-KEY"}}


@pytest.fixture
def transformed():
    return pd.DataFrame({"month": ["2012-01", "2012-01"], "rid_base": ["S0192301201A"] * 2,
                         "resale_identifier": pd.Series(["S0192301201A001", "S0192301201A002"], dtype="string")})


def test_hash_is_keyed_deterministic_and_drops_plain_id(cfg, transformed, monkeypatch):
    monkeypatch.setenv("HDB_HASH_KEY", KEY)
    a, source = hash_identifier(transformed, cfg)
    b, _ = hash_identifier(transformed, cfg)
    assert source == "env"
    assert a["resale_identifier_hash"].tolist() == b["resale_identifier_hash"].tolist()
    assert a["resale_identifier_hash"].is_unique and a["resale_identifier_hash"].str.len().eq(64).all()
    assert not {"resale_identifier", "rid_base"} & set(a.columns)


def test_dictionary_attack_with_plain_sha256_fails(cfg, transformed, monkeypatch):
    monkeypatch.setenv("HDB_HASH_KEY", KEY)
    hashed, _ = hash_identifier(transformed, cfg)
    guess = hashlib.sha256(b"S0192301201A001").hexdigest()    # attacker knows the format, not the key
    assert guess not in set(hashed["resale_identifier_hash"])


def test_different_key_gives_different_hashes(cfg, transformed, monkeypatch):
    monkeypatch.setenv("HDB_HASH_KEY", KEY)
    a, _ = hash_identifier(transformed, cfg)
    monkeypatch.setenv("HDB_HASH_KEY", "z" * 32)
    b, _ = hash_identifier(transformed, cfg)
    assert set(a["resale_identifier_hash"]).isdisjoint(b["resale_identifier_hash"])


def test_key_rules(cfg, monkeypatch):
    monkeypatch.delenv("HDB_HASH_KEY", raising=False)
    assert get_key(cfg)[1] == "demo"
    cfg["hashing"]["allow_demo_key"] = False
    with pytest.raises(RuntimeError):
        get_key(cfg)
    monkeypatch.setenv("HDB_HASH_KEY", "short")
    with pytest.raises(ValueError):
        get_key(cfg)
