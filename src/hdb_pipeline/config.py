"""Load pipeline configuration from YAML and resolve paths against the repo root."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "pipeline.yaml"


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Read the YAML config and turn every entry under `paths` into an absolute Path."""
    with open(path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    cfg["paths"] = {key: (REPO_ROOT / value).resolve() for key, value in cfg["paths"].items()}
    return cfg
