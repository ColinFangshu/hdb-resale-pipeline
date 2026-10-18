"""HDB resale flat prices ETL pipeline."""

import sys

import pandas as pd

# Fail fast with a clear message rather than an obscure dtype error deep in a stage.
if sys.version_info < (3, 10):
    raise RuntimeError(
        f"Python >= 3.10 required, found {sys.version.split()[0]}. "
        "See README: create the 'hdb' environment and select its Jupyter kernel."
    )
if int(pd.__version__.split(".")[0]) < 2:
    raise RuntimeError(
        f"pandas >= 2.0 required, found {pd.__version__}. Run `pip install -e \".[dev]\"` "
        "in the project environment and select its Jupyter kernel."
    )
