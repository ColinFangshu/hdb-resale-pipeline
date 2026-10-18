# HDB Resale Flat Prices: ETL Pipeline (Jan 2012 – Dec 2017)

A Python (pandas) ETL pipeline that extracts HDB resale flat prices from [data.gov.sg](https://data.gov.sg/collections/189/view), validates them against the **Jan 2012 authoritative set**, and produces five output groups for the Data Science Team: **Raw, Cleaned, Quarantined, Transformed and Hashed**.

- **Run it:** `hdb_resale_pipeline.ipynb`. It runs top to bottom, with commentary and saved outputs.
- **Findings and assumptions:** [`REPORT.md`](REPORT.md)
- **Logic:** `src/hdb_pipeline/`. One module per stage, each covered by unit tests.
- **Part 2, AWS architecture:** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), with diagrams in `docs/architecture/` (PNG plus editable draw.io).

---

## Quick start

Requires **Python ≥ 3.10** (developed on 3.11) and an internet connection for the first download.

```bash
git clone <this-repo-url> && cd hdb-resale-pipeline

# 1. Create an environment (conda or venv, whichever you prefer)
conda create -n hdb python=3.11 -y && conda activate hdb
#   or: python3.11 -m venv .venv && source .venv/bin/activate

# 2. Install the package and dev tools
python -m pip install -e ".[dev]"

# 3. (Optional) set the hashing key; without it a labelled demo key is used
export HDB_HASH_KEY="$(python -c 'import secrets; print(secrets.token_hex(32))')"

# 4. Run the tests
python -m pytest -q

# 5. Run the pipeline
python -m ipykernel install --user --name hdb --display-name "Python (hdb)"
jupyter notebook hdb_resale_pipeline.ipynb      # Kernel -> Restart & Run All
```

> **macOS tip:** if `python` points at another interpreter (e.g. a Homebrew alias), use the environment's interpreter explicitly: `$CONDA_PREFIX/bin/python -m ...`.

**Running without the API.** The raw files are committed in `data/raw/`. Set `OFFLINE = True` in notebook section 1 to skip the network: the pipeline then checks each committed file against its checksum in `data/raw/_manifest.json` and carries on. Use this if data.gov.sg is down or rate-limiting you (HTTP 429).

---

## Pipeline

```
data.gov.sg API
     │  1. Extract       find datasets from collection metadata → download as-is → checksum manifest
     ▼
 data/raw/  ─────────────────────────────────────────────────────────── RAW
     │  2. Standardise   filter to 2012-01..2017-12 → match Jan 2012 schema → cast types → add lineage
     ▼
 data/staging/master.parquet          (113,053 rows)
     │  3. Profile       coverage, nulls, category values vs Jan 2012, distributions, duplicates
     │  4. Validate      11 rules with reason codes
     ├──────────────► data/quarantined/  (8,270) ───────────────────── QUARANTINED
     ▼
     │  5. Enrich        remaining lease @ 1 Jan 2027 + price-anomaly flags
 data/cleaned/        (104,783) ────────────────────────────────────── CLEANED
     │  6. Transform     Resale Identifier, made unique with a fixed-order sequence
 data/transformed/    (104,783) ────────────────────────────────────── TRANSFORMED
     │  7. De-identify   HMAC-SHA256 with a secret key; plain identifier removed
 data/hashed/         (104,783) ────────────────────────────────────── HASHED
```

Row counts are checked automatically: `raw in scope = cleaned + quarantined`, and `cleaned = transformed = hashed`.

| Requirement | Where |
|---|---|
| 1. Master dataset with Jan 2012 as the authoritative set | `standardise.py`, notebook §2 |
| 2. Data profiling | `profile.py`, notebook §3, §4a |
| 3. Validate town / flat type / flat model / storey range, quarantine failures | `validate.py`, notebook §4 |
| 4. Remaining lease as of 1 Jan 2027 | `enrich.py`, notebook §5a |
| 5. Anomalous resale prices | `enrich.py`, notebook §5b–5c |
| 6. Extra profiling, cleaning and validation rules | `validate.py`, `profile.py` |
| 7, 14. Insights, assumptions, profiling report | [`REPORT.md`](REPORT.md) |
| 8. Resale Identifier, made unique | `transform.py`, notebook §6 |
| 9. Irreversible, dictionary-attack-resistant hash | `hashing.py`, notebook §7 |
| 10–11. Output groups | `data/{raw,cleaned,quarantined,transformed,hashed}/`, notebook §8 |

---

## Repository layout

```
├── hdb_resale_pipeline.ipynb   # runs and documents the pipeline (with cell outputs)
├── REPORT.md                   # data profiling report: assumptions, insights, quarantine analysis
├── config/pipeline.yaml        # every setting: scope, rules, thresholds, paths
├── src/hdb_pipeline/
│   ├── extract.py              # data.gov.sg client, dataset discovery, as-is download, manifest
│   ├── standardise.py          # master dataset, schema conformance, type casting, lineage
│   ├── profile.py              # reusable profiling checks
│   ├── validate.py             # rule-based validation → cleaned / quarantined
│   ├── enrich.py               # remaining lease, price-anomaly flags
│   ├── transform.py            # Resale Identifier
│   ├── hashing.py              # HMAC-SHA256 de-identification
│   └── io.py                   # writes output groups (Parquet + CSV)
├── tests/                      # pytest unit tests, one file per stage
├── docs/
│   ├── ARCHITECTURE.md         # Part 2: ingestion & exploitation architecture on AWS
│   └── architecture/           # diagrams: .png (submission) + .drawio (editable)
└── data/
    ├── raw/                    # source files as published + _manifest.json (committed)
    ├── staging/                # intermediate master dataset (regenerated, not committed)
    ├── cleaned/  quarantined/  transformed/  hashed/   # output groups
```

**What's committed:** raw CSVs (the input files) and each output group as **Parquet**. The quarantined CSV is also committed, so it can be read on GitHub. Other CSV copies (about 30 MB each) are recreated when you run the notebook.

---

## Key design decisions

| Decision | Why |
|---|---|
| **pandas, run locally** rather than Spark | About 113k rows. Reviewers can run it with `pip install`, with no cluster needed. Part 2 covers how this would scale on AWS |
| **Logic in a package, notebook as orchestrator** | Testable, reusable code. The notebook explains and runs it |
| **Everything configurable** (`config/pipeline.yaml`) | No hardcoded dataset IDs, dates, thresholds or paths. Datasets are found from the API's metadata |
| **Quarantine, don't drop** | Every rejected row keeps its lineage (`_source_dataset_id`, `_source_row`) and all of its reason codes, so it can be fixed and reprocessed |
| **Fail loudly on broken checks** | Row counts, uniqueness and checksums are checked inside the pipeline itself, not just in tests |
| **Anomalies flagged, not removed** | An unusual price may be genuine. The Data Science Team decides |
| **HMAC-SHA256 with a secret key** | A plain hash could be broken by hashing every possible identifier. A random salt per row would make hashes differ between runs |

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `scope.start_month` / `end_month` | `2012-01` / `2017-12` | Pipeline window |
| `validation.reference_month` | `2012-01` | The authoritative set |
| `enrich.lease_as_of` | `2027-01-01` | Date for remaining lease |
| `enrich.anomaly.z_threshold` | `3.5` | Robust z-score cut-off |
| `hashing.key_env_var` | `HDB_HASH_KEY` | Environment variable holding the secret key |
| `hashing.allow_demo_key` | `true` | Set to `false` in production so a missing key is an error |

Optional: `DATA_GOV_SG_API_KEY` raises the data.gov.sg rate limits if set.
