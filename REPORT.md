# Data Profiling Report: HDB Resale Flat Prices, Jan 2012 – Dec 2017

This report covers **requirement 7** (insights and assumptions) and **requirement 14** (how the data behaves through the pipeline, and what ended up in Quarantined). Every number here is produced by `hdb_resale_pipeline.ipynb`, and the notebook section is given for each one.

**At a glance**

| Stage | Rows | Notes |
|---|---:|---|
| Raw, in scope (4 source files) | 113,053 | 2012-01 to 2017-12, from 4 of the 5 files in the data.gov.sg collection |
| Cleaned | 104,783 (92.7%) | Passed every validation rule |
| Quarantined | 8,270 (7.3%) | Kept with reason codes; nothing is dropped |
| Transformed | 104,783 | + unique Resale Identifier |
| Hashed | 104,783 | + HMAC-SHA256 of the identifier, plain identifier removed |

`cleaned + quarantined = raw in scope` and `cleaned = transformed = hashed`. The pipeline checks this itself and fails if it doesn't hold.

---

## 1. Assumptions

| # | Assumption | Why | Effect |
|---|---|---|---|
| A1 | The **Jan 2012 authoritative set** means the Jan 2012 rows of the *2000 – Feb 2012* file. It sets both the **schema** (columns and their order) and the **allowed values** for town, flat_type, flat_model and storey_range | Jan 2012 isn't a file of its own. That file's coverage is the only one containing 2012-01, and the code works this out from the manifest rather than hardcoding it | 1,559 reference rows: 26 towns, 7 flat types, 13 flat models, 12 storey ranges |
| A2 | "Strict" means **strict on values, not on formatting**. Matching ignores case and extra whitespace, and passing values are rewritten to the reference spelling | `IMPROVED` and `Improved` are the same flat model. Failing on capitalisation would quarantine good data | No effect on this data (it has no case or whitespace problems). The rule is there for future refreshes |
| A3 | Columns that exist in a source but not in the Jan 2012 schema are **kept with a `src_` prefix**, not dropped | `src_remaining_lease` (2015 onwards) is a free cross-check for requirement 4 | One extra column, empty before 2015 |
| A4 | Every lease starts on **1 Jan of `lease_commence_date`** and runs **99 years** | The data only gives the start *year* | Remaining lease at 1 Jan 2027 always has 0 months (see §3.5 for the size of this error) |
| A5 | **Exact duplicates** across the 10 business columns are quarantined. The first copy, ordered by source file then row, stays in Cleaned | The brief puts duplicates in Quarantined. The data has no unit number, so a true duplicate can't be told apart from two similar sales | 451 rows quarantined as duplicates |
| A6 | **Price anomalies are flagged, not quarantined** | An unusual price may be genuine. Whether to exclude it is an analysis decision for the Data Science Team | 1,433 rows flagged (1.37%) and kept in Cleaned |
| A7 | The Resale Identifier's average price is taken over **Cleaned** rows (anomaly-flagged rows included), truncated to whole dollars before taking the first 3 digits | Requirement 8 says "using the validated data" | — |
| A8 | Raw files are stored **byte-for-byte as published**, with a SHA-256 checksum in `data/raw/_manifest.json`. Scope filtering happens after the raw layer | "Process the data file as-is, without modifications" | Raw CSVs hold the full published files (e.g. 1990s rows and post-2017 rows). They're filtered out in the standardise step |

---

## 2. Source data behaviour (raw → master)

*Notebook sections 1 and 2.*

- **Four files cover the scope, and they're defined on two different bases.** Jan–Feb 2012 come from an **approval-date** file. Mar 2012 onwards come from **registration-date** files. A sale approved in Feb 2012 may only be *registered* in Mar 2012 or later, so the data may contain a small overlap or gap at that boundary. It can't be detected because there's no transaction key. Analysts should be aware of this when comparing Feb and Mar 2012.
- **The schema is stable** apart from `remaining_lease`, which appears in 2015. It's an integer number of years in 2015–16 and text in 2017, in three spellings: `61 years 04 months`, `54 years 01 month` and `60 years`. All three are parsed.
- **The data is structurally clean.** There are no nulls, no stray whitespace, every number converts to its type and every month is `YYYY-MM`. All of the quality problems are about meaning, not format.
- **28 prices have cents, and 25 of them end in `.88`** (e.g. $888,888.88): "lucky number" pricing. They're genuine, so prices are stored as floats.
- **The Jan 2017+ file is still growing.** A re-pull during development added 218 Oct 2026 rows and changed nothing in 2012–2017. The extract step only downloads a file again when the source's `lastUpdatedAt` changes, and the checksum shows whether a committed raw file has been altered.
- **The collection-level metadata can't be trusted for coverage.** It says coverage ends in Apr 2024, while the 2017+ file runs to Oct 2026. The pipeline uses each dataset's own coverage instead.
- **The API rate-limits heavily** (HTTP 429 on almost every download request). Retry with exponential backoff handled it.

---

## 3. Profiling insights (master and cleaned)

*Notebook sections 3–5.*

### 3.1 Coverage
All 72 months are present in the master dataset (between 886 and 2,360 sales a month). **Cleaned has 69, because Mar–May 2012 are 100% quarantined** (see §4.2).

### 3.2 Category values compared with Jan 2012
- `town` and `flat_type` **match Jan 2012 exactly**. There are no new values in 2012–2017.
- `flat_model` has **7 values never seen in Jan 2012**: DBSS (583 rows), Type S1 (179), Type S2 (78), Improved-Maisonette (15), Premium Maisonette (8), Premium Apartment Loft (6), 2-room (1). Most are **newer products that first appear on the resale market from 2014**.
- `storey_range` drift has two different causes:
  1. **5-floor bands** (`01 TO 05`, `06 TO 10`, …), about 6,840 rows, appear **only in Mar–May 2012**. That's a change in how floors were recorded at the switch to registration-date files, not bad data.
  2. **Floors above 36** (`37 TO 39` … `49 TO 51`), about 210 rows, are genuine high-rise flats that simply didn't sell in Jan 2012.

### 3.3 Prices
- Median price eased from **$455k (2013) to $408k (2016–17)**, and median price per sqm from about $4,885 to about $4,257. That's consistent with the market cooling after 2013. The anomaly detection groups by year so this trend isn't mistaken for anomalies.
- **Outliers that are genuine**: nine 3 ROOM homes of 180–280 sqm selling for about $1M are HDB **Terrace** houses (Jalan Ma'mor and Jalan Bahagia, Whampoa, 1972 lease). This shaped the anomaly design (§3.4).

### 3.4 Price anomalies (requirement 5)
- **Method**: robust z-score of price per sqm, `0.6745 × (x − median) / MAD`, flagged when `|z| > 3.5`. Each sale is compared with the most specific peer group that has at least 20 sales: `town × flat_type × flat_model × lease_decade × year`, falling back step by step to wider groups. Hard price bounds act as a second rule.
- **Design lessons from this data**:
  - Falling back to `flat_type` alone flagged **57 of 70 terrace houses**. The rule became **"never drop `flat_model`"**, which brought it down to 4.
  - Dropping `town` before `lease_decade` flagged premium central locations (Toa Payoh, Queenstown), so lease age is dropped first.
- **Result**: **1,433 rows flagged (1.37%)**, 1,210 above their peers and 223 below. The flags cluster on particular streets (e.g. Bedok Central's 2010-lease blocks, Serangoon Central Dr, Jurong West St 52). That points to **block-level premiums the peer groups can't see**: a newer block, a high floor, a view, or the location within a town. The flags are **candidates to review, not errors**.
- **The mechanism is proven with injected anomalies** (notebook §5c): a ×3 price typo, a missing digit and a $1 placeholder are all caught, and the unchanged control row isn't flagged. No real rows break the hard bounds.

### 3.5 Remaining lease (requirement 4)
- At 1 Jan 2027: **38 to 87 years (median 60)**. Months are always 0 under assumption A4.
- **Cross-check against HDB's published remaining lease**, applying our method at each sale date:

| Period (HDB's format) | Rows | Median difference (ours − HDB) | 90% range |
|---|---:|---:|---|
| 2015–16 (whole years) | 36,678 | 0 months | −8 to +8 months |
| 2017 (years + months) | 19,916 | **−6 months** | −11 to 0 months |

  Our figure is about 6 months shorter on average. That's what assuming a 1 Jan start produces when real leases start partway through the year. Accuracy to the month would need the lease start month.

### 3.6 Resale Identifier (requirement 8)
- **22.6% of cleaned rows (23,639) share their 12-character base ID with another row.** The largest group has 14 sales. The main cause is that the spec rule **removes block letters**, so `436A`, `436B` and `436C FERNVALE RD` all become `436`. About 20,400 rows have lettered blocks. And every sale in one month, town and flat type shares the same average, so the price part doesn't separate them.
- **Fix**: a 3-digit sequence number within each base ID, in a fixed order based on the data, giving `S0192301201A001`. All 104,783 IDs are unique, all are 15 characters long, the base ID is always the first 12, and reruns give the same IDs.
- **Spec ambiguity**: an average of $1M or more gives a 7-digit number, so its first 3 digits look like an average of about $100k (5 rows here; no clash in practice because no average in this data is below $100k).
- **Limitation**: the numbering is stable for this fixed history. If late records were added to a past month, numbers in that group could shift. Production should assign IDs once and store them.

### 3.7 De-identification (requirement 9)
- **HMAC-SHA256 with a secret key** (`HDB_HASH_KEY`). It's irreversible, collision-free in practice, deterministic, and resistant to dictionary attacks: hashing every candidate ID with plain SHA-256 matches **0** of our hashes. The plain identifier and its `rid_*` parts are removed from Hashed.
- **Caveat**: Hashed still contains the Cleaned columns (block, month, town, …), as the brief requires. Someone could *recompute* the plain identifier from them, but without the key they can't link it to a hash. If the identifier itself were sensitive, the columns it's built from would need to be generalised too.

---

## 4. The Quarantined dataset

*Notebook section 4.*

### 4.1 What's in it
| Reason code(s) | Rows |
|---|---:|
| `INVALID_STOREY_RANGE` | 6,952 |
| `INVALID_FLAT_MODEL` | 775 |
| `DUPLICATE` | 442 |
| `INVALID_FLAT_MODEL\|INVALID_STOREY_RANGE` | 92 |
| `INVALID_STOREY_RANGE\|DUPLICATE` | 6 |
| `INVALID_FLAT_MODEL\|DUPLICATE` | 3 |
| **Total** | **8,270** |

Rules that caught nothing on this data, but are there and tested: `CAST_ERROR`, `INVALID_MONTH`, `MISSING_VALUE`, `INVALID_TOWN`, `INVALID_FLAT_TYPE`, `NON_POSITIVE_PRICE`, `NON_POSITIVE_AREA`, `LEASE_AFTER_SALE`.

By source file: Mar 2012–Dec 2014 → 7,200; 2015–16 → 475; 2017 → 593; Jan–Feb 2012 → 2.

### 4.2 Key findings
1. **⚠️ Mar–May 2012 are entirely missing from Cleaned.** All 6,838 sales in those months used 5-floor storey bands. This follows the strict rule, but **the Data Science Team must treat these three months as missing** in any time series. The rows are kept in full in Quarantined. Restoring them would need a looser rule (e.g. accepting 5-floor bands as their own category). They can't be mapped onto 3-floor bands without guessing the floor.
2. **Strict validation hides newer flat types.** DBSS and Type S1/S2 (840 rows from 2014) are quarantined, so Cleaned can't be used to analyse those products. That matters for questions about premium or design-build flats.
3. **Duplicates spike in Apr–May 2014** (65 and 122, against a median of 2 a month; the next-highest months reach about 20). In May 2014, **97 of 122 duplicate pairs sit on consecutive raw-file rows**. That looks like a **double-load at the source** rather than separate identical sales, and it supports quarantining duplicates.
4. **Quarantined rows are pricier than cleaned ones** (median $457k against $422k). That's mostly because they're concentrated in 2012, when prices were higher, plus the DBSS premium. **Quarantining barely changes the flat-type mix** (no type moves by more than 0.4 percentage points), but it does tilt Cleaned slightly towards later, lower-priced years.

### 4.3 Recommendations
- Ask HDB about the **Apr–May 2014 duplicates** and the **Mar–May 2012 storey-band convention**.
- If the Data Science Team needs 2012 continuity or DBSS analysis, add a **"lenient" validation profile** in config that accepts 5-floor bands and post-2012 flat models as their own categories. Quarantined already holds everything needed to reprocess them.

---

## 5. Data quality rules: full reference

| Stage | Rule | Action |
|---|---|---|
| Extract | SHA-256 checksum against the manifest (offline mode) | Fail the pipeline |
| Standardise | Schema matches Jan 2012; numeric cast; month format | Record in `_cast_errors` / keep the row |
| Standardise | `len(master) == Σ rows in scope` | Fail the pipeline |
| Validate | 11 rules (see notebook §4) | Quarantine with reason codes |
| Validate | `cleaned + quarantined == master`, no row in both | Fail the pipeline |
| Enrich | `PEER_OUTLIER`, `PRICE_OUT_OF_RANGE` | Flag (`is_price_anomaly`, `anomaly_reasons`) |
| Transform | Final identifiers unique; sequence fits in 3 digits | Fail the pipeline |
| Hash | Hashes unique; row count unchanged | Fail the pipeline |
