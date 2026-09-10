# University Chapters Data Product

A thin Azure-oriented medallion pipeline that ingests university chapter data from a public ArcGIS FeatureServer and publishes a consumer-ready Gold data product.

Runtime data is written under `data/` and is not source code. The tracked repository remains focused on `README.md`, `DATA_PRODUCT_CONTRACT.md`, `src/`, `tests/`, and `fixtures/`.

## Architecture

```text
ArcGIS FeatureServer
        |
        v
   BRONZE (Raw)
        |
        v
 PySpark + Data Quality
        |
        +--------------------+
        |                    |
        v                    v
 QUARANTINE              SILVER
 (DQ-Q1)                    |
                             v
                           GOLD
                    (Consumer Product)
```

## Scope

The pipeline ingests only:

- California (`CA`)
- Oregon (`OR`)
- Washington (`WA`)

The source may legitimately contain zero records for OR or WA.

## Source

**ArcGIS FeatureServer API**

`https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query`

Query parameters:

```text
where=State IN ('CA','OR','WA')
outFields=*
returnGeometry=true
f=json
```

These request parameters are defined in `config.yml` and passed to `requests` without being hardcoded in `src/ingest.py`. The raw API response is stored as `payload.json` in Bronze.

The upstream API already filters records to `Status = ACTIVE`.

---

# Technology

- **Python + requests** — API ingestion
- **PySpark** — Silver and Gold transformations
- **Local filesystem** — ADLS-style medallion layout
- **Parquet** — Silver, Gold, and Quarantine storage
- **Pytest** — automated verification

Simple cleanup and casts use PySpark functions. Medium-complexity Bronze-to-Silver classification and deduplication use PySpark SQL.

The solution runs locally without requiring an Azure subscription.

---

# Repository Structure

```text
.
├── src/
│   ├── ingest.py
│   ├── transform.py
│   ├── dq.py
│   └── settings.py
│
├── tests/
│   ├── test_dq.py
│   └── test_pipeline.py
│
├── fixtures/
│   └── dq_test_data.json
│
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quarantine/
│
├── DATA_PRODUCT_CONTRACT.md
├── config.yml
├── requirements.txt
└── README.md
```

Runtime paths, source URL, and state scope are defined in `config.yml`. `src/settings.py` loads that YAML file; configuration values are not hard-coded in Python.

---

# Quick Start

## Prerequisites

- Python 3.11+
- Java 17+
- Internet access for live API ingestion

## 1. Clone

```bash
git clone <REPOSITORY_URL>
cd <REPOSITORY_NAME>
```

## 2. Create Virtual Environment

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
```

## 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

On macOS with Homebrew OpenJDK 17, set Java before running Spark:

```bash
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

## 4. Run the Pipeline

### Ingest API → Bronze

```bash
python -m src.ingest
```

The direct equivalent also works from the project root:

```bash
python src/ingest.py
```

This:

- Calls the ArcGIS FeatureServer API
- Filters `CA`, `OR`, and `WA`
- Stores raw/near-raw data in Bronze
- Adds ingestion metadata such as `ingest_run_id`

### Bronze → Silver → Gold

```bash
python -m src.transform
```

This:

- Reads Bronze data
- Flattens and types source fields
- Applies DQ rules
- Writes invalid records to Quarantine
- Writes valid records to Silver
- Projects the consumer-facing columns and publishes clean and warning records to Gold

The normal transform selects the Bronze run with the newest `ingested_at` in `metadata.json`. It does not provide manual historical run selection for this take-home.

For a deterministic offline run that seeds Bronze with one clean, one warning, and one quarantine row, use:

```bash
python -m src.transform --fixture
```

---

# Data Quality

## DQ-Q1 — Quarantine

A record is quarantined when longitude or latitude is:

- Missing or null
- Non-numeric
- Longitude outside `[-180, 180]`
- Latitude outside `[-90, 90]`

Result:

```text
reason_code = INVALID_COORDINATES
```

Quarantined records never enter Silver or Gold.

## DQ-W1 — Warning

A record receives a warning when `city` is:

- Null
- Blank
- `UNKNOWN` (case-insensitive)

Result:

```text
dq_status   = WARNING
dq_warnings = ["MISSING_OR_UNKNOWN_CITY"]
```

The record continues to Silver and Gold.

## Clean Records

```text
dq_status   = OK
dq_warnings = []
```

---

# Synthetic DQ Data

The live API may contain only clean records.

The fixture includes synthetic records to demonstrate both required DQ paths:

```text
Invalid coordinates
        |
        v
   QUARANTINE


Missing / UNKNOWN city
        |
        v
WARNING → SILVER → GOLD
```

---

# Verify the Solution

Run all automated tests:

```bash
pytest -q
```

The tests verify:

- DQ-Q1 records are quarantined
- DQ-Q1 records never appear in Gold
- DQ-W1 records appear in Gold
- DQ-W1 records have `dq_status = WARNING`
- Clean records have `dq_status = OK`
- Empty OR/WA does not fail the pipeline

Run the tests after installing dependencies and configuring Java:

```bash
python -m pytest -q
```

---

# Output Layout

```text
data/
├── bronze/
│   └── university_chapters/
│       └── <run_id>/
│
├── silver/
│   └── university_chapters/
│
├── gold/
│   └── university_chapters/
│       └── v1/
│
└── quarantine/
    └── university_chapters/
        └── <run_id>/
```

Quarantine and Gold are intentionally separate outputs.

---

# Gold Data Product

**Path:** `data/gold/university_chapters/v1/`

**Grain:** One row per university chapter.

| Column | Type | Description |
|---|---|---|
| `chapter_id` | string | Stable business key from `ChapterID` |
| `chapter_name` | string | Source `University_Chapter` |
| `city` | string | Source `City`; warning rows may be null/unknown |
| `state` | string | `CA`, `OR`, or `WA` from `State` |
| `longitude` | double | WGS84 `geometry.x` |
| `latitude` | double | WGS84 `geometry.y` |
| `dq_status` | string | `OK` or `WARNING` |
| `dq_warnings` | array<string> | Empty or warning codes |
| `ingest_run_id` | string | Producing run identifier |
| `ingested_at` | timestamp | UTC ingestion timestamp |

See [DATA_PRODUCT_CONTRACT.md](DATA_PRODUCT_CONTRACT.md) for the complete consumer contract.

---

# Run Metrics

Each pipeline run logs:

```text
rows_in
rows_quarantined
rows_warned
rows_ok
```

Example:

```text
rows_in          = 5
rows_quarantined = 1
rows_warned      = 1
rows_ok          = 3
```

---

# Failure Policy

The pipeline fails loudly when:

- The API request fails
- The complete source batch is empty
- California unexpectedly returns zero records

The following is valid:

```text
CA > 0, OR = 0, WA = 0
```

OR and WA are allowed to be empty.

---

# Idempotency

Gold uses an overwrite-based publish strategy for this take-home assignment.

Re-running the pipeline does not create duplicate Gold records.

---

# Trade-offs

## Why local Spark?

Local PySpark and an ADLS-style folder layout were chosen to make the solution:

- Easy to clone and run
- Independent of Azure credentials
- Reproducible for reviewers
- Representative of an Azure medallion architecture

## What would be added in production?

- ADLS Gen2
- Delta Lake
- Azure Databricks, Fabric, or Synapse Spark
- Scheduled orchestration
- CI/CD
- Monitoring and alerting
- Data catalog and lineage
- Schema evolution controls
- Delta `MERGE` for incremental/idempotent publishing
- Production authentication and managed identities

These items are intentionally outside the scope of this assignment.

---

# Submission

1. Push the repository to GitHub.
2. Grant access to `dpl-tech-uk`.
3. Share the repository URL.
