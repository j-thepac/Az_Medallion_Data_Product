# University Chapters Data Product Contract

## Ownership

- **Name:** University Chapters Data Product
- **Technical owner:** Data Engineering team
- **Consumers:** Analytics, reporting, and downstream data products
- **Classification:** Public source data; no PII expected
- **Grain:** One row per `chapter_id`

## Interface

Gold path: `data/gold/university_chapters/v1/`

| Column | Type | Nullable | Description |
|---|---|---:|---|
| `chapter_id` | string | No | Stable business key |
| `chapter_name` | string | No | Source `University_Chapter` |
| `city` | string | Yes | Source city; warning rows may be null/unknown |
| `state` | string | No | `CA`, `OR`, or `WA` |
| `longitude` | double | No | WGS84 longitude |
| `latitude` | double | No | WGS84 latitude |
| `dq_status` | string | No | `OK` or `WARNING` |
| `dq_warnings` | array<string> | No | Empty or warning codes |
| `ingest_run_id` | string | No | Producing ingestion run |
| `ingested_at` | timestamp | No | UTC ingestion timestamp |

## Freshness

The intended SLA is a daily refresh by 06:00 UTC. Local execution is manual for this assignment.

## Quality

- **DQ-Q1:** Missing, non-numeric, or out-of-range coordinates are written to quarantine with `INVALID_COORDINATES` and never enter Silver or Gold.
- **DQ-W1:** Null, blank, or case-insensitive `UNKNOWN` city values remain in Silver and Gold with `dq_status = WARNING` and `MISSING_OR_UNKNOWN_CITY`.
- OR and WA may legitimately contain zero rows.
- An empty whole batch fails the run and does not publish Gold.
- An unexpected zero CA count fails the live ingestion validation.
- Every run reports `rows_in`, `rows_quarantined`, `rows_warned`, and `rows_ok`.

## Versioning and publishing

Gold is versioned under `v1`. Breaking schema changes require a new versioned path. The local publisher overwrites the Silver and Gold snapshots, making reruns idempotent-ish for this take-home.
