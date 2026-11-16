# Design: payments ingestion (milestone 1)

## Goal
Copy every settlement from the partner API into PostgreSQL, safely and repeatably. Running the job twice must give the same row count.

## Components
| Component | Role |
|---|---|
| Settlement API (`tools/settlement_api.py`, Docker) | Source. Cursor-paginated, rate limited, fails ~5% of requests. |
| Ingestion job (`src/ingestion.py`, Docker) | Fetches pages, validates rows, writes to Postgres. |
| PostgreSQL 16 (Docker) | Holds `raw_settlements`, `rejected_records`, `ingest_state`. |

## Data flow
1. Read the saved cursor from `ingest_state` (none = start at page one).
2. `GET /v1/settlements?limit=N&cursor=...` (retry on 429/503 with backoff).
3. Validate each row against the schema. Bad rows go to `rejected_records` with a reason; good rows continue.
4. In ONE transaction: `INSERT ... ON CONFLICT (txn_id) DO NOTHING`, then save `next_cursor`. Commit.
5. Repeat until `next_cursor` is null, then the cursor is cleared so the next run rescans from the start (needed later for late batches).

Settings (`DATABASE_URL`, `API_URL`, `PAGE_SIZE`, `MAX_RETRIES`, `BACKOFF_SECONDS`) come from environment variables.

## Why a rerun adds no rows
`txn_id` is the primary key and inserts ignore conflicts, so replaying any page is harmless. Rows and cursor commit together, so a crash never leaves the cursor ahead of the data (no gaps). The worst case is re-reading one page, which is ignored.

## What can fail
| Failure | Handling |
|---|---|
| API 503 / 429 / network error | Retry with exponential backoff (honour `Retry-After`), give up after `MAX_RETRIES`. The cursor stays on the last committed page, so the next run resumes. |
| Malformed row (missing field, bad type or value) | Quarantined in `rejected_records`; the run continues. |
| Crash mid-run | Resume from the saved cursor; duplicates ignored. |
| Postgres down | Job exits non-zero; nothing partial is committed. |
| Schema drift in the API | Rows rejected with a reason, visible in `rejected_records`. |

## Cost
Local Docker only, so no cloud cost. The 1M-row ledger at 500 rows/page is about 2,000 requests; at the 20 req/s limit that is under two minutes plus retries. Postgres storage is roughly 150-200 MB. On AWS later the main costs would be a small RDS instance and compute time; idempotent page-sized transactions keep retries cheap.
