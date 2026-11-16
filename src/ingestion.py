"""Load the settlement API into PostgreSQL with a saved cursor. Safe to rerun."""
import json
import logging
import os
import sys
import time
from datetime import datetime

import psycopg2
import requests

log = logging.getLogger("ingestion")

SOURCE = "settlements"
STATUSES = {"settled", "reversed", "pending"}
REQUIRED = ["txn_id", "merchant_id", "account_id", "amount_minor", "currency", "status", "created_at"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_settlements (
    txn_id       TEXT PRIMARY KEY,
    merchant_id  TEXT NOT NULL,
    account_id   TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency     CHAR(3) NOT NULL,
    status       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    settled_at   TIMESTAMPTZ,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS rejected_records (
    id          BIGSERIAL PRIMARY KEY,
    payload     JSONB NOT NULL,
    reason      TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ingest_state (
    source     TEXT PRIMARY KEY,
    cursor     TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

INSERT = """
INSERT INTO raw_settlements (txn_id, merchant_id, account_id, amount_minor, currency, status, created_at, settled_at)
VALUES (%(txn_id)s, %(merchant_id)s, %(account_id)s, %(amount_minor)s, %(currency)s, %(status)s, %(created_at)s, %(settled_at)s)
ON CONFLICT (txn_id) DO NOTHING
"""


class Settings:
    """All configuration comes from environment variables."""

    def __init__(self, env=None):
        env = os.environ if env is None else env
        self.database_url = env["DATABASE_URL"]
        self.api_url = env.get("API_URL", "http://localhost:8080").rstrip("/")
        self.page_size = int(env.get("PAGE_SIZE", "500"))
        self.max_retries = int(env.get("MAX_RETRIES", "8"))
        self.backoff = float(env.get("BACKOFF_SECONDS", "0.5"))


def _ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate(record):
    """Return (clean_row, None) or (None, reason)."""
    if not isinstance(record, dict):
        return None, "record is not an object"
    for field in REQUIRED:
        if record.get(field) in (None, ""):
            return None, f"missing {field}"
    try:
        amount = int(record["amount_minor"])
    except (TypeError, ValueError):
        return None, "amount_minor is not an integer"
    currency = str(record["currency"])
    if len(currency) != 3 or not currency.isalpha():
        return None, "currency must be 3 letters"
    if record["status"] not in STATUSES:
        return None, f"unknown status {record['status']!r}"
    try:
        created = _ts(record["created_at"])
        settled = _ts(record["settled_at"]) if record.get("settled_at") else None
    except (TypeError, ValueError, AttributeError):
        return None, "bad timestamp"
    return {
        "txn_id": str(record["txn_id"]),
        "merchant_id": str(record["merchant_id"]),
        "account_id": str(record["account_id"]),
        "amount_minor": amount,
        "currency": currency.upper(),
        "status": record["status"],
        "created_at": created,
        "settled_at": settled,
    }, None


def fetch_page(session, settings, cursor, sleep=time.sleep):
    params = {"limit": settings.page_size}
    if cursor:
        params["cursor"] = cursor
    for attempt in range(settings.max_retries + 1):
        try:
            resp = session.get(f"{settings.api_url}/v1/settlements", params=params, timeout=30)
        except requests.RequestException as exc:
            log.warning("request error: %s", exc)
            resp = None
        if resp is not None and resp.status_code == 200:
            body = resp.json()
            if not isinstance(body, dict) or not isinstance(body.get("data"), list):
                raise ValueError("unexpected response shape")
            return body
        if resp is not None and resp.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"API returned {resp.status_code}: {resp.text[:200]}")
        if attempt == settings.max_retries:
            break
        delay = min(settings.backoff * (2 ** attempt), 30)
        if resp is not None and resp.headers.get("Retry-After"):
            delay = max(delay, float(resp.headers["Retry-After"]))
        sleep(delay)
    raise RuntimeError("API still failing after retries")


def load_cursor(conn):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT cursor FROM ingest_state WHERE source = %s", (SOURCE,))
        row = cur.fetchone()
    return row[0] if row else None


def write_page(conn, rows, rejects, next_cursor):
    """Rows, rejects and the cursor commit in one transaction."""
    with conn, conn.cursor() as cur:
        for row in rows:
            cur.execute(INSERT, row)
        for payload, reason in rejects:
            cur.execute("INSERT INTO rejected_records (payload, reason) VALUES (%s, %s)",
                        (json.dumps(payload, default=str), reason))
        cur.execute(
            "INSERT INTO ingest_state (source, cursor) VALUES (%s, %s) "
            "ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()",
            (SOURCE, next_cursor),
        )


def run(settings, sleep=time.sleep):
    conn = psycopg2.connect(settings.database_url)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMA)
        cursor = load_cursor(conn)
        if cursor:
            log.info("resuming from saved cursor")
        session = requests.Session()
        pages = total = rejected = 0
        while True:
            body = fetch_page(session, settings, cursor, sleep)
            rows, rejects = [], []
            for record in body["data"]:
                clean, reason = validate(record)
                if clean:
                    rows.append(clean)
                else:
                    rejects.append((record, reason))
            cursor = body.get("next_cursor")
            write_page(conn, rows, rejects, cursor)
            pages += 1
            total += len(rows)
            rejected += len(rejects)
            if pages % 50 == 0:
                log.info("pages=%d rows_seen=%d rejected=%d", pages, total, rejected)
            if not cursor:
                break
        log.info("done: pages=%d rows_seen=%d rejected=%d", pages, total, rejected)
        return {"pages": pages, "rows": total, "rejected": rejected}
    finally:
        conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        settings = Settings()
    except KeyError as exc:
        log.error("missing environment variable %s", exc)
        sys.exit(2)
    run(settings)


if __name__ == "__main__":
    main()
