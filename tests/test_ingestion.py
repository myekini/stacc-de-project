import csv
import importlib.util
import os
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

import psycopg2
import pytest
import requests

from src import ingestion

ROOT = Path(__file__).resolve().parent.parent
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://payments:payments@localhost:5432/payments")

spec = importlib.util.spec_from_file_location("settlement_api", ROOT / "tools" / "settlement_api.py")
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)

HEADER = ["txn_id", "merchant_id", "account_id", "amount_minor", "currency", "status", "created_at", "settled_at"]
GOOD = dict(zip(HEADER, ["t1", "m", "a", "100", "NGN", "settled", "2024-01-01T00:00:00+00:00", ""]))
ROWS = 1234


def make_ledger(path, n):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for i in range(n):
            w.writerow([f"t{i:05d}", "m1", "a1", 1000 + i, "NGN", "settled",
                        f"2024-01-01T00:00:{i % 60:02d}+00:00", f"2024-01-02T00:00:{i % 60:02d}+00:00"])


@pytest.fixture
def db():
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=3)
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL not reachable; run `docker compose up -d postgres`")
    with conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS raw_settlements, rejected_records, ingest_state")
    yield conn
    conn.close()


@pytest.fixture
def server(tmp_path):
    ledger_path = tmp_path / "ledger.csv"
    make_ledger(ledger_path, ROWS)
    ledger = api.Ledger(str(ledger_path), 0)
    # 20% failure rate: ingestion has to retry its way through.
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), api.make_handler(ledger, 0, 0.2))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()


def settings(url):
    return ingestion.Settings({"DATABASE_URL": DATABASE_URL, "API_URL": url, "PAGE_SIZE": "200",
                               "BACKOFF_SECONDS": "0.001", "MAX_RETRIES": "20"})


def count(conn, table="raw_settlements"):
    with conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def no_sleep(_seconds):
    pass


def test_rerun_adds_no_rows(db, server):
    ingestion.run(settings(server), sleep=no_sleep)
    first = count(db)
    assert first == ROWS
    ingestion.run(settings(server), sleep=no_sleep)
    assert count(db) == first


def test_resumes_from_saved_cursor(db, server):
    s = settings(server)
    # Simulate a crash after the first page.
    page = ingestion.fetch_page(requests.Session(), s, None, no_sleep)
    rows = [ingestion.validate(r)[0] for r in page["data"]]
    with db, db.cursor() as cur:
        cur.execute(ingestion.SCHEMA)
    ingestion.write_page(db, rows, [], page["next_cursor"])
    assert ingestion.load_cursor(db) == page["next_cursor"]
    ingestion.run(s, sleep=no_sleep)
    assert count(db) == ROWS


def test_validate_accepts_good_row():
    row, reason = ingestion.validate({**GOOD, "currency": "ngn"})
    assert reason is None
    assert row["amount_minor"] == 100 and row["currency"] == "NGN" and row["settled_at"] is None


@pytest.mark.parametrize("change", [
    {"txn_id": ""}, {"amount_minor": "abc"}, {"currency": "NG"}, {"status": "weird"}, {"created_at": "yesterday"},
])
def test_validate_rejects_bad_rows(change):
    row, reason = ingestion.validate({**GOOD, **change})
    assert row is None and reason


def test_bad_rows_are_quarantined_not_loaded(db):
    bad = {**GOOD, "txn_id": "t2", "status": "weird"}
    with db, db.cursor() as cur:
        cur.execute(ingestion.SCHEMA)
    ingestion.write_page(db, [ingestion.validate(GOOD)[0]], [(bad, "unknown status")], None)
    assert count(db) == 1 and count(db, "rejected_records") == 1


def test_settings_come_from_environment():
    s = ingestion.Settings({"DATABASE_URL": "postgresql://x", "API_URL": "http://api:1/", "PAGE_SIZE": "7"})
    assert (s.database_url, s.api_url, s.page_size) == ("postgresql://x", "http://api:1", 7)
