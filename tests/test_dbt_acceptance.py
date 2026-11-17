"""Run the real dbt project against PostgreSQL and check what it builds."""
import os
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import pytest

from src import ingestion

dbt_main = pytest.importorskip("dbt.cli.main", reason="dbt is not installed; pip install -r requirements-dbt.txt")

ROOT = Path(__file__).resolve().parent.parent
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://payments:payments@localhost:5432/payments")
SCHEMA = "dbt_acceptance"


@pytest.fixture
def env(monkeypatch):
    u = urlparse(DATABASE_URL)
    for key, value in {
        "DBT_HOST": u.hostname, "DBT_PORT": str(u.port or 5432), "DBT_USER": u.username,
        "DBT_PASSWORD": u.password or "", "DBT_DBNAME": u.path.lstrip("/"), "DBT_SCHEMA": SCHEMA,
    }.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def conn(env):
    try:
        c = psycopg2.connect(DATABASE_URL, connect_timeout=3)
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL not reachable; run `docker compose up -d postgres`")
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        cur.execute("DROP TABLE IF EXISTS raw_settlements, rejected_records, ingest_state CASCADE")
        cur.execute(ingestion.SCHEMA)
    yield c
    with c.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    c.close()


@pytest.fixture
def dbt(tmp_path):
    def run(*args):
        res = dbt_main.dbtRunner().invoke(
            [*args, "--project-dir", str(ROOT), "--profiles-dir", str(ROOT),
             "--target-path", str(tmp_path / "target"), "--log-path", str(tmp_path / "logs")]
        )
        return res
    return run


def add_txn(conn, txn_id, merchant="M00001", status="settled", amount=1000, day="2024-03-01", ingested="2024-03-02"):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw_settlements (txn_id, merchant_id, account_id, amount_minor, currency, status,
                                            created_at, settled_at, ingested_at)
               VALUES (%s, %s, 'a1', %s, 'NGN', %s, %s::timestamptz, %s::timestamptz + interval '1 day', %s::timestamptz)""",
            (txn_id, merchant, amount, status, f"{day}T10:00:00+00:00", f"{day}T10:00:00+00:00", ingested),
        )


def seed_raw(conn, n=20):
    for i in range(n):
        # t000 arrives in an older batch than the rest, so it is outside the "newest batch" window
        add_txn(conn, f"t{i:03d}", merchant=f"M0000{1 + i % 4}", amount=500 + i,
                status=["settled", "settled", "reversed", "pending"][i % 4],
                ingested="2024-03-01" if i == 0 else "2024-03-02")


def scalar(conn, sql, *args):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()[0]


def failed_tests(result):
    return sorted(r.node.name for r in result.result.results if r.status in ("fail", "error"))


def test_build_runs_every_model_and_all_tests_pass(conn, dbt):
    seed_raw(conn)
    res = dbt("build")
    assert res.success, failed_tests(res)
    results = res.result.results
    kinds = {r.node.resource_type.value for r in results}
    assert {"seed", "model", "snapshot", "test"} <= kinds
    assert sum(1 for r in results if r.node.resource_type.value == "test") >= 20
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.stg_settlements") == 20
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.fct_payments") == 20


def test_staging_tests_catch_bad_data(conn, dbt):
    seed_raw(conn, 4)
    add_txn(conn, "bad1", status="refunded")            # status not allowed
    add_txn(conn, "bad2", amount=-5)                    # amount must be positive
    res = dbt("build", "--exclude", "fct_payments")
    assert not res.success
    names = " ".join(failed_tests(res))
    assert "accepted_values_stg_settlements_status" in names
    assert "assert_amounts_are_positive" in names


def test_incremental_mart_only_processes_new_rows_and_rerun_is_stable(conn, dbt):
    seed_raw(conn, 8)
    assert dbt("build").success
    before = scalar(conn, f"SELECT count(*) FROM {SCHEMA}.fct_payments")
    # mark an old row; an incremental run must not rewrite it
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {SCHEMA}.fct_payments SET net_amount_minor = -1 WHERE txn_id = 't000'")
    assert dbt("build").success                           # rerun with no new data
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.fct_payments") == before
    assert scalar(conn, f"SELECT net_amount_minor FROM {SCHEMA}.fct_payments WHERE txn_id = 't000'") == -1

    add_txn(conn, "new1", ingested="2024-04-01")
    add_txn(conn, "new2", ingested="2024-04-01", status="reversed")
    res = dbt("build")
    assert res.success, failed_tests(res)
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.fct_payments") == before + 2
    assert scalar(conn, f"SELECT count(*) FROM (SELECT txn_id FROM {SCHEMA}.fct_payments GROUP BY 1 HAVING count(*) > 1) d") == 0
    assert scalar(conn, f"SELECT net_amount_minor FROM {SCHEMA}.fct_payments WHERE txn_id = 't000'") == -1  # untouched

    # a full refresh rebuilds everything and gives the same row set
    assert dbt("build", "--full-refresh", "--select", "fct_payments").success
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.fct_payments") == before + 2
    assert scalar(conn, f"SELECT net_amount_minor FROM {SCHEMA}.fct_payments WHERE txn_id = 't000'") != -1


def test_snapshot_keeps_merchant_history(conn, dbt):
    seed_raw(conn, 8)
    assert dbt("build").success
    merchants = scalar(conn, f"SELECT count(*) FROM {SCHEMA}.stg_merchants")
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.merchants_snapshot") == merchants
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.merchants_snapshot WHERE dbt_valid_to IS NULL") == merchants

    # snapshot again with no change: nothing new
    assert dbt("snapshot").success
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.merchants_snapshot") == merchants

    # M00001 changes category: a new profile row, then snapshot (seed is not reloaded)
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {SCHEMA}.merchant_profiles VALUES ('M00001', 'Lagos Fresh Foods', 'restaurant', 'low', '2024-09-01')")
    assert dbt("snapshot").success                         # staging models are views, so they read the new row
    with conn.cursor() as cur:
        cur.execute(f"""SELECT category, dbt_valid_to IS NULL FROM {SCHEMA}.merchants_snapshot
                        WHERE merchant_id = 'M00001' ORDER BY dbt_valid_from""")
        rows = cur.fetchall()
    assert rows == [("grocery", False), ("restaurant", True)]
    # nobody else gained a row
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.merchants_snapshot") == merchants + 1
    assert scalar(conn, f"SELECT count(*) FROM {SCHEMA}.merchants_snapshot WHERE dbt_valid_to IS NULL") == merchants
