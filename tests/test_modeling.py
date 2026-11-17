import os
from datetime import date

import psycopg2
import pytest

from src import ingestion, warehouse

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://payments:payments@localhost:5432/payments")

DROP = """DROP TABLE IF EXISTS cum_merchant_activity, fact_settlement, stg_merchant_profile,
          dim_merchant, dim_account, dim_status, dim_date, raw_settlements, rejected_records, ingest_state CASCADE"""


@pytest.fixture
def conn():
    try:
        c = psycopg2.connect(DATABASE_URL, connect_timeout=3)
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL not reachable; run `docker compose up -d postgres`")
    with c, c.cursor() as cur:
        cur.execute(DROP)
        cur.execute(ingestion.SCHEMA)
    warehouse.apply_schema(c)
    yield c
    c.close()


def add_txn(conn, txn_id, merchant, day, amount=1000, status="settled", account="a1"):
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw_settlements (txn_id, merchant_id, account_id, amount_minor, currency, status, created_at, settled_at)
               VALUES (%s, %s, %s, %s, 'NGN', %s, %s::timestamptz, %s::timestamptz + interval '1 day')""",
            (txn_id, merchant, account, amount, status, f"{day}T10:00:00+00:00", f"{day}T10:00:00+00:00"),
        )


def profile(merchant, name, category, risk, eff):
    return {"merchant_id": merchant, "merchant_name": name, "category": category, "risk_tier": risk, "effective_date": eff}


def versions(conn, merchant="m1"):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT category, risk_tier, valid_from, valid_to, is_current
               FROM dim_merchant WHERE merchant_id = %s ORDER BY valid_from""",
            (merchant,),
        )
        return cur.fetchall()


def scalar(conn, sql, *args):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()[0]


def cum_rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM cum_merchant_activity ORDER BY as_of_date, merchant_id")
        return cur.fetchall()


# ---------- SCD Type 2 ----------

def test_changed_attribute_adds_a_version_and_keeps_the_old_one(conn):
    add_txn(conn, "t1", "m1", "2024-01-10")
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "low", "2024-01-01"),
                                   profile("m1", "Shop", "retail", "high", "2024-02-01")])
    warehouse.scd2_merchants(conn)
    rows = versions(conn)
    assert [(r[0], r[1]) for r in rows] == [("Unknown", "unrated"), ("retail", "low"), ("retail", "high")]
    # versions are contiguous, only the last is current and open ended
    assert rows[0][3] == rows[1][2] == date(2024, 1, 1)
    assert rows[1][3] == rows[2][2] == date(2024, 2, 1)
    assert [r[4] for r in rows] == [False, False, True]
    assert rows[2][3] == date(9999, 12, 31)


def test_unchanged_profile_does_not_add_a_version(conn):
    add_txn(conn, "t1", "m1", "2024-01-10")
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "low", "2024-01-01"),
                                   profile("m1", "Shop", "retail", "low", "2024-03-01")])
    warehouse.scd2_merchants(conn)
    assert len(versions(conn)) == 2


def test_scd2_is_idempotent(conn):
    add_txn(conn, "t1", "m1", "2024-01-10")
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "low", "2024-01-01"),
                                   profile("m1", "Shop", "retail", "high", "2024-02-01")])
    warehouse.scd2_merchants(conn)
    before = versions(conn)
    warehouse.scd2_merchants(conn)
    warehouse.scd2_merchants(conn)
    assert versions(conn) == before


def test_late_profile_arrives_in_a_later_run(conn):
    add_txn(conn, "t1", "m1", "2024-01-10")
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "low", "2024-01-01")])
    warehouse.scd2_merchants(conn)
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "high", "2024-02-01")])
    warehouse.scd2_merchants(conn)
    assert [r[1] for r in versions(conn)] == ["unrated", "low", "high"]
    assert scalar(conn, "SELECT count(*) FROM dim_merchant WHERE merchant_id='m1' AND is_current") == 1


def test_fact_points_at_the_version_valid_on_the_transaction_date(conn):
    add_txn(conn, "jan", "m1", "2024-01-15")
    add_txn(conn, "feb", "m1", "2024-02-10")
    add_txn(conn, "old", "m1", "2023-06-01")
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "low", "2024-01-01"),
                                   profile("m1", "Shop", "retail", "high", "2024-02-01")])
    warehouse.scd2_merchants(conn)
    warehouse.load_facts(conn)
    risk = {t: scalar(conn, """SELECT m.risk_tier FROM fact_settlement f
                               JOIN dim_merchant m USING (merchant_key) WHERE f.txn_id = %s""", t)
            for t in ("jan", "feb", "old")}
    assert risk == {"jan": "low", "feb": "high", "old": "unrated"}


def test_old_facts_keep_their_version_after_a_new_change(conn):
    add_txn(conn, "jan", "m1", "2024-01-15")
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "low", "2024-01-01")])
    warehouse.scd2_merchants(conn)
    warehouse.load_facts(conn)
    key = scalar(conn, "SELECT merchant_key FROM fact_settlement WHERE txn_id='jan'")
    warehouse.load_profiles(conn, [profile("m1", "Shop", "retail", "high", "2024-02-01")])
    warehouse.scd2_merchants(conn)
    warehouse.load_facts(conn)
    assert scalar(conn, "SELECT merchant_key FROM fact_settlement WHERE txn_id='jan'") == key


def test_facts_load_is_idempotent(conn):
    for i in range(5):
        add_txn(conn, f"t{i}", "m1", "2024-01-10")
    warehouse.scd2_merchants(conn)
    assert warehouse.load_facts(conn) == 5
    assert warehouse.load_facts(conn) == 0
    assert scalar(conn, "SELECT count(*) FROM fact_settlement") == 5


# ---------- Cumulative table ----------

def seed_activity(conn):
    add_txn(conn, "a1", "m1", "2024-01-01", 100)
    add_txn(conn, "a2", "m1", "2024-01-01", 50, status="reversed")
    add_txn(conn, "a3", "m2", "2024-01-02", 70)
    add_txn(conn, "a4", "m1", "2024-01-04", 10)
    warehouse.scd2_merchants(conn)
    warehouse.load_facts(conn)
    warehouse.build_cumulative_from(conn, "2024-01-01", "2024-01-04")


def test_cumulative_totals(conn):
    seed_activity(conn)
    with conn.cursor() as cur:
        cur.execute("""SELECT merchant_id, first_txn_date, last_txn_date, active_days, txn_count,
                              reversed_count, total_amount_minor, settled_amount_minor
                       FROM cum_merchant_activity WHERE as_of_date = '2024-01-04' ORDER BY merchant_id""")
        rows = cur.fetchall()
    assert rows == [
        ("m1", date(2024, 1, 1), date(2024, 1, 4), 2, 3, 1, 160, 110),
        ("m2", date(2024, 1, 2), date(2024, 1, 2), 1, 1, 0, 70, 70),
    ]
    # a merchant that has not appeared yet has no row for earlier days
    assert scalar(conn, "SELECT count(*) FROM cum_merchant_activity WHERE as_of_date='2024-01-01' AND merchant_id='m2'") == 0
    # days without activity carry the totals forward
    assert scalar(conn, "SELECT txn_count FROM cum_merchant_activity WHERE as_of_date='2024-01-03' AND merchant_id='m1'") == 2


def test_rebuilding_one_day_leaves_the_table_unchanged(conn):
    seed_activity(conn)
    before = cum_rows(conn)
    for day in ("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"):
        warehouse.build_cumulative_day(conn, day)
        assert cum_rows(conn) == before


def test_cannot_build_a_day_after_a_gap(conn):
    seed_activity(conn)
    with pytest.raises(ValueError):
        warehouse.build_cumulative_day(conn, "2024-01-09")


def test_full_run_twice_gives_same_tables(conn):
    add_txn(conn, "a1", "m1", "2024-01-01", 100)
    add_txn(conn, "a2", "m2", "2024-01-03", 70)
    warehouse.run(DATABASE_URL)
    counts = [scalar(conn, f"SELECT count(*) FROM {t}") for t in ("fact_settlement", "dim_merchant", "cum_merchant_activity")]
    conn.commit()
    warehouse.run(DATABASE_URL)
    conn.commit()
    assert counts == [scalar(conn, f"SELECT count(*) FROM {t}") for t in ("fact_settlement", "dim_merchant", "cum_merchant_activity")]
