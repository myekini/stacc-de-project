"""Build the star schema from raw_settlements: dimensions (SCD2 merchant), facts, cumulative table."""
import csv
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import psycopg2

log = logging.getLogger("warehouse")
SCHEMA_FILE = Path(__file__).resolve().parent.parent / "models" / "schema.sql"
FAR_FUTURE = "9999-12-31"
EPOCH = "1900-01-01"
UTC_DAY = "(r.created_at AT TIME ZONE 'UTC')::date"


def _day(value):
    return date.fromisoformat(value) if isinstance(value, str) else value


def apply_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text())
    conn.commit()


def load_profiles(conn, rows):
    """Stage merchant profile snapshots (merchant_id, merchant_name, category, risk_tier, effective_date)."""
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """INSERT INTO stg_merchant_profile VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (merchant_id, effective_date) DO UPDATE
                   SET merchant_name = EXCLUDED.merchant_name, category = EXCLUDED.category,
                       risk_tier = EXCLUDED.risk_tier""",
                (r["merchant_id"], r["merchant_name"], r["category"], r["risk_tier"], r["effective_date"]),
            )
    conn.commit()


def load_profiles_csv(conn, path):
    with open(path, newline="") as f:
        load_profiles(conn, list(csv.DictReader(f)))


def scd2_merchants(conn):
    """Apply staged profile changes to dim_merchant, keeping history. Idempotent.

    Limits: a snapshot dated on or before the current version's start is ignored (no back-dated
    corrections), and a merchant's first version uses placeholder attributes until a profile exists.
    """
    with conn.cursor() as cur:
        # 1. Every merchant we have seen gets a first version.
        cur.execute(
            f"""INSERT INTO dim_merchant (merchant_id, merchant_name, category, risk_tier, valid_from, valid_to, is_current)
                SELECT m, 'Unknown', 'Unknown', 'unrated', DATE '{EPOCH}', DATE '{FAR_FUTURE}', TRUE
                FROM (SELECT merchant_id AS m FROM raw_settlements
                      UNION SELECT merchant_id FROM stg_merchant_profile) seen
                WHERE NOT EXISTS (SELECT 1 FROM dim_merchant d WHERE d.merchant_id = seen.m)"""
        )
        # 2. Replay snapshots oldest first: close the current version if attributes changed, open a new one.
        cur.execute("SELECT DISTINCT effective_date FROM stg_merchant_profile ORDER BY 1")
        for (eff,) in cur.fetchall():
            cur.execute(
                """UPDATE dim_merchant d SET valid_to = s.effective_date, is_current = FALSE
                   FROM stg_merchant_profile s
                   WHERE s.effective_date = %s AND d.merchant_id = s.merchant_id AND d.is_current
                     AND d.valid_from < s.effective_date
                     AND (d.merchant_name, d.category, d.risk_tier)
                         IS DISTINCT FROM (s.merchant_name, s.category, s.risk_tier)""",
                (eff,),
            )
            cur.execute(
                f"""INSERT INTO dim_merchant (merchant_id, merchant_name, category, risk_tier, valid_from, valid_to, is_current)
                    SELECT s.merchant_id, s.merchant_name, s.category, s.risk_tier, s.effective_date, DATE '{FAR_FUTURE}', TRUE
                    FROM stg_merchant_profile s
                    WHERE s.effective_date = %s
                      AND NOT EXISTS (SELECT 1 FROM dim_merchant d WHERE d.merchant_id = s.merchant_id AND d.is_current)""",
                (eff,),
            )
    conn.commit()


def load_facts(conn):
    """Fill dim_date / dim_account and insert new settlements into fact_settlement. Idempotent."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO dim_date
               SELECT to_char(d, 'YYYYMMDD')::int, d, EXTRACT(year FROM d), EXTRACT(month FROM d),
                      EXTRACT(day FROM d), EXTRACT(isodow FROM d), EXTRACT(isodow FROM d) >= 6
               FROM generate_series(
                   (SELECT min(created_at AT TIME ZONE 'UTC')::date FROM raw_settlements),
                   (SELECT max(greatest(created_at, coalesce(settled_at, created_at)) AT TIME ZONE 'UTC')::date
                    FROM raw_settlements),
                   interval '1 day') AS g(d)
               ON CONFLICT DO NOTHING"""
        )
        cur.execute(
            "INSERT INTO dim_account (account_id) SELECT DISTINCT account_id FROM raw_settlements ON CONFLICT DO NOTHING"
        )
        cur.execute(
            f"""INSERT INTO fact_settlement
                SELECT r.txn_id,
                       to_char({UTC_DAY}, 'YYYYMMDD')::int,
                       to_char((r.settled_at AT TIME ZONE 'UTC')::date, 'YYYYMMDD')::int,
                       m.merchant_key, a.account_key, s.status_key, r.currency, r.amount_minor
                FROM raw_settlements r
                JOIN dim_merchant m ON m.merchant_id = r.merchant_id
                                   AND {UTC_DAY} >= m.valid_from AND {UTC_DAY} < m.valid_to
                JOIN dim_account a ON a.account_id = r.account_id
                JOIN dim_status s ON s.status = r.status
                ON CONFLICT (txn_id) DO NOTHING"""
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


CUM_DELETE = "DELETE FROM cum_merchant_activity WHERE as_of_date = %(day)s"
CUM_INSERT = """
INSERT INTO cum_merchant_activity
WITH today AS (
    SELECT m.merchant_id,
           count(*)                                      AS n,
           count(*) FILTER (WHERE s.status = 'reversed') AS reversed,
           sum(f.amount_minor)                           AS total,
           coalesce(sum(f.amount_minor) FILTER (WHERE s.status = 'settled'), 0) AS settled
    FROM fact_settlement f
    JOIN dim_merchant m ON m.merchant_key = f.merchant_key
    JOIN dim_status s   ON s.status_key = f.status_key
    WHERE f.created_date_key = to_char(%(day)s::date, 'YYYYMMDD')::int
    GROUP BY m.merchant_id
), prev AS (
    SELECT * FROM cum_merchant_activity WHERE as_of_date = %(day)s::date - 1
)
SELECT %(day)s::date,
       coalesce(p.merchant_id, t.merchant_id),
       coalesce(p.first_txn_date, CASE WHEN t.n IS NOT NULL THEN %(day)s::date END),
       CASE WHEN t.n IS NOT NULL THEN %(day)s::date ELSE p.last_txn_date END,
       coalesce(p.active_days, 0) + (t.n IS NOT NULL)::int,
       coalesce(p.txn_count, 0) + coalesce(t.n, 0),
       coalesce(p.reversed_count, 0) + coalesce(t.reversed, 0),
       coalesce(p.total_amount_minor, 0) + coalesce(t.total, 0),
       coalesce(p.settled_amount_minor, 0) + coalesce(t.settled, 0)
FROM prev p FULL JOIN today t ON t.merchant_id = p.merchant_id
"""


def build_cumulative_day(conn, day):
    """(Re)build one day of cum_merchant_activity = previous day's rows + that day's facts.

    Rebuilding a built day replaces its rows with identical ones. If that day's facts changed, use
    build_cumulative_from() so later days are recomputed too.
    """
    day = _day(day)
    with conn.cursor() as cur:
        cur.execute("SELECT min(as_of_date) FROM cum_merchant_activity")
        first = cur.fetchone()[0]
        if first is not None and day > first:
            cur.execute("SELECT 1 FROM cum_merchant_activity WHERE as_of_date = %s LIMIT 1", (day - timedelta(days=1),))
            if cur.fetchone() is None:
                raise ValueError(f"cannot build {day}: previous day {day - timedelta(days=1)} is missing")
        cur.execute(CUM_DELETE, {"day": day})
        cur.execute(CUM_INSERT, {"day": day})
    conn.commit()


def _fact_range(conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT min(d.full_date), max(d.full_date)
               FROM fact_settlement f JOIN dim_date d ON d.date_key = f.created_date_key"""
        )
        return cur.fetchone()


def build_cumulative_from(conn, start, end=None):
    """Build every day from start to end (default: last fact date). Returns the number of days built."""
    day, end = _day(start), _day(end) if end else _fact_range(conn)[1]
    n = 0
    while end is not None and day <= end:
        build_cumulative_day(conn, day)
        day += timedelta(days=1)
        n += 1
    return n


def run(database_url, profiles_csv=None):
    conn = psycopg2.connect(database_url)
    try:
        apply_schema(conn)
        if profiles_csv:
            load_profiles_csv(conn, profiles_csv)
        scd2_merchants(conn)
        new = load_facts(conn)
        first = _fact_range(conn)[0]
        days = build_cumulative_from(conn, first) if first else 0
        log.info("new facts=%d cumulative days built=%d", new, days)
        return {"new_facts": new, "days": days}
    finally:
        conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    url = os.environ.get("DATABASE_URL")
    if not url:
        log.error("missing environment variable DATABASE_URL")
        sys.exit(2)
    run(url, os.environ.get("MERCHANT_PROFILES_CSV"))


if __name__ == "__main__":
    main()
