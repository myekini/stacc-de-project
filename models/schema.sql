-- Star schema for payments (milestone 2). Safe to run repeatedly.
-- Grain of fact_settlement: ONE ROW PER SETTLEMENT TRANSACTION (txn_id).

-- ---------- Dimensions ----------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,          -- yyyymmdd
    full_date   DATE NOT NULL UNIQUE,
    year        SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    day         SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,            -- 1 = Monday
    is_weekend  BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_status (
    status_key SMALLINT PRIMARY KEY,
    status     TEXT NOT NULL UNIQUE
);
INSERT INTO dim_status (status_key, status)
VALUES (1, 'settled'), (2, 'reversed'), (3, 'pending')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_account (       -- Type 1: one row per account
    account_key BIGSERIAL PRIMARY KEY,
    account_id  TEXT NOT NULL UNIQUE
);

-- SCD Type 2: every change to name / category / risk tier adds a new row.
-- A version is valid for valid_from <= day < valid_to (valid_to is exclusive).
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_key  BIGSERIAL PRIMARY KEY,       -- surrogate key, one per VERSION
    merchant_id   TEXT NOT NULL,               -- natural / business key
    merchant_name TEXT NOT NULL,
    category      TEXT NOT NULL,
    risk_tier     TEXT NOT NULL,
    valid_from    DATE NOT NULL,
    valid_to      DATE NOT NULL DEFAULT '9999-12-31',
    is_current    BOOLEAN NOT NULL DEFAULT TRUE,
    CHECK (valid_from < valid_to)
);
-- At most one current version per merchant, and one version per start date.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_merchant_current
    ON dim_merchant (merchant_id) WHERE is_current;
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_merchant_version
    ON dim_merchant (merchant_id, valid_from);

-- Input feed of merchant profile snapshots: "from this date, merchant X looks like this".
CREATE TABLE IF NOT EXISTS stg_merchant_profile (
    merchant_id    TEXT NOT NULL,
    merchant_name  TEXT NOT NULL,
    category       TEXT NOT NULL,
    risk_tier      TEXT NOT NULL,
    effective_date DATE NOT NULL,
    PRIMARY KEY (merchant_id, effective_date)
);

-- ---------- Fact ----------
CREATE TABLE IF NOT EXISTS fact_settlement (
    txn_id           TEXT PRIMARY KEY,
    created_date_key INTEGER  NOT NULL REFERENCES dim_date (date_key),
    settled_date_key INTEGER  REFERENCES dim_date (date_key),
    merchant_key     BIGINT   NOT NULL REFERENCES dim_merchant (merchant_key),  -- version valid on created date
    account_key      BIGINT   NOT NULL REFERENCES dim_account (account_key),
    status_key       SMALLINT NOT NULL REFERENCES dim_status (status_key),
    currency         CHAR(3)  NOT NULL,
    amount_minor     BIGINT   NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_created_date ON fact_settlement (created_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_merchant ON fact_settlement (merchant_key);

-- ---------- Cumulative table ----------
-- One row per merchant per day: everything the merchant has done up to and including as_of_date.
-- Built as (previous day's row) + (that day's facts), so rebuilding a day gives the same rows.
CREATE TABLE IF NOT EXISTS cum_merchant_activity (
    as_of_date           DATE NOT NULL,
    merchant_id          TEXT NOT NULL,
    first_txn_date       DATE,
    last_txn_date        DATE,
    active_days          INTEGER NOT NULL,
    txn_count            BIGINT  NOT NULL,
    reversed_count       BIGINT  NOT NULL,
    total_amount_minor   BIGINT  NOT NULL,
    settled_amount_minor BIGINT  NOT NULL,
    PRIMARY KEY (as_of_date, merchant_id)
);
