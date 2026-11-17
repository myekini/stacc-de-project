# Bus matrix: payments (milestone 2)

## Grain
**One row in `fact_settlement` = one settlement transaction (`txn_id`).** Nothing is pre-aggregated, so any total can be rebuilt from it.

## Facts
| Fact table | Measures | Notes |
|---|---|---|
| `fact_settlement` | `amount_minor` (additive, minor units), `currency` | Count of rows = number of transactions. Amounts only add up within one currency. |
| `cum_merchant_activity` | `txn_count`, `reversed_count`, `total_amount_minor`, `settled_amount_minor`, `active_days`, `first_txn_date`, `last_txn_date` | Cumulative snapshot: one row per merchant per day, "everything up to and including `as_of_date`". |

## Dimensions
| Dimension | Key | History | Attributes |
|---|---|---|---|
| `dim_date` | `date_key` (yyyymmdd) | none | date, year, month, day, day of week, weekend flag |
| `dim_merchant` | `merchant_key` (one per version), `merchant_id` | **SCD Type 2** | name, category, risk tier, `valid_from`, `valid_to`, `is_current` |
| `dim_account` | `account_key`, `account_id` | Type 1 (no history) | account id |
| `dim_status` | `status_key` | none | settled, reversed, pending |

## Matrix
| Business process | Date (created) | Date (settled) | Merchant | Account | Status |
|---|---|---|---|---|---|
| Settlement transaction (`fact_settlement`) | x | x | x | x | x |
| Merchant activity to date (`cum_merchant_activity`) | x | | x (by `merchant_id`) | | |

`settled_date_key` is the same `dim_date` playing a second role. Currency stays on the fact as a degenerate attribute because it has no attributes of its own.

## Which dimension keeps history
**`dim_merchant`, with SCD Type 2.** A merchant's category or risk tier changes over time, and a payment should be reported with the values that were true when it happened (for example, volume by risk tier at the time).

- A change closes the old row (`valid_to` = change date, `is_current` = false) and opens a new one. A row is valid for `valid_from <= day < valid_to`.
- `fact_settlement.merchant_key` points at the version valid on the transaction's created date, so old facts keep their old attributes.
- Unchanged profiles add no rows, and replaying the same feed changes nothing.
- A merchant's first row has placeholder attributes (`Unknown`) from 1900-01-01 until a profile arrives, so no fact is orphaned.
- Limit: a snapshot dated on or before the current version's start date is ignored (no back-dated corrections).

`dim_account` stays Type 1: nobody has asked what an account looked like in the past, so extra rows would add cost and no value.

## Cumulative table
`cum_merchant_activity(as_of_date, merchant_id)` is built one day at a time as **previous day's row + that day's facts**.
- A rebuild deletes that day's rows and recomputes them from the previous day and the facts, so rebuilding gives identical rows.
- Merchants with no activity on a day carry their totals forward; a merchant has no rows before its first transaction.
- It is keyed on `merchant_id`, not `merchant_key`, so activity adds up across every history version.
- A day can only be built if the previous day exists. If a day's facts change, rebuild from that day forward (`build_cumulative_from`).

## Design choice: why a star schema
- **The questions are "how much, how many, by what".** Totals by day, merchant, status or account are one fact table joined to small dimensions, with no long chains of joins.
- **The fact table is append-only and has a natural grain.** Each transaction is one row, so the load is a safe `INSERT ... ON CONFLICT DO NOTHING` and reruns add nothing (matches milestone 1).
- **Dimensions are small, readable and shared.** Analysts filter by merchant category or weekday without knowing the source layout, and a new fact (for example refunds) reuses `dim_date` and `dim_merchant`.
- **History is a dimension-level decision.** SCD2 on one dimension gives point-in-time reporting without storing history in the facts.
- **Why not a normalised (3NF) model:** it is better for writing transactions, but every report would need more joins, and this data is read far more than it is changed.
- **Why not one wide table:** merchant attributes would repeat on every row, and a profile change would mean rewriting millions of rows instead of adding one.
- **Trade-off:** duplicated attributes across versions and a more careful load (dimensions before facts). That is worth it for fast, simple reads.

## Files
- Tables: [`models/schema.sql`](../models/schema.sql)
- Build code: [`src/warehouse.py`](../src/warehouse.py) (`python -m src.warehouse`)
- Tests: [`tests/test_warehouse.py`](../tests/test_warehouse.py)
