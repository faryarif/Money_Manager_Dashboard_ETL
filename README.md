# Money Manager Dashboard ETL

ETL pipeline for Money Manager `.mmbak` backups -> Supabase -> Streamlit.

## What it does

- Reads the native Money Manager SQLite backup.
- Normalizes accounts, categories, currencies, transactions, tags, and recurring transactions.
- Uses Money Manager UUIDs as stable keys.
- Upserts instead of blindly appending.
- Detects an identical backup by SHA-256 and skips it.
- Keeps deleted source rows for auditability, while dashboard views hide them.
- Flags future/recurring transactions as projected.
- Separates income, expense, transfer, and balance/difference adjustments.
- Keeps raw source records in JSONB for troubleshooting.

## 1. Create the Supabase schema

Run `schema.sql` once in the Supabase SQL Editor.

The ETL is designed to use a server-side Supabase service-role key from Streamlit secrets.
Do not expose that key in browser-side JavaScript or client-side code.

## 2. Configure Streamlit secrets

`.streamlit/secrets.toml`:

```toml
SUPABASE_URL = "https://YOUR_PROJECT.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "YOUR_SERVER_SIDE_SERVICE_ROLE_KEY"
```

Do not commit this file to GitHub.

## 3. Install

```bash
pip install -r requirements.txt
```

## 4. CLI import

```bash
python mm_etl.py backup.mmbak
```

## 5. Streamlit upload

Copy `streamlit_component.py` and `mm_etl.py` into the dashboard repository, then:

```python
from streamlit_component import render_mmbak_uploader

render_mmbak_uploader()
```

Upload the next `.mmbak`, click Import, and the ETL will update Supabase.
The dashboard can then query `v_mm_cashflow`, `v_mm_transfers`, `v_mm_adjustments`,
and `v_mm_account_ledger`.

## Important financial treatment

Money Manager uses paired rows for transfers (`3` / `4`). These are not income or
expense and are excluded from `v_mm_cashflow`.

Types `7` and `8` are retained as adjustments rather than silently treating them
as normal income/expense. This matters for balance differences and investment
valuation changes.

Future transactions from recurring transactions are retained but flagged
`is_projected = true` and excluded from the actual cash-flow view.

## Next dashboard layer

The intended dashboard can now be built entirely against the normalized Supabase
tables/views instead of the Money Manager SQLite structure.
