"""
Money Manager .mmbak -> Supabase ETL

Usage from Streamlit:
    from mm_etl import import_mmbak
    result = import_mmbak(uploaded_file)

Usage from CLI:
    python mm_etl.py backup.mmbak

Environment variables:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY

The service-role key must stay server-side (e.g. Streamlit secrets).
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from supabase import create_client


BATCH_SIZE = 500
CORE_DATA_EPOCH = datetime(2001, 1, 1)

TYPE_NAMES = {
    0: "income",
    1: "expense",
    3: "transfer_out",
    4: "transfer_in",
    7: "balance_adjustment",
    8: "difference_adjustment",
}


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _json_row(row: pd.Series) -> dict:
    result = {}
    for key, value in row.items():
        value = _clean(value)
        result[key] = value
    return result


def _coredata_datetime(value: Any) -> datetime | None:
    """Money Manager stores ZDATE as seconds from Apple's/Core Data's 2001 epoch."""
    if value is None or pd.isna(value):
        return None
    try:
        return CORE_DATA_EPOCH + timedelta(seconds=float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _date_from_text(value: Any) -> date | None:
    if value is None or pd.isna(value) or not str(value).strip():
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _source_uid(row: pd.Series, uid_col: str, pk_col: str = "Z_PK") -> str:
    uid = row.get(uid_col)
    if uid is not None and not pd.isna(uid) and str(uid).strip():
        return str(uid)
    return f"pk:{int(row[pk_col])}"


def _read_sqlite(source: bytes):
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".mmbak", delete=False)
    try:
        tmp.write(source)
        tmp.close()
        con = sqlite3.connect(tmp.name)
        con.row_factory = sqlite3.Row
        return con, tmp.name
    except Exception:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        raise


def _close_sqlite(con: sqlite3.Connection, path: str) -> None:
    con.close()
    try:
        os.unlink(path)
    except OSError:
        pass


def _table(con: sqlite3.Connection, name: str) -> pd.DataFrame:
    return pd.read_sql_query(f'SELECT * FROM "{name}"', con)


def _records(df: pd.DataFrame) -> list[dict]:
    return [{k: _clean(v) for k, v in row.items()} for row in df.to_dict("records")]


def _upsert_in_chunks(client, table: str, rows: list[dict], on_conflict: str) -> None:
    if not rows:
        return
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i+BATCH_SIZE]
        client.table(table).upsert(chunk, on_conflict=on_conflict).execute()


def _build_accounts(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        uid = _source_uid(r, "ZUID")
        rows.append({
            "source_uid": uid,
            "source_pk": int(r["Z_PK"]) if _clean(r["Z_PK"]) is not None else None,
            "name": str(r.get("ZNICNAME") or r.get("ZCARD_ACCOUNT_NAME") or uid),
            "nickname": _clean(r.get("ZMEMOTITLE")),
            "account_type": int(r["ZTYPE"]) if _clean(r.get("ZTYPE")) is not None else None,
            "group_id": int(r["ZGROUP_ID"]) if _clean(r.get("ZGROUP_ID")) is not None else None,
            "group_uid": _clean(r.get("ZGROUPUID")),
            "currency_uid": _clean(r.get("ZCURRENCYUID")),
            "is_deleted": bool(r.get("ZISDEL") == 1),
            "is_reflect": bool(r.get("ZISREFLECT") == 1),
            "raw": _json_row(r),
        })
    return rows


def _build_categories(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        uid = _source_uid(r, "ZUID")
        rows.append({
            "source_uid": uid,
            "source_pk": int(r["Z_PK"]) if _clean(r["Z_PK"]) is not None else None,
            "name": str(r.get("ZNAME") or uid),
            "parent_uid": _clean(r.get("ZPUID")),
            "category_type": int(r["ZDOTYPE"]) if _clean(r.get("ZDOTYPE")) is not None else None,
            "is_deleted": bool(r.get("ZISDEL") == 1),
            "raw": _json_row(r),
        })
    return rows


def _build_currencies(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        uid = _source_uid(r, "ZUID")
        rows.append({
            "source_uid": uid,
            "source_pk": int(r["Z_PK"]) if _clean(r["Z_PK"]) is not None else None,
            "iso": _clean(r.get("ZISO")),
            "main_iso": _clean(r.get("ZMAINISO")),
            "symbol": _clean(r.get("ZSYMBOL")),
            "rate": _clean(r.get("ZRATE")),
            "is_main": bool(r.get("ZISMAINCURRENCY") == 1),
            "is_deleted": bool(r.get("ZISDEL") == 1),
            "raw": _json_row(r),
        })
    return rows


def _build_transactions(df: pd.DataFrame, today: date) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        uid = _source_uid(r, "ZUID")
        dt = _coredata_datetime(r.get("ZDATE"))
        tx_date = _date_from_text(r.get("ZTXDATESTR")) or (dt.date() if dt else None)

        typ_raw = r.get("ZDO_TYPE")
        try:
            typ = int(typ_raw)
        except (TypeError, ValueError):
            typ = -1

        rows.append({
            "source_uid": uid,
            "source_pk": int(r["Z_PK"]) if _clean(r["Z_PK"]) is not None else None,
            "transaction_date": tx_date.isoformat() if tx_date else None,
            "transaction_datetime": dt.isoformat() if dt else None,
            "amount": _clean(r.get("ZAMOUNT")) or 0,
            "amount_account": _clean(r.get("ZAMOUNTACCOUNT")),
            "amount_sub": _clean(r.get("ZAMOUNTSUB")),
            "account_uid": _clean(r.get("ZASSETUID")),
            "category_uid": _clean(r.get("ZCATEGORYUID")),
            "currency_uid": _clean(r.get("ZCURRENCYUID")),
            "to_account_uid": _clean(r.get("ZTOASSETUID")),
            "opposite_account_id": int(r["ZOPPOSITEAID"]) if _clean(r.get("ZOPPOSITEAID")) is not None else None,
            "transaction_type": typ,
            "transaction_type_name": TYPE_NAMES.get(typ, "unknown"),
            "transfer_uid": _clean(r.get("ZTXUIDTRANS")),
            "content": _clean(r.get("ZCONTENT")),
            "memo": _clean(r.get("ZMEMO")),
            "paid": _clean(r.get("ZPAID")),
            "mark": _clean(r.get("ZMARK")),
            "is_deleted": bool(r.get("ZISDEL") == 1),
            "is_projected": bool(tx_date and tx_date > today),
            "source_date_text": _clean(r.get("ZTXDATESTR")),
            "raw": _json_row(r),
        })
    return rows


def _build_tags(tx_tag_df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in tx_tag_df.iterrows():
        uid = _source_uid(r, "ZUID")
        rows.append({
            "source_uid": uid,
            "transaction_uid": _clean(r.get("ZTXUID")) or f"pk:{int(r['ZTX'])}",
            "tag_uid": _clean(r.get("ZTAGUID")),
            "source_pk": int(r["Z_PK"]) if _clean(r["Z_PK"]) is not None else None,
            "raw": _json_row(r),
        })
    return rows


def _build_recurring(df: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        uid = _source_uid(r, "ZUID")
        def epoch_date(v):
            if v is None or pd.isna(v) or float(v) == 0:
                return None
            try:
                return (CORE_DATA_EPOCH + timedelta(seconds=float(v))).date().isoformat()
            except Exception:
                return None

        rows.append({
            "source_uid": uid,
            "source_pk": int(r["Z_PK"]) if _clean(r["Z_PK"]) is not None else None,
            "account_uid": _clean(r.get("ZASSETUID")),
            "to_account_uid": _clean(r.get("ZTOASSETUID")),
            "category_uid": _clean(r.get("ZCATEGORYUID")),
            "currency_uid": _clean(r.get("ZCURRENCYUID")),
            "transaction_type": int(r["ZDOTYPE"]) if _clean(r.get("ZDOTYPE")) is not None else None,
            "amount": _clean(r.get("ZAMOUNT_SUB")) if _clean(r.get("ZAMOUNT_SUB")) is not None else _clean(r.get("ZAMOUNTSUB")),
            "next_date": epoch_date(r.get("ZNEXTDATE")),
            "end_date": epoch_date(r.get("ZENDDATE")),
            "repeat_type": int(r["ZREPEATTYPE"]) if _clean(r.get("ZREPEATTYPE")) is not None else None,
            "memo": _clean(r.get("ZMEMO")),
            "payee": _clean(r.get("ZPAYEE")),
            "is_deleted": bool(r.get("ZISDEL") == 1),
            "raw": _json_row(r),
        })
    return rows


def import_mmbak(file_or_bytes, file_name: str | None = None) -> dict:
    """
    Import one Money Manager .mmbak into Supabase.

    The function is safe to run repeatedly:
    - identical backup file: skipped by SHA-256
    - newer backup: upserts by Money Manager UUID
    - deleted source rows remain in Supabase but are hidden from dashboard views
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. "
            "Set them in Streamlit secrets or environment variables."
        )

    if hasattr(file_or_bytes, "getvalue"):
        source = file_or_bytes.getvalue()
        file_name = file_name or getattr(file_or_bytes, "name", "upload.mmbak")
    elif isinstance(file_or_bytes, (bytes, bytearray)):
        source = bytes(file_or_bytes)
        file_name = file_name or "upload.mmbak"
    else:
        path = Path(file_or_bytes)
        source = path.read_bytes()
        file_name = file_name or path.name

    file_hash = hashlib.sha256(source).hexdigest()
    client = create_client(url, key)

    # Same exact backup => no work and no duplicates.
    existing = (
        client.table("mm_import_batches")
        .select("id,imported_at,source_transaction_count")
        .eq("file_sha256", file_hash)
        .limit(1)
        .execute()
    )
    if existing.data:
        row = existing.data[0]
        return {
            "status": "skipped",
            "reason": "identical_backup_already_imported",
            "batch_id": row["id"],
            "transactions": row.get("source_transaction_count", 0),
        }

    con, temp_path = _read_sqlite(source)
    try:
        assets = _table(con, "ZASSET")
        categories = _table(con, "ZCATEGORY")
        currencies = _table(con, "ZCURRENCY")
        transactions = _table(con, "ZINOUTCOME")
        tx_tags = _table(con, "ZTXTAG") if "ZTXTAG" in [r[0] for r in con.execute("select name from sqlite_master where type='table'")] else pd.DataFrame()
        recurring = _table(con, "ZREPEATTRANSACTION") if "ZREPEATTRANSACTION" in [r[0] for r in con.execute("select name from sqlite_master where type='table'")] else pd.DataFrame()

        account_rows = _build_accounts(assets)
        category_rows = _build_categories(categories)
        currency_rows = _build_currencies(currencies)
        transaction_rows = _build_transactions(transactions, date.today())
        tag_rows = _build_tags(tx_tags) if not tx_tags.empty else []
        recurring_rows = _build_recurring(recurring) if not recurring.empty else []

        # Dimension tables first because transactions reference them.
        _upsert_in_chunks(client, "mm_currencies", currency_rows, "source_uid")
        _upsert_in_chunks(client, "mm_accounts", account_rows, "source_uid")
        _upsert_in_chunks(client, "mm_categories", category_rows, "source_uid")

        batch = (
            client.table("mm_import_batches")
            .insert({
                "file_name": file_name,
                "file_sha256": file_hash,
                "source_transaction_count": len(transaction_rows),
                "source_account_count": len(account_rows),
                "source_category_count": len(category_rows),
                "source_tag_count": len(tag_rows),
            })
            .select("id")
            .execute()
        )
        batch_id = batch.data[0]["id"]

        for row in transaction_rows:
            row["import_batch_id"] = batch_id

        _upsert_in_chunks(client, "mm_transactions", transaction_rows, "source_uid")

        # Remove/reinsert only the tag links present in this backup would be
        # more complex because deleted tag links can disappear. Since tags are
        # small, upsert is sufficient for normal incremental backups.
        _upsert_in_chunks(client, "mm_transaction_tags", tag_rows, "source_uid")
        _upsert_in_chunks(client, "mm_recurring_transactions", recurring_rows, "source_uid")

        return {
            "status": "imported",
            "batch_id": batch_id,
            "file_name": file_name,
            "sha256": file_hash,
            "transactions": len(transaction_rows),
            "accounts": len(account_rows),
            "categories": len(category_rows),
            "tags": len(tag_rows),
            "recurring": len(recurring_rows),
            "active_transactions": int(
                ((transactions["ZISDEL"].fillna(0) == 0)).sum()
            ),
            "projected_transactions": int(
                sum(r["is_projected"] and not r["is_deleted"] for r in transaction_rows)
            ),
        }
    finally:
        _close_sqlite(con, temp_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import Money Manager .mmbak into Supabase")
    parser.add_argument("file", help="Path to .mmbak file")
    args = parser.parse_args()

    result = import_mmbak(args.file)
    print(json.dumps(result, indent=2, default=str))
