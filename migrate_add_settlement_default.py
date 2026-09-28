"""ONE-TIME migration: add communities.settlement_default.

Additive only (IF NOT EXISTS - safe to re-run), backs the new "default settlement method
for a cashed-out player" setting on the PayBox setup screen (see schema.sql for the full
column comment). Run once against production with the venv that has psycopg2 +
python-dotenv installed:

    python migrate_add_settlement_default.py
"""
import os

import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DATABASE_URL = os.environ["DATABASE_URL"]


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE communities "
                "ADD COLUMN IF NOT EXISTS settlement_default TEXT NOT NULL DEFAULT 'direct'"
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM communities")
            total = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM communities WHERE settlement_default = 'direct'")
            defaulted = cur.fetchone()[0]
        print(f"OK - settlement_default column present. {defaulted}/{total} communities at the 'direct' default.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
