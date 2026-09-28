"""ONE-TIME real production migration - Phase 7 cutover.

Applies schema.sql to the real `public` schema (additive only - never drops or resets
anything, never touches shared_state or any existing table) and copies the current
shared_state blob into the new tables. Verifies the migration the same way Phase 1's
dry-run did (reconstruct the new tables back into the original shape and diff against
the source blob) before declaring success.

Hard-aborts if the new tables already exist in public, rather than risk duplicating or
corrupting data on a second run. Do not re-run this against production after a PASS.
"""
import sys

import psycopg2

import migrate_to_relational as m


def apply_schema_to_public(conn):
    with conn.cursor() as cur:
        cur.execute("SET search_path TO public")
        cur.execute(m.SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
    conn.commit()


def main():
    conn = psycopg2.connect(m.DATABASE_URL)
    try:
        print("Loading shared_state (read-only)...")
        state = m.load_shared_state(conn)
        print(f"  {len(state.get('communities', []))} communities, {len(state.get('games', []))} games, "
              f"{len(state.get('globalPlayers', {}))} global players in source blob.")

        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.communities')")
            if cur.fetchone()[0] is not None:
                print("ABORT: public.communities already exists - this script has already been run.")
                print("Refusing to run again against production to avoid duplicating/corrupting data.")
                sys.exit(1)

        print("Applying schema.sql to the real public schema (additive only)...")
        apply_schema_to_public(conn)

        print("Inserting real data into public...")
        m.SCRATCH_SCHEMA = "public"  # insert_data/reconstruct_state/row_counts all read this at call time
        m.insert_data(conn, state)

        print("Row counts in public (new tables only):")
        for table, count in m.row_counts(conn).items():
            print(f"  {table}: {count}")

        print("Reconstructing from public and diffing against the source blob...")
        reconstructed = m.reconstruct_state(conn)
        original_projection = m.normalize_original(state)
        diff = m.diff_report(original_projection, reconstructed)

        if diff is None:
            print("\nPASS - production migration verified correct.")
        else:
            print("\nFAIL - mismatch found (nothing pre-existing was touched, only new tables were added "
                  "- safe to investigate before deciding whether to proceed):\n")
            print(diff)
            sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
