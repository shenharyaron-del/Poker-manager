"""One-off migration: shared_state JSON blob -> relational tables (schema.sql).

Phase 1 of the migration plan (.claude/plans/virtual-cuddling-wreath.md):
this script is DRY-RUN ONLY. It never touches the real `shared_state` row
or the public schema - it inserts into a disposable `migration_test`
schema, then reconstructs the original JSON shape from those tables and
diffs it against the real blob, to prove the transformation is faithful
before anything is migrated for real in a later phase.

Run with the venv that has psycopg2 + python-dotenv installed:
    .venv\\Scripts\\python.exe migrate_to_relational.py
"""
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_SQL_PATH = BASE_DIR / "schema.sql"
SCRATCH_SCHEMA = "migration_test"
DATABASE_URL = os.environ["DATABASE_URL"]


def ts_to_ms(dt):
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)


def ms_to_ts(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def normalize(x):
    """Recursively round Decimal/float so DB round-trips compare equal to
    the original JSON numbers regardless of numeric type."""
    if isinstance(x, dict):
        return {k: normalize(v) for k, v in x.items()}
    if isinstance(x, list):
        return [normalize(v) for v in x]
    if isinstance(x, Decimal):
        return round(float(x), 6)
    if isinstance(x, float):
        return round(x, 6)
    return x


def sort_by(lst, key):
    return sorted(lst, key=lambda d: (d.get(key) is None, d.get(key)))


# ---------- Load the real blob (read-only) ----------

def load_shared_state(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM shared_state WHERE id = 1")
        row = cur.fetchone()
    if not row:
        raise SystemExit("No shared_state row found (id=1) - nothing to migrate.")
    return json.loads(row[0])


# ---------- Scratch schema setup ----------

def reset_scratch_schema(conn):
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCRATCH_SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {SCRATCH_SCHEMA}")
        cur.execute(f"SET search_path TO {SCRATCH_SCHEMA}")
        cur.execute(SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
    conn.commit()


# ---------- Fold legacy single paybox link (same rule as the client, see
# index.html's openPayboxSettings handler) ----------

def resolve_paybox_links(c):
    paybox_links = c.get("payboxLinks") or []
    active_id = c.get("activePayboxLinkId")
    if not paybox_links and c.get("payboxLink"):
        legacy_id = f"legacy-{c['id']}"
        paybox_links = [{"id": legacy_id, "name": "פייבוקס", "link": c["payboxLink"]}]
        active_id = legacy_id
    return paybox_links, active_id


# ---------- Insert ----------

def insert_data(conn, state):
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {SCRATCH_SCHEMA}")

        for c in state.get("communities", []):
            paybox_links, active_paybox_link_id = resolve_paybox_links(c)

            cur.execute(
                "INSERT INTO communities (id, name, created_by, created_by_name, chip_ratio, active_paybox_link_id, last_participants) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (c["id"], c.get("name"), c.get("createdBy"), c.get("createdByName"),
                 c.get("chipRatio"), active_paybox_link_id, Json(c.get("lastParticipants")) if c.get("lastParticipants") is not None else None),
            )

            for i, cv in enumerate(c.get("chipValues") or []):
                cur.execute(
                    "INSERT INTO chip_values (id, community_id, image, value, sort_order) VALUES (%s,%s,%s,%s,%s)",
                    (cv["id"], c["id"], cv.get("image"), cv.get("value"), i),
                )

            for pl in paybox_links:
                cur.execute(
                    "INSERT INTO paybox_links (id, community_id, name, link) VALUES (%s,%s,%s,%s)",
                    (pl["id"], c["id"], pl.get("name"), pl.get("link")),
                )

            for p in c.get("roster") or []:
                cur.execute(
                    "INSERT INTO roster_players (id, community_id, name, client_id) VALUES (%s,%s,%s,%s)",
                    (p["id"], c["id"], p.get("name"), p.get("clientId")),
                )

        for client_id, gp in (state.get("globalPlayers") or {}).items():
            cur.execute(
                "INSERT INTO global_players (client_id, photo, phone, seat_position) VALUES (%s,%s,%s,%s)",
                (client_id, gp.get("photo"), gp.get("phone"), gp.get("seatPosition")),
            )

        for g in state.get("games", []):
            cur.execute(
                "INSERT INTO games (id, community_id, name, date, created_by_name, closed, seats, live_chips) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (g["id"], g.get("communityId"), g.get("name"), g.get("date"), g.get("createdByName"),
                 bool(g.get("closed", False)), Json(g.get("seats") or []), Json(g.get("liveChips") or {})),
            )

            for p in g.get("players") or []:
                cur.execute(
                    "INSERT INTO game_players (game_id, player_id, name) VALUES (%s,%s,%s) "
                    "ON CONFLICT (game_id, player_id) DO NOTHING",
                    (g["id"], p["id"], p.get("name")),
                )

            for b in g.get("buyins") or []:
                cur.execute(
                    "INSERT INTO buyins (id, game_id, player_id, amount, ts) VALUES (%s,%s,%s,%s,%s)",
                    (b["id"], g["id"], b.get("playerId"), b.get("amount"), ms_to_ts(b.get("ts"))),
                )

            for player_id, co in (g.get("cashouts") or {}).items():
                cur.execute(
                    "INSERT INTO cashouts (game_id, player_id, amount, ts, arranged_with) VALUES (%s,%s,%s,%s,%s)",
                    (g["id"], player_id, co.get("amount"), ms_to_ts(co.get("ts")), Json(co.get("arrangedWith") or [])),
                )

            for pp in g.get("payboxPayments") or []:
                cur.execute(
                    "INSERT INTO paybox_payments (id, game_id, player_id, amount, ts) VALUES (%s,%s,%s,%s,%s)",
                    (pp["id"], g["id"], pp.get("playerId"), pp.get("amount"), ms_to_ts(pp.get("ts"))),
                )

            for pp in g.get("playerPayments") or []:
                cur.execute(
                    "INSERT INTO player_payments (id, game_id, from_player_id, to_player_id, amount, ts) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (pp["id"], g["id"], pp.get("fromPlayerId"), pp.get("toPlayerId"), pp.get("amount"), ms_to_ts(pp.get("ts"))),
                )

    conn.commit()


def row_counts(conn):
    tables = ["communities", "chip_values", "paybox_links", "roster_players", "global_players",
              "games", "game_players", "buyins", "cashouts", "paybox_payments", "player_payments"]
    counts = {}
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {SCRATCH_SCHEMA}")
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
    return counts


# ---------- Reconstruct the original JSON shape from the scratch tables ----------

def reconstruct_state(conn):
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {SCRATCH_SCHEMA}")

        cur.execute("SELECT id, name, created_by, created_by_name, chip_ratio, active_paybox_link_id, last_participants FROM communities")
        communities = []
        for cid, name, created_by, created_by_name, chip_ratio, active_id, last_participants in cur.fetchall():
            cur.execute("SELECT id, image, value FROM chip_values WHERE community_id=%s ORDER BY sort_order", (cid,))
            chip_values = [{"id": r[0], "image": r[1], "value": r[2]} for r in cur.fetchall()]

            cur.execute("SELECT id, name, link FROM paybox_links WHERE community_id=%s", (cid,))
            paybox_links = [{"id": r[0], "name": r[1], "link": r[2]} for r in cur.fetchall()]

            cur.execute("SELECT id, name, client_id FROM roster_players WHERE community_id=%s", (cid,))
            roster = []
            for pid, pname, client_id in cur.fetchall():
                entry = {"id": pid, "name": pname}
                if client_id:
                    entry["clientId"] = client_id
                roster.append(entry)

            communities.append({
                "id": cid, "name": name, "createdBy": created_by, "createdByName": created_by_name,
                "chipRatio": chip_ratio, "activePayboxLinkId": active_id,
                "chipValues": chip_values, "payboxLinks": paybox_links, "roster": roster,
                "lastParticipants": last_participants,
            })

        cur.execute("SELECT client_id, photo, phone, seat_position FROM global_players")
        global_players = {}
        for client_id, photo, phone, seat_position in cur.fetchall():
            entry = {}
            if photo is not None:
                entry["photo"] = photo
            if phone is not None:
                entry["phone"] = phone
            if seat_position is not None:
                entry["seatPosition"] = seat_position
            global_players[client_id] = entry

        cur.execute("SELECT id, community_id, name, date, created_by_name, closed, seats, live_chips FROM games")
        games = []
        for gid, community_id, name, date, created_by_name, closed, seats, live_chips in cur.fetchall():
            cur.execute("SELECT player_id, name FROM game_players WHERE game_id=%s", (gid,))
            players = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]

            cur.execute("SELECT id, player_id, amount, ts FROM buyins WHERE game_id=%s", (gid,))
            buyins = [{"id": r[0], "playerId": r[1], "amount": r[2], "ts": ts_to_ms(r[3])} for r in cur.fetchall()]

            cur.execute("SELECT player_id, amount, ts, arranged_with FROM cashouts WHERE game_id=%s", (gid,))
            cashouts = {}
            for player_id, amount, ts, arranged_with in cur.fetchall():
                cashouts[player_id] = {"amount": amount, "ts": ts_to_ms(ts), "arrangedWith": arranged_with or []}

            cur.execute("SELECT id, player_id, amount, ts FROM paybox_payments WHERE game_id=%s", (gid,))
            paybox_payments = [{"id": r[0], "playerId": r[1], "amount": r[2], "ts": ts_to_ms(r[3])} for r in cur.fetchall()]

            cur.execute("SELECT id, from_player_id, to_player_id, amount, ts FROM player_payments WHERE game_id=%s", (gid,))
            player_payments = [{"id": r[0], "fromPlayerId": r[1], "toPlayerId": r[2], "amount": r[3], "ts": ts_to_ms(r[4])}
                                for r in cur.fetchall()]

            games.append({
                "id": gid, "communityId": community_id, "name": name,
                "date": date.isoformat() if date else None,
                "createdByName": created_by_name, "closed": closed,
                "seats": seats, "liveChips": live_chips,
                "players": players, "buyins": buyins, "cashouts": cashouts,
                "payboxPayments": paybox_payments, "playerPayments": player_payments,
            })

    return {"communities": communities, "games": games, "globalPlayers": global_players}


# ---------- Project the original blob down to exactly what we migrate,
# applying the same legacy-paybox-fold rule the migration itself applies ----------

def normalize_original(state):
    communities = []
    for c in state.get("communities", []):
        paybox_links, active_paybox_link_id = resolve_paybox_links(c)
        roster = []
        for p in c.get("roster") or []:
            entry = {"id": p["id"], "name": p.get("name")}
            if p.get("clientId"):
                entry["clientId"] = p["clientId"]
            roster.append(entry)
        communities.append({
            "id": c["id"], "name": c.get("name"), "createdBy": c.get("createdBy"),
            "createdByName": c.get("createdByName"), "chipRatio": c.get("chipRatio"),
            "activePayboxLinkId": active_paybox_link_id,
            "chipValues": [{"id": cv["id"], "image": cv.get("image"), "value": cv.get("value")}
                           for cv in (c.get("chipValues") or [])],
            "payboxLinks": [{"id": pl["id"], "name": pl.get("name"), "link": pl.get("link")} for pl in paybox_links],
            "roster": roster,
            "lastParticipants": c.get("lastParticipants"),
        })

    global_players = {}
    for client_id, gp in (state.get("globalPlayers") or {}).items():
        entry = {}
        if gp.get("photo") is not None:
            entry["photo"] = gp["photo"]
        if gp.get("phone") is not None:
            entry["phone"] = gp["phone"]
        if gp.get("seatPosition") is not None:
            entry["seatPosition"] = gp["seatPosition"]
        global_players[client_id] = entry

    games = []
    for g in state.get("games", []):
        cashouts = {}
        for player_id, co in (g.get("cashouts") or {}).items():
            cashouts[player_id] = {"amount": co.get("amount"), "ts": co.get("ts"), "arrangedWith": co.get("arrangedWith") or []}
        games.append({
            "id": g["id"], "communityId": g.get("communityId"), "name": g.get("name"),
            "date": g.get("date"), "createdByName": g.get("createdByName"),
            "closed": bool(g.get("closed", False)),
            "seats": g.get("seats") or [], "liveChips": g.get("liveChips") or {},
            "players": [{"id": p["id"], "name": p.get("name")} for p in (g.get("players") or [])],
            "buyins": [{"id": b["id"], "playerId": b.get("playerId"), "amount": b.get("amount"), "ts": b.get("ts")}
                       for b in (g.get("buyins") or [])],
            "cashouts": cashouts,
            "payboxPayments": [{"id": pp["id"], "playerId": pp.get("playerId"), "amount": pp.get("amount"), "ts": pp.get("ts")}
                                for pp in (g.get("payboxPayments") or [])],
            "playerPayments": [{"id": pp["id"], "fromPlayerId": pp.get("fromPlayerId"), "toPlayerId": pp.get("toPlayerId"),
                                 "amount": pp.get("amount"), "ts": pp.get("ts")}
                                for pp in (g.get("playerPayments") or [])],
        })

    return {"communities": communities, "games": games, "globalPlayers": global_players}


# ---------- Order-independent comparison ----------

def canonicalize(state):
    state = normalize(state)
    for c in state["communities"]:
        c["chipValues"] = sort_by(c["chipValues"], "id")
        c["payboxLinks"] = sort_by(c["payboxLinks"], "id")
        c["roster"] = sort_by(c["roster"], "id")
    state["communities"] = sort_by(state["communities"], "id")
    for g in state["games"]:
        g["players"] = sort_by(g["players"], "id")
        g["buyins"] = sort_by(g["buyins"], "id")
        g["payboxPayments"] = sort_by(g["payboxPayments"], "id")
        g["playerPayments"] = sort_by(g["playerPayments"], "id")
    state["games"] = sort_by(state["games"], "id")
    return state


def diff_report(original, reconstructed):
    a, b = canonicalize(original), canonicalize(reconstructed)
    # Dict/list equality (not a JSON string compare) - int 25 and float 25.0
    # are the same value, but json.dumps would render them differently and
    # produce a false-positive mismatch since NUMERIC columns come back as
    # Decimal and get normalized to float while the original JSON may have
    # stored a plain int.
    if a == b:
        return None

    # Narrow down to which top-level entity actually differs, for a useful report.
    lines = []
    a_communities = {c["id"]: c for c in a["communities"]}
    b_communities = {c["id"]: c for c in b["communities"]}
    for cid in sorted(set(a_communities) | set(b_communities)):
        if a_communities.get(cid) != b_communities.get(cid):
            lines.append(f"community {cid} differs:")
            lines.append(f"  original:      {json.dumps(a_communities.get(cid), ensure_ascii=False, sort_keys=True)}")
            lines.append(f"  reconstructed: {json.dumps(b_communities.get(cid), ensure_ascii=False, sort_keys=True)}")

    a_games = {g["id"]: g for g in a["games"]}
    b_games = {g["id"]: g for g in b["games"]}
    for gid in sorted(set(a_games) | set(b_games)):
        if a_games.get(gid) != b_games.get(gid):
            lines.append(f"game {gid} differs:")
            lines.append(f"  original:      {json.dumps(a_games.get(gid), ensure_ascii=False, sort_keys=True)}")
            lines.append(f"  reconstructed: {json.dumps(b_games.get(gid), ensure_ascii=False, sort_keys=True)}")

    if a["globalPlayers"] != b["globalPlayers"]:
        lines.append("globalPlayers differs:")
        lines.append(f"  original:      {json.dumps(a['globalPlayers'], ensure_ascii=False, sort_keys=True)}")
        lines.append(f"  reconstructed: {json.dumps(b['globalPlayers'], ensure_ascii=False, sort_keys=True)}")

    return "\n".join(lines) if lines else "(diff detected but no single entity pinpointed - check list lengths / stray fields)"


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        print("Loading shared_state (read-only)...")
        state = load_shared_state(conn)
        print(f"  {len(state.get('communities', []))} communities, {len(state.get('games', []))} games, "
              f"{len(state.get('globalPlayers', {}))} global players in source blob.")

        print(f"Resetting scratch schema '{SCRATCH_SCHEMA}' and applying schema.sql...")
        reset_scratch_schema(conn)

        print("Inserting into scratch schema...")
        insert_data(conn, state)

        print("Row counts in scratch schema:")
        for table, count in row_counts(conn).items():
            print(f"  {table}: {count}")

        print("Reconstructing original shape from scratch schema and diffing...")
        reconstructed = reconstruct_state(conn)
        original_projection = normalize_original(state)
        diff = diff_report(original_projection, reconstructed)

        if diff is None:
            print("\nPASS - reconstructed data matches the original blob exactly (within migrated fields).")
        else:
            print("\nFAIL - mismatch found:\n")
            print(diff)
            sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
