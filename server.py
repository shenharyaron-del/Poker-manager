import base64
import hashlib
import json
import os
import queue
import re
import secrets
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import anthropic
import psycopg2
import psycopg2.pool
from psycopg2.extras import Json
from flask import Flask, Response, jsonify, request, send_from_directory

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATABASE_URL = os.environ["DATABASE_URL"]
# Unset (the default, real production) means every connection uses Postgres's own default
# search_path (`public`) - unaffected. Set (a staging/local deployment only) prepends this
# schema, so the relational-DB migration's v2 tables resolve there while everything else
# in this file keeps working exactly the same way, just against an isolated copy of the
# data instead of the live one. See RELATIONAL_API_DEFAULT below and .claude/plans.
DB_SCHEMA = os.environ.get("DB_SCHEMA")
DATA_URL_RE = re.compile(r"^data:(image/[a-zA-Z0-9.+-]+);base64,(.+)$", re.DOTALL)
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# Render sets this automatically to the deployed commit's SHA - lets Settings show
# exactly which version is live, with no separate manual version number to keep in sync.
APP_VERSION = os.environ.get("RENDER_GIT_COMMIT", "local")[:7]
# Per-deployment, not per-database - deliberately read from the environment, not the
# shared app_settings table (which every deployment pointed at this DB would see alike).
# Lets a second Render service (or a local run) serve the exact same index.html with the
# relational-DB migration's client code turned on, for testing, without the real
# production deployment ever picking it up. See .claude/plans and USE_RELATIONAL_API in
# index.html.
RELATIONAL_API_DEFAULT = os.environ.get("RELATIONAL_API_DEFAULT", "false").lower() == "true"
# The one account that's a super admin from the moment it's ever created, with no other
# admin needed to grant it - every other account's role defaults to plain "user".
SUPER_ADMIN_EMAIL = os.environ.get("SUPER_ADMIN_EMAIL", "").strip().lower()
BREVO_API_KEY = os.environ.get("BREVO_API_KEY")
LOGIN_CODE_TTL_MINUTES = 10

app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB, enough for a chip photo

# ---------- Push instant "state changed" notifications over SSE ----------
# One Queue per connected browser tab; save_state() drops a message in every queue
# right after it commits, so every other open tab refetches within the same second
# instead of waiting for its next poll. Single-process assumption (Render runs one
# instance of this app) - fine for a small poker group, would need a real pub/sub
# (Redis etc.) if this ever ran across multiple instances.
_subscribers = []
_subscribers_lock = threading.Lock()


def _broadcast_state_changed():
    with _subscribers_lock:
        subs = list(_subscribers)
    for q in subs:
        try:
            q.put_nowait("changed")
        except queue.Full:
            pass


_INIT_STATEMENTS = (
    "CREATE TABLE IF NOT EXISTS shared_state ("
    "  id INTEGER PRIMARY KEY CHECK (id = 1),"
    "  data TEXT NOT NULL"
    ")",
    "CREATE TABLE IF NOT EXISTS identities ("
    "  client_id TEXT PRIMARY KEY,"
    "  data TEXT NOT NULL"
    ")",
    # id is a content hash (see upload_photo) - uploading the same bytes twice is a
    # harmless no-op instead of storing a duplicate copy.
    "CREATE TABLE IF NOT EXISTS photos ("
    "  id TEXT PRIMARY KEY,"
    "  data TEXT NOT NULL"
    ")",
    # A person's real identity, keyed by email instead of a per-device id - name/photo/
    # phone/role/myPlayers live here so they follow the person across devices, not just
    # the one they first set them up on.
    "CREATE TABLE IF NOT EXISTS accounts ("
    "  email TEXT PRIMARY KEY,"
    "  data TEXT NOT NULL"
    ")",
    # Which device (clientId) is allowed to act as which account without re-entering a
    # login code - written once, right after that device verifies a code for that email.
    "CREATE TABLE IF NOT EXISTS trusted_devices ("
    "  client_id TEXT PRIMARY KEY,"
    "  email TEXT NOT NULL"
    ")",
    # One outstanding login code per email at a time - requesting a new one replaces
    # whatever was there before, and a used or expired code is deleted outright.
    "CREATE TABLE IF NOT EXISTS login_codes ("
    "  email TEXT PRIMARY KEY,"
    "  code TEXT NOT NULL,"
    "  expires_at TIMESTAMP NOT NULL"
    ")",
    # Small key/value store for global admin toggles (see skip_login_verification,
    # require_verify_before_admin_settings) - deliberately separate from the client's
    # shared_state blob, which the server otherwise never has to parse.
    "CREATE TABLE IF NOT EXISTS app_settings ("
    "  key TEXT PRIMARY KEY,"
    "  value TEXT NOT NULL"
    ")",
)


def _ensure_schema(conn):
    with conn.cursor() as cur:
        # CREATE TABLE IF NOT EXISTS isn't fully race-safe in Postgres - two connections
        # can both see "doesn't exist yet" and both try to create it (only happens once,
        # the first time a table is ever needed, under concurrent requests), and the
        # loser gets a duplicate-key error on the system catalog instead of silently
        # doing nothing. That's harmless (the table exists either way) but would
        # otherwise surface as a real 500 to whoever's request lost the race.
        for statement in _INIT_STATEMENTS:
            try:
                cur.execute(statement)
            except psycopg2.errors.DuplicateTable:
                conn.rollback()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
    conn.commit()


# A small pool of connections instead of one shared connection serialized behind a
# single lock. One shared connection meant every request - reads and writes, from
# completely different players - had to wait in line behind every other in-flight
# request, even though Postgres itself already handles real concurrent access safely
# (save_state's atomic UPSERT correctly resolves conflicting writes to the same row on
# its own). That self-imposed serialization is what turned into multi-second waits to
# add a player or buy in when several phones were active near-simultaneously at the
# table. Separate pooled connections remove that artificial bottleneck.
_MIN_POOL_CONNS = 3
_MAX_POOL_CONNS = 10
_db_pool = None
_db_pool_lock = threading.Lock()


def _get_pool():
    global _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            connect_kwargs = {"options": f"-c search_path={DB_SCHEMA},public"} if DB_SCHEMA else {}
            pool = psycopg2.pool.ThreadedConnectionPool(_MIN_POOL_CONNS, _MAX_POOL_CONNS, DATABASE_URL, **connect_kwargs)
            conn = pool.getconn()
            try:
                _ensure_schema(conn)
            finally:
                pool.putconn(conn)
            _db_pool = pool
    return _db_pool


@contextmanager
def get_db():
    pool = _get_pool()
    conn = pool.getconn()
    close_bad = False
    try:
        yield conn
    except psycopg2.Error:
        # This connection is in a bad/unknown state (e.g. an idle timeout on Supabase's
        # side) - close it instead of returning it to the pool, so the pool opens a
        # fresh one next time it's needed. Only this one request degrades, not the app.
        close_bad = True
        raise
    finally:
        pool.putconn(conn, close=close_bad)


# In-memory mirror of the `shared_state` row plus its hash. GET /api/state and GET
# /api/state/hash are polled constantly (every refresh tick, every syncBeforeMutate call
# before a table action) but the data itself rarely changes between polls - serving both
# straight from here means those reads never touch Postgres at all, only save_state()
# does. Valid only because Render runs a single instance of this process; a multi-instance
# deployment would need a shared cache (Redis etc.) instead, since each instance would
# otherwise carry its own out-of-sync copy.
_state_cache = None
_state_hash_cache = None
_state_cache_lock = threading.Lock()


def _default_state_json():
    return json.dumps({"communities": [], "games": []})


def _load_state_from_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM shared_state WHERE id = 1")
            row = cur.fetchone()
        conn.commit()
    return row[0] if row else _default_state_json()


def _get_cached_state():
    global _state_cache, _state_hash_cache
    with _state_cache_lock:
        if _state_cache is not None:
            return _state_cache, _state_hash_cache
    # Cache miss (first request since this process started) - fill it from the DB. Two
    # requests racing here at startup both hit the DB once each, harmlessly.
    data = _load_state_from_db()
    state_hash = hashlib.md5(data.encode("utf-8")).hexdigest()
    with _state_cache_lock:
        _state_cache = data
        _state_hash_cache = state_hash
    return data, state_hash


def _set_cached_state(data):
    global _state_cache, _state_hash_cache
    with _state_cache_lock:
        _state_cache = data
        _state_hash_cache = hashlib.md5(data.encode("utf-8")).hexdigest()


@app.route("/")
def index():
    # Always revalidate with the server before using a cached copy, so a deploy is picked
    # up on the next reload instead of a phone browser silently reusing old JS indefinitely.
    # Flask still sets ETag/Last-Modified, so an unchanged file comes back as a cheap 304.
    response = send_from_directory(STATIC_DIR, "index.html")
    response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response


@app.route("/static/<path:filename>")
def static_files(filename):
    response = send_from_directory(STATIC_DIR, filename)
    response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response


@app.route("/sw.js")
def service_worker():
    # Served from the root path (not /static/) so its default scope covers the whole
    # origin - a service worker registered from a subpath can only control pages under
    # that same subpath.
    response = send_from_directory(STATIC_DIR, "sw.js")
    response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response


@app.route("/api/version")
def version():
    return jsonify({"version": APP_VERSION, "relationalApiEnabled": RELATIONAL_API_DEFAULT})


@app.route("/api/state", methods=["GET"])
def get_state():
    data, _ = _get_cached_state()
    return data, 200, {"Content-Type": "application/json"}


@app.route("/api/state/hash")
def get_state_hash():
    # The client polls /api/state every few seconds and on every SSE "changed" event, but
    # the state (including every player's photo, base64-encoded) usually hasn't actually
    # changed between polls - resending the full multi-hundred-KB blob every time burns
    # through bandwidth fast with several phones open over a whole poker night.
    #
    # The natural fix is a conditional GET (ETag + 304), but Render's edge (Cloudflare)
    # silently strips a custom ETag response header before it reaches the browser, so
    # that never actually worked - the client could never learn what the last ETag was.
    # This tiny endpoint sidesteps that: the client fetches just this hash first (a
    # plain, tiny JSON body - confirmed to pass through untouched, unlike the header) and
    # only fetches the full /api/state when the hash has actually changed.
    _, state_hash = _get_cached_state()
    return jsonify({"hash": state_hash})


@app.route("/api/state", methods=["POST"])
def save_state():
    data = request.get_data(as_text=True)
    json.loads(data)  # reject anything that isn't valid JSON before storing it
    # Optional optimistic-concurrency check: the client sends the hash of the state it
    # built this save on top of. If the shared state has moved on since then (another
    # device saved first), reject instead of silently overwriting whatever they just
    # changed - the client re-syncs, reapplies its own change on the new base, and
    # retries. Callers that don't pass this skip the check entirely (unchanged behavior).
    base_hash = request.args.get("baseHash")
    with get_db() as conn:
        with conn.cursor() as cur:
            if base_hash:
                # Fold the check into the write itself as a single atomic statement
                # instead of a separate SELECT then WRITE - Postgres's own row lock
                # makes this check-and-write atomic on its own, one round trip instead
                # of two. rowcount is 0 only when a row already existed and its hash
                # didn't match (a real conflict) - a first-ever insert always proceeds.
                cur.execute(
                    "INSERT INTO shared_state (id, data) VALUES (1, %s) "
                    "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data "
                    "WHERE md5(shared_state.data) = %s",
                    (data, base_hash),
                )
                if cur.rowcount == 0:
                    conn.commit()
                    return jsonify({"conflict": True}), 409
            else:
                cur.execute(
                    "INSERT INTO shared_state (id, data) VALUES (1, %s) "
                    "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
                    (data,),
                )
        conn.commit()
    _set_cached_state(data)
    _broadcast_state_changed()
    # Hand back the hash of what was just saved so the client can use it as next save's
    # baseHash without an extra round trip to re-fetch it (see mutateAndSave). Computed
    # locally from `data` rather than read back from the shared cache, since a concurrent
    # request could update that cache in between and hand back the wrong hash.
    new_hash = hashlib.md5(data.encode("utf-8")).hexdigest()
    return jsonify({"ok": True, "hash": new_hash})


@app.route("/api/events")
def events():
    def stream():
        q = queue.Queue(maxsize=10)
        with _subscribers_lock:
            _subscribers.append(q)
        try:
            yield ": connected\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield ": keep-alive\n\n"  # keeps the connection from idling out
        finally:
            with _subscribers_lock:
                if q in _subscribers:
                    _subscribers.remove(q)

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------- Phase 2 of the relational-DB migration: new read-only endpoints ----------
# Query the tables from schema.sql (see migrate_to_relational.py and
# .claude/plans/virtual-cuddling-wreath.md). Nothing in the app calls these yet - they sit
# dormant alongside the existing /api/state blob until a later phase wires index.html to
# them. Kept under /api/v2/ so the eventual cutover is just switching which prefix the
# client calls, with the old endpoints untouched until then.
#
# The real `public` schema doesn't have these tables yet (that only happens at the final
# production migration, Phase 7) - these routes will 500 until then, which is fine since
# nothing reachable from the UI calls them.


def _num(x):
    # NUMERIC columns come back from psycopg2 as Decimal, which Flask's JSON encoder
    # silently renders as a JSON string ("0.25") instead of a number - matches neither
    # the original blob's plain numbers nor what client-side arithmetic expects.
    return float(x) if x is not None else None


def _row_to_community(row):
    (cid, name, created_by, created_by_name, chip_ratio, active_paybox_link_id, updated_at, last_participants) = row
    return {
        "id": cid, "name": name, "createdBy": created_by, "createdByName": created_by_name,
        "chipRatio": _num(chip_ratio), "activePayboxLinkId": active_paybox_link_id,
        "updatedAt": updated_at.isoformat() if updated_at else None,
        "lastParticipants": last_participants,
    }


@app.route("/api/v2/communities", methods=["GET"])
def v2_list_communities():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, created_by, created_by_name, chip_ratio, active_paybox_link_id, updated_at, last_participants "
                "FROM communities ORDER BY name"
            )
            communities = [_row_to_community(r) for r in cur.fetchall()]

            for c in communities:
                cur.execute(
                    "SELECT id, image, value FROM chip_values WHERE community_id = %s ORDER BY sort_order",
                    (c["id"],),
                )
                c["chipValues"] = [{"id": r[0], "image": r[1], "value": _num(r[2])} for r in cur.fetchall()]

                cur.execute(
                    "SELECT id, name, link FROM paybox_links WHERE community_id = %s",
                    (c["id"],),
                )
                c["payboxLinks"] = [{"id": r[0], "name": r[1], "link": r[2]} for r in cur.fetchall()]

                cur.execute(
                    "SELECT id, name, client_id FROM roster_players WHERE community_id = %s",
                    (c["id"],),
                )
                roster = []
                for pid, pname, client_id in cur.fetchall():
                    entry = {"id": pid, "name": pname}
                    if client_id:
                        entry["clientId"] = client_id
                    roster.append(entry)
                c["roster"] = roster
        conn.commit()
    return jsonify(communities)


@app.route("/api/v2/communities/<community_id>/games", methods=["GET"])
def v2_list_games(community_id):
    # Summaries only (no buyins/cashouts) - matches the games-list view, which never
    # needs a game's full detail, only enough to render one row per game.
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, community_id, name, date, created_by_name, closed, updated_at "
                "FROM games WHERE community_id = %s ORDER BY date DESC",
                (community_id,),
            )
            games = [_row_to_game_summary(r) for r in cur.fetchall()]
        conn.commit()
    return jsonify(games)


@app.route("/api/v2/games/<game_id>", methods=["GET"])
def v2_get_game(game_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, community_id, name, date, created_by_name, closed, seats, live_chips, updated_at "
                "FROM games WHERE id = %s",
                (game_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            gid, community_id, name, date, created_by_name, closed, seats, live_chips, updated_at = row

            cur.execute("SELECT player_id, name FROM game_players WHERE game_id = %s", (gid,))
            players = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]

            cur.execute("SELECT id, player_id, amount, ts FROM buyins WHERE game_id = %s ORDER BY ts", (gid,))
            buyins = [
                {"id": r[0], "playerId": r[1], "amount": _num(r[2]), "ts": int(r[3].timestamp() * 1000) if r[3] else None}
                for r in cur.fetchall()
            ]

            cur.execute("SELECT player_id, amount, ts, arranged_with FROM cashouts WHERE game_id = %s", (gid,))
            cashouts = {}
            for player_id, amount, ts, arranged_with in cur.fetchall():
                cashouts[player_id] = {
                    "amount": _num(amount),
                    "ts": int(ts.timestamp() * 1000) if ts else None,
                    "arrangedWith": arranged_with or [],
                }

            cur.execute(
                "SELECT id, player_id, amount, ts FROM paybox_payments WHERE game_id = %s ORDER BY ts", (gid,)
            )
            paybox_payments = [
                {"id": r[0], "playerId": r[1], "amount": _num(r[2]), "ts": int(r[3].timestamp() * 1000) if r[3] else None}
                for r in cur.fetchall()
            ]

            cur.execute(
                "SELECT id, from_player_id, to_player_id, amount, ts FROM player_payments "
                "WHERE game_id = %s ORDER BY ts",
                (gid,),
            )
            player_payments = [
                {
                    "id": r[0], "fromPlayerId": r[1], "toPlayerId": r[2], "amount": _num(r[3]),
                    "ts": int(r[4].timestamp() * 1000) if r[4] else None,
                }
                for r in cur.fetchall()
            ]
        conn.commit()

    return jsonify({
        "id": gid, "communityId": community_id, "name": name,
        "date": date.isoformat() if date else None,
        "createdByName": created_by_name, "closed": closed,
        "updatedAt": updated_at.isoformat() if updated_at else None,
        "seats": seats, "liveChips": live_chips,
        "players": players, "buyins": buyins, "cashouts": cashouts,
        "payboxPayments": paybox_payments, "playerPayments": player_payments,
    })


@app.route("/api/v2/games/<game_id>/outcomes", methods=["GET"])
def v2_game_outcomes(game_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT player_id, outcome FROM player_outcomes WHERE game_id = %s",
                (game_id,),
            )
            outcomes = [{"playerId": r[0], "outcome": _num(r[1])} for r in cur.fetchall()]
        conn.commit()
    return jsonify(outcomes)


@app.route("/api/v2/communities/<community_id>/outcomes", methods=["GET"])
def v2_community_outcomes(community_id):
    # Per-player lifetime net across the whole community - the SQL equivalent of the
    # client's playerNetProfit(), computed by the DB instead of looping over every game's
    # JSON in JS.
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT player_id, SUM(outcome) AS net, COUNT(*) AS games_played "
                "FROM player_outcomes WHERE community_id = %s GROUP BY player_id",
                (community_id,),
            )
            totals = [{"playerId": r[0], "net": _num(r[1]), "gamesPlayed": r[2]} for r in cur.fetchall()]
        conn.commit()
    return jsonify(totals)


# ---------- Phase 3: write endpoints for the community/roster feature area ----------
# Each write is scoped to a single community/row instead of the whole app-state blob, so
# unrelated concurrent edits (a different community, a different game) never contend.
# Structural changes (INSERT a new roster row, DELETE one) don't need a conflict token -
# two people adding different players, or one delete racing another, both resolve fine on
# their own. Field edits on the community row itself (name/chipRatio/chipValues/
# activePayboxLinkId) DO use one - the client echoes back the `updatedAt` it last saw
# (from a GET), and a mismatch (someone else edited first) returns 409 instead of quietly
# overwriting their change. This replaces mutateAndSave's whole-blob hash retry loop with
# a plain single-row optimistic-concurrency check, backed by a real DB constraint instead
# of hand-rolled JS retry logic.


@app.route("/api/v2/communities", methods=["POST"])
def v2_create_community():
    body = request.get_json(silent=True) or {}
    cid, name = body.get("id"), body.get("name")
    if not cid or not name:
        return jsonify({"error": "id and name required"}), 400
    created_by, created_by_name = body.get("createdBy"), body.get("createdByName")
    creator_player_id, creator_player_name = body.get("creatorPlayerId"), body.get("creatorPlayerName")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO communities (id, name, created_by, created_by_name) VALUES (%s,%s,%s,%s)",
                (cid, name, created_by, created_by_name),
            )
            if creator_player_id:
                cur.execute(
                    "INSERT INTO roster_players (id, community_id, name, client_id) VALUES (%s,%s,%s,%s)",
                    (creator_player_id, cid, creator_player_name, created_by),
                )
        conn.commit()
    return jsonify({"id": cid}), 201


@app.route("/api/v2/communities/<community_id>", methods=["PATCH"])
def v2_update_community(community_id):
    body = request.get_json(silent=True) or {}
    updated_at = body.get("updatedAt")
    if not updated_at:
        return jsonify({"error": "updatedAt required"}), 400
    fields, params = [], []
    if "name" in body:
        fields.append("name = %s"); params.append(body["name"])
    if "chipRatio" in body:
        fields.append("chip_ratio = %s"); params.append(body["chipRatio"])
    if "activePayboxLinkId" in body:
        fields.append("active_paybox_link_id = %s"); params.append(body["activePayboxLinkId"])
    if "lastParticipants" in body:
        fields.append("last_participants = %s"); params.append(Json(body["lastParticipants"]))
    if not fields:
        return jsonify({"error": "no fields to update"}), 400
    fields.append("updated_at = now()")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE communities SET {', '.join(fields)} "
                "WHERE id = %s AND updated_at = %s::timestamptz RETURNING updated_at",
                (*params, community_id, updated_at),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return jsonify({"conflict": True}), 409
        conn.commit()
    return jsonify({"ok": True, "updatedAt": row[0].isoformat()})


@app.route("/api/v2/communities/<community_id>", methods=["DELETE"])
def v2_delete_community(community_id):
    # ON DELETE CASCADE on every community_id/game_id FK takes care of chip_values,
    # paybox_links, roster_players, games and everything under those games in one
    # statement - see schema.sql.
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM communities WHERE id = %s", (community_id,))
            deleted = cur.rowcount
        conn.commit()
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/v2/communities/<community_id>/duplicate", methods=["POST"])
def v2_duplicate_community(community_id):
    body = request.get_json(silent=True) or {}
    new_id, new_name = body.get("id"), body.get("name")
    if not new_id or not new_name:
        return jsonify({"error": "id and name required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT created_by, created_by_name, chip_ratio FROM communities WHERE id = %s",
                (community_id,),
            )
            src = cur.fetchone()
            if not src:
                return jsonify({"error": "source community not found"}), 404
            created_by, created_by_name, chip_ratio = src
            cur.execute(
                "INSERT INTO communities (id, name, created_by, created_by_name, chip_ratio) "
                "VALUES (%s,%s,%s,%s,%s)",
                (new_id, new_name, created_by, created_by_name, chip_ratio),
            )

            cur.execute(
                "SELECT id, image, value, sort_order FROM chip_values WHERE community_id = %s", (community_id,)
            )
            for cv_id, image, value, sort_order in cur.fetchall():
                cur.execute(
                    "INSERT INTO chip_values (id, community_id, image, value, sort_order) VALUES (%s,%s,%s,%s,%s)",
                    (secrets.token_hex(4), new_id, image, value, sort_order),
                )

            cur.execute("SELECT id, name, link FROM paybox_links WHERE community_id = %s", (community_id,))
            paybox_id_map = {}
            for pl_id, pl_name, link in cur.fetchall():
                new_pl_id = secrets.token_hex(4)
                paybox_id_map[pl_id] = new_pl_id
                cur.execute(
                    "INSERT INTO paybox_links (id, community_id, name, link) VALUES (%s,%s,%s,%s)",
                    (new_pl_id, new_id, pl_name, link),
                )

            cur.execute("SELECT active_paybox_link_id FROM communities WHERE id = %s", (community_id,))
            old_active = cur.fetchone()[0]
            if old_active and old_active in paybox_id_map:
                cur.execute(
                    "UPDATE communities SET active_paybox_link_id = %s WHERE id = %s",
                    (paybox_id_map[old_active], new_id),
                )

            cur.execute("SELECT id, name, client_id FROM roster_players WHERE community_id = %s", (community_id,))
            for p_id, p_name, client_id in cur.fetchall():
                cur.execute(
                    "INSERT INTO roster_players (id, community_id, name, client_id) VALUES (%s,%s,%s,%s)",
                    (secrets.token_hex(4), new_id, p_name, client_id),
                )
        conn.commit()
    return jsonify({"id": new_id}), 201


@app.route("/api/v2/communities/<community_id>/roster", methods=["POST"])
def v2_add_roster_player(community_id):
    # Unconditional insert, no duplicate-name check - matches the client's current
    # data-add-roster behavior exactly (confirmed during Phase 3 research).
    body = request.get_json(silent=True) or {}
    player_id, name = body.get("id"), body.get("name")
    if not player_id or not name:
        return jsonify({"error": "id and name required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO roster_players (id, community_id, name) VALUES (%s,%s,%s)",
                (player_id, community_id, name),
            )
        conn.commit()
    return jsonify({"id": player_id, "name": name}), 201


@app.route("/api/v2/communities/<community_id>/roster/<player_id>", methods=["PATCH"])
def v2_update_roster_player(community_id, player_id):
    # Added during Phase 7: linking an existing roster row to an account (clientId) after
    # the fact - the backfill path in autoSeatSelf/autoJoinViaInvite for a player who
    # self-identified before global-player linking existed. Structural (single row,
    # single field), so no conflict token needed, same reasoning as the rest of Phase 3.
    body = request.get_json(silent=True) or {}
    if "clientId" not in body:
        return jsonify({"error": "clientId required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE roster_players SET client_id = %s WHERE id = %s AND community_id = %s",
                (body["clientId"], player_id, community_id),
            )
            updated = cur.rowcount
        conn.commit()
    if not updated:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/v2/communities/<community_id>/roster/<player_id>", methods=["DELETE"])
def v2_remove_roster_player(community_id, player_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT client_id FROM roster_players WHERE id = %s AND community_id = %s",
                (player_id, community_id),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            client_id = row[0]
            cur.execute("DELETE FROM roster_players WHERE id = %s AND community_id = %s", (player_id, community_id))
            if client_id:
                # Same orphan-cleanup the client does today: only drop global_players once
                # no roster entry anywhere still points at this account.
                cur.execute(
                    "DELETE FROM global_players WHERE client_id = %s "
                    "AND NOT EXISTS (SELECT 1 FROM roster_players WHERE client_id = %s)",
                    (client_id, client_id),
                )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/v2/players/<client_id>/name", methods=["PATCH"])
def v2_rename_player_everywhere(client_id):
    # Cross-community by design - a logged-in player's roster entries in every community
    # they've joined share one clientId and should all show the same name.
    body = request.get_json(silent=True) or {}
    name = body.get("name")
    if not name:
        return jsonify({"error": "name required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE roster_players SET name = %s WHERE client_id = %s", (name, client_id))
            updated = cur.rowcount
        conn.commit()
    return jsonify({"ok": True, "updated": updated})


@app.route("/api/v2/communities/<community_id>/chip-values", methods=["PUT"])
def v2_set_chip_values(community_id):
    # Full replace, not incremental - matches the client's chipSetup save exactly (it
    # always sends the complete list, never a delta).
    body = request.get_json(silent=True) or {}
    values, chip_ratio, updated_at = body.get("chipValues"), body.get("chipRatio"), body.get("updatedAt")
    if values is None or not updated_at:
        return jsonify({"error": "chipValues and updatedAt required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE communities SET chip_ratio = %s, updated_at = now() "
                "WHERE id = %s AND updated_at = %s::timestamptz RETURNING updated_at",
                (chip_ratio, community_id, updated_at),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return jsonify({"conflict": True}), 409
            cur.execute("DELETE FROM chip_values WHERE community_id = %s", (community_id,))
            for i, cv in enumerate(values):
                cur.execute(
                    "INSERT INTO chip_values (id, community_id, image, value, sort_order) VALUES (%s,%s,%s,%s,%s)",
                    (cv["id"], community_id, cv.get("image"), cv.get("value"), i),
                )
        conn.commit()
    return jsonify({"ok": True, "updatedAt": row[0].isoformat()})


@app.route("/api/v2/communities/<community_id>/paybox-links", methods=["POST"])
def v2_add_paybox_link(community_id):
    body = request.get_json(silent=True) or {}
    link_id, name, link = body.get("id"), body.get("name"), body.get("link")
    if not link_id or not link:
        return jsonify({"error": "id and link required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO paybox_links (id, community_id, name, link) VALUES (%s,%s,%s,%s)",
                (link_id, community_id, name, link),
            )
            # First link added becomes the active one automatically, same as the client.
            cur.execute(
                "UPDATE communities SET active_paybox_link_id = %s "
                "WHERE id = %s AND active_paybox_link_id IS NULL",
                (link_id, community_id),
            )
        conn.commit()
    return jsonify({"id": link_id}), 201


@app.route("/api/v2/communities/<community_id>/paybox-links", methods=["PUT"])
def v2_edit_paybox_links(community_id):
    # Bulk in-place edit of name/link on existing entries - matches data-save-paybox-entries.
    body = request.get_json(silent=True) or {}
    links = body.get("links")
    if links is None:
        return jsonify({"error": "links required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            for pl in links:
                cur.execute(
                    "UPDATE paybox_links SET name = %s, link = %s WHERE id = %s AND community_id = %s",
                    (pl.get("name"), pl.get("link"), pl["id"], community_id),
                )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/v2/communities/<community_id>/paybox-links/<link_id>", methods=["DELETE"])
def v2_delete_paybox_link(community_id, link_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM paybox_links WHERE id = %s AND community_id = %s", (link_id, community_id))
            deleted = cur.rowcount
            cur.execute("SELECT active_paybox_link_id FROM communities WHERE id = %s", (community_id,))
            row = cur.fetchone()
            if row and row[0] == link_id:
                cur.execute("SELECT id FROM paybox_links WHERE community_id = %s LIMIT 1", (community_id,))
                fallback = cur.fetchone()
                cur.execute(
                    "UPDATE communities SET active_paybox_link_id = %s WHERE id = %s",
                    (fallback[0] if fallback else None, community_id),
                )
        conn.commit()
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


# ---------- Phase 4: write endpoints for the seating/buy-in feature area ----------
# Buy-ins/cashouts/game_players are separate rows, so structural writes (add, delete-by-id)
# need no conflict token, same reasoning as Phase 3's roster endpoints.
#
# Seats are different: `games.seats` is one JSONB array column, and two players seating
# themselves in the SAME game at the SAME time is exactly the scenario that caused the
# real invite-join lost-update race fixed earlier this app's life (one save's seats array
# silently overwriting the other's). Rather than a read-modify-write + updated_at token
# (which would just rebuild that same race at smaller scope), every seat mutation below is
# a single atomic UPDATE using jsonb_set against the CURRENT row under Postgres's own row
# lock - "claim this seat only if it's still empty" is one statement with a WHERE guard, so
# two concurrent claims of the same seat resolve to exactly one winner with no app-level
# retry loop at all.


def _row_to_game_summary(row):
    return {
        "id": row[0], "communityId": row[1], "name": row[2],
        "date": row[3].isoformat() if row[3] else None,
        "createdByName": row[4], "closed": row[5],
        "updatedAt": row[6].isoformat() if row[6] else None,
    }


@app.route("/api/v2/games", methods=["POST"])
def v2_create_game():
    body = request.get_json(silent=True) or {}
    gid, community_id = body.get("id"), body.get("communityId")
    if not gid or not community_id:
        return jsonify({"error": "id and communityId required"}), 400
    name, date, created_by_name = body.get("name"), body.get("date"), body.get("createdByName")
    creator_player_id, creator_player_name = body.get("creatorPlayerId"), body.get("creatorPlayerName")
    with get_db() as conn:
        with conn.cursor() as cur:
            seats = [None] * 9
            if creator_player_id:
                seats[0] = creator_player_id
            cur.execute(
                "INSERT INTO games (id, community_id, name, date, created_by_name, seats, live_chips) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (gid, community_id, name, date, created_by_name, Json(seats),
                 Json({creator_player_id: 0} if creator_player_id else {})),
            )
            if creator_player_id:
                cur.execute(
                    "INSERT INTO game_players (game_id, player_id, name) VALUES (%s,%s,%s) "
                    "ON CONFLICT (game_id, player_id) DO NOTHING",
                    (gid, creator_player_id, creator_player_name),
                )
        conn.commit()
    return jsonify({"id": gid}), 201


@app.route("/api/v2/games/<game_id>", methods=["PATCH"])
def v2_update_game(game_id):
    body = request.get_json(silent=True) or {}
    updated_at = body.get("updatedAt")
    if not updated_at:
        return jsonify({"error": "updatedAt required"}), 400
    fields, params = [], []
    if "name" in body:
        fields.append("name = %s"); params.append(body["name"])
    if "closed" in body:
        fields.append("closed = %s"); params.append(bool(body["closed"]))
    if not fields:
        return jsonify({"error": "no fields to update"}), 400
    fields.append("updated_at = now()")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE games SET {', '.join(fields)} "
                "WHERE id = %s AND updated_at = %s::timestamptz RETURNING updated_at",
                (*params, game_id, updated_at),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return jsonify({"conflict": True}), 409
        conn.commit()
    return jsonify({"ok": True, "updatedAt": row[0].isoformat()})


@app.route("/api/v2/games/<game_id>", methods=["DELETE"])
def v2_delete_game(game_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM games WHERE id = %s", (game_id,))
            deleted = cur.rowcount
        conn.commit()
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/players", methods=["POST"])
def v2_add_game_player(game_id):
    # The "everyone who's ever sat here" list - upsert, never removed just because a seat
    # gets cleared (only the full remove-from-game purge below drops someone from it).
    body = request.get_json(silent=True) or {}
    player_id, name = body.get("id"), body.get("name")
    if not player_id:
        return jsonify({"error": "id required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO game_players (game_id, player_id, name) VALUES (%s,%s,%s) "
                "ON CONFLICT (game_id, player_id) DO NOTHING",
                (game_id, player_id, name),
            )
        conn.commit()
    return jsonify({"ok": True}), 201


@app.route("/api/v2/games/<game_id>/seats/<int:seat_index>", methods=["POST"])
def v2_claim_seat(game_id, seat_index):
    # Atomic "sit here only if it's still empty" - see the section comment above. Also
    # upserts game_players and initializes live_chips for this player, in the same
    # transaction, matching seatPlayerIfRoom's bundled side effects client-side.
    body = request.get_json(silent=True) or {}
    player_id, name = body.get("playerId"), body.get("name")
    if not player_id:
        return jsonify({"error": "playerId required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            # `seats -> i` returns SQL NULL (not JSON null) for an out-of-range index, so
            # the "slot is empty" check alone rejects the append case (growing 9->10
            # seats) - the array_length branch below covers exactly that: the slot is the
            # very next position past the current end. Anything further out of range
            # falls through to the conflict branch rather than risking jsonb_set silently
            # appending at the wrong position.
            cur.execute(
                "UPDATE games SET seats = jsonb_set(seats, %s, %s::jsonb, true), updated_at = now() "
                "WHERE id = %s AND ("
                "  jsonb_array_length(seats) = %s "
                "  OR (jsonb_array_length(seats) > %s AND seats -> %s = 'null'::jsonb)"
                ") RETURNING seats",
                ([str(seat_index)], json.dumps(player_id), game_id, seat_index, seat_index, seat_index),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return jsonify({"conflict": True}), 409
            cur.execute(
                "INSERT INTO game_players (game_id, player_id, name) VALUES (%s,%s,%s) "
                "ON CONFLICT (game_id, player_id) DO NOTHING",
                (game_id, player_id, name),
            )
            cur.execute(
                "UPDATE games SET live_chips = live_chips || %s::jsonb "
                "WHERE id = %s AND NOT (live_chips ? %s)",
                (json.dumps({player_id: 0}), game_id, player_id),
            )
        conn.commit()
    return jsonify({"ok": True, "seats": row[0]})


@app.route("/api/v2/games/<game_id>/seats/<int:seat_index>", methods=["DELETE"])
def v2_clear_seat(game_id, seat_index):
    # Clearing a seat also drops that player's buy-ins in THIS game and their live_chips
    # entry, matching removeSeat/"clear seat" - but NOT game_players, which keeps their
    # history visible (see the removeFromGame purge below for the full-removal version).
    with get_db() as conn:
        with conn.cursor() as cur:
            # Read the OLD occupant before clearing - RETURNING on the same UPDATE would
            # read the value after it's already been set to null (confirmed by testing:
            # the cleared player's buy-ins and live_chips entry silently survived).
            cur.execute("SELECT seats -> %s FROM games WHERE id = %s", (seat_index, game_id))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            player_id = row[0]  # psycopg2 decodes the jsonb scalar directly - str or None
            cur.execute(
                "UPDATE games SET seats = jsonb_set(seats, %s, 'null'::jsonb), updated_at = now() "
                "WHERE id = %s",
                ([str(seat_index)], game_id),
            )
            if player_id:
                cur.execute("DELETE FROM buyins WHERE game_id = %s AND player_id = %s", (game_id, player_id))
                cur.execute(
                    "UPDATE games SET live_chips = live_chips - %s WHERE id = %s", (player_id, game_id)
                )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/seats/swap", methods=["PUT"])
def v2_swap_seats(game_id):
    # Single atomic UPDATE reading both old values off the same row image under the row
    # lock - no read-then-write round trip, so nothing to race against.
    body = request.get_json(silent=True) or {}
    from_index, to_index = body.get("fromIndex"), body.get("toIndex")
    if from_index is None or to_index is None:
        return jsonify({"error": "fromIndex and toIndex required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE games SET seats = jsonb_set(jsonb_set(seats, %s, seats -> %s), %s, seats -> %s), "
                "updated_at = now() WHERE id = %s RETURNING seats",
                ([str(to_index)], from_index, [str(from_index)], to_index, game_id),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
        conn.commit()
    return jsonify({"ok": True, "seats": row[0]})


@app.route("/api/v2/games/<game_id>/players/<player_id>", methods=["DELETE"])
def v2_remove_game_player(game_id, player_id):
    # Full purge - the removeFromGame flow: drops game_players, buy-ins, paybox payments,
    # the cashout, live_chips, and clears their seat, all in one transaction. Reopens a
    # closed game if it was closed only because everyone (including this player) had
    # cashed out - matches maybeAutoCloseNight's inverse.
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT seats, closed FROM games WHERE id = %s", (game_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            seats, closed = row

            cur.execute("DELETE FROM game_players WHERE game_id = %s AND player_id = %s", (game_id, player_id))
            cur.execute("DELETE FROM buyins WHERE game_id = %s AND player_id = %s", (game_id, player_id))
            cur.execute("DELETE FROM paybox_payments WHERE game_id = %s AND player_id = %s", (game_id, player_id))
            cur.execute("DELETE FROM cashouts WHERE game_id = %s AND player_id = %s", (game_id, player_id))
            cur.execute(
                "UPDATE games SET live_chips = live_chips - %s WHERE id = %s", (player_id, game_id)
            )
            if player_id in (seats or []):
                # Clear every matching seat, not just the first - defensive against a
                # player somehow ending up in more than one seat rather than leaving a
                # stray duplicate behind.
                new_seats = [None if s == player_id else s for s in seats]
                cur.execute("UPDATE games SET seats = %s WHERE id = %s", (Json(new_seats), game_id))
            if closed:
                cur.execute("UPDATE games SET closed = false, updated_at = now() WHERE id = %s", (game_id,))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/buyins", methods=["POST"])
def v2_add_buyin(game_id):
    body = request.get_json(silent=True) or {}
    buyin_id, player_id, amount = body.get("id"), body.get("playerId"), body.get("amount")
    if not buyin_id or not player_id or amount is None:
        return jsonify({"error": "id, playerId and amount required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO buyins (id, game_id, player_id, amount, ts) VALUES (%s,%s,%s,%s,now())",
                (buyin_id, game_id, player_id, amount),
            )
        conn.commit()
    return jsonify({"id": buyin_id}), 201


@app.route("/api/v2/games/<game_id>/buyins/bulk", methods=["POST"])
def v2_add_buyins_bulk(game_id):
    body = request.get_json(silent=True) or {}
    buyins = body.get("buyins")
    if not buyins:
        return jsonify({"error": "buyins required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            for b in buyins:
                cur.execute(
                    "INSERT INTO buyins (id, game_id, player_id, amount, ts) VALUES (%s,%s,%s,%s,now())",
                    (b["id"], game_id, b["playerId"], b["amount"]),
                )
        conn.commit()
    return jsonify({"ok": True, "count": len(buyins)}), 201


@app.route("/api/v2/games/<game_id>/buyins/<buyin_id>", methods=["DELETE"])
def v2_delete_buyin(game_id, buyin_id):
    # The only single-buy-in mutation the client has is delete - there's no edit-in-place
    # for a buy-in (only for a cashout, see PUT cashouts below); correcting one is always
    # delete-then-re-add on the client side.
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM buyins WHERE id = %s AND game_id = %s", (buyin_id, game_id))
            deleted = cur.rowcount
        conn.commit()
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/cashouts/<player_id>", methods=["PUT"])
def v2_set_cashout(game_id, player_id):
    # Upsert - covers both a fresh cash-out (quickEnd/leave) and the edit-pencil on an
    # already-cashed-out player's amount. `chips` is optional and, when given, updates
    # live_chips in the same transaction (editCashout/quickEnd/leave all do this together
    # client-side today).
    body = request.get_json(silent=True) or {}
    amount, arranged_with, chips = body.get("amount"), body.get("arrangedWith"), body.get("chips")
    if amount is None:
        return jsonify({"error": "amount required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cashouts (game_id, player_id, amount, ts, arranged_with) "
                "VALUES (%s,%s,%s,now(),%s) "
                "ON CONFLICT (game_id, player_id) DO UPDATE SET amount = EXCLUDED.amount, "
                "arranged_with = EXCLUDED.arranged_with",
                (game_id, player_id, amount, Json(arranged_with or [])),
            )
            if chips is not None:
                cur.execute(
                    "UPDATE games SET live_chips = live_chips || %s::jsonb WHERE id = %s",
                    (json.dumps({player_id: chips}), game_id),
                )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/cashouts/<player_id>", methods=["DELETE"])
def v2_delete_cashout(game_id, player_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM cashouts WHERE game_id = %s AND player_id = %s", (game_id, player_id))
            deleted = cur.rowcount
        conn.commit()
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/live-chips", methods=["PATCH"])
def v2_update_live_chips(game_id):
    body = request.get_json(silent=True) or {}
    player_id, chips = body.get("playerId"), body.get("chips")
    if not player_id or chips is None:
        return jsonify({"error": "playerId and chips required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE games SET live_chips = live_chips || %s::jsonb WHERE id = %s",
                (json.dumps({player_id: chips}), game_id),
            )
            updated = cur.rowcount
        conn.commit()
    if not updated:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


# ---------- Phase 5: write endpoints for the settlement/payments feature area ----------
# Smaller than Phases 3-4 by design - cashout upsert/delete and its optional
# arrangedWith already landed in Phase 4 (PUT /api/v2/games/:id/cashouts/:playerId
# already accepts an `arrangedWith` body field), so this phase only needed to add the two
# payment-record tables. All structural (add/delete by id), so no conflict token needed -
# same reasoning as buy-ins.
#
# One finding worth flagging rather than silently fixing: the client's own
# removeFromGame purges g.payboxPayments for the removed player but NOT g.playerPayments
# (confirmed via catalog - lines 3979-3998 filter payboxPayments only). Phase 4's
# v2_remove_game_player deliberately mirrors that exact (asymmetric) behavior rather than
# "fixing" it, since that's a pre-existing client bug independent of this migration, not
# something this phase should quietly change.


@app.route("/api/v2/games/<game_id>/paybox-payments", methods=["POST"])
def v2_add_paybox_payment(game_id):
    body = request.get_json(silent=True) or {}
    payment_id, player_id, amount = body.get("id"), body.get("playerId"), body.get("amount")
    if not payment_id or not player_id or amount is None:
        return jsonify({"error": "id, playerId and amount required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO paybox_payments (id, game_id, player_id, amount, ts) VALUES (%s,%s,%s,%s,now())",
                (payment_id, game_id, player_id, amount),
            )
        conn.commit()
    return jsonify({"id": payment_id}), 201


@app.route("/api/v2/games/<game_id>/paybox-payments/<payment_id>", methods=["DELETE"])
def v2_delete_paybox_payment(game_id, payment_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM paybox_payments WHERE id = %s AND game_id = %s", (payment_id, game_id)
            )
            deleted = cur.rowcount
        conn.commit()
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/player-payments", methods=["POST"])
def v2_add_player_payment(game_id):
    body = request.get_json(silent=True) or {}
    payment_id = body.get("id")
    from_player_id, to_player_id, amount = body.get("fromPlayerId"), body.get("toPlayerId"), body.get("amount")
    if not payment_id or not from_player_id or not to_player_id or amount is None:
        return jsonify({"error": "id, fromPlayerId, toPlayerId and amount required"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO player_payments (id, game_id, from_player_id, to_player_id, amount, ts) "
                "VALUES (%s,%s,%s,%s,%s,now())",
                (payment_id, game_id, from_player_id, to_player_id, amount),
            )
        conn.commit()
    return jsonify({"id": payment_id}), 201


@app.route("/api/v2/games/<game_id>/player-payments/<payment_id>", methods=["DELETE"])
def v2_delete_player_payment(game_id, payment_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM player_payments WHERE id = %s AND game_id = %s", (payment_id, game_id)
            )
            deleted = cur.rowcount
        conn.commit()
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/v2/games/<game_id>/payments/player/<player_id>", methods=["DELETE"])
def v2_reset_player_payments(game_id, player_id):
    # The "return to game after cashout, reset payment" flow - clears every paybox
    # payment FROM this player and every player-payment where they're either side
    # (fromPlayerId or toPlayerId), matching data-confirm-return-reset-payment exactly
    # (unlike removeFromGame, this one IS symmetric on the client - see the section
    # comment above for the asymmetric case).
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM paybox_payments WHERE game_id = %s AND player_id = %s", (game_id, player_id)
            )
            paybox_deleted = cur.rowcount
            cur.execute(
                "DELETE FROM player_payments WHERE game_id = %s AND (from_player_id = %s OR to_player_id = %s)",
                (game_id, player_id, player_id),
            )
            player_deleted = cur.rowcount
        conn.commit()
    return jsonify({"ok": True, "payboxDeleted": paybox_deleted, "playerPaymentsDeleted": player_deleted})


@app.route("/api/identity", methods=["GET"])
def get_identity():
    client_id = request.args.get("clientId", "")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM identities WHERE client_id = %s", (client_id,))
            row = cur.fetchone()
        conn.commit()
    if row:
        return row[0], 200, {"Content-Type": "application/json"}
    return jsonify(None)


@app.route("/api/identity", methods=["POST"])
def save_identity():
    client_id = request.args.get("clientId", "")
    data = request.get_data(as_text=True)
    json.loads(data)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO identities (client_id, data) VALUES (%s, %s) "
                "ON CONFLICT (client_id) DO UPDATE SET data = EXCLUDED.data",
                (client_id, data),
            )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/photos", methods=["POST"])
def upload_photo():
    # Photos (avatars, chip-count photos) used to live inline in shared_state's JSON
    # blob, so every one of them was re-sent on every single save/load of the whole
    # app - a buy-in that changes a few bytes of real data still had to transfer every
    # player's photo along with it. Storing them here instead, referenced by id, means
    # routine saves carry just that id (a short string) and browsers fetch/cache each
    # photo once via GET below instead of redownloading it on every poll.
    data_url = request.get_data(as_text=True)
    if not DATA_URL_RE.match(data_url):
        return jsonify({"error": "unsupported image data"}), 400
    # Content-hash id: uploading the same bytes twice (e.g. two players picking the same
    # built-in-style custom avatar) reuses the same row instead of storing it twice, and
    # makes this endpoint naturally idempotent - migrating existing photos can be re-run
    # safely if it's ever interrupted partway through.
    photo_id = hashlib.sha256(data_url.encode("utf-8")).hexdigest()[:24]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO photos (id, data) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                (photo_id, data_url),
            )
        conn.commit()
    return jsonify({"id": photo_id})


@app.route("/api/photos/<photo_id>")
def get_photo(photo_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM photos WHERE id = %s", (photo_id,))
            row = cur.fetchone()
        conn.commit()
    if not row:
        return jsonify({"error": "not found"}), 404
    match = DATA_URL_RE.match(row[0])
    if not match:
        return jsonify({"error": "corrupt photo data"}), 500
    media_type, b64data = match.group(1), match.group(2)
    response = Response(base64.b64decode(b64data), mimetype=media_type)
    # id is a content hash (see upload_photo) - the same id always means the same bytes,
    # so this is safe to cache forever; a changed photo gets a new id/URL rather than
    # ever overwriting this one.
    response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return response


def _get_account(email):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM accounts WHERE email = %s", (email,))
            row = cur.fetchone()
        conn.commit()
    return json.loads(row[0]) if row else None


def _default_account(email):
    return {
        "name": None,
        "photo": None,
        "phone": None,
        "role": "superAdmin" if email == SUPER_ADMIN_EMAIL else "user",
        "myPlayers": {},
        "playerId": None,
    }


def _claim_player_id(account, local_id):
    # The canonical player identity (roster/buy-in linking) shared by every device on
    # this account - before this, each device kept its own random local id, so logging
    # into the same account from a second device (or after localStorage got cleared)
    # silently split one person into two different roster players. Claimed once by
    # whichever device syncs first; every other device just adopts the same value.
    if not account.get("playerId") and local_id:
        account["playerId"] = local_id
    return account


def _save_account(email, account):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO accounts (email, data) VALUES (%s, %s) "
                "ON CONFLICT (email) DO UPDATE SET data = EXCLUDED.data",
                (email, json.dumps(account)),
            )
        conn.commit()


def _get_setting(key, default=False):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app_settings WHERE key = %s", (key,))
            row = cur.fetchone()
        conn.commit()
    return (row[0] == "true") if row else default


def _set_setting(key, value):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO app_settings (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, "true" if value else "false"),
            )
        conn.commit()


def _trusted_email(client_id):
    if not client_id:
        return None
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT email FROM trusted_devices WHERE client_id = %s", (client_id,))
            row = cur.fetchone()
        conn.commit()
    return row[0] if row else None


def _send_login_code_email(email, code):
    if not BREVO_API_KEY:
        raise RuntimeError("BREVO_API_KEY not configured")
    payload = json.dumps({
        # Must be a sender verified in the Brevo account - an unverified/made-up address
        # is silently never delivered (no bounce, no event logged) rather than rejected.
        "sender": {"name": "Poker Manager", "email": "Poker.Manager44@gmail.com"},
        "to": [{"email": email}],
        "subject": f"קוד הכניסה שלך: {code}",
        "htmlContent": (
            f"<div dir='rtl' style='font-family:sans-serif;font-size:16px;'>"
            f"קוד הכניסה שלך ל-Poker Manager:<br>"
            f"<b style='font-size:28px;letter-spacing:4px;'>{code}</b><br>"
            f"בתוקף ל-{LOGIN_CODE_TTL_MINUTES} דקות.</div>"
        ),
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.brevo.com/v3/smtp/email",
        data=payload,
        method="POST",
        headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


@app.route("/api/auth/request-code", methods=["POST"])
def request_login_code():
    body = request.get_json(silent=True) or {}
    email = str(body.get("email", "")).strip().lower()
    if not EMAIL_RE.match(email):
        return jsonify({"error": "invalid email"}), 400
    code = f"{secrets.randbelow(1000000):06d}"
    expires_at = datetime.utcnow() + timedelta(minutes=LOGIN_CODE_TTL_MINUTES)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO login_codes (email, code, expires_at) VALUES (%s, %s, %s) "
                "ON CONFLICT (email) DO UPDATE SET code = EXCLUDED.code, expires_at = EXCLUDED.expires_at",
                (email, code, expires_at),
            )
        conn.commit()
    try:
        _send_login_code_email(email, code)
    except (urllib.error.URLError, RuntimeError) as e:
        return jsonify({"error": f"failed to send email: {e}"}), 502
    return jsonify({"ok": True})


@app.route("/api/auth/check-email")
def check_email():
    # Read-only, no side effects - lets the client ask "does this email already have a
    # device linked?" before ever sending a code, so it can offer "add this device" vs
    # "replace the existing one" up front instead of surprising the user after the fact.
    email = str(request.args.get("email", "")).strip().lower()
    if not EMAIL_RE.match(email):
        return jsonify({"error": "invalid email"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM trusted_devices WHERE email = %s LIMIT 1", (email,))
            has_device = cur.fetchone() is not None
        conn.commit()
    return jsonify({"hasDevice": has_device})


@app.route("/api/auth/verify-code", methods=["POST"])
def verify_login_code():
    body = request.get_json(silent=True) or {}
    email = str(body.get("email", "")).strip().lower()
    code = str(body.get("code", "")).strip()
    client_id = str(body.get("clientId", "")).strip()
    local_id = str(body.get("localId", "")).strip()
    # Only meaningful once the code itself has been verified below - a device choice
    # made at the email step is just intent until then, never acted on early.
    replace_existing = bool(body.get("replace"))
    if not email or not code or not client_id:
        return jsonify({"error": "missing email/code/clientId"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT code, expires_at FROM login_codes WHERE email = %s", (email,))
            row = cur.fetchone()
            if not row or row[0] != code or row[1] < datetime.utcnow():
                conn.commit()
                return jsonify({"error": "invalid or expired code"}), 400
            cur.execute("DELETE FROM login_codes WHERE email = %s", (email,))
            if replace_existing:
                cur.execute("DELETE FROM trusted_devices WHERE email = %s AND client_id != %s", (email, client_id))
            cur.execute(
                "INSERT INTO trusted_devices (client_id, email) VALUES (%s, %s) "
                "ON CONFLICT (client_id) DO UPDATE SET email = EXCLUDED.email",
                (client_id, email),
            )
        conn.commit()
    account = _get_account(email) or _default_account(email)
    _claim_player_id(account, local_id)
    _save_account(email, account)
    return jsonify({"email": email, **account})


@app.route("/api/auth/quick-login", methods=["POST"])
def quick_login():
    # Two independent ways in here, either is enough:
    #  - The email is brand new (no trusted_devices row at all) - nobody's claimed this
    #    identity yet, so there's nothing a code would protect. Always allowed, no
    #    setting needed.
    #  - The email already exists (including the super admin's own) - refused (falls
    #    back to a real code) unless a super admin has explicitly turned "חייב קוד אימות
    #    לאימייל קיים" OFF. Defaults to on (secure) if never set.
    body = request.get_json(silent=True) or {}
    email = str(body.get("email", "")).strip().lower()
    client_id = str(body.get("clientId", "")).strip()
    local_id = str(body.get("localId", "")).strip()
    replace_existing = bool(body.get("replace"))
    if not EMAIL_RE.match(email) or not client_id:
        return jsonify({"error": "invalid email/clientId"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM trusted_devices WHERE email = %s LIMIT 1", (email,))
            already_registered = cur.fetchone() is not None
            if already_registered and _get_setting("require_verify_existing_email", True):
                conn.commit()
                return jsonify({"error": "already registered"}), 409
            if replace_existing:
                cur.execute("DELETE FROM trusted_devices WHERE email = %s AND client_id != %s", (email, client_id))
            cur.execute(
                "INSERT INTO trusted_devices (client_id, email) VALUES (%s, %s) "
                "ON CONFLICT (client_id) DO UPDATE SET email = EXCLUDED.email",
                (client_id, email),
            )
        conn.commit()
    account = _get_account(email) or _default_account(email)
    _claim_player_id(account, local_id)
    _save_account(email, account)
    return jsonify({"email": email, **account})


@app.route("/api/auth/settings")
def get_auth_settings():
    return jsonify({
        "requireVerifyExistingEmail": _get_setting("require_verify_existing_email", True),
        "requireVerifyBeforeAdminSettings": _get_setting("require_verify_before_admin_settings", False),
    })


@app.route("/api/auth/settings", methods=["POST"])
def set_auth_settings():
    ok, err = _require_super_admin(request.args.get("clientId", ""))
    if not ok:
        return err
    body = request.get_json(silent=True) or {}
    if "requireVerifyExistingEmail" in body:
        _set_setting("require_verify_existing_email", bool(body["requireVerifyExistingEmail"]))
    if "requireVerifyBeforeAdminSettings" in body:
        _set_setting("require_verify_before_admin_settings", bool(body["requireVerifyBeforeAdminSettings"]))
    return jsonify({"ok": True})


@app.route("/api/auth/session")
def auth_session():
    email = _trusted_email(request.args.get("clientId", ""))
    if not email:
        return jsonify({"loggedIn": False})
    account = _get_account(email) or _default_account(email)
    return jsonify({"loggedIn": True, "email": email, **account})


@app.route("/api/auth/claim-player-id", methods=["POST"])
def claim_player_id():
    # Called only when the session response came back without a playerId yet (a brand
    # new account, or one that predates this field) - claims this device's own local id
    # as the account's shared one, or returns whichever id another device already won.
    body = request.get_json(silent=True) or {}
    email = _trusted_email(str(body.get("clientId", "")).strip())
    if not email:
        return jsonify({"error": "not logged in"}), 401
    local_id = str(body.get("localId", "")).strip()
    account = _get_account(email) or _default_account(email)
    _claim_player_id(account, local_id)
    _save_account(email, account)
    return jsonify({"playerId": account.get("playerId")})


@app.route("/api/auth/account", methods=["POST"])
def update_account():
    email = _trusted_email(request.args.get("clientId", ""))
    if not email:
        return jsonify({"error": "not logged in"}), 401
    updates = request.get_json(silent=True) or {}
    account = _get_account(email) or _default_account(email)
    # role is deliberately not settable here - see set_account_role, which is the only
    # path allowed to change it, and checks the requester is themselves a super admin.
    for key in ("name", "photo", "phone", "myPlayers"):
        if key in updates:
            account[key] = updates[key]
    _save_account(email, account)
    return jsonify({"email": email, **account})


@app.route("/api/auth/set-role", methods=["POST"])
def set_account_role():
    ok, err = _require_super_admin(request.args.get("clientId", ""))
    if not ok:
        return err
    body = request.get_json(silent=True) or {}
    target_email = str(body.get("email", "")).strip().lower()
    new_role = body.get("role")
    if new_role not in ("user", "superAdmin") or not target_email:
        return jsonify({"error": "invalid request"}), 400
    target_account = _get_account(target_email) or _default_account(target_email)
    target_account["role"] = new_role
    _save_account(target_email, target_account)
    return jsonify({"ok": True})


def _require_super_admin(client_id):
    """Returns (True, None) if this device belongs to a super admin, else (False, error_response)."""
    email = _trusted_email(client_id)
    if not email:
        return False, (jsonify({"error": "not logged in"}), 401)
    account = _get_account(email) or _default_account(email)
    if account.get("role") != "superAdmin":
        return False, (jsonify({"error": "forbidden"}), 403)
    return True, None


@app.route("/api/auth/accounts")
def list_accounts():
    ok, err = _require_super_admin(request.args.get("clientId", ""))
    if not ok:
        return err
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT email, data FROM accounts ORDER BY email")
            rows = cur.fetchall()
        conn.commit()
    return jsonify([{"email": r[0], **json.loads(r[1])} for r in rows])


@app.route("/api/auth/delete-account", methods=["POST"])
def delete_account():
    ok, err = _require_super_admin(request.args.get("clientId", ""))
    if not ok:
        return err
    body = request.get_json(silent=True) or {}
    target_email = str(body.get("email", "")).strip().lower()
    if not target_email:
        return jsonify({"error": "missing email"}), 400
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM accounts WHERE email = %s", (target_email,))
            cur.execute("DELETE FROM trusted_devices WHERE email = %s", (target_email,))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/vision", methods=["POST"])
def vision():
    body = request.get_json(force=True) or {}
    data_urls = body.get("dataUrls")
    if not data_urls:
        single = body.get("dataUrl", "")
        data_urls = [single] if single else []
    prompt_text = body.get("promptText", "")
    if not data_urls:
        return jsonify({"error": "unsupported image data"}), 400

    content = []
    for data_url in data_urls:
        match = DATA_URL_RE.match(data_url)
        if not match:
            return jsonify({"error": "unsupported image data"}), 400
        media_type, b64data = match.group(1), match.group(2)
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": b64data,
            },
        })
    content.append({"type": "text", "text": prompt_text})

    client = anthropic.Anthropic()
    try:
        response = client.messages.create(
            model="claude-sonnet-5",
            max_tokens=1000,
            output_config={"effort": "low"},
            messages=[{"role": "user", "content": content}],
        )
    except anthropic.RateLimitError:
        return jsonify({"error": "rate limited, try again shortly"}), 429
    except anthropic.APIStatusError as e:
        return jsonify({"error": f"Claude API error: {e.status_code}"}), 502
    except anthropic.APIConnectionError:
        return jsonify({"error": "could not reach Claude API"}), 502

    text = "\n".join(block.text for block in response.content if block.type == "text")
    cleaned = text.replace("```json", "").replace("```", "").strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return jsonify({"error": "could not parse model response"}), 502
    return jsonify(parsed)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # Opens _MIN_POOL_CONNS real connections to Supabase right now, before the app is
    # reachable at all, instead of lazily on whichever request happens to arrive first.
    # The initial page load fires several requests at once (state, identity, version,
    # session, auth settings - see index.html's init()), and each of those used to pay
    # its own multi-second connection-setup cost in parallel the first time anyone loaded
    # the app after a cold start - directly observed as a minute-plus "loading" screen
    # while testing the staging environment. This moves that cost to server startup,
    # where nobody's watching it happen.
    _get_pool()
    # threaded=True is required now - an SSE connection (/api/events) stays open
    # indefinitely, and the default single-threaded dev server would block every
    # other request behind it for as long as any one tab stays connected.
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("DEBUG") == "1", threaded=True)
