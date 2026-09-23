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
from flask import Flask, Response, jsonify, request, send_from_directory

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATABASE_URL = os.environ["DATABASE_URL"]
DATA_URL_RE = re.compile(r"^data:(image/[a-zA-Z0-9.+-]+);base64,(.+)$", re.DOTALL)
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# Render sets this automatically to the deployed commit's SHA - lets Settings show
# exactly which version is live, with no separate manual version number to keep in sync.
APP_VERSION = os.environ.get("RENDER_GIT_COMMIT", "local")[:7]
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
_MIN_POOL_CONNS = 1
_MAX_POOL_CONNS = 10
_db_pool = None
_db_pool_lock = threading.Lock()


def _get_pool():
    global _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            pool = psycopg2.pool.ThreadedConnectionPool(_MIN_POOL_CONNS, _MAX_POOL_CONNS, DATABASE_URL)
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
    return jsonify({"version": APP_VERSION})


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
    #  - The email already exists (including the super admin's own) - only allowed when
    #    a super admin has explicitly turned "כניסה ללא קוד אימות" on; refused otherwise,
    #    and the client falls back to a real code.
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
            if already_registered and not _get_setting("skip_login_verification", False):
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
        "skipLoginVerification": _get_setting("skip_login_verification", False),
        "requireVerifyBeforeAdminSettings": _get_setting("require_verify_before_admin_settings", False),
    })


@app.route("/api/auth/settings", methods=["POST"])
def set_auth_settings():
    ok, err = _require_super_admin(request.args.get("clientId", ""))
    if not ok:
        return err
    body = request.get_json(silent=True) or {}
    if "skipLoginVerification" in body:
        _set_setting("skip_login_verification", bool(body["skipLoginVerification"]))
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
    # threaded=True is required now - an SSE connection (/api/events) stays open
    # indefinitely, and the default single-threaded dev server would block every
    # other request behind it for as long as any one tab stays connected.
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("DEBUG") == "1", threaded=True)
