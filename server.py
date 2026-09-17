import hashlib
import json
import os
import queue
import re
import threading
from contextlib import contextmanager
from pathlib import Path

import anthropic
import psycopg2
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
# Render sets this automatically to the deployed commit's SHA - lets Settings show
# exactly which version is live, with no separate manual version number to keep in sync.
APP_VERSION = os.environ.get("RENDER_GIT_COMMIT", "local")[:7]

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
    "CREATE TABLE IF NOT EXISTS state_history ("
    "  id SERIAL PRIMARY KEY,"
    "  data TEXT NOT NULL,"
    "  saved_at TIMESTAMP NOT NULL DEFAULT now()"
    ")",
)


def _new_connection():
    conn = psycopg2.connect(DATABASE_URL)
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
    return conn


# One connection, reused for the process's whole lifetime, instead of opening a fresh
# one (full TCP+TLS handshake to Supabase, plus the CREATE TABLE checks above) on every
# single request - that round trip was adding several seconds to every API call. A lock
# serializes access across Flask's request threads, which is fine at this app's traffic
# (a poker group's phones, not a high-concurrency service) since each query is now just
# the query itself, no connection setup. If the connection dies underneath us (e.g. an
# idle timeout on Supabase's side), the failing request's query raises and we drop the
# connection so the *next* request reconnects - one request degrades, not the whole app.
_db_conn = None
_db_lock = threading.Lock()


@contextmanager
def get_db():
    global _db_conn
    with _db_lock:
        if _db_conn is None or _db_conn.closed:
            _db_conn = _new_connection()
        try:
            yield _db_conn
        except psycopg2.Error:
            try:
                _db_conn.close()
            except Exception:
                pass
            _db_conn = None
            raise


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
    # Every routine action (a buy-in, a seat change) saves too - snapshotting on every one
    # of those would burn through the retained history within seconds during an active
    # game and leave nothing useful to restore. The client marks only the saves that
    # follow a meaningful checkpoint (a game ending, a community/game created or deleted)
    # with ?checkpoint=1 - only those get a history entry.
    checkpoint = request.args.get("checkpoint") == "1"
    # Optional optimistic-concurrency check: the client sends the hash of the state it
    # built this save on top of. If the shared state has moved on since then (another
    # device saved first), reject instead of silently overwriting whatever they just
    # changed - the client re-syncs, reapplies its own change on the new base, and
    # retries. Callers that don't pass this skip the check entirely (unchanged behavior).
    base_hash = request.args.get("baseHash")
    if base_hash:
        _, current_hash = _get_cached_state()
        if base_hash != current_hash:
            return jsonify({"conflict": True}), 409
    with get_db() as conn:
        with conn.cursor() as cur:
            if checkpoint:
                # Snapshot whatever was there before this overwrites it, so a bad save
                # (accidental or a bug) can be rolled back - keep only the last few, this
                # is a safety net for undoing a recent mistake, not a full audit log.
                cur.execute("SELECT data FROM shared_state WHERE id = 1")
                prev = cur.fetchone()
                if prev:
                    cur.execute("INSERT INTO state_history (data) VALUES (%s)", (prev[0],))
                    cur.execute(
                        "DELETE FROM state_history WHERE id NOT IN "
                        "(SELECT id FROM state_history ORDER BY saved_at DESC LIMIT 3)"
                    )
            cur.execute(
                "INSERT INTO shared_state (id, data) VALUES (1, %s) "
                "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
                (data,),
            )
        conn.commit()
    _set_cached_state(data)
    _broadcast_state_changed()
    return jsonify({"ok": True})


@app.route("/api/state/history")
def get_state_history():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, saved_at FROM state_history ORDER BY saved_at DESC")
            rows = cur.fetchall()
        conn.commit()
    return jsonify([{"id": r[0], "savedAt": r[1].isoformat()} for r in rows])


@app.route("/api/state/history/<int:history_id>", methods=["GET"])
def get_state_history_entry(history_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM state_history WHERE id = %s", (history_id,))
            row = cur.fetchone()
        conn.commit()
    if not row:
        return jsonify({"error": "not found"}), 404
    return row[0], 200, {"Content-Type": "application/json"}


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
