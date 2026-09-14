import hashlib
import json
import os
import queue
import re
import threading
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


def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS shared_state ("
            "  id INTEGER PRIMARY KEY CHECK (id = 1),"
            "  data TEXT NOT NULL"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS identities ("
            "  client_id TEXT PRIMARY KEY,"
            "  data TEXT NOT NULL"
            ")"
        )
    conn.commit()
    return conn


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


@app.route("/api/version")
def version():
    return jsonify({"version": APP_VERSION})


@app.route("/api/state", methods=["GET"])
def get_state():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM shared_state WHERE id = 1")
        row = cur.fetchone()
    conn.close()
    if row:
        return row[0], 200, {"Content-Type": "application/json"}
    return jsonify({"communities": [], "games": []})


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
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM shared_state WHERE id = 1")
        row = cur.fetchone()
    conn.close()
    data = row[0] if row else json.dumps({"communities": [], "games": []})
    return jsonify({"hash": hashlib.md5(data.encode("utf-8")).hexdigest()})


@app.route("/api/state", methods=["POST"])
def save_state():
    data = request.get_data(as_text=True)
    json.loads(data)  # reject anything that isn't valid JSON before storing it
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO shared_state (id, data) VALUES (1, %s) "
            "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
            (data,),
        )
    conn.commit()
    conn.close()
    _broadcast_state_changed()
    return jsonify({"ok": True})


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
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM identities WHERE client_id = %s", (client_id,))
        row = cur.fetchone()
    conn.close()
    if row:
        return row[0], 200, {"Content-Type": "application/json"}
    return jsonify(None)


@app.route("/api/identity", methods=["POST"])
def save_identity():
    client_id = request.args.get("clientId", "")
    data = request.get_data(as_text=True)
    json.loads(data)
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO identities (client_id, data) VALUES (%s, %s) "
            "ON CONFLICT (client_id) DO UPDATE SET data = EXCLUDED.data",
            (client_id, data),
        )
    conn.commit()
    conn.close()
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
