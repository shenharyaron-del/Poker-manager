import json
import os
import re
from pathlib import Path

import anthropic
import psycopg2
from flask import Flask, jsonify, request, send_from_directory

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATABASE_URL = os.environ["DATABASE_URL"]
DATA_URL_RE = re.compile(r"^data:(image/[a-zA-Z0-9.+-]+);base64,(.+)$", re.DOTALL)

app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB, enough for a chip photo


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
    return send_from_directory(STATIC_DIR, "index.html")


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
    return jsonify({"ok": True})


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
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("DEBUG") == "1")
