"""Local staging runner for the relational-DB migration (see
.claude/plans/virtual-cuddling-wreath.md and USE_RELATIONAL_API in static/index.html).

Points server.py at the Phase 1 scratch schema (migration_test) instead of the real
production `public` schema, and turns on the relational-API client code for every
browser that loads this server - safe to click through without touching any real data.
Reachable from a phone on the same WiFi via this machine's LAN IP (Flask binds to all
addresses), same as the regular local dev server.

Re-run `migrate_to_relational.py` first if you want a fresh copy of production data in
the scratch schema - this script only points at it, it doesn't reset it.

Run via the "poker-app-staging" launch.json configuration, or directly:
    .venv\\Scripts\\python.exe run_staging.py
"""
import os

os.environ["DB_SCHEMA"] = "migration_test"
os.environ["RELATIONAL_API_DEFAULT"] = "true"

import server

if __name__ == "__main__":
    # threaded=True is required - see the matching comment at the bottom of server.py.
    # An SSE connection (/api/events) stays open indefinitely; without this, the default
    # single-threaded dev server blocks every other request behind it once any tab opens
    # one (exactly what happened testing this script the first time: the page hung on
    # "loading" forever after its first few requests).
    server.app.run(host="0.0.0.0", port=5050, threaded=True)
