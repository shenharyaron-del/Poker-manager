# Poker Manager - server + database version

This is the real multi-user version of the Poker Manager app: a small Python
(Flask) backend backed by a real Postgres database (hosted free on
Supabase), serving the same UI that used to run only inside a Claude
artifact. Every user who opens the site now shares the same data, stored in
that database instead of in Claude's artifact storage.

## What changed from the artifact version

- `window.storage.get/set` calls -> `fetch` calls to `/api/state` and
  `/api/identity` (see `static/index.html`), backed by a Postgres database
  on Supabase (`server.py`).
- The "AI vision" chip-recognition feature now calls **our own server**
  (`/api/vision`), which holds the real Anthropic API key and calls Claude
  server-side - the key is never exposed to the browser.
- All game logic, UI, and styling is unchanged.

## Project layout

```
app/
  server.py          Flask app + Postgres (Supabase) persistence + Claude vision proxy
  requirements.txt
  static/index.html  The app (adapted client)
  .env.example        Copy to .env and fill in your API key + database URL
```

## Database: Supabase (free, persists forever)

The app stores its data in a free Supabase Postgres database instead of a
local file - that way the data survives even when the free hosting tier
restarts or sleeps (a local SQLite file would get wiped on those restarts).

1. Create a free project at https://supabase.com.
2. In the project, click **Connect** -> **Transaction pooler**, copy the
   connection string, and replace `[YOUR-PASSWORD]` with your real database
   password.
3. Put it in `.env` as `DATABASE_URL=...`.

Note: use the **pooler** connection string (`...pooler.supabase.com:6543`),
not the "Direct connection" one - the direct address is IPv6-only and won't
resolve on most networks/hosts.

## Run it locally

```bash
cd app
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt      # Windows
# source .venv/bin/activate && pip install -r requirements.txt   # macOS/Linux

copy .env.example .env      # Windows
# cp .env.example .env      # macOS/Linux
# then edit .env: paste your real ANTHROPIC_API_KEY and DATABASE_URL

.venv\Scripts\python server.py
```

Open http://127.0.0.1:5000 - anyone on your network who can reach that
address/port will see and edit the same shared data.

Get an API key at https://console.anthropic.com/settings/keys. The chip-photo
feature won't work without one, but the rest of the app (communities, games,
buy-ins, settlements) works fine even with no key set.

## Deploying so it's reachable from anywhere

Pick a host that runs Python apps (Render, Railway, Fly.io, or a small VPS).
General steps for any of them:

1. Push this `app/` folder to a Git repo (GitHub is easiest).
2. Create a new "Web Service" from that repo.
3. Build command: `pip install -r requirements.txt`
4. Start command: `python server.py` (the platform sets `PORT` for you -
   `server.py` already reads it from the environment).
5. Add an environment variable `ANTHROPIC_API_KEY` with your real key in the
   platform's dashboard - never commit it to the repo.
6. SQLite lives in `data/poker.db` next to the code. On most free tiers this
   file persists across restarts but is wiped on a redeploy - check your
   platform's "persistent disk" option if you want it to survive deploys.

### Free vs paid

- Free tier (Render/Fly.io free plan): $0/month, may sleep after inactivity
  (a few seconds' delay on the first request after idling).
- Always-on hobby plan: roughly $5-7/month.
- Custom domain (optional): ~$10-15/year. Every platform above also gives you
  a free subdomain if you don't need a custom name.
- Claude vision calls: pay-per-use, roughly $0.01-0.05 per photo analyzed -
  add a small amount of billing credit to your Anthropic account.

## Notes on the current data model

- `shared_state` table: one row holding the entire app state (communities +
  games) as JSON - mirrors exactly what the artifact version stored.
- `identities` table: one row per browser (`client_id`, generated and stored
  in that browser's `localStorage`), holding the typed-in display name.

This keeps 100% of the existing client-side game logic untouched. If the app
grows to many communities/games, this is the natural place to move to real
relational tables (one row per community/game/player) instead of one JSON
blob - but for a small poker group, this is simpler and works fine.
