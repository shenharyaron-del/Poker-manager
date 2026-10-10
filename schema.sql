-- Target relational schema for the Poker Manager app, replacing the single
-- shared_state.data JSON blob. See .claude/plans/virtual-cuddling-wreath.md
-- for the migration plan this belongs to.
--
-- Applied by migrate_to_relational.py, which sets search_path to a scratch
-- schema first during dry-run verification. Ids are kept as the existing
-- client-generated uid() TEXT strings - no remapping needed anywhere.

CREATE TABLE communities (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_by TEXT,
  created_by_name TEXT,
  chip_ratio NUMERIC,
  active_paybox_link_id TEXT,
  -- Same idea as active_paybox_link_id, for the group's Bit link instead - added via an
  -- idempotent ALTER TABLE in server.py's own startup init, not by re-running this file.
  active_bit_link_id TEXT,
  -- Which settlement method a cashed-out player's panel defaults to on the table screen -
  -- 'direct' (player-to-player transfer) or 'paybox' (the community's shared PayBox link,
  -- only actually offered when active_paybox_link_id is set - see renderActionPanel).
  -- Added post-Phase-7, not part of the original schema.
  settlement_default TEXT NOT NULL DEFAULT 'direct',
  -- Who played the last time a night was ended here (endNight/endBulk set this) - used
  -- to quick-preselect the same group when starting a new night. Discovered during
  -- Phase 7 client conversion; not part of the original Phase 3 schema.
  last_participants JSONB,
  -- Optimistic-concurrency token for PATCH/PUT writes (see the /api/v2 write
  -- endpoints in server.py) - the client echoes back the value it last saw;
  -- a mismatch means someone else edited this community first.
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE chip_values (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id) ON DELETE CASCADE,
  image TEXT,
  value NUMERIC,
  sort_order INT
);

CREATE TABLE paybox_links (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id) ON DELETE CASCADE,
  name TEXT,
  link TEXT
);

-- Same shape as paybox_links - a community's group Bit link(s). Added post-Phase-7 via
-- server.py's own idempotent startup init, not by re-running this file against the live DB.
CREATE TABLE bit_links (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id) ON DELETE CASCADE,
  name TEXT,
  link TEXT
);

CREATE TABLE roster_players (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  client_id TEXT NULL,
  -- Community-level admin flag (the admin screen) - grants table-admin rights in every
  -- game under this community, on top of whoever created the community/game (who always
  -- have them regardless of this flag). Added post-Phase-7 via an idempotent
  -- ALTER TABLE ... ADD COLUMN IF NOT EXISTS in server.py's own startup init, not by
  -- re-running this file against the live DB.
  is_admin BOOLEAN NOT NULL DEFAULT false
);

-- Cross-community canonical player data (photo/phone/preferred seat),
-- keyed by the account's permanent playerId (== roster_players.client_id).
CREATE TABLE global_players (
  client_id TEXT PRIMARY KEY,
  photo TEXT,
  phone TEXT,
  seat_position INT
);

CREATE TABLE games (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id) ON DELETE CASCADE,
  name TEXT,
  date DATE,
  created_by_name TEXT,
  closed BOOLEAN DEFAULT false,
  -- Ephemeral in-progress state, not append-only history - JSONB is the
  -- right fit, same rationale as arranged_with on cashouts below.
  seats JSONB,
  live_chips JSONB DEFAULT '{}',
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Everyone who has EVER sat in this game (for history/payout display) -
-- not removed when a player is unseated, only games.seats goes null there.
--
-- player_id here (and on buyins/cashouts/payments below) is intentionally
-- NOT a foreign key into roster_players. Removing a player from a
-- community's roster (c.roster = c.roster.filter(...) client-side) hard-
-- deletes their roster row but leaves old games' history referencing that
-- id untouched - confirmed against real production data during the
-- Phase 1 dry-run (2 such orphaned references across 3 games). A strict
-- FK would either block that removal or force a cascade that destroys
-- history, neither of which matches current behavior.
CREATE TABLE game_players (
  game_id TEXT REFERENCES games(id) ON DELETE CASCADE,
  player_id TEXT,
  name TEXT,
  PRIMARY KEY (game_id, player_id)
);

CREATE TABLE buyins (
  id TEXT PRIMARY KEY,
  game_id TEXT REFERENCES games(id) ON DELETE CASCADE,
  player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ
);

CREATE TABLE cashouts (
  game_id TEXT REFERENCES games(id) ON DELETE CASCADE,
  player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ,
  arranged_with JSONB,
  PRIMARY KEY (game_id, player_id)
);

CREATE TABLE paybox_payments (
  id TEXT PRIMARY KEY,
  game_id TEXT REFERENCES games(id) ON DELETE CASCADE,
  player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ
);

CREATE TABLE player_payments (
  id TEXT PRIMARY KEY,
  game_id TEXT REFERENCES games(id) ON DELETE CASCADE,
  from_player_id TEXT,
  to_player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ
);

-- The table screen's activity feed, for action types that leave no other trace to derive
-- a feed entry from afterward (a deleted buy-in, a player returned to the table, a seat
-- removed outright, the whole night declared over) - unlike a buy-in or cash-out, which
-- are already durably recorded as their own rows above and don't need a separate log.
-- actor_name is who actually performed the action (may differ from player_id - an admin
-- acting on someone else's behalf, see the admin screen), for the feed's "בשם X"
-- attribution. Added post-Phase-7 via server.py's own idempotent startup init, not by
-- re-running this file against the live DB.
CREATE TABLE game_events (
  id TEXT PRIMARY KEY,
  game_id TEXT REFERENCES games(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  type TEXT NOT NULL,
  player_id TEXT,
  actor_name TEXT,
  amount NUMERIC
);

CREATE INDEX ON roster_players(community_id);
CREATE INDEX ON buyins(game_id);
CREATE INDEX ON cashouts(game_id);
CREATE INDEX ON games(community_id);
CREATE INDEX ON game_events(game_id);

-- Derived, not stored - avoids drift if a buy-in/cashout is later edited
-- (the app already has an edit-pencil for a cashed-out player's amount).
--
-- Sourced from buyins/games rather than roster_players, so a player who
-- was later removed from the roster still shows up in their old games'
-- outcomes (community_id comes from games, which is never selectively
-- deleted the way individual roster rows are - see the comment on
-- game_players above).
CREATE VIEW player_outcomes AS
SELECT b.player_id, g.community_id, b.game_id,
  COALESCE(c.amount, 0) - COALESCE(SUM(b.amount), 0) AS outcome
FROM buyins b
JOIN games g ON g.id = b.game_id
LEFT JOIN cashouts c ON c.game_id = b.game_id AND c.player_id = b.player_id
GROUP BY b.player_id, g.community_id, b.game_id, c.amount;
