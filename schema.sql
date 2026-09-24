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
  active_paybox_link_id TEXT
);

CREATE TABLE chip_values (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id),
  image TEXT,
  value NUMERIC,
  sort_order INT
);

CREATE TABLE paybox_links (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id),
  name TEXT,
  link TEXT
);

CREATE TABLE roster_players (
  id TEXT PRIMARY KEY,
  community_id TEXT REFERENCES communities(id),
  name TEXT NOT NULL,
  client_id TEXT NULL
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
  community_id TEXT REFERENCES communities(id),
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
  game_id TEXT REFERENCES games(id),
  player_id TEXT,
  name TEXT,
  PRIMARY KEY (game_id, player_id)
);

CREATE TABLE buyins (
  id TEXT PRIMARY KEY,
  game_id TEXT REFERENCES games(id),
  player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ
);

CREATE TABLE cashouts (
  game_id TEXT REFERENCES games(id),
  player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ,
  arranged_with JSONB,
  PRIMARY KEY (game_id, player_id)
);

CREATE TABLE paybox_payments (
  id TEXT PRIMARY KEY,
  game_id TEXT REFERENCES games(id),
  player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ
);

CREATE TABLE player_payments (
  id TEXT PRIMARY KEY,
  game_id TEXT REFERENCES games(id),
  from_player_id TEXT,
  to_player_id TEXT,
  amount NUMERIC,
  ts TIMESTAMPTZ
);

CREATE INDEX ON roster_players(community_id);
CREATE INDEX ON buyins(game_id);
CREATE INDEX ON cashouts(game_id);
CREATE INDEX ON games(community_id);

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
