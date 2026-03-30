"""
migrate_to_supabase.py
======================
Reads swiss_floorball_lupl.db and pushes all data to Supabase.

Usage:
  pip install requests
  python migrate_to_supabase.py

Set SUPABASE_SERVICE_KEY below (Settings → API → service_role key).
"""

import sqlite3, json, requests, time, logging, os
from itertools import islice

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = "https://ibqwotgrzgrwvejtphnh.supabase.co"
SUPABASE_SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlicXdvdGdyemdyd3ZlanRwaG5oIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0ODI3NDY3MywiZXhwIjoyMDYzODUwNjczfQ.WMk8HcsqHMJjlfxrtyqUywfpDApbz-YOCS0u1IxdxeE"

DB_PATH  = "swiss_floorball_lupl.db"
BATCH    = 500   # rows per upsert request

# ── Helpers ───────────────────────────────────────────────────────────────────

HEADERS = {
    "apikey":        SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates",
}

def upsert(table, rows):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers=HEADERS, data=json.dumps(rows, default=str))
    if r.status_code not in (200, 201):
        log.error(f"  {table} upsert failed [{r.status_code}]: {r.text[:300]}")
        r.raise_for_status()

def batched(iterable, n):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, n))
        if not chunk:
            break
        yield chunk

def push_table(table, rows, label=None):
    label = label or table
    total = len(rows)
    log.info(f"  Pushing {total} rows → {table}…")
    done = 0
    for batch in batched(rows, BATCH):
        upsert(table, batch)
        done += len(batch)
        if done % 5000 == 0 or done == total:
            log.info(f"    {done}/{total}")
        time.sleep(0.05)
    log.info(f"  ✓ {table}: {total} rows done")

# ── League normalization (mirror fetch_lupl.py) ───────────────────────────────

LEAGUE_MAP = {
    "Herren L-UPL":                  "Herren NLA",
    "Herren SML":                    "Herren NLA",
    "Herren Aktive KF 3. Liga":      "Herren 3. Liga",
    "Mobiliar Unihockey Cup Männer": "Mobiliar Unihockey Cup Herren",
}

def norm_league(name):
    return LEAGUE_MAP.get(name, name) if name else name

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if SUPABASE_SERVICE_KEY == "PASTE_YOUR_SERVICE_ROLE_KEY_HERE":
        log.error("Set SUPABASE_SERVICE_KEY first!")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    log.info(f"Opened {DB_PATH}")

    # ── 1. Games (already migrated — skip if rows exist) ─────────────────────
    count = requests.get(
        f"{SUPABASE_URL}/rest/v1/fb_games?select=game_id&limit=1",
        headers=HEADERS
    ).json()
    if not count:
        games = [dict(r) for r in conn.execute("SELECT * FROM games ORDER BY date")]
        push_table("fb_games", games)
    else:
        log.info("  fb_games already populated — skipping")

    # ── 2. Goals (with scorer_name / assist_name from name_map) ──────────────
    # Load name map — supports both old schema (abbrev→full) and new (abbrev+team→full)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(name_map)")}
    abbrev_team_to_full = {}
    if "team_raw" in cols:
        for abbrev, team, full in conn.execute(
            "SELECT abbrev_name, team_raw, full_name FROM name_map"
        ):
            abbrev_team_to_full[(abbrev, team)] = full
    else:
        # Old schema: key by (abbrev, None) — best effort without team context
        for abbrev, full in conn.execute(
            "SELECT abbrev_name, full_name FROM name_map"
        ):
            abbrev_team_to_full[(abbrev, None)] = full

    def resolve(abbrev, team_raw):
        if not abbrev:
            return None
        result = abbrev_team_to_full.get((abbrev, team_raw))
        if result is None:
            result = abbrev_team_to_full.get((abbrev, None))
        return result or abbrev

    GOAL_COLS = [
        "game_id", "team_scored_id", "team_conceded_id", "team_scored_raw",
        "team_conceded_raw", "scorer_raw", "assist_raw", "scorer_name",
        "assist_name", "minute", "minute_seconds", "period", "score_at_goal",
        "date", "season", "league", "league_group", "home_team_raw",
        "away_team_raw", "home_team_id", "away_team_id",
    ]

    goal_rows = []
    for r in conn.execute("""
        SELECT g.*,
               gm.time as game_time, gm.league, gm.league_group,
               gm.result, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id, gm.location_city
        FROM goals g JOIN games gm ON g.game_id = gm.game_id
        ORDER BY g.goal_id
    """):
        raw = dict(r)
        raw["league"] = norm_league(raw.get("league"))
        team_raw = raw.get("team_scored_raw", "")
        raw["scorer_name"] = resolve(raw.get("scorer_raw"), team_raw)
        raw["assist_name"]  = resolve(raw.get("assist_raw"),  team_raw)
        # keep only the exact columns in fb_goals
        goal_rows.append({c: raw.get(c) for c in GOAL_COLS})

    push_table("fb_goals", goal_rows)

    # ── 3. Penalties ──────────────────────────────────────────────────────────
    PEN_COLS = [
        "game_id", "team_id", "team_raw", "player_raw", "minute",
        "minute_seconds", "period", "duration_min", "reason", "date",
        "season", "league", "home_team_raw", "away_team_raw",
        "home_team_id", "away_team_id",
    ]

    pen_rows = []
    for r in conn.execute("""
        SELECT p.*,
               gm.league, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id
        FROM penalties p JOIN games gm ON p.game_id = gm.game_id
        ORDER BY p.penalty_id
    """):
        raw = dict(r)
        raw["league"] = norm_league(raw.get("league"))
        pen_rows.append({c: raw.get(c) for c in PEN_COLS})

    push_table("fb_penalties", pen_rows)

    # ── 4. Player meta ────────────────────────────────────────────────────────
    # Most-common position per player
    pos_map = {}
    for name, pos in conn.execute("""
        SELECT player_raw, position FROM lineups
        WHERE position IS NOT NULL
        GROUP BY player_raw, position
        ORDER BY player_raw, COUNT(*) DESC
    """):
        pos_map.setdefault(name, pos)

    gp_total = {name: gp for name, gp in conn.execute(
        "SELECT player_raw, COUNT(*) FROM lineups GROUP BY player_raw"
    )}
    gp_seasons = {}
    for name, season, gp in conn.execute(
        "SELECT player_raw, season, COUNT(*) FROM lineups GROUP BY player_raw, season"
    ):
        gp_seasons.setdefault(name, {})[str(season)] = gp

    player_game_ids = {}
    for name, gid in conn.execute(
        "SELECT player_raw, game_id FROM lineups ORDER BY date, game_id"
    ):
        player_game_ids.setdefault(name, []).append(gid)

    player_ids = {}
    for name, pid in conn.execute(
        "SELECT player_raw, player_id FROM lineups WHERE player_id IS NOT NULL"
        " GROUP BY player_raw ORDER BY season DESC"
    ):
        player_ids.setdefault(name, pid)

    # Team history from goals (resolved names)
    team_game_ids = {}
    for scorer, assist, game_id, team_raw in conn.execute(
        "SELECT scorer_raw, assist_raw, game_id, team_scored_raw FROM goals"
        " WHERE team_scored_raw IS NOT NULL"
    ):
        for abbrev in (scorer, assist):
            if not abbrev:
                continue
            full = abbrev_team_to_full.get((abbrev, team_raw)) or abbrev_team_to_full.get((abbrev, None)) or abbrev
            th = team_game_ids.setdefault(full, {})
            th.setdefault(team_raw, set()).add(game_id)

    all_players = set(list(gp_total.keys()) + list(pos_map.keys()))
    meta_rows = []
    for name in all_players:
        raw_th = team_game_ids.get(name, {})
        team_gp = {team: len(gids) for team, gids in
                   sorted(raw_th.items(), key=lambda x: -len(x[1]))}
        meta_rows.append({
            "player_name": name,
            "pos":         pos_map.get(name),
            "gp":          gp_total.get(name, 0),
            "pid":         player_ids.get(name),
            "team_gp":     team_gp,
            "gp_s":        gp_seasons.get(name, {}),
            "gids":        player_game_ids.get(name, []),
        })

    push_table("fb_player_meta", meta_rows)

    conn.close()
    log.info("\n✅  Migration complete!")

if __name__ == "__main__":
    main()
