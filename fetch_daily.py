"""Fast daily refresh: import finished games from live_games_cache.

Flow:
1. Read live_games_cache from Supabase (last 3 days)
2. Parse game IDs from the cached JSON
3. For each game not in SQLite: fetch details, goals, lineups from API
4. Sync new games to Supabase
"""
import sys, os, time, json, logging
from datetime import datetime, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from fetch_lupl import (
    get_db, fetch_and_store_goals, fetch_and_store_lineup,
    build_name_map, api_get, unwrap, SLEEP,
    SUPABASE_URL, SUPABASE_SERVICE_KEY,
    _sb_upsert, _batched,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()


# ── Step 1: Get finished game IDs from Supabase cache ─────────────────────

def get_cached_games():
    """Read finished games from live_games_cache (last 3 days)."""
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set")
        return []

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }
    cutoff = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    url = f"{SUPABASE_URL}/rest/v1/live_games_cache?select=game_date,data&game_date=gte.{cutoff}"
    r = SESSION.get(url, headers=headers)
    if r.status_code != 200:
        log.error(f"Cache fetch failed: {r.status_code} {r.text[:200]}")
        return []

    games = []
    for row in r.json():
        payload = row.get("data")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not payload:
            continue
        for league in payload.get("leagues", []):
            league_name = league.get("league", "")
            for g in league.get("games", []):
                time_str = g.get("time", "")
                result = g.get("result", "")
                # Only finished games
                if "beendet" not in time_str.lower():
                    continue
                if not result or result in ("-:-", "-", ""):
                    continue
                games.append({
                    "id": str(g["id"]),
                    "league": league_name,
                    "date": g.get("date", row.get("game_date", "")),
                })
    return games


# ── Step 2: Fetch game details from API ───────────────────────────────────

def fetch_and_store_game(conn, gid, league_label, game_date, season):
    """GET /api/games/:id → parse home/away/result → store in SQLite."""
    raw = api_get(f"games/{gid}", {})
    if not raw:
        return None

    data = unwrap(raw)
    headers = data.get("headers", [])
    rows = data.get("regions", [{}])[0].get("rows", [])
    if not rows:
        return None

    cells = rows[0].get("cells", [])
    key_to_idx = {h.get("key", ""): i for i, h in enumerate(headers)}

    def cell_val(key):
        idx = key_to_idx.get(key)
        if idx is None or idx >= len(cells):
            return None, []
        c = cells[idx]
        text = c.get("text", [None])[0] if c.get("text") else None
        ids = c.get("link", {}).get("ids", [])
        return text, ids

    home_name, home_ids = cell_val("home_name")
    away_name, away_ids = cell_val("away_name")
    result_text, _ = cell_val("result")
    time_text, _ = cell_val("time")
    location_text, _ = cell_val("location")

    if not home_name or not away_name or not result_text:
        return None

    home_id = home_ids[0] if home_ids else 0
    away_id = away_ids[0] if away_ids else 0

    weekday_map = {0: "Mo", 1: "Di", 2: "Mi", 3: "Do", 4: "Fr", 5: "Sa", 6: "So"}
    try:
        weekday = weekday_map.get(datetime.strptime(game_date, "%Y-%m-%d").weekday(), "")
    except Exception:
        weekday = ""

    conn.execute("""
        INSERT OR IGNORE INTO games
          (game_id, home_team_id, away_team_id, home_team_raw, away_team_raw,
           date, weekday, time, season, league, result, location)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (gid, home_id, away_id, home_name, away_name,
          game_date, weekday, time_text, season, league_label,
          result_text, location_text))
    conn.commit()

    return (home_id, away_id, home_name, away_name, game_date, weekday)


# ── Step 3: Sync new games to Supabase ────────────────────────────────────

def sync_games_to_supabase(conn, game_ids):
    """Push only the specified games + goals + penalties to Supabase."""
    if not SUPABASE_SERVICE_KEY or not game_ids:
        return

    LEAGUE_MAP = {"Herren L-UPL": "Herren NLA", "Herren SML": "Herren NLA"}
    def nl(name): return LEAGUE_MAP.get(name, name) if name else name

    # Name map
    cols = {r[1] for r in conn.execute("PRAGMA table_info(name_map)")}
    name_lookup = {}
    if "team_raw" in cols:
        for abbrev, team, full in conn.execute("SELECT abbrev_name, team_raw, full_name FROM name_map"):
            name_lookup[(abbrev, team)] = full

    def resolve(abbrev, team_raw):
        if not abbrev:
            return None
        return name_lookup.get((abbrev, team_raw)) or name_lookup.get((abbrev, None)) or abbrev

    ph = ",".join("?" * len(game_ids))

    # Lineups
    lineup_lookup = {}
    for gid, tid, player, is_home in conn.execute(
        f"SELECT l.game_id, l.team_id, l.player_raw, "
        f"  CASE WHEN l.team_id = g.home_team_id THEN 1 ELSE 0 END as is_home "
        f"FROM lineups l JOIN games g ON l.game_id = g.game_id "
        f"WHERE g.game_id IN ({ph})", game_ids
    ):
        if gid not in lineup_lookup:
            lineup_lookup[gid] = {"home_lineup": [], "away_lineup": []}
        key = "away_lineup" if is_home else "home_lineup"
        lineup_lookup[gid][key].append(player)

    # Games
    games = [dict(r) for r in conn.execute(f"SELECT * FROM games WHERE game_id IN ({ph})", game_ids)]
    for g in games:
        lu = lineup_lookup.get(g["game_id"], {})
        g["home_lineup"] = lu.get("home_lineup", [])
        g["away_lineup"] = lu.get("away_lineup", [])
    if games:
        _sb_upsert("fb_games", games)
    log.info(f"    fb_games: {len(games)} rows")

    # Goals
    GOAL_COLS = [
        "game_id", "team_scored_id", "team_conceded_id", "team_scored_raw",
        "team_conceded_raw", "scorer_raw", "assist_raw", "scorer_name",
        "assist_name", "minute", "minute_seconds", "period", "score_at_goal",
        "date", "season", "league", "league_group", "home_team_raw",
        "away_team_raw", "home_team_id", "away_team_id",
    ]
    goal_rows = []
    for r in conn.execute(f"""
        SELECT g.*, gm.league, gm.league_group, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id
        FROM goals g JOIN games gm ON g.game_id = gm.game_id
        WHERE gm.game_id IN ({ph})
    """, game_ids):
        raw = dict(r)
        raw["league"] = nl(raw.get("league"))
        raw["scorer_name"] = resolve(raw.get("scorer_raw"), raw.get("team_scored_raw", ""))
        raw["assist_name"] = resolve(raw.get("assist_raw"), raw.get("team_scored_raw", ""))
        goal_rows.append({c: raw.get(c) for c in GOAL_COLS})
    if goal_rows:
        _sb_upsert("fb_goals", goal_rows)
    log.info(f"    fb_goals: {len(goal_rows)} rows")

    # Penalties
    PEN_COLS = [
        "game_id", "team_id", "team_raw", "player_raw", "minute",
        "minute_seconds", "period", "duration_min", "reason", "date",
        "season", "league", "home_team_raw", "away_team_raw",
        "home_team_id", "away_team_id", "player_name",
    ]
    pen_rows = []
    for r in conn.execute(f"""
        SELECT p.*, gm.league, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id
        FROM penalties p JOIN games gm ON p.game_id = gm.game_id
        WHERE gm.game_id IN ({ph})
    """, game_ids):
        raw = dict(r)
        raw["league"] = nl(raw.get("league"))
        raw["player_name"] = resolve(raw.get("player_raw"), raw.get("team_raw", ""))
        pen_rows.append({c: raw.get(c) for c in PEN_COLS})
    if pen_rows:
        _sb_upsert("fb_penalties", pen_rows)
    log.info(f"    fb_penalties: {len(pen_rows)} rows")


# ── Main ──────────────────────────────────────────────────────────────────

def run():
    log.info("=== Fast Daily Refresh ===")
    season = 2025

    # 1. Get game IDs from cache
    cached = get_cached_games()
    log.info(f"  {len(cached)} finished games in cache (last 3 days)")

    if not cached:
        log.info("  Nothing to process.")
        return

    # 2. Filter to new games
    conn = get_db()
    new_games = [g for g in cached if not conn.execute("SELECT 1 FROM games WHERE game_id=?", (g["id"],)).fetchone()]
    log.info(f"  {len(new_games)} new games to import")

    if not new_games:
        log.info("  All games already in DB.")
        conn.close()
        return

    # 3. Fetch each game
    imported = []
    total_goals = total_pen = 0

    for g in new_games:
        gid = g["id"]
        time.sleep(SLEEP)

        result = fetch_and_store_game(conn, gid, g["league"], g["date"], season)
        if not result:
            log.warning(f"    Could not store game {gid}")
            continue

        home_id, away_id, home_name, away_name, iso_date, weekday = result

        # Goals + penalties
        time.sleep(SLEEP)
        ng, np = fetch_and_store_goals(conn, gid, home_id, away_id,
                                       home_name, away_name, iso_date, weekday, season)
        conn.commit()
        total_goals += ng
        total_pen += np

        # Lineups
        time.sleep(SLEEP)
        for is_home, tid, tname in [(0, away_id, away_name), (1, home_id, home_name)]:
            time.sleep(SLEEP)
            fetch_and_store_lineup(conn, gid, tid, is_home, tname, season, iso_date)
        conn.commit()

        imported.append(gid)
        log.info(f"    ✓ {home_name} vs {away_name} [{iso_date}] {ng}G {np}P ({g['league']})")

    log.info(f"\n── Results ─────────────────")
    log.info(f"  Imported: {len(imported)} games, {total_goals} goals, {total_pen} penalties")

    # 4. Sync to Supabase
    if imported:
        log.info("\n── Building name map…")
        conn.execute("DELETE FROM name_map")
        conn.commit()
        build_name_map(conn)

        log.info("\n── Syncing to Supabase…")
        sync_games_to_supabase(conn, imported)

    conn.close()
    log.info("\n✓ Done!")


if __name__ == "__main__":
    run()
