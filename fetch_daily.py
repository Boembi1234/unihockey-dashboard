"""Fast daily refresh: import finished games from live_games_cache.

Flow:
1. Read live_games_cache from Supabase (last 3 days)
2. Parse game IDs from the cached JSON (only "Spiel beendet")
3. For each game not in SQLite: use store_game + fetch_and_store_goals from fetch_lupl
4. Sync new games to Supabase
"""
import sys, os, time, json, logging
from datetime import datetime, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from fetch_lupl import (
    get_db, store_game, fetch_and_store_goals,
    build_lineup_map, api_get, unwrap, SLEEP,
    SUPABASE_URL, SUPABASE_SERVICE_KEY,
    _sb_upsert, _batched,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()


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
        log.error(f"Cache fetch failed: {r.status_code}")
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
                t = g.get("time", "")
                result = g.get("result", "")
                if "beendet" not in t.lower():
                    continue
                if not result or result in ("-:-", "-", ""):
                    continue
                games.append({
                    "id": str(g["id"]),
                    "league": league_name,
                    "date": g.get("date", row.get("game_date", "")),
                })
    return games


def fetch_game_row(game_id):
    """Fetch the game row from the games list API (same format store_game expects)."""
    # The games API with mode=current returns rows we can pass to store_game
    raw = api_get(f"games/{game_id}", {})
    if not raw:
        return None, None
    data = unwrap(raw)

    # The detail API returns a single row — convert to the format store_game expects
    headers = data.get("headers", [])
    rows = data.get("regions", [{}])[0].get("rows", [])
    if not rows:
        return None, None

    # Build a fake row matching the game list format that store_game expects
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
        return None, None

    home_id = home_ids[0] if home_ids else 0
    away_id = away_ids[0] if away_ids else 0

    # Parse phase and league_group from subtitle
    subtitle = data.get("subtitle", "")
    sub_lower = subtitle.lower()

    # Detect phase
    phase = "Qualifikation"
    if "playoff" in sub_lower or "abstieg" in sub_lower or "superfinal" in sub_lower or "final" in sub_lower:
        if "halbfinal" in sub_lower:
            phase = "Halbfinal"
        elif "viertelfinal" in sub_lower:
            phase = "Viertelfinal"
        elif "superfinal" in sub_lower:
            phase = "Superfinal"
        elif "final" in sub_lower and "halbfinal" not in sub_lower and "viertelfinal" not in sub_lower:
            phase = "Final"
        else:
            phase = "Playoff"

    # Detect league_group from subtitle (e.g. "Gruppe 1" or "Gruppe 2")
    import re as _re
    league_group = None
    grp_match = _re.search(r'Gruppe\s+\d+', subtitle)
    if grp_match:
        league_group = grp_match.group(0)

    return {
        "home_id": home_id,
        "away_id": away_id,
        "home_name": home_name,
        "away_name": away_name,
        "result": result_text,
        "time": time_text,
        "location": location_text,
        "phase": phase,
        "league_group": league_group,
    }, subtitle


def sync_games_to_supabase(conn, game_ids):
    """Push only the specified games + goals + penalties to Supabase."""
    if not SUPABASE_SERVICE_KEY or not game_ids:
        return

    LEAGUE_MAP = {"Herren L-UPL": "Herren NLA", "Herren SML": "Herren NLA"}
    def nl(name): return LEAGUE_MAP.get(name, name) if name else name

    ph = ",".join("?" * len(game_ids))

    # Lineups — store player_ids, swap because API stores with reversed team_ids
    lineup_lookup = {}
    for gid, pid, is_home in conn.execute(
        f"SELECT l.game_id, l.player_id, "
        f"  CASE WHEN l.team_id = g.home_team_id THEN 1 ELSE 0 END as is_home "
        f"FROM lineups l JOIN games g ON l.game_id = g.game_id "
        f"WHERE l.player_id IS NOT NULL AND g.game_id IN ({ph})", game_ids
    ):
        if gid not in lineup_lookup:
            lineup_lookup[gid] = {"home_lineup": [], "away_lineup": []}
        key = "away_lineup" if is_home else "home_lineup"
        lineup_lookup[gid][key].append(pid)

    # Games
    games = [dict(r) for r in conn.execute(f"SELECT * FROM games WHERE game_id IN ({ph})", game_ids)]
    for g in games:
        lu = lineup_lookup.get(g["game_id"], {})
        g["home_lineup"] = lu.get("home_lineup", [])
        g["away_lineup"] = lu.get("away_lineup", [])
    if games:
        _sb_upsert("fb_games", games)
    log.info(f"    fb_games: {len(games)} rows")

    # Goals — scorer_name/assist_name/scorer_id/assist_id come from SQLite directly
    GOAL_COLS = [
        "game_id", "team_scored_id", "team_conceded_id", "team_scored_raw",
        "team_conceded_raw", "scorer_raw", "assist_raw",
        "scorer_id", "assist_id", "scorer_name", "assist_name",
        "minute", "minute_seconds", "period", "score_at_goal",
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
        goal_rows.append({c: raw.get(c) for c in GOAL_COLS})
    if goal_rows:
        _sb_upsert("fb_goals", goal_rows)
    log.info(f"    fb_goals: {len(goal_rows)} rows")

    # Penalties — player_id/player_name come from SQLite directly
    PEN_COLS = [
        "game_id", "team_id", "team_raw", "player_raw",
        "player_id", "player_name",
        "minute", "minute_seconds", "period", "duration_min", "reason",
        "date", "season", "league", "home_team_raw", "away_team_raw",
        "home_team_id", "away_team_id",
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
        pen_rows.append({c: raw.get(c) for c in PEN_COLS})
    if pen_rows:
        _sb_upsert("fb_penalties", pen_rows)
    log.info(f"    fb_penalties: {len(pen_rows)} rows")

    # Players — sync unique players from lineups for imported games
    player_rows = []
    for row in conn.execute(f"""
        SELECT player_id, player_raw, position, COUNT(*) as gp
        FROM lineups
        WHERE player_id IS NOT NULL AND game_id IN ({ph})
        GROUP BY player_id
    """, game_ids):
        player_rows.append({
            "player_id": row[0],
            "player_name": row[1],
            "position": row[2],
            "games_played": row[3],
        })
    if player_rows:
        _sb_upsert("fb_players", player_rows)
    log.info(f"    fb_players: {len(player_rows)} rows")


def run():
    log.info("=== Fast Daily Refresh ===")
    season = 2025

    cached = get_cached_games()
    log.info(f"  {len(cached)} finished games in cache (last 3 days)")

    if not cached:
        log.info("  Nothing to process.")
        return

    conn = get_db()
    new_games = [g for g in cached if not conn.execute("SELECT 1 FROM games WHERE game_id=?", (g["id"],)).fetchone()]
    log.info(f"  {len(new_games)} new games to import")

    if not new_games:
        log.info("  All games already in DB.")
        conn.close()
        return

    imported = []
    total_goals = total_pen = 0

    for g in new_games:
        gid = g["id"]
        time.sleep(SLEEP)

        # Fetch game detail from API
        detail, subtitle = fetch_game_row(gid)
        if not detail:
            log.warning(f"    Could not fetch game {gid}")
            continue

        home_id = detail["home_id"]
        away_id = detail["away_id"]
        home_name = detail["home_name"]
        away_name = detail["away_name"]
        iso_date = g["date"]

        weekday_map = {0: "Mo", 1: "Di", 2: "Mi", 3: "Do", 4: "Fr", 5: "Sa", 6: "So"}
        try:
            weekday = weekday_map.get(datetime.strptime(iso_date, "%Y-%m-%d").weekday(), "")
        except Exception:
            weekday = ""

        # Store game in SQLite
        try:
            conn.execute("""
                INSERT OR IGNORE INTO games
                  (game_id, home_team_id, away_team_id, home_team_raw, away_team_raw,
                   date, weekday, time, season, league, league_group, result, location, phase, subtitle)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (gid, home_id, away_id, home_name, away_name,
                  iso_date, weekday, detail["time"], season, g["league"],
                  detail.get("league_group"), detail["result"], detail["location"],
                  detail.get("phase", "Qualifikation"), subtitle))
            conn.commit()
        except Exception as e:
            log.warning(f"    Insert error for {gid}: {e}")
            continue

        # Fetch lineups FIRST → build name→ID map
        lineup_map = build_lineup_map(conn, gid, home_id, away_id,
                                       home_name, away_name, season, iso_date)
        conn.commit()

        # Fetch goals + penalties with ID resolution
        time.sleep(SLEEP)
        result = fetch_and_store_goals(conn, gid, home_id, away_id,
                                       home_name, away_name, iso_date, weekday, season,
                                       lineup_map=lineup_map)
        ng, np = result if isinstance(result, tuple) else (0, 0)
        conn.commit()
        total_goals += ng
        total_pen += np

        imported.append(gid)
        log.info(f"    ✓ {home_name} vs {away_name} [{iso_date}] {ng}G {np}P ({g['league']})")

    log.info(f"\n── Results ─────────────────")
    log.info(f"  Imported: {len(imported)} games, {total_goals} goals, {total_pen} penalties")

    if imported:
        log.info("\n── Syncing to Supabase…")
        sync_games_to_supabase(conn, imported)

    conn.close()
    log.info("\n✓ Done!")


if __name__ == "__main__":
    run()
