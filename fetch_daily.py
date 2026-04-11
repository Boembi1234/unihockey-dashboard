"""Fast daily refresh: fetch game details for recently finished games.

Reads game IDs from the Supabase live_games_cache (last 3 days),
then fetches goals/lineups/penalties for each game not already in the DB.

Typical runtime: < 1 minute.
"""
import sys, os, time, json, logging
from datetime import datetime, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from fetch_lupl import (
    get_db, fetch_and_store_goals, fetch_and_store_lineup,
    backfill_game_phases, build_name_map,
    api_get, unwrap, SLEEP, SUPABASE_URL, SUPABASE_SERVICE_KEY,
    _sb_upsert, _sb_headers, _batched, SCHEMA,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()


def get_recent_game_ids():
    """Read finished game IDs from Supabase live_games_cache (last 3 days)."""
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set")
        return []

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }

    cutoff = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    url = f"{SUPABASE_URL}/rest/v1/live_games_cache?select=game_date,payload&game_date=gte.{cutoff}"
    r = SESSION.get(url, headers=headers)
    if r.status_code != 200:
        log.error(f"Failed to fetch live_games_cache: {r.status_code}")
        return []

    games = []
    for row in r.json():
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not payload:
            continue
        for league in payload.get("leagues", []):
            league_name = league.get("league", "")
            for g in league.get("games", []):
                result = g.get("result", "")
                if result and result not in ("-:-", "-", ""):
                    games.append({
                        "id": str(g["id"]),
                        "league": league_name,
                        "date": g.get("date", row.get("game_date", "")),
                        "homeTeam": g.get("homeTeam", ""),
                        "awayTeam": g.get("awayTeam", ""),
                        "result": result,
                    })
    return games


def fetch_game_detail(game_id):
    """Fetch game detail from API to get team IDs, date, location etc."""
    raw = api_get(f"games/{game_id}", {})
    if not raw:
        return None
    return unwrap(raw)


def store_game_from_detail(conn, gid, detail, league_label, season):
    """Parse game detail API response and store in SQLite."""
    # The detail response has attribute list with game info
    attrs = {}
    for region in detail.get("regions", []):
        for row in region.get("rows", []):
            cells = row.get("cells", [])
            if len(cells) >= 2:
                key = cells[0].get("text", [""])[0].strip() if cells[0].get("text") else ""
                val = cells[1].get("text", [""])[0].strip() if cells[1].get("text") else ""
                link = cells[1].get("link", {})
                if key:
                    attrs[key] = {"text": val, "link": link, "ids": link.get("ids", [])}

    # Try to get team info from tabs/context
    context = detail.get("context", {})
    tabs = detail.get("tabs", [])

    # Extract teams from the game detail
    home_name = ""
    away_name = ""
    home_id = 0
    away_id = 0
    result = ""
    date_str = ""
    time_str = ""
    location = ""

    # Parse from title which often has "Home vs Away"
    title = detail.get("title", "")

    # Try to get from the game list API instead (more reliable)
    raw2 = api_get("games", {"mode": "list", "game_id": gid})
    if raw2:
        data2 = unwrap(raw2)
        for region in data2.get("regions", []):
            for row in region.get("rows", []):
                cells = row.get("cells", [])
                row_gid = None
                for cell in cells:
                    link = cell.get("link", {})
                    if link.get("page") == "game_detail":
                        ids = link.get("ids", [])
                        if ids:
                            row_gid = str(ids[0])
                if row_gid == gid:
                    # Found our game row — use store_game from fetch_lupl
                    from fetch_lupl import store_game
                    return store_game(conn, gid, row, season, league_label)

    return None


def sync_new_to_supabase(conn, game_ids):
    """Sync only the specified game IDs to Supabase."""
    if not SUPABASE_SERVICE_KEY or not game_ids:
        return

    from fetch_lupl import norm_league
    LEAGUE_MAP_LOCAL = {"Herren L-UPL": "Herren NLA", "Herren SML": "Herren NLA"}
    def nl(name): return LEAGUE_MAP_LOCAL.get(name, name) if name else name

    # Load name map
    cols = {r[1] for r in conn.execute("PRAGMA table_info(name_map)")}
    abbrev_team_to_full = {}
    if "team_raw" in cols:
        for abbrev, team, full in conn.execute(
            "SELECT abbrev_name, team_raw, full_name FROM name_map"
        ):
            abbrev_team_to_full[(abbrev, team)] = full

    def resolve(abbrev, team_raw):
        if not abbrev: return None
        return (abbrev_team_to_full.get((abbrev, team_raw))
                or abbrev_team_to_full.get((abbrev, None))
                or abbrev)

    placeholders = ",".join("?" * len(game_ids))

    # Lineup lookup
    lineup_lookup = {}
    for gid, tid, player, is_home in conn.execute(
        f"SELECT l.game_id, l.team_id, l.player_raw, "
        f"  CASE WHEN l.team_id = g.home_team_id THEN 1 ELSE 0 END as is_home "
        f"FROM lineups l JOIN games g ON l.game_id = g.game_id "
        f"WHERE g.game_id IN ({placeholders})", game_ids
    ):
        if gid not in lineup_lookup:
            lineup_lookup[gid] = {"home_lineup": [], "away_lineup": []}
        key = "home_lineup" if is_home else "away_lineup"
        lineup_lookup[gid][key].append(player)

    # Games
    games = [dict(r) for r in conn.execute(
        f"SELECT * FROM games WHERE game_id IN ({placeholders})", game_ids
    )]
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
        WHERE gm.game_id IN ({placeholders})
    """, game_ids):
        raw = dict(r)
        raw["league"] = nl(raw.get("league"))
        t = raw.get("team_scored_raw", "")
        raw["scorer_name"] = resolve(raw.get("scorer_raw"), t)
        raw["assist_name"] = resolve(raw.get("assist_raw"), t)
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
        WHERE gm.game_id IN ({placeholders})
    """, game_ids):
        raw = dict(r)
        raw["league"] = nl(raw.get("league"))
        raw["player_name"] = resolve(raw.get("player_raw"), raw.get("team_raw", ""))
        pen_rows.append({c: raw.get(c) for c in PEN_COLS})
    if pen_rows:
        _sb_upsert("fb_penalties", pen_rows)
    log.info(f"    fb_penalties: {len(pen_rows)} rows")


def run():
    log.info("=== Fast Daily Refresh ===")

    # Step 1: Get recently finished game IDs from Supabase cache
    recent = get_recent_game_ids()
    log.info(f"  Found {len(recent)} finished games in live cache (last 3 days)")

    if not recent:
        log.info("  Nothing to process.")
        return

    conn = get_db()

    # Step 2: Filter to games not already in DB
    new_games = []
    for g in recent:
        if not conn.execute("SELECT 1 FROM games WHERE game_id=?", (g["id"],)).fetchone():
            new_games.append(g)

    log.info(f"  {len(new_games)} new games to import")

    if not new_games:
        log.info("  All games already in DB. Done.")
        conn.close()
        return

    # Step 3: Fetch details for each new game
    total_goals = total_pen = 0
    imported_ids = []
    season = 2025

    for g in new_games:
        gid = g["id"]
        log.info(f"\n  Fetching {g['homeTeam']} vs {g['awayTeam']} ({g['league']})…")

        # Use the game events API to get the game row for store_game
        time.sleep(SLEEP)
        result = store_game_from_detail(conn, gid, {}, g["league"], season)
        if result is None:
            log.warning(f"    Could not store game {gid}")
            continue

        home_id, away_id, home_name, away_name, iso_date, weekday = result
        time.sleep(SLEEP)
        ng, np = fetch_and_store_goals(conn, gid, home_id, away_id,
                                       home_name, away_name, iso_date, weekday, season)
        conn.commit()
        total_goals += ng
        total_pen += np

        # Fetch lineups
        time.sleep(SLEEP)
        for is_home, tid, tname in [(0, away_id, away_name), (1, home_id, home_name)]:
            time.sleep(SLEEP)
            fetch_and_store_lineup(conn, gid, tid, is_home, tname, season, iso_date)
        conn.commit()
        imported_ids.append(gid)
        log.info(f"    ✓ {home_name} vs {away_name} [{iso_date}] {ng}G {np}P")

    log.info(f"\n── Results ─────────────────")
    log.info(f"  Imported: {len(imported_ids)} games")
    log.info(f"  Goals:    {total_goals}")
    log.info(f"  Penalties:{total_pen}")

    if imported_ids:
        log.info("\n── Building name map…")
        conn.execute("DELETE FROM name_map")
        conn.commit()
        build_name_map(conn)

        log.info("\n── Syncing new games to Supabase…")
        sync_new_to_supabase(conn, imported_ids)

    conn.close()
    log.info("\n✓ Done!")


if __name__ == "__main__":
    run()
