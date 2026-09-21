"""Daily refresh: import finished games with lineups, goals and penalties.

Which games — two sources, both only ever yield FINISHED games:
  a. live_games_cache (last 3 days, "Spiel beendet"). The app fills it from the
     SU API's mode=current, a curated selection: top leagues, U21, cups.
  b. fb_games fixtures of the last FIXTURE_DAYS days that have a result. The
     fixtures of all leagues come from fetch_upcoming.py, the result from
     fetch_results.py (written after the final whistle only). This is what
     brings in 1.-5. Liga, KF, U14-U21 and the regional junior leagues.
     `--since YYYY-MM-DD` widens the window (backfill).

Flow:
1. Sync whatever an earlier, aborted run imported but did not sync
2. For each game not in SQLite: game row + lineups + goals/penalties, one
   transaction per game (fetch_lupl)
3. Every BATCH games: sync them to Supabase and remember that they are synced
"""
import sys, os, re, time, json, logging, argparse
from datetime import datetime, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from fetch_lupl import (
    get_db, store_game, fetch_and_store_goals,
    build_lineup_map, api_get, unwrap, SLEEP,
    SUPABASE_URL, SUPABASE_SERVICE_KEY,
    _sb_upsert, _batched, FANTASY_LEAGUES,
)
from game_result import check_result, REVIEW, NONE

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()

FIXTURE_DAYS = 7      # fixture source: how far back a normal run looks
BATCH        = 100    # games per Supabase sync

# Stats are imported for every league down to U14. Below that: no player stats.
BELOW_U14 = {"Junioren E Regional", "Juniorinnen D Regional"}

# fetch_upcoming.py names a few leagues differently from the cache. Stats have
# always been filed under the cache's names, so the fixture source maps to them
# ("Herren L-UPL" goes to Supabase as "Herren NLA", see sync_games_to_supabase).
FIXTURE_TO_STATS_LEAGUE = {
    "Herren NLA":          "Herren L-UPL",
    "Damen NLA":           "Damen L-UPL",
    "Mobiliar Cup Herren": "Mobiliar Unihockey Cup Männer",
    "Mobiliar Cup Damen":  "Mobiliar Unihockey Cup Frauen",
}

_U21_NOSPACE_RE = re.compile(r"^(Junior(?:en|innen) U\d{2})([A-D])$")


def canonical_league(name):
    """Collapse whitespace; "Junioren U21A" → "Junioren U21 A". Since 19.09.2026
    mode=current drops that space, which filed the same league under two names."""
    name = " ".join((name or "").split())
    return _U21_NOSPACE_RE.sub(r"\1 \2", name)


def wanted_league(league):
    low = (league or "").lower()
    return bool(league) and "test" not in low and league not in BELOW_U14


def season_from_date(iso_date):
    """Saison = Startjahr, so wie fb_games es seit 2005 durchgehend hält.

    Eine Saison laeuft von August bis Mai: ein Spiel im September 2026 gehoert
    zu season 2026, ein Playoff-Spiel im April 2027 ebenfalls. Nicht von der
    SU-API uebernehmen — die fuehrt ihre Saison 2025 bis in den September 2026
    hinein, wodurch der erste Spieltag der neuen Saison falsch einsortiert wird.
    """
    try:
        d = datetime.strptime(iso_date, "%Y-%m-%d")
    except (ValueError, TypeError):
        d = datetime.now()
    return d.year if d.month >= 7 else d.year - 1


def _sync_season(game):
    """Season to send to Supabase for a games row: always derived from the date.

    SQLite can hold a stale value — everything imported before 13.09.2026 was
    stored with a hard-coded 2025, and a re-sync (repair_games.py) pushed that
    into fb_games again, which dropped the game from the fantasy scoring.
    Only an unparsable date falls back to the stored season.
    """
    try:
        datetime.strptime(game["date"], "%Y-%m-%d")
    except (ValueError, TypeError):
        log.warning(f"    {game['game_id']}: unparsable date {game['date']!r} — keeping season {game['season']}")
        return game["season"]
    season = season_from_date(game["date"])
    if season != game["season"]:
        log.warning(f"    {game['game_id']}: SQLite has season {game['season']}, "
                    f"date {game['date']} says {season} — syncing {season}")
    return season


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

    seen = set()
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
                gid = str(g.get("id", ""))
                if not gid or gid in seen:
                    continue
                t = g.get("time", "")
                result = g.get("result", "")
                if "beendet" not in t.lower():
                    continue
                if not result or result in ("-:-", "-", ""):
                    continue
                seen.add(gid)
                games.append({
                    "id": gid,
                    "league": canonical_league(league_name),
                    "date": g.get("date", row.get("game_date", "")),
                })
    return games


def get_fixture_games(since):
    """Finished games from fb_games: dated `since`..today, with a result, in a
    league we keep stats for. fetch_results.py only writes final results, so a
    result means the game is over."""
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }
    today = datetime.now().strftime("%Y-%m-%d")
    games, offset, page = [], 0, 1000
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/fb_games?select=game_id,league,date"
               f"&date=gte.{since}&date=lte.{today}&result=not.is.null"
               f"&order=game_id&limit={page}&offset={offset}")
        r = SESSION.get(url, headers=headers)
        if r.status_code != 200:
            log.error(f"  fb_games read failed [{r.status_code}]: {r.text[:300]}")
            r.raise_for_status()
        rows = r.json()
        for row in rows:
            league = canonical_league(row.get("league"))
            if not wanted_league(league) or not row.get("date"):
                continue
            games.append({
                "id": str(row["game_id"]),
                "league": FIXTURE_TO_STATS_LEAGUE.get(league, league),
                "date": row["date"],
            })
        if len(rows) < page:
            return games
        offset += page


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
        if isinstance(c, str):
            return c, []
        if not isinstance(c, dict):
            return str(c) if c else None, []
        text = c.get("text", [None])[0] if c.get("text") else None
        ids = c.get("link", {}).get("ids", [])
        return text, ids

    home_name, home_ids = cell_val("home_name")
    away_name, away_ids = cell_val("away_name")
    result_text, _ = cell_val("result")
    # The whole result cell — ["0:0", "(1:4, 1:4, 1:4)"] — for check_result()
    res_idx = key_to_idx.get("result")
    res_cell = cells[res_idx] if res_idx is not None and res_idx < len(cells) else None
    result_parts = (res_cell.get("text") if isinstance(res_cell, dict) else None) or [result_text]
    time_text, _ = cell_val("time")
    location_text, _ = cell_val("location")

    if not home_name or not away_name or not result_text:
        return None, None
    if check_result(result_parts)[1] == NONE:    # "-:-": cancelled, nothing to import
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
        "result_parts": result_parts,
        "time": time_text,
        "location": location_text,
        "phase": phase,
        "league_group": league_group,
    }, subtitle


def _existing_results(game_ids):
    """{game_id: result} for the fb_games rows that already have a result."""
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }
    found = {}
    for batch in _batched([str(g) for g in game_ids], 150):
        url = (f"{SUPABASE_URL}/rest/v1/fb_games?select=game_id,result"
               f"&result=not.is.null&game_id=in.({','.join(batch)})")
        r = SESSION.get(url, headers=headers)
        if r.status_code != 200:
            log.error(f"    fb_games read failed [{r.status_code}]: {r.text[:300]}")
            r.raise_for_status()
        found.update({str(row["game_id"]): row["result"] for row in r.json()})
    return found


def _sb_delete_games(table, game_ids):
    """Delete the rows of these games from fb_goals / fb_penalties → rows deleted."""
    assert table in ("fb_goals", "fb_penalties")
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Prefer": "return=representation",
    }
    deleted = 0
    for batch in _batched([str(g) for g in game_ids], 100):
        assert batch and all(g.isdigit() for g in batch)     # never an unfiltered DELETE
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=game_id&game_id=in.({','.join(batch)})"
        r = SESSION.delete(url, headers=headers)
        if r.status_code not in (200, 204):
            log.error(f"    {table} delete failed [{r.status_code}]: {r.text[:300]}")
            r.raise_for_status()
        deleted += len(r.json()) if r.text else 0
    return deleted


def sync_games_to_supabase(conn, game_ids):
    """Push only the specified games + goals + penalties to Supabase."""
    if not SUPABASE_SERVICE_KEY or not game_ids:
        return

    # fb_goals / fb_penalties have no natural key (goal_id is not sent), so an
    # upsert cannot recognise rows it wrote before. Clearing the games first
    # makes the sync repeatable: a run that died after syncing — or the app's
    # admin import — no longer leaves every goal in there twice.
    for table in ("fb_goals", "fb_penalties"):
        n = _sb_delete_games(table, game_ids)
        if n:
            log.warning(f"    {table}: {n} rows of these games were already there — replaced")

    LEAGUE_MAP = {"Herren L-UPL": "Herren NLA", "Herren SML": "Herren NLA"}
    def nl(name): return LEAGUE_MAP.get(name, name) if name else name

    ph = ",".join("?" * len(game_ids))

    # Lineups — store player_ids, swap because API stores with reversed team_ids
    # (see LEGACY STORAGE CONVENTION in fetch_lupl.build_lineup_map)
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
    # A result that is already in fb_games stays: it may have been entered or
    # corrected by hand, and Tipps have been scored against it.
    kept_results = _existing_results(game_ids)
    # Goals and penalties reuse the game's season, so the three tables always agree.
    season_by_game = {}
    for g in games:
        kept = kept_results.get(str(g["game_id"]))
        if kept is not None:
            if kept != g.get("result"):
                log.warning(f"    {g['game_id']}: fb_games already has result {kept!r}, "
                            f"SQLite has {g.get('result')!r} — keeping fb_games")
            g["result"] = kept
        g["season"] = season_by_game[g["game_id"]] = _sync_season(g)
        lu = lineup_lookup.get(g["game_id"], {})
        g["home_lineup"] = lu.get("home_lineup", [])
        g["away_lineup"] = lu.get("away_lineup", [])
        # Gleiche Normalisierung wie bei Goals und Penalties — sonst steht in
        # fb_games "Herren L-UPL", waehrend fb_goals "Herren NLA" fuehrt.
        g["league"] = nl(g.get("league"))
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
        raw["season"] = season_by_game.get(raw["game_id"], raw.get("season"))
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
        raw["season"] = season_by_game.get(raw["game_id"], raw.get("season"))
        pen_rows.append({c: raw.get(c) for c in PEN_COLS})
    if pen_rows:
        _sb_upsert("fb_penalties", pen_rows)
    log.info(f"    fb_penalties: {len(pen_rows)} rows")

    # Players — everyone who appears in these games, but games_played is the
    # total over ALL games in SQLite. (Counting only this batch used to reset
    # career totals to 1 on every run.) Roster stand-in rows don't count.
    player_rows = []
    for row in conn.execute(f"""
        SELECT player_id, player_raw, position,
               SUM(CASE WHEN COALESCE(source, 'lineup') = 'lineup' THEN 1 ELSE 0 END) AS gp
        FROM lineups
        WHERE player_id IN (
            SELECT DISTINCT player_id FROM lineups
            WHERE player_id IS NOT NULL AND game_id IN ({ph})
        )
        GROUP BY player_id
    """, game_ids):
        player_rows.append({
            "player_id": row[0],
            "player_name": row[1],
            "position": row[2],
            "games_played": row[3],
        })
    for batch in _batched(player_rows, 500):
        _sb_upsert("fb_players", batch)
    log.info(f"    fb_players: {len(player_rows)} rows")


# ── Sync bookkeeping ─────────────────────────────────────────────────────────
# A game is imported into SQLite first and synced to Supabase afterwards. With
# a thousand games per weekend a run can die in between; `synced_games` is how
# the next run knows what is still owed to Supabase.

def ensure_sync_table(conn):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='synced_games'").fetchone()
    conn.execute("CREATE TABLE IF NOT EXISTS synced_games (game_id TEXT PRIMARY KEY, synced_at TEXT)")
    if not exists:
        # Everything imported before this table existed has been synced.
        conn.execute("INSERT OR IGNORE INTO synced_games (game_id, synced_at) "
                     "SELECT game_id, 'before 2026-09-22' FROM games")
    conn.commit()


def unsynced_games(conn):
    return [r[0] for r in conn.execute(
        "SELECT game_id FROM games WHERE game_id NOT IN (SELECT game_id FROM synced_games) "
        "ORDER BY date, game_id")]


def _mark_synced(conn, game_ids):
    now = datetime.now().isoformat(timespec="seconds")
    conn.executemany("INSERT OR REPLACE INTO synced_games (game_id, synced_at) VALUES (?, ?)",
                     [(g, now) for g in game_ids])
    conn.commit()


def sync_batch(conn, game_ids):
    """Sync and mark as synced. If the batch fails, go game by game so one bad
    game cannot hold back the rest. Returns the game ids that failed."""
    if not game_ids:
        return []
    try:
        sync_games_to_supabase(conn, game_ids)
        _mark_synced(conn, game_ids)
        return []
    except Exception as e:
        log.error(f"    batch sync failed ({e}) — retrying game by game")
    failed = []
    for gid in game_ids:
        try:
            sync_games_to_supabase(conn, [gid])
            _mark_synced(conn, [gid])
        except Exception as e:
            failed.append(gid)
            log.error(f"    {gid}: sync failed, stays unsynced for the next run — {e}")
    return failed


# ── Import ───────────────────────────────────────────────────────────────────

_WEEKDAYS = {0: "Mo", 1: "Di", 2: "Mi", 3: "Do", 4: "Fr", 5: "Sa", 6: "So"}


def import_game(conn, g):
    """Import one finished game into SQLite → (goals, penalties), or None if it
    has to be retried on the next run. Game + lineups + events are ONE
    transaction: if any step fails, the game is rolled back completely. (Before,
    the game was committed first — a failed events call left it in SQLite
    without goals, and skip-if-exists meant it was never retried.)"""
    gid = g["id"]
    time.sleep(SLEEP)

    detail, subtitle = fetch_game_row(gid)
    if not detail:
        log.warning(f"    Could not fetch game {gid}")
        return None

    home_id = detail["home_id"]
    away_id = detail["away_id"]
    home_name = detail["home_name"]
    away_name = detail["away_name"]
    iso_date = g["date"]

    # Saison pro Spiel aus dem Spieldatum ableiten, nicht global setzen.
    season = season_from_date(iso_date)

    try:
        weekday = _WEEKDAYS.get(datetime.strptime(iso_date, "%Y-%m-%d").weekday(), "")
    except Exception:
        weekday = ""

    # Same rule as fetch_upcoming.py — without it the sync would overwrite the
    # city that is already in fb_games with NULL.
    location = detail["location"]
    location_city = location.split()[-1] if location else None

    # Fantasy leagues: if swiss unihockey hasn't published a lineup, use the
    # team roster + fantasy pool instead, so goals resolve and the players
    # count as played.
    roster_fb = g["league"] in FANTASY_LEAGUES

    try:
        conn.execute("""
            INSERT OR IGNORE INTO games
              (game_id, home_team_id, away_team_id, home_team_raw, away_team_raw,
               date, weekday, time, season, league, league_group, result, location,
               location_city, phase, subtitle)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (gid, home_id, away_id, home_name, away_name,
              iso_date, weekday, detail["time"], season, g["league"],
              detail.get("league_group"), detail["result"], location,
              location_city, detail.get("phase", "Qualifikation"), subtitle))

        # Fetch lineups FIRST → build name→ID map
        lineup_report = {}
        lineup_map = build_lineup_map(conn, gid, home_id, away_id,
                                      home_name, away_name, season, iso_date,
                                      roster_fallback=roster_fb, report=lineup_report)

        if roster_fb and min(s["total"] for s in lineup_report.values()) == 0:
            conn.rollback()
            log.warning(f"    {gid}: no lineup AND no roster for one side — rolled back, retry next run")
            return None

        # Fetch goals + penalties with ID resolution
        time.sleep(SLEEP)
        result = fetch_and_store_goals(conn, gid, home_id, away_id,
                                       home_name, away_name, iso_date, weekday, season,
                                       lineup_map=lineup_map)
        if not isinstance(result, tuple):
            conn.rollback()
            log.warning(f"    {gid}: game events fetch failed — rolled back, retry next run")
            return None

        # A 0:0 from the API is never taken over (see game_result.py — 1096439
        # came as 0:0 with 15 goals). The game and its goals are kept, the
        # result stays open; fetch_results.py reports it until it is entered.
        _, res_status, res_note = check_result(detail["result_parts"])
        if res_status == REVIEW:
            conn.execute("UPDATE games SET result = NULL WHERE game_id = ?", (gid,))
            log.warning(f"    {gid}: result NOT taken over — {res_note} "
                        f"({result[0]} goals imported). Enter it by hand in fb_games.")

        conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning(f"    Import error for {gid}: {e}")
        return None

    ng, np = result
    lu = " / ".join(
        f"{s['lineup']}" + (f"+{s['roster']}r" if s["roster"] else "") + (f"+{s['pool']}p" if s["pool"] else "")
        for s in (lineup_report.get("home"), lineup_report.get("away")) if s)
    log.info(f"    ✓ {home_name} vs {away_name} [{iso_date}] {ng}G {np}P lineup {lu} "
             f"({g['league']}, season {season})")
    return ng, np


def run(since=None, dry_run=False, max_minutes=0):
    started = time.monotonic()
    log.info(f"=== Daily Refresh{' (DRY RUN — nothing is imported or written)' if dry_run else ''} ===")
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set")
        sys.exit(1)

    conn = get_db()
    ensure_sync_table(conn)

    # 1. What an aborted run still owes Supabase
    owed = unsynced_games(conn)
    sync_failed = []
    if owed:
        log.info(f"  {len(owed)} games imported earlier but not synced yet")
        if not dry_run:
            for batch in _batched(owed, BATCH):
                sync_failed += sync_batch(conn, batch)

    # 2. Candidates: cache first, its league names win for games in both sources
    cached = get_cached_games()
    log.info(f"  {len(cached)} finished games in cache (last 3 days)")
    if not since:
        since = (datetime.now() - timedelta(days=FIXTURE_DAYS)).strftime("%Y-%m-%d")
    fixtures = get_fixture_games(since)
    log.info(f"  {len(fixtures)} finished games in fb_games since {since} (leagues down to U14)")

    seen, candidates = set(), []
    for g in cached + fixtures:
        if g["id"] not in seen:
            seen.add(g["id"])
            candidates.append(g)
    new_games = [g for g in candidates
                 if not conn.execute("SELECT 1 FROM games WHERE game_id=?", (g["id"],)).fetchone()]
    new_games.sort(key=lambda g: (g["date"], g["id"]))
    log.info(f"  {len(new_games)} new games to import")

    by_league = {}
    for g in new_games:
        by_league[g["league"]] = by_league.get(g["league"], 0) + 1
    for league, n in sorted(by_league.items(), key=lambda x: (-x[1], x[0])):
        log.info(f"    {n:>5}  {league}")

    if dry_run or not new_games:
        if not new_games:
            log.info("  All games already in DB.")
        conn.close()
        if sync_failed:
            sys.exit(1)
        return

    # 3. Import, syncing every BATCH games
    imported, batch = 0, []
    total_goals = total_pen = skipped = 0
    out_of_time = False

    for g in new_games:
        if max_minutes and (time.monotonic() - started) > max_minutes * 60:
            out_of_time = True
            break
        result = import_game(conn, g)
        if result is None:
            skipped += 1
            continue
        total_goals += result[0]
        total_pen += result[1]
        imported += 1
        batch.append(g["id"])
        if len(batch) >= BATCH:
            log.info(f"\n── Syncing {len(batch)} games to Supabase ({imported}/{len(new_games)} imported)…")
            sync_failed += sync_batch(conn, batch)
            batch = []

    if batch:
        log.info(f"\n── Syncing {len(batch)} games to Supabase…")
        sync_failed += sync_batch(conn, batch)

    log.info(f"\n── Results ─────────────────")
    log.info(f"  Imported: {imported} games, {total_goals} goals, {total_pen} penalties"
             f" ({skipped} skipped, retried next run)")
    if out_of_time:
        left = len(new_games) - imported - skipped
        log.warning(f"  Stopped after {max_minutes} minutes — {left} games left for the next run")
    conn.close()

    if sync_failed:
        log.error(f"  {len(sync_failed)} games could not be synced: {sync_failed[:20]}")
        sys.exit(1)
    log.info("\n✓ Done!")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Import finished games with lineups, goals and penalties")
    ap.add_argument("--since", default="", help="fixture source from this date on (YYYY-MM-DD), "
                                                f"default: the last {FIXTURE_DAYS} days")
    ap.add_argument("--dry-run", action="store_true", help="only list what would be imported")
    ap.add_argument("--max-minutes", type=int, default=0,
                    help="stop importing after this many minutes (0 = no limit); the rest follows next run")
    args = ap.parse_args()
    if args.since:
        try:
            datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"--since must be YYYY-MM-DD, got {args.since!r}")
    run(since=args.since or None, dry_run=args.dry_run, max_minutes=args.max_minutes)
