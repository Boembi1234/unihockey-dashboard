"""
fetch_lupl.py
=============
Fetches all L-UPL games + goals from the Swiss Unihockey API.

Correct API parameters (from the leagues dropdown):
  https://api-v2.swissunihockey.ch/api/games?mode=list&league=24&game_class=11&season=2025

Usage:
  pip install requests
  python fetch_lupl.py

Edit TARGET_LEAGUES and SEASONS in the CONFIG section to fetch other leagues.
All available leagues are listed at the bottom of this file.
"""

import sqlite3, requests, re, time, json, os, logging
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════
# CONFIGURATION  ← edit these
# ══════════════════════════════════════════════════════════════════════

BASE_URL  = "https://api-v2.swissunihockey.ch/api"
DB_PATH   = "swiss_floorball_lupl.db"
JSON_PATH = "data.json"
SLEEP     = 0.4

# Which leagues to fetch — add/remove entries as needed
# Full league list at the bottom of this file
TARGET_LEAGUES = [
    {"league": 24, "game_class": 11, "label": "Herren L-UPL"},
    # {"league":  3, "game_class": 11, "label": "Herren 1. Liga"},
    # {"league": 24, "game_class": 21, "label": "Damen L-UPL"},
    # {"league":  2, "game_class": 11, "label": "Herren NLB"},
]

SEASONS = [2025, 2024, 2023, 2022]  # 2021 and earlier return 400 from the API

# ══════════════════════════════════════════════════════════════════════
# LEAGUE NORMALIZATION
# ══════════════════════════════════════════════════════════════════════

LEAGUE_MAP = {
    "Herren L-UPL":                  "Herren NLA",
    "Herren Aktive KF 3. Liga":      "Herren 3. Liga",
    "Mobiliar Unihockey Cup Männer": "Mobiliar Unihockey Cup Herren",
}

def norm_league(name):
    return LEAGUE_MAP.get(name, name) if name else name

def derive_club(name):
    return re.sub(r'\s+(II|III|IV|V|VI|I|Ost|Bern)$', '', name.strip())

# ══════════════════════════════════════════════════════════════════════
# DATABASE SCHEMA
# ══════════════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS teams (
    team_id   INTEGER PRIMARY KEY,
    name      TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS games (
    game_id       TEXT PRIMARY KEY,
    home_team_id  INTEGER, away_team_id  INTEGER,
    home_team_raw TEXT,    away_team_raw TEXT,
    date TEXT, weekday TEXT, time TEXT, season INTEGER,
    league TEXT, league_group TEXT, result TEXT,
    location TEXT, location_city TEXT
);
CREATE TABLE IF NOT EXISTS goals (
    goal_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id          TEXT,
    team_scored_id   INTEGER, team_conceded_id  INTEGER,
    team_scored_raw  TEXT,    team_conceded_raw TEXT,
    scorer_raw TEXT, assist_raw TEXT,
    minute TEXT, minute_seconds INTEGER, period INTEGER,
    score_at_goal TEXT, date TEXT, weekday TEXT, season INTEGER
);
CREATE INDEX IF NOT EXISTS idx_goals_game   ON goals(game_id);
CREATE INDEX IF NOT EXISTS idx_goals_scorer ON goals(scorer_raw);
CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);
CREATE TABLE IF NOT EXISTS penalties (
    penalty_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id        TEXT,
    team_id        INTEGER, team_raw       TEXT,
    player_raw     TEXT,
    minute         TEXT,    minute_seconds INTEGER, period INTEGER,
    duration_min   INTEGER, reason         TEXT,
    date TEXT, weekday TEXT, season INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pen_game   ON penalties(game_id);
CREATE INDEX IF NOT EXISTS idx_pen_player ON penalties(player_raw);
CREATE INDEX IF NOT EXISTS idx_pen_team   ON penalties(team_id);
"""

# ══════════════════════════════════════════════════════════════════════
# API HELPERS
# ══════════════════════════════════════════════════════════════════════

SESSION = requests.Session()
SESSION.headers["Accept"] = "application/json"

def api_get(endpoint, params=None, retries=3):
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"  Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(1.5)
    return None

def unwrap(resp):
    if isinstance(resp, dict) and "data" in resp:
        return resp["data"]
    return resp or {}

def cell_text(cell, index=0):
    if not isinstance(cell, dict):
        return str(cell) if cell else ""
    t = cell.get("text", "")
    if isinstance(t, list):
        return t[index] if index < len(t) else (t[0] if t else "")
    return t or ""

_RELATIVE_DATES = {
    "heute":    0,   # today
    "gestern": -1,   # yesterday
    "morgen":   1,   # tomorrow
}

def parse_date(s):
    if not s: return None, None
    s = s.strip().split(" ")[0]   # drop time if combined "DD.MM.YYYY HH:MM"
    # Handle German relative date words
    from datetime import timedelta, date as _date
    lower = s.lower()
    if lower in _RELATIVE_DATES:
        d = _date.today() + timedelta(days=_RELATIVE_DATES[lower])
        return d.isoformat(), d.strftime("%A")
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s, fmt).date()
            return d.isoformat(), d.strftime("%A")
        except ValueError:
            pass
    return s, None

def minute_to_seconds(s):
    try:
        m, sec = s.strip().split(":")
        return int(m) * 60 + int(sec)
    except Exception:
        return None

def seconds_to_period(secs):
    if secs is None: return None
    if secs <= 1200: return 1
    if secs <= 2400: return 2
    if secs <= 3600: return 3
    return 4

def parse_scorer_assist(s):
    if not s: return None, None
    m = re.match(r"^(.*?)\s*\(([^)]+)\)\s*$", s.strip())
    return (m.group(1).strip(), m.group(2).strip()) if m else (s.strip(), None)

def team_hash(name):
    return abs(hash(name)) % (10**8) if name else None

# ══════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

def upsert_team(conn, team_id, name):
    conn.execute("INSERT OR IGNORE INTO teams(team_id, name) VALUES (?,?)", (team_id, name))

# ══════════════════════════════════════════════════════════════════════
# FETCH GAME ROWS  ← KEY FIX: correct params, no "mode" needed
# ══════════════════════════════════════════════════════════════════════

def fetch_game_rows(league, game_class, season):
    """
    GET /api/games?mode=list&league=24&game_class=11&season=2025
    The API navigates rounds via slider.prev/next using a 'round' param.
    We start at the latest round, then follow 'prev' to collect all rounds.
    """
    game_rows  = {}
    base_params = {
        "mode":       "list",
        "league":     league,
        "game_class": game_class,
        "season":     season,
    }
    visited_rounds = set()
    round_param    = None  # None = start at default (latest) round

    while True:
        params = dict(base_params)
        if round_param is not None:
            params["round"] = round_param

        raw = api_get("games", params)
        if not raw:
            break

        data  = unwrap(raw)
        found = 0

        for region in data.get("regions", []):
            for row in region.get("rows", []):
                gid = None
                for cell in row.get("cells", []):
                    link = cell.get("link") or {}
                    if link.get("page") == "game_detail":
                        ids = link.get("ids", [])
                        if ids:
                            gid = str(ids[0])
                            break
                if gid and gid not in game_rows:
                    game_rows[gid] = row
                    found += 1

        label = f"round={round_param}" if round_param else "latest"
        log.info(f"    [{label}]: {found} new games")

        # Navigate backwards through all rounds via slider.prev
        slider = data.get("slider", {})
        prev   = slider.get("prev", {}).get("set_in_context", {}).get("round")
        if prev and prev not in visited_rounds:
            visited_rounds.add(prev)
            round_param = prev
            time.sleep(SLEEP)
        else:
            break

    return game_rows

# ══════════════════════════════════════════════════════════════════════
# STORE GAME
# ══════════════════════════════════════════════════════════════════════

def _cell_link_id(cell):
    """Return first ID from a cell's link, or None."""
    link = cell.get("link") or {}
    ids  = link.get("ids", [])
    return ids[0] if ids else None

def store_game(conn, game_id, row, season, league_label):
    cells = row.get("cells", [])
    # New API layout (9 cells):
    #   0: date+time  1: location  2: home_name  3: home_logo
    #   4: "-"        5: away_logo 6: away_name  7: result    8: empty
    # Old layout (5-6 cells):
    #   0: date  1: loc  [2: league]  2/3: home  3/4: away  4/5: result
    if len(cells) >= 8:
        datetime_raw = cell_text(cells[0], 0)        # "07.03.2026 17:00"
        parts        = datetime_raw.split(" ", 1)
        date_raw     = parts[0]
        time_raw     = parts[1] if len(parts) > 1 else ""
        loc_raw      = cell_text(cells[1], 0)
        loc_city     = ""
        league_raw   = league_label
        league_grp   = ""
        home_name    = cell_text(cells[2], 0)
        home_id      = _cell_link_id(cells[2]) or team_hash(home_name)
        away_name    = cell_text(cells[6], 0)
        away_id      = _cell_link_id(cells[6]) or team_hash(away_name)
        result       = cell_text(cells[7], 0)
    elif len(cells) >= 6:
        date_raw   = cell_text(cells[0], 0)
        time_raw   = cell_text(cells[0], 1)
        loc_raw    = cell_text(cells[1], 0)
        loc_city   = cell_text(cells[1], 1)
        league_raw = cell_text(cells[2], 0) or league_label
        league_grp = cell_text(cells[2], 1)
        home_name  = cell_text(cells[3], 0)
        away_name  = cell_text(cells[4], 0)
        result     = cell_text(cells[5], 0)
        home_id    = team_hash(home_name)
        away_id    = team_hash(away_name)
    elif len(cells) >= 5:
        date_raw   = cell_text(cells[0], 0)
        time_raw   = cell_text(cells[0], 1)
        loc_raw    = cell_text(cells[1], 0)
        loc_city   = cell_text(cells[1], 1)
        league_raw = league_label
        league_grp = ""
        home_name  = cell_text(cells[2], 0)
        away_name  = cell_text(cells[3], 0)
        result     = cell_text(cells[4], 0)
        home_id    = team_hash(home_name)
        away_id    = team_hash(away_name)
    else:
        return None

    if not result or result in ("-:-", "-", ""):
        return None

    iso_date, weekday = parse_date(date_raw)
    league  = norm_league(league_raw)

    if home_name: upsert_team(conn, home_id, home_name)
    if away_name: upsert_team(conn, away_id, away_name)

    conn.execute("""
        INSERT OR IGNORE INTO games
          (game_id, home_team_id, away_team_id, home_team_raw, away_team_raw,
           date, weekday, time, season, league, league_group, result, location, location_city)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (game_id, home_id, away_id, home_name, away_name,
          iso_date, weekday, time_raw, season, league, league_grp,
          result, loc_raw, loc_city))

    return home_id, away_id, home_name, away_name, iso_date, weekday

# ══════════════════════════════════════════════════════════════════════
# FETCH + STORE GOALS
# ══════════════════════════════════════════════════════════════════════

_PEN_RE = re.compile(r"^(\d+)'-Strafe(?:\s*\(([^)]*)\))?$")

def parse_penalty(event_raw):
    """Return (duration_min, reason) or (None, None) if not a penalty."""
    m = _PEN_RE.match(event_raw.strip())
    if m:
        return int(m.group(1)), (m.group(2) or "").strip() or "Unbekannt"
    return None, None

def fetch_and_store_goals(conn, game_id, home_id, away_id,
                          home_name, away_name, game_date, weekday, season,
                          penalties_only=False):
    raw = api_get(f"game_events/{game_id}")
    if not raw:
        return 0

    stored_goals = stored_pen = 0
    for region in unwrap(raw).get("regions", []):
        for row in region.get("rows", []):
            cells = row.get("cells", [])
            if len(cells) < 4:
                continue
            minute_raw = cell_text(cells[0])
            event_raw  = cell_text(cells[1])
            team_raw   = cell_text(cells[2])
            player_raw = cell_text(cells[3])

            # ── Goals ────────────────────────────────────────────────
            if not penalties_only and "Torschütze" in event_raw:
                score_m       = re.search(r"(\d+:\d+)", event_raw)
                score_at_goal = score_m.group(1) if score_m else None
                scorer, assist = parse_scorer_assist(player_raw)
                if team_raw.strip() == home_name.strip():
                    scored_id, conceded_id = home_id, away_id
                    scored_raw, conceded_raw = home_name, away_name
                else:
                    scored_id, conceded_id = away_id, home_id
                    scored_raw, conceded_raw = away_name, home_name
                secs = minute_to_seconds(minute_raw)
                conn.execute("""
                    INSERT INTO goals
                      (game_id, team_scored_id, team_conceded_id,
                       team_scored_raw, team_conceded_raw,
                       scorer_raw, assist_raw, minute, minute_seconds,
                       period, score_at_goal, date, weekday, season)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (game_id, scored_id, conceded_id, scored_raw, conceded_raw,
                      scorer, assist, minute_raw, secs, seconds_to_period(secs),
                      score_at_goal, game_date, weekday, season))
                stored_goals += 1

            # ── Penalties ────────────────────────────────────────────
            elif "Strafe" in event_raw and "Strafenende" not in event_raw:
                duration, reason = parse_penalty(event_raw)
                if duration is None:
                    continue
                pen_team_id = home_id if team_raw.strip() == home_name.strip() else away_id
                secs = minute_to_seconds(minute_raw)
                conn.execute("""
                    INSERT INTO penalties
                      (game_id, team_id, team_raw, player_raw,
                       minute, minute_seconds, period,
                       duration_min, reason, date, weekday, season)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (game_id, pen_team_id, team_raw.strip(), player_raw.strip() or None,
                      minute_raw, secs, seconds_to_period(secs),
                      duration, reason, game_date, weekday, season))
                stored_pen += 1

    return stored_goals, stored_pen

# ══════════════════════════════════════════════════════════════════════
# BACKFILL PENALTIES for already-stored games
# ══════════════════════════════════════════════════════════════════════

def backfill_penalties(conn):
    """Fetch penalty events for every stored game that has no penalties yet."""
    all_games = conn.execute(
        "SELECT game_id, home_team_id, away_team_id, home_team_raw, away_team_raw, "
        "date, weekday, season FROM games"
    ).fetchall()
    done_ids = {r[0] for r in conn.execute("SELECT DISTINCT game_id FROM penalties")}
    todo = [g for g in all_games if g[0] not in done_ids]
    if not todo:
        log.info("  Penalties already up-to-date.")
        return
    log.info(f"  Backfilling penalties for {len(todo)} games…")
    total_pen = 0
    for i, (gid, home_id, away_id, home_name, away_name, gdate, weekday, season) in enumerate(todo, 1):
        time.sleep(SLEEP)
        _, n = fetch_and_store_goals(conn, gid, home_id, away_id,
                                     home_name, away_name, gdate, weekday, season,
                                     penalties_only=True)
        total_pen += n
        if i % 50 == 0:
            conn.commit()
            log.info(f"    {i}/{len(todo)} games processed…")
    conn.commit()
    log.info(f"  Backfill complete: {total_pen} penalties stored.")

# ══════════════════════════════════════════════════════════════════════
# EXPORT data.json (dashboard-compatible)
# ══════════════════════════════════════════════════════════════════════

def export_json(conn):
    games = [dict(r) for r in conn.execute("SELECT * FROM games ORDER BY date")]
    if not games:
        log.warning("No games to export!")
        return

    ph    = ",".join("?" * len(games))
    g_ids = [g['game_id'] for g in games]

    goals = []
    for r in conn.execute(f"""
        SELECT g.*, gm.time as game_time, gm.league, gm.league_group,
               gm.result, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id, gm.location_city
        FROM goals g JOIN games gm ON g.game_id = gm.game_id
        WHERE g.game_id IN ({ph})
    """, g_ids):
        row = dict(r)
        row['league'] = norm_league(row.get('league'))
        goals.append(row)

    # Build clubs → teams → leagues hierarchy
    teams_info = {}
    for g in games:
        for side in ('home', 'away'):
            tid  = g[f'{side}_team_id']
            name = g[f'{side}_team_raw']
            lg   = g.get('league', '')
            lgr  = g.get('league_group', '')
            if not tid or not name: continue
            teams_info.setdefault(tid, {'name': name, 'leagues': defaultdict(int)})
            teams_info[tid]['leagues'][(lg, lgr)] += 1

    # Include orphan IDs from goals
    for gol in goals:
        for id_f, name_f in [('team_scored_id','team_scored_raw'),
                              ('team_conceded_id','team_conceded_raw')]:
            tid  = gol[id_f]
            name = gol[name_f]
            lg   = gol.get('league', '')
            if not tid or not name or tid in teams_info: continue
            teams_info[tid] = {'name': name, 'leagues': defaultdict(int)}
            teams_info[tid]['leagues'][(lg, '')] += 1

    clubs_map = {}
    for tid, info in teams_info.items():
        club  = derive_club(info['name'])
        tname = info['name']
        clubs_map.setdefault(club, {})
        clubs_map[club].setdefault(tname, {'ids': set(), 'leagues': {}})
        clubs_map[club][tname]['ids'].add(tid)
        for (lg, lgr), cnt in info['leagues'].items():
            ex = clubs_map[club][tname]['leagues']
            if lg not in ex:
                ex[lg] = {'league': lg, 'league_group': lgr, 'game_count': 0}
            ex[lg]['game_count'] += cnt

    clubs_list = []
    for club_name, teams in sorted(clubs_map.items()):
        teams_list = [
            {'name': tname, 'team_ids': sorted(d['ids']),
             'leagues': sorted(d['leagues'].values(), key=lambda x: -x['game_count'])}
            for tname, d in sorted(teams.items())
        ]
        clubs_list.append({'name': club_name, 'teams': teams_list})

    # Penalties joined with game context
    penalties = []
    for r in conn.execute(f"""
        SELECT p.*, gm.league, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id, gm.result
        FROM penalties p JOIN games gm ON p.game_id = gm.game_id
        WHERE p.game_id IN ({ph})
    """, g_ids):
        row = dict(r)
        row['league'] = norm_league(row.get('league'))
        penalties.append(row)

    seasons = sorted(set(g['season'] for g in games if g.get('season')))
    data    = {'clubs': clubs_list, 'goals': goals, 'games': games,
               'penalties': penalties, 'seasons': seasons}

    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    size_mb = os.path.getsize(JSON_PATH) / 1024 / 1024
    log.info(f"\n✅  Exported to {JSON_PATH}")
    log.info(f"    Clubs:     {len(clubs_list)}")
    log.info(f"    Games:     {len(games)}")
    log.info(f"    Goals:     {len(goals)}")
    log.info(f"    Penalties: {len(penalties)}")
    log.info(f"    Seasons:   {seasons}")
    log.info(f"    Size:      {size_mb:.2f} MB")

# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def run():
    log.info("=== Swiss Floorball — League Fetcher ===")
    for lg in TARGET_LEAGUES:
        log.info(f"  {lg['label']}  (league={lg['league']}, game_class={lg['game_class']})")
    log.info(f"  Seasons: {SEASONS}")
    log.info("")

    conn = get_db()
    total_games = total_goals = total_pen = 0

    for lg_cfg in TARGET_LEAGUES:
        league     = lg_cfg["league"]
        game_class = lg_cfg["game_class"]
        label      = lg_cfg["label"]

        for season in SEASONS:
            log.info(f"\n{label}  season {season}/{season+1}…")
            game_rows = fetch_game_rows(league, game_class, season)
            log.info(f"  Found {len(game_rows)} games")

            for gid, row in game_rows.items():
                if conn.execute("SELECT 1 FROM games WHERE game_id=?", (gid,)).fetchone():
                    log.debug(f"  Skip {gid} (already stored)")
                    continue

                result = store_game(conn, gid, row, season, label)
                if result is None:
                    continue

                home_id, away_id, home_name, away_name, iso_date, weekday = result
                time.sleep(SLEEP)
                ng, np = fetch_and_store_goals(conn, gid, home_id, away_id,
                                               home_name, away_name, iso_date, weekday, season)
                conn.commit()
                total_games += 1
                total_goals += ng
                total_pen   += np
                log.info(f"  ✓ {home_name} vs {away_name}  [{iso_date}]  {ng} goals  {np} pen")

    log.info(f"\n── New games ─────────────────")
    log.info(f"  Games stored: {total_games}")
    log.info(f"  Goals stored: {total_goals}")
    log.info(f"  Penalties:    {total_pen}")
    log.info(f"\n── Backfilling penalties for existing games…")
    backfill_penalties(conn)
    export_json(conn)
    conn.close()


if __name__ == "__main__":
    run()


# ══════════════════════════════════════════════════════════════════════
# AVAILABLE LEAGUES (from Swiss Unihockey API dropdown)
# ══════════════════════════════════════════════════════════════════════
#
# To fetch a different league, add it to TARGET_LEAGUES above:
#   {"league": X, "game_class": Y, "label": "Name"}
#
# Herren L-UPL          league=24, game_class=11   ← default
# Damen L-UPL           league=24, game_class=21
# Herren NLB            league=2,  game_class=11
# Damen NLB             league=2,  game_class=21
# Herren 1. Liga        league=3,  game_class=11
# Herren 1. Liga        league=3,  game_class=12
# Damen 1. Liga         league=3,  game_class=21
# Herren 2. Liga        league=4,  game_class=11
# Herren 3. Liga        league=5,  game_class=11
# Herren 4. Liga        league=6,  game_class=11
# Herren 5. Liga        league=7,  game_class=12
# Junioren U14 A        league=13, game_class=14
# Junioren U16 A        league=13, game_class=16
# Junioren U18 A        league=13, game_class=18
# Junioren U21 A        league=13, game_class=19
# Junioren U14 B        league=14, game_class=14
# Junioren U16 B        league=14, game_class=16
# Junioren U18 B        league=14, game_class=18
# Junioren D Regional   league=12, game_class=34
# Junioren E Regional   league=12, game_class=35
# Herren Supercup       league=23, game_class=11
