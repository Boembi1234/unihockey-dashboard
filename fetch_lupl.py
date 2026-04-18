
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

# Supabase — set SUPABASE_SERVICE_KEY as an environment variable or paste here
SUPABASE_URL         = "https://ibqwotgrzgrwvejtphnh.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SLEEP     = 0.4

# Which leagues to fetch — add/remove entries as needed
# Full league list at the bottom of this file
# ── TEST MODE: L-UPL + NLB + 1. Liga Gruppe 2 (2025). Restore FULL_LEAGUES below for full import. ──
TARGET_LEAGUES = [
    # ── Damen L-UPL / NLA full historical import ──
    {"league": 24, "game_class": 21, "label": "Damen L-UPL", "seasons": [2025, 2024, 2023, 2022]},
    {"league":  1, "game_class": 21, "label": "Damen NLA",
     "seasons": [2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013]},
    {"league": 10, "game_class": 21, "label": "Damen NLA",
     "seasons": [2012, 2011, 2010, 2009, 2008, 2007]},
    {"league":  1, "game_class": 21, "label": "Damen NLA", "seasons": [2006, 2005]},
]

ALL_LEAGUES = [
    # ══════════════════════════════════════════════════════════════════
    # Current season only — restore to TARGET_LEAGUES after test
    # ══════════════════════════════════════════════════════════════════

    # ── Herren Grossfeld ──
    {"league": 24, "game_class": 11, "label": "Herren L-UPL", "seasons": [2025]},
    {"league":  2, "game_class": 11, "label": "Herren NLB", "seasons": [2025]},
    {"league":  3, "game_class": 11, "label": "Herren 1. Liga", "seasons": [2025]},
    {"league":  4, "game_class": 11, "label": "Herren 2. Liga", "seasons": [2025]},
    {"league":  5, "game_class": 11, "label": "Herren 3. Liga", "seasons": [2025]},
    {"league":  6, "game_class": 11, "label": "Herren 4. Liga", "seasons": [2025]},

    # ── Herren Kleinfeld ──
    {"league":  3, "game_class": 12, "label": "Herren 1. Liga KF", "seasons": [2025]},
    {"league":  4, "game_class": 12, "label": "Herren 2. Liga KF", "seasons": [2025]},

    # ── Damen Grossfeld ──
    {"league": 24, "game_class": 21, "label": "Damen L-UPL", "seasons": [2025]},
    {"league":  2, "game_class": 21, "label": "Damen NLB", "seasons": [2025]},
    {"league":  3, "game_class": 21, "label": "Damen 1. Liga", "seasons": [2025]},


    # ── Damen Kleinfeld ──
    {"league":  3, "game_class": 22, "label": "Damen 1. Liga KF", "seasons": [2025]},

    # ── Junioren A (national) ──
    {"league": 13, "game_class": 19, "label": "Junioren U21 A", "seasons": [2025]},
    {"league": 13, "game_class": 18, "label": "Junioren U18 A", "seasons": [2025]},
    {"league": 13, "game_class": 16, "label": "Junioren U16 A", "seasons": [2025]},
    {"league": 13, "game_class": 14, "label": "Junioren U14 A", "seasons": [2025]},

    # ── Junioren B ──
    {"league": 14, "game_class": 19, "label": "Junioren U21 B", "seasons": [2025]},
    {"league": 14, "game_class": 18, "label": "Junioren U18 B", "seasons": [2025]},
    {"league": 14, "game_class": 16, "label": "Junioren U16 B", "seasons": [2025]},
    {"league": 14, "game_class": 14, "label": "Junioren U14 B", "seasons": [2025]},

    # ── Juniorinnen ──
    {"league": 13, "game_class": 26, "label": "Juniorinnen U21 A", "seasons": [2025]},
    {"league": 13, "game_class": 28, "label": "Juniorinnen U17 A", "seasons": [2025]},
]

# FULL_LEAGUES — uncomment and assign to TARGET_LEAGUES for full import:
# TARGET_LEAGUES = [
#     {"league": 24, "game_class": 11, "label": "Herren L-UPL", "seasons": [2025, 2024, 2023, 2022]},
#     {"league":  1, "game_class": 11, "label": "Herren NLA",
#      "seasons": [2021,2020,2019,2018,2017,2016,2015,2014,2013,
#                  2006,2005,2004,2003,2002,2001,2000,1999,1998,1997]},
#     {"league": 10, "game_class": 11, "label": "Herren NLA",
#      "seasons": [2012,2011,2010,2009,2008,2007]},
#     {"league":  2, "game_class": 11, "label": "Herren NLB",
#      "seasons": [2025,2024,2023,2022,2021,2020,2019,2018,2017,2016,
#                  2015,2014,2013,2012,2011,2010,2009,2008,2007,2006,
#                  2005,2004,2003,2002,2001,2000,1999,1998,1997]},
#     {"league": 3, "game_class": 11, "group": "Gruppe 1", "label": "Herren 1. Liga",
#      "seasons": [2025, 2024, 2023, 2022, 2021, 2020, 2019]},
#     {"league": 3, "game_class": 11, "group": "Gruppe 2", "label": "Herren 1. Liga",
#      "seasons": [2025, 2024, 2023, 2022, 2021, 2020, 2019]},
# ]

SEASONS = []  # per-league seasons defined above

# ══════════════════════════════════════════════════════════════════════
# LEAGUE NORMALIZATION
# ══════════════════════════════════════════════════════════════════════

LEAGUE_MAP = {
    "Herren L-UPL":                  "Herren NLA",
    "Herren SML":                    "Herren NLA",
    "Herren Aktive KF 3. Liga":      "Herren 3. Liga",
    "Mobiliar Unihockey Cup Männer": "Mobiliar Unihockey Cup Herren",
}

def norm_league(name):
    return LEAGUE_MAP.get(name, name) if name else name

_CLUB_ALIASES = {
    "Waldkirch-St. Gallen": "WASA St. Gallen",
}

def derive_club(name):
    name = name.strip()
    name = re.sub(r'\s+(II|III|IV|V|VI|I|Ost|Bern)$', '', name)
    return _CLUB_ALIASES.get(name, name)

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
    location TEXT, location_city TEXT,
    phase TEXT DEFAULT 'Qualifikation',
    subtitle TEXT
);
CREATE TABLE IF NOT EXISTS goals (
    goal_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id          TEXT,
    team_scored_id   INTEGER, team_conceded_id  INTEGER,
    team_scored_raw  TEXT,    team_conceded_raw TEXT,
    scorer_raw TEXT, assist_raw TEXT,
    scorer_id  INTEGER, assist_id  INTEGER,
    scorer_name TEXT, assist_name TEXT,
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
    player_id      INTEGER, player_name    TEXT,
    minute         TEXT,    minute_seconds INTEGER, period INTEGER,
    duration_min   INTEGER, reason         TEXT,
    date TEXT, weekday TEXT, season INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pen_game   ON penalties(game_id);
CREATE INDEX IF NOT EXISTS idx_pen_player ON penalties(player_raw);
CREATE INDEX IF NOT EXISTS idx_pen_team   ON penalties(team_id);
CREATE TABLE IF NOT EXISTS lineups (
    lineup_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id       TEXT,
    team_id       INTEGER,
    team_raw      TEXT,
    player_raw    TEXT,
    player_id     INTEGER,
    jersey_number TEXT,
    position      TEXT,
    season        INTEGER,
    date          TEXT,
    UNIQUE(game_id, team_id, player_raw)
);
CREATE INDEX IF NOT EXISTS idx_lin_player ON lineups(player_raw);
CREATE INDEX IF NOT EXISTS idx_lin_game   ON lineups(game_id);
CREATE TABLE IF NOT EXISTS name_map (
    abbrev_name TEXT NOT NULL,
    team_raw    TEXT NOT NULL,
    full_name   TEXT NOT NULL,
    PRIMARY KEY (abbrev_name, team_raw)
);
CREATE TABLE IF NOT EXISTS player_statistics_cache (
    pid        INTEGER PRIMARY KEY,
    stats_json TEXT    NOT NULL,
    fetched_at TEXT    NOT NULL
);
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

def fetch_player_statistics(pid):
    """Fetch official per-season statistics for a player (all leagues)."""
    raw = api_get(f"players/{pid}/statistics")
    if not raw:
        return []
    rows = []
    for region in unwrap(raw).get("regions", []):
        for row in region.get("rows", []):
            cells = row.get("cells", [])
            if len(cells) < 7:
                continue
            games_str = cell_text(cells[3])
            try:
                games = int(games_str)
            except (ValueError, TypeError):
                continue  # skip header rows
            rows.append({
                "season":  cell_text(cells[0]),
                "liga":    cell_text(cells[1]),
                "verein":  cell_text(cells[2]),
                "games":   games,
                "goals":   int(cell_text(cells[4]) or 0),
                "assists": int(cell_text(cells[5]) or 0),
                "points":  int(cell_text(cells[6]) or 0),
            })
    return rows


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
    # Migrate name_map if it uses the old single-column primary key schema
    cols = {r[1] for r in conn.execute("PRAGMA table_info(name_map)")}
    if cols and 'team_raw' not in cols:
        log.info("  Migrating name_map to team-aware schema (dropping old table)…")
        conn.execute("DROP TABLE IF EXISTS name_map")
        conn.commit()
    conn.executescript(SCHEMA)
    conn.commit()
    # Migrate: add new columns to existing tables (safe if already present)
    migrations = [
        ("goals", "scorer_id", "INTEGER"),
        ("goals", "assist_id", "INTEGER"),
        ("goals", "scorer_name", "TEXT"),
        ("goals", "assist_name", "TEXT"),
        ("penalties", "player_id", "INTEGER"),
        ("penalties", "player_name", "TEXT"),
        ("games", "subtitle", "TEXT"),
    ]
    for table, col, typ in migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
        except Exception:
            pass  # column already exists
    # Create indexes on new columns (safe after migration)
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_goals_scorer_id ON goals(scorer_id)",
        "CREATE INDEX IF NOT EXISTS idx_goals_assist_id ON goals(assist_id)",
    ]:
        try:
            conn.execute(idx_sql)
        except Exception:
            pass
    conn.commit()
    return conn

def upsert_team(conn, team_id, name):
    conn.execute("INSERT OR IGNORE INTO teams(team_id, name) VALUES (?,?)", (team_id, name))

# ══════════════════════════════════════════════════════════════════════
# FETCH GAME ROWS  ← KEY FIX: correct params, no "mode" needed
# ══════════════════════════════════════════════════════════════════════

def discover_groups(league, game_class, season):
    """Discover available groups from the API tabs structure.
    Makes ONE API call and reads the nested tab entries to find all groups."""
    raw = api_get("games", {
        "mode": "list",
        "league": league,
        "game_class": game_class,
        "season": season,
    })
    if not raw:
        return []
    data = unwrap(raw)

    # Look through tabs → entries → sub-entries for matching league/game_class
    for tab in data.get("tabs", []):
        for entry in tab.get("entries", []):
            ctx = entry.get("set_in_context", {})
            if ctx.get("league") == league and ctx.get("game_class") == game_class:
                # Found our league — extract group names from sub-entries
                groups = []
                for sub in entry.get("entries", []):
                    g = sub.get("set_in_context", {}).get("group")
                    if g and g not in ("Nachtragsspiele", "Spielfortführung"):
                        groups.append(g)
                return groups

    # Fallback: check context for current group
    ctx = data.get("context", {})
    if ctx.get("group"):
        return [ctx["group"]]

    return []


def fetch_game_rows(league, game_class, season, group=None):
    """
    GET /api/games?mode=list&league=24&game_class=11&season=2025[&group=Gruppe+1]
    The API navigates rounds via slider.prev/next using a 'round' param.
    We start at the latest round, then follow 'prev' to collect all rounds.

    Returns (game_rows, game_region):
      game_rows   — {gid: row}
      game_region — {gid: region_title}  e.g. "Gruppe 1" / "Gruppe 2"
    """
    game_rows   = {}
    game_region = {}  # gid → region title (e.g. "Gruppe 1")
    base_params = {
        "mode":       "list",
        "league":     league,
        "game_class": game_class,
        "season":     season,
    }
    if group:
        base_params["group"] = group
    visited_rounds = set()
    round_param    = None  # None = start at default (latest) round

    while True:
        params = dict(base_params)
        if round_param is not None:
            params["round"] = round_param

        raw = api_get("games", params)
        if not raw:
            break

        data    = unwrap(raw)
        regions = data.get("regions", [])
        found   = 0

        for i, region in enumerate(regions, 1):
            # Use the region title when present; fall back to "Gruppe N" when
            # there are multiple regions (e.g. 1. Liga has Gruppe 1 / Gruppe 2).
            region_title = (region.get("title") or region.get("text") or "").strip()
            if not region_title and len(regions) > 1:
                region_title = f"Gruppe {i}"

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
                    game_rows[gid]   = row
                    game_region[gid] = region_title
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

    # Also navigate forward from the latest round to catch playout/relegation rounds
    round_param = None  # reset to latest
    while True:
        params = dict(base_params)
        if round_param is not None:
            params["round"] = round_param

        raw = api_get("games", params)
        if not raw:
            break

        data    = unwrap(raw)
        regions = data.get("regions", [])
        found   = 0

        for i, region in enumerate(regions, 1):
            region_title = (region.get("title") or region.get("text") or "").strip()
            if not region_title and len(regions) > 1:
                region_title = f"Gruppe {i}"

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
                    game_rows[gid]   = row
                    game_region[gid] = region_title
                    found += 1

        if found > 0:
            label = f"round={round_param}" if round_param else "latest"
            log.info(f"    [{label} fwd]: {found} new games")

        slider = data.get("slider", {})
        nxt    = slider.get("next", {}).get("set_in_context", {}).get("round")
        if nxt and nxt not in visited_rounds:
            visited_rounds.add(nxt)
            round_param = nxt
            time.sleep(SLEEP)
        else:
            break

    return game_rows, game_region

# ══════════════════════════════════════════════════════════════════════
# STORE GAME
# ══════════════════════════════════════════════════════════════════════

def _cell_link_id(cell):
    """Return first ID from a cell's link, or None."""
    link = cell.get("link") or {}
    ids  = link.get("ids", [])
    return ids[0] if ids else None

def store_game(conn, game_id, row, season, league_label, league_group_override=None):
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

    # Allow caller to supply a league_group (e.g. "Gruppe 1" from region title)
    if league_group_override:
        league_grp = league_group_override

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
                          penalties_only=False, lineup_map=None):
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

                # Resolve to player IDs via lineup map
                scorer_name, scorer_pid = resolve_player(lineup_map, scorer, scored_raw)
                assist_name, assist_pid = resolve_player(lineup_map, assist, scored_raw)

                secs = minute_to_seconds(minute_raw)
                conn.execute("""
                    INSERT INTO goals
                      (game_id, team_scored_id, team_conceded_id,
                       team_scored_raw, team_conceded_raw,
                       scorer_raw, assist_raw,
                       scorer_id, assist_id, scorer_name, assist_name,
                       minute, minute_seconds,
                       period, score_at_goal, date, weekday, season)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (game_id, scored_id, conceded_id, scored_raw, conceded_raw,
                      scorer, assist,
                      scorer_pid, assist_pid, scorer_name, assist_name,
                      minute_raw, secs, seconds_to_period(secs),
                      score_at_goal, game_date, weekday, season))
                stored_goals += 1

            # ── Penalties ────────────────────────────────────────────
            elif "Strafe" in event_raw and "Strafenende" not in event_raw:
                duration, reason = parse_penalty(event_raw)
                if duration is None:
                    continue
                pen_team_id = home_id if team_raw.strip() == home_name.strip() else away_id
                pen_team_raw = team_raw.strip()
                pen_player = player_raw.strip() or None

                # Resolve penalty player to ID
                pen_full_name, pen_pid = resolve_player(lineup_map, pen_player, pen_team_raw)

                secs = minute_to_seconds(minute_raw)
                conn.execute("""
                    INSERT INTO penalties
                      (game_id, team_id, team_raw, player_raw,
                       player_id, player_name,
                       minute, minute_seconds, period,
                       duration_min, reason, date, weekday, season)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (game_id, pen_team_id, pen_team_raw, pen_player,
                      pen_pid, pen_full_name,
                      minute_raw, secs, seconds_to_period(secs),
                      duration, reason, game_date, weekday, season))
                stored_pen += 1

    return stored_goals, stored_pen

# ══════════════════════════════════════════════════════════════════════
# BACKFILL PENALTIES for already-stored games
# ══════════════════════════════════════════════════════════════════════

def phase_from_label(label):
    """Map a round slider label to a human-readable phase name."""
    if not label:
        return 'Qualifikation'
    low = label.lower()
    if 'superfinal' in low:
        return 'Superfinal'
    if '1/2' in low and ('play' in low or 'final' in low):
        return 'Halbfinal'
    if ('halb' in low and 'final' in low):
        return 'Halbfinal'
    if '1/4' in low and ('play' in low or 'final' in low):
        return 'Viertelfinal'
    if 'viertel' in low and 'final' in low:
        return 'Viertelfinal'
    if 'playout' in low or 'play-out' in low:
        return 'Playout'
    if 'abstieg' in low:
        return 'Playout'
    if 'playoff' in low or 'play-off' in low or 'final' in low:
        return 'Playoff'
    return 'Qualifikation'


def backfill_game_phases(conn):
    """Traverse all rounds for every stored league+season and write the phase column."""
    # Ensure column exists (for DBs created before this change)
    try:
        conn.execute("ALTER TABLE games ADD COLUMN phase TEXT DEFAULT 'Qualifikation'")
        conn.commit()
        log.info("  Added 'phase' column to games table.")
    except Exception:
        pass  # column already exists

    # Build set of (league, season) combos that already have playoff games tagged
    already_done = set()
    rows = conn.execute(
        "SELECT DISTINCT league, season FROM games WHERE phase != 'Qualifikation' AND phase IS NOT NULL AND phase != ''"
    ).fetchall()
    for r in rows:
        already_done.add((r[0], r[1]))
    if already_done:
        log.info(f"  Phases: {len(already_done)} league/season combos already have playoff tags; skipping those.")

    # Use TARGET_LEAGUES config to know which league+season combos to traverse
    updated = 0
    for lg_cfg in TARGET_LEAGUES:
        league_id  = lg_cfg['league']
        game_class = lg_cfg['game_class']
        label_name = lg_cfg['label']
        for season in lg_cfg.get('seasons', SEASONS):
            # Skip seasons we already tagged (use label_name as stored in DB)
            if (label_name, season) in already_done or (norm_league(label_name), season) in already_done:
                log.info(f"  Phases: {label_name} {season} already tagged, skipping.")
                continue

            group = lg_cfg.get('group')
            log.info(f"  Phases: league={league_id} ({label_name}{' '+group if group else ''}) season={season}…")
            base_params = {'mode': 'list', 'league': league_id,
                           'game_class': game_class, 'season': season}
            if group:
                base_params['group'] = group

            # BFS over all rounds: follow both prev AND next to handle APIs
            # that return round 1 by default (old seasons) or last round (recent seasons)
            visited = set()
            queue   = [None]  # None = default starting round

            while queue:
                round_param = queue.pop(0)
                visit_key   = round_param if round_param is not None else '__default__'
                if visit_key in visited:
                    continue
                visited.add(visit_key)

                params = dict(base_params)
                if round_param is not None:
                    params['round'] = round_param

                raw = api_get('games', params)
                if not raw:
                    continue

                data   = unwrap(raw)
                slider = data.get('slider', {})
                label  = slider.get('text', '')
                phase  = phase_from_label(label)

                # Collect all game_ids in this round
                gids_in_round = []
                for region in data.get('regions', []):
                    for row in region.get('rows', []):
                        for cell in row.get('cells', []):
                            lnk = cell.get('link') or {}
                            if lnk.get('page') == 'game_detail':
                                for gid in lnk.get('ids', []):
                                    gids_in_round.append(str(gid))

                if gids_in_round:
                    ph = ','.join('?' * len(gids_in_round))
                    conn.execute(
                        f"UPDATE games SET phase=?, subtitle=? WHERE game_id IN ({ph})",
                        [phase, label or None] + gids_in_round
                    )
                    if phase != 'Qualifikation':
                        updated += len(gids_in_round)
                    log.info(f"    [{repr(label)}] → {phase}: {len(gids_in_round)} games")

                # Enqueue prev and next rounds
                prev = slider.get('prev', {}).get('set_in_context', {}).get('round')
                nxt  = slider.get('next', {}).get('set_in_context', {}).get('round')
                if prev and prev not in visited:
                    queue.append(prev)
                if nxt and nxt not in visited:
                    queue.append(nxt)

                time.sleep(SLEEP)

    conn.commit()
    log.info(f"  Phases: updated {updated} playoff/final games.")


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
# NAME MATCHING  (abbreviated scorer names → full lineup names)
# ══════════════════════════════════════════════════════════════════════

def _norm(s):
    """Normalize a name fragment for fuzzy comparison."""
    s = s.lower().strip()
    for old, new in [('ü','u'),('ö','o'),('ä','a'),('ß','ss'),
                     ('é','e'),('è','e'),('ê','e'),('à','a'),('â','a'),
                     ('î','i'),('ô','o'),('û','u'),("'",''),('-','')]:
        s = s.replace(old, new)
    return s

def _parse_abbrev(name):
    """'D. Hasenbühler' → ('D', 'Hasenbühler').  Returns (None,None) if not abbreviated."""
    m = re.match(r'^([A-Za-z])\.\s+(.+)$', name.strip())
    return (m.group(1).upper(), m.group(2).strip()) if m else (None, None)

def _match_abbrev(abbrev, full_names):
    """Return the single full name that matches the abbreviation, or None."""
    initial, last = _parse_abbrev(abbrev)
    if not initial:
        return None
    last_n = _norm(last)
    hits = [f for f in full_names
            if f and f[0].upper() == initial
            and _norm(f.split()[-1]) == last_n]
    return hits[0] if len(hits) == 1 else None

def build_name_map(conn):
    """Match every abbreviated scorer/penalty name to a full lineup name.
    Uses (abbrev_name, team_raw) as composite key for team-aware disambiguation.
    Only confident (unique per team) matches are stored.

    NOTE: Matches against ALL players in a game (not filtered by team_id)
    because lineup team_ids may be reversed for some historical games."""
    existing = conn.execute("SELECT COUNT(*) FROM name_map").fetchone()[0]
    if existing > 0:
        log.info(f"  name_map already has {existing} entries — skipping rebuild.")
        return

    log.info("  Building name_map from lineups + game events…")
    mapping   = {}   # (abbrev, team_raw) → full  (confirmed)
    conflicts = set()

    def try_add(abbrev, team_raw, lineup_names):
        if not abbrev or not team_raw or not lineup_names:
            return
        key = (abbrev, team_raw)
        if key in conflicts:
            return
        full = _match_abbrev(abbrev, lineup_names)
        if not full:
            return
        if key in mapping:
            if mapping[key] != full:
                conflicts.add(key)
                del mapping[key]
        else:
            mapping[key] = full

    # Preload lineups grouped by game_id → [full_name, ...]
    # (uses all players in the game to handle reversed team_ids)
    lineup_by_game = {}
    for gid, player in conn.execute("SELECT game_id, player_raw FROM lineups"):
        lineup_by_game.setdefault(gid, []).append(player)

    # Match goal scorers / assisters
    for gid, scorer, assist, team_raw in conn.execute(
        "SELECT game_id, scorer_raw, assist_raw, team_scored_raw FROM goals"
        " WHERE team_scored_raw IS NOT NULL"
    ):
        game_lineup = lineup_by_game.get(gid, [])
        try_add(scorer, team_raw, game_lineup)
        try_add(assist, team_raw, game_lineup)

    # Match penalty players
    for gid, player, team_raw in conn.execute(
        "SELECT game_id, player_raw, team_raw FROM penalties"
        " WHERE player_raw IS NOT NULL AND team_raw IS NOT NULL"
    ):
        game_lineup = lineup_by_game.get(gid, [])
        try_add(player, team_raw, game_lineup)

    conn.executemany(
        "INSERT OR REPLACE INTO name_map (abbrev_name, team_raw, full_name) VALUES (?,?,?)",
        [(abbrev, team, full) for (abbrev, team), full in mapping.items()]
    )
    conn.commit()
    log.info(f"  name_map: {len(mapping)} matches built, {len(conflicts)} conflicts skipped.")

# ══════════════════════════════════════════════════════════════════════
# LINEUPS  (GET /api/games/:game_id/teams/:is_home/players)
# ══════════════════════════════════════════════════════════════════════

def fetch_and_store_lineup(conn, game_id, team_id, is_home, team_name, season, date):
    """Fetch the lineup for one side of a game. Returns player count stored."""
    raw = api_get(f"games/{game_id}/teams/{is_home}/players")
    if not raw:
        return 0
    count = 0
    for region in unwrap(raw).get("regions", []):
        for row in region.get("rows", []):
            cells = row.get("cells", [])
            if len(cells) < 3:
                continue
            jersey  = cell_text(cells[0]) or None
            # position cell may have ["Verteidiger"] or ["Verteidiger","Captain"] or [None]
            pos_list = cells[1].get("text", []) if isinstance(cells[1], dict) else []
            position = next((p for p in pos_list if p), None)
            player   = cell_text(cells[2]) or None
            if not player:
                continue
            # player_id lives in cells[2].link.ids[0]
            pid = None
            try:
                pid = cells[2].get("link", {}).get("ids", [None])[0]
            except Exception:
                pass
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO lineups
                      (game_id, team_id, team_raw, player_raw, player_id,
                       jersey_number, position, season, date)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (game_id, team_id, team_name, player, pid,
                      jersey, position, season, date))
                count += 1
            except Exception as e:
                log.debug(f"  lineup insert skip: {e}")
    return count

def backfill_lineups(conn):
    """Fetch lineups for every stored game that has none yet."""
    all_games = conn.execute(
        "SELECT game_id, home_team_id, away_team_id, home_team_raw, away_team_raw, "
        "date, season FROM games"
    ).fetchall()
    done_ids = {r[0] for r in conn.execute("SELECT DISTINCT game_id FROM lineups")}
    todo = [g for g in all_games if g[0] not in done_ids]
    if not todo:
        log.info("  Lineups already up-to-date.")
        return
    log.info(f"  Backfilling lineups for {len(todo)} games (2 API calls each)…")
    total = 0
    for i, (gid, home_id, away_id, home_name, away_name, gdate, season) in enumerate(todo, 1):
        for is_home, team_id, team_name in [(0, away_id, away_name), (1, home_id, home_name)]:
            time.sleep(SLEEP)
            total += fetch_and_store_lineup(conn, gid, team_id, is_home,
                                            team_name, season, gdate)
        if i % 50 == 0:
            conn.commit()
            log.info(f"    {i}/{len(todo)} games processed…")
    conn.commit()
    log.info(f"  Lineup backfill complete: {total} player-game records stored.")


# ══════════════════════════════════════════════════════════════════════
# LINEUP MAP — resolve abbreviated names to player IDs at import time
# ══════════════════════════════════════════════════════════════════════

def _make_abbrevs(full_name):
    """Generate all possible abbreviation forms the API might use.
    'Daniel Hasenbühler'        → ['D. Hasenbühler']
    'Andrin Galante Carlström'  → ['A. Galante Carlström', 'A. Carlström']
    'Daniels Janis Anis'        → ['D. Janis Anis', 'D. Anis']
    'Yann Andrea Ruh'           → ['Y. Andrea Ruh', 'Y. Ruh']
    'Rahul Chiplunkar'          → ['R. Chiplunkar', 'Ra. Chiplunkar']
    Returns list of abbreviation strings."""
    parts = full_name.strip().split()
    if len(parts) < 2:
        return []
    first = parts[0]
    rest = parts[1:]
    abbrevs = []
    # Full form: initial + everything after first name
    abbrevs.append(f"{first[0]}. {' '.join(rest)}")
    # Short form: initial + last word only (for multi-word names)
    if len(rest) > 1:
        abbrevs.append(f"{first[0]}. {rest[-1]}")
    # Multi-char initial forms: "Ra.", "Ro.", "Matt.", "Math." (API uses for disambiguation)
    for length in range(2, min(len(first), 5) + 1):
        abbrevs.append(f"{first[:length]}. {' '.join(rest)}")
        if len(rest) > 1:
            abbrevs.append(f"{first[:length]}. {rest[-1]}")
    return abbrevs


def build_lineup_map(conn, game_id, home_id, away_id, home_name, away_name, season, date):
    """Fetch lineups for BOTH teams, store in SQLite, return name→ID map.

    Returns dict: (abbrev_or_full, team_raw) → (full_name, player_id)
    Collisions (same abbreviation, same team) are marked (None, None).
    """
    name_map = {}

    for is_home, team_id, team_name in [(1, home_id, home_name), (0, away_id, away_name)]:
        time.sleep(SLEEP)
        raw = api_get(f"games/{game_id}/teams/{is_home}/players")
        if not raw:
            continue
        for region in unwrap(raw).get("regions", []):
            for row in region.get("rows", []):
                cells = row.get("cells", [])
                if len(cells) < 3:
                    continue
                jersey   = cell_text(cells[0]) or None
                pos_list = cells[1].get("text", []) if isinstance(cells[1], dict) else []
                position = next((p for p in pos_list if p), None)
                player   = cell_text(cells[2]) or None
                if not player:
                    continue
                pid = None
                try:
                    pid = cells[2].get("link", {}).get("ids", [None])[0]
                except Exception:
                    pass

                # Store in SQLite lineups table
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO lineups
                          (game_id, team_id, team_raw, player_raw, player_id,
                           jersey_number, position, season, date)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (game_id, team_id, team_name, player, pid,
                          jersey, position, season, date))
                except Exception:
                    pass

                # Exact full name key
                name_map[(player, team_name)] = (player, pid)

                # All possible abbreviated forms
                for abbrev in _make_abbrevs(player):
                    key = (abbrev, team_name)
                    if key in name_map:
                        if name_map[key][0] != player:
                            name_map[key] = (None, None)  # collision
                    else:
                        name_map[key] = (player, pid)

    return name_map


def resolve_player(lineup_map, abbrev_name, team_raw):
    """Look up abbreviated name → (full_name, player_id).
    Tries team-scoped first, then any team (handles reversed team_ids)."""
    if not abbrev_name or not lineup_map:
        return None, None
    # 1. Exact team match
    entry = lineup_map.get((abbrev_name, team_raw))
    if entry and entry[0]:
        return entry
    # 2. Fallback: any team in the game
    for (name, _team), (full, pid) in lineup_map.items():
        if name == abbrev_name and full:
            return full, pid
    return None, None


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

    # Load team-aware name resolution: (abbrev, team_raw) → full_name
    abbrev_team_to_full = {}
    for abbrev, team, full in conn.execute(
        "SELECT abbrev_name, team_raw, full_name FROM name_map"
    ):
        abbrev_team_to_full[(abbrev, team)] = full

    def resolve_name(abbrev, team_raw):
        """Resolve abbreviated scorer/assist name to full lineup name, or return as-is."""
        if not abbrev:
            return None
        return abbrev_team_to_full.get((abbrev, team_raw), abbrev)

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
        team_raw = row.get('team_scored_raw', '')
        if row.get('scorer_raw'):
            row['scorer_name'] = resolve_name(row['scorer_raw'], team_raw)
        if row.get('assist_raw'):
            row['assist_name'] = resolve_name(row['assist_raw'], team_raw)
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

    # ── Player meta: position + exact games played from lineups ──────────
    # Most-common non-null position per player
    pos_map = {}
    for name, pos in conn.execute("""
        SELECT player_raw, position FROM lineups
        WHERE position IS NOT NULL
        GROUP BY player_raw, position
        ORDER BY player_raw, COUNT(*) DESC
    """):
        pos_map.setdefault(name, pos)   # first row = most frequent position

    # Total games played per player
    gp_total = {name: gp for name, gp in conn.execute(
        "SELECT player_raw, COUNT(*) FROM lineups GROUP BY player_raw")}

    # Games played per player per season (for per-season filtering in dashboard)
    gp_seasons = {}
    for name, season, gp in conn.execute(
        "SELECT player_raw, season, COUNT(*) FROM lineups GROUP BY player_raw, season"
    ):
        gp_seasons.setdefault(name, {})[str(season)] = gp

    # Game IDs per full name, ordered by date
    player_game_ids = {}
    for full_name, game_id in conn.execute(
        "SELECT player_raw, game_id FROM lineups ORDER BY date, game_id"
    ):
        player_game_ids.setdefault(full_name, []).append(game_id)

    # Swiss Unihockey API player_id per player (most recent season's id)
    player_ids = {}
    for name, pid in conn.execute(
        "SELECT player_raw, player_id FROM lineups WHERE player_id IS NOT NULL"
        " GROUP BY player_raw ORDER BY season DESC"
    ):
        player_ids.setdefault(name, pid)  # keep first (most recent season)

    # Team history from goals: resolve scorer/assist to full name using team-aware lookup,
    # then attribute game to that full-name player. player_meta keys are always full names.
    team_game_ids = {}   # full_name → {team_name → set(game_ids)}
    for scorer, assist, game_id, team_raw in conn.execute(
        "SELECT scorer_raw, assist_raw, game_id, team_scored_raw FROM goals"
        " WHERE team_scored_raw IS NOT NULL"
    ):
        for abbrev in (scorer, assist):
            if not abbrev:
                continue
            full = abbrev_team_to_full.get((abbrev, team_raw), abbrev)
            th = team_game_ids.setdefault(full, {})
            th.setdefault(team_raw, set()).add(game_id)

    # player_meta always keyed by full lineup name (no more abbreviated keys)
    player_meta = {}
    for full_name in set(list(gp_total.keys()) + list(pos_map.keys())):
        raw_th = team_game_ids.get(full_name, {})
        team_gp = {team: len(gids) for team, gids in
                   sorted(raw_th.items(), key=lambda x: -len(x[1]))}
        player_meta[full_name] = {
            "pos":   pos_map.get(full_name),
            "gp":    gp_total.get(full_name, 0),
            "gp_s":  gp_seasons.get(full_name, {}),
            "gids":  player_game_ids.get(full_name, []),
            "pid":   player_ids.get(full_name),
            "team_gp": team_gp,
        }

    # API per-player stats intentionally not exported — game data is the single source of truth.

    data = {'clubs': clubs_list, 'goals': goals, 'games': games,
            'penalties': penalties, 'seasons': seasons, 'player_meta': player_meta}

    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    size_mb = os.path.getsize(JSON_PATH) / 1024 / 1024
    log.info(f"\n✅  Exported to {JSON_PATH}")
    log.info(f"    Clubs:     {len(clubs_list)}")
    log.info(f"    Games:     {len(games)}")
    log.info(f"    Goals:     {len(goals)}")
    log.info(f"    Penalties:   {len(penalties)}")
    log.info(f"    PlayerMeta:  {len(player_meta)} players")
    log.info(f"    Seasons:     {seasons}")
    log.info(f"    Size:        {size_mb:.2f} MB")

# ══════════════════════════════════════════════════════════════════════
# SUPABASE SYNC
# ══════════════════════════════════════════════════════════════════════

def _sb_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",
    }

def _sb_upsert(table, rows):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = _sb_headers()
    # Use upsert (INSERT ... ON CONFLICT DO UPDATE) to avoid duplicates
    headers["Prefer"] = "resolution=merge-duplicates"
    r = SESSION.post(url, headers=headers,
                     data=json.dumps(rows, default=str))
    if r.status_code not in (200, 201):
        log.error(f"  Supabase {table} failed [{r.status_code}]: {r.text[:300]}")
        r.raise_for_status()

def _batched(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def sync_to_supabase(conn):
    """Push all new/changed rows from SQLite to Supabase."""
    if not SUPABASE_SERVICE_KEY:
        log.warning("  SUPABASE_SERVICE_KEY not set — skipping Supabase sync.")
        return

    log.info("  Syncing to Supabase…")

    LEAGUE_MAP_LOCAL = {
        "Herren L-UPL":                  "Herren NLA",
        "Herren SML":                    "Herren NLA",
        "Herren Aktive KF 3. Liga":      "Herren 3. Liga",
        "Mobiliar Unihockey Cup Männer": "Mobiliar Unihockey Cup Herren",
    }
    def nl(name): return LEAGUE_MAP_LOCAL.get(name, name) if name else name

    # ── Build lineup lookup: game_id → { home_lineup: [pid,...], away_lineup: [pid,...] }
    lineup_lookup = {}
    for gid, pid, is_home in conn.execute(
        "SELECT l.game_id, l.player_id, "
        "  CASE WHEN l.team_id = g.home_team_id THEN 1 ELSE 0 END as is_home "
        "FROM lineups l JOIN games g ON l.game_id = g.game_id "
        "WHERE l.player_id IS NOT NULL"
    ):
        if gid not in lineup_lookup:
            lineup_lookup[gid] = {"home_lineup": [], "away_lineup": []}
        key = "away_lineup" if is_home else "home_lineup"
        lineup_lookup[gid][key].append(pid)

    # ── Games ──────────────────────────────────────────────────────────
    games = [dict(r) for r in conn.execute("SELECT * FROM games ORDER BY date")]
    for g in games:
        lu = lineup_lookup.get(g["game_id"], {})
        g["home_lineup"] = lu.get("home_lineup", [])
        g["away_lineup"] = lu.get("away_lineup", [])
    for batch in _batched(games, 500):
        _sb_upsert("fb_games", batch)
    log.info(f"    fb_games: {len(games)} rows synced ({len(lineup_lookup)} with lineups)")

    # ── Goals (scorer_name/assist_name now stored directly in SQLite) ──
    GOAL_COLS = [
        "game_id", "team_scored_id", "team_conceded_id", "team_scored_raw",
        "team_conceded_raw", "scorer_raw", "assist_raw",
        "scorer_id", "assist_id", "scorer_name", "assist_name",
        "minute", "minute_seconds", "period", "score_at_goal",
        "date", "season", "league", "league_group", "home_team_raw",
        "away_team_raw", "home_team_id", "away_team_id",
    ]
    goal_rows = []
    for r in conn.execute("""
        SELECT g.*, gm.league, gm.league_group, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id
        FROM goals g JOIN games gm ON g.game_id = gm.game_id
    """):
        raw = dict(r)
        raw["league"] = nl(raw.get("league"))
        goal_rows.append({c: raw.get(c) for c in GOAL_COLS})
    for batch in _batched(goal_rows, 500):
        _sb_upsert("fb_goals", batch)
    log.info(f"    fb_goals: {len(goal_rows)} rows synced")

    # ── Penalties ──────────────────────────────────────────────────────
    PEN_COLS = [
        "game_id", "team_id", "team_raw", "player_raw",
        "player_id", "player_name",
        "minute", "minute_seconds", "period", "duration_min", "reason",
        "date", "season", "league", "home_team_raw", "away_team_raw",
        "home_team_id", "away_team_id",
    ]
    pen_rows = []
    for r in conn.execute("""
        SELECT p.*, gm.league, gm.home_team_raw, gm.away_team_raw,
               gm.home_team_id, gm.away_team_id
        FROM penalties p JOIN games gm ON p.game_id = gm.game_id
    """):
        raw = dict(r)
        raw["league"] = nl(raw.get("league"))
        pen_rows.append({c: raw.get(c) for c in PEN_COLS})
    for batch in _batched(pen_rows, 500):
        _sb_upsert("fb_penalties", batch)
    log.info(f"    fb_penalties: {len(pen_rows)} rows synced")

    # ── Players (unique players from lineups) ─────────────────────────
    player_rows = []
    for row in conn.execute("""
        SELECT player_id, player_raw, position,
               COUNT(*) as gp
        FROM lineups
        WHERE player_id IS NOT NULL
        GROUP BY player_id
    """):
        player_rows.append({
            "player_id": row[0],
            "player_name": row[1],
            "position": row[2],
            "games_played": row[3],
        })
    if player_rows:
        for batch in _batched(player_rows, 500):
            _sb_upsert("fb_players", batch)
    log.info(f"    fb_players: {len(player_rows)} rows synced")
    log.info("  ✅ Supabase sync complete.")


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
        explicit_group = lg_cfg.get("group")

        for season in lg_cfg.get("seasons", SEASONS):
            # Discover groups if none specified
            if explicit_group:
                groups_to_fetch = [explicit_group]
            else:
                time.sleep(SLEEP)
                discovered = discover_groups(league, game_class, season)
                if discovered:
                    groups_to_fetch = discovered
                    log.info(f"\n{label}  season {season}/{season+1} — discovered {len(discovered)} groups: {discovered}")
                else:
                    groups_to_fetch = [None]  # no groups, fetch default

            for grp in groups_to_fetch:
                group_tag = f" ({grp})" if grp else ""
                log.info(f"\n{label}{group_tag}  season {season}/{season+1}…")
                game_rows, game_region = fetch_game_rows(league, game_class, season, group=grp)
                log.info(f"  Found {len(game_rows)} games")

                # Determine league_group label for storage
                league_group_label = grp if grp else None

                for gid, row in game_rows.items():
                    if conn.execute("SELECT 1 FROM games WHERE game_id=?", (gid,)).fetchone():
                        log.debug(f"  Skip {gid} (already stored)")
                        continue

                    result = store_game(conn, gid, row, season, label,
                                        league_group_override=league_group_label or game_region.get(gid))
                    if result is None:
                        continue

                    home_id, away_id, home_name, away_name, iso_date, weekday = result

                    # Fetch lineups FIRST → build name→ID map
                    lineup_map = build_lineup_map(conn, gid, home_id, away_id,
                                                   home_name, away_name, season, iso_date)

                    # Then fetch goals/penalties with ID resolution
                    time.sleep(SLEEP)
                    ng, np = fetch_and_store_goals(conn, gid, home_id, away_id,
                                                   home_name, away_name, iso_date, weekday, season,
                                                   lineup_map=lineup_map)
                    conn.commit()
                    total_games += 1
                    total_goals += ng
                    total_pen   += np
                    log.info(f"  ✓ {home_name} vs {away_name}  [{iso_date}]  {ng} goals  {np} pen  ({len(lineup_map)} players)")

    log.info(f"\n── New games ─────────────────")
    log.info(f"  Games stored: {total_games}")
    log.info(f"  Goals stored: {total_goals}")
    log.info(f"  Penalties:    {total_pen}")
    log.info(f"\n── Backfilling penalties for existing games…")
    backfill_penalties(conn)
    log.info(f"\n── Backfilling lineups for existing games…")
    backfill_lineups(conn)
    log.info(f"\n── Backfilling game phases (Qualifikation/Playoffs)…")
    backfill_game_phases(conn)
    log.info(f"\n── Syncing to Supabase…")
    sync_to_supabase(conn)
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
