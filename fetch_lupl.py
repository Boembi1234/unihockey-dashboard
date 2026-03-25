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
    {"league": 24, "game_class": 11, "label": "Herren L-UPL", "seasons": [2025, 2024, 2023, 2022]},
    {"league":  1, "game_class": 11, "label": "Herren NLA",
     "seasons": [2021,2020,2019,2018,2017,2016,2015,2014,2013,
                 2006,2005,2004,2003,2002,2001,2000,1999,1998,1997]},
    {"league": 10, "game_class": 11, "label": "Herren SML",
     "seasons": [2012,2011,2010,2009,2008,2007]},
    # {"league":  3, "game_class": 11, "label": "Herren 1. Liga"},
    # {"league": 24, "game_class": 21, "label": "Damen L-UPL"},
    # {"league":  2, "game_class": 11, "label": "Herren NLB"},
]

SEASONS = []  # per-league seasons defined above

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
    phase TEXT DEFAULT 'Qualifikation'
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
    abbrev_name TEXT PRIMARY KEY,
    full_name   TEXT NOT NULL
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

def phase_from_label(label):
    """Map a round slider label to a human-readable phase name."""
    if not label:
        return 'Qualifikation'
    low = label.lower()
    if 'superfinal' in low:
        return 'Superfinal'
    if 'final' in low and 'halb' in low:
        return 'Halbfinal'
    if 'final' in low and 'viertel' in low:
        return 'Viertelfinal'
    if 'playoff' in low or 'final' in low:
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

            log.info(f"  Phases: league={league_id} ({label_name}) season={season}…")
            base_params = {'mode': 'list', 'league': league_id,
                           'game_class': game_class, 'season': season}

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

                if gids_in_round and phase != 'Qualifikation':
                    ph = ','.join('?' * len(gids_in_round))
                    conn.execute(
                        f"UPDATE games SET phase=? WHERE game_id IN ({ph})",
                        [phase] + gids_in_round
                    )
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
    Only confident (unique) matches are stored."""
    existing = conn.execute("SELECT COUNT(*) FROM name_map").fetchone()[0]
    if existing > 0:
        log.info(f"  name_map already has {existing} entries — skipping rebuild.")
        return

    log.info("  Building name_map from lineups + game events…")
    mapping   = {}   # abbrev → full  (confirmed)
    conflicts = set()

    def try_add(abbrev, full_names):
        if not abbrev or abbrev in conflicts:
            return
        full = _match_abbrev(abbrev, full_names)
        if not full:
            return
        if abbrev in mapping:
            if mapping[abbrev] != full:
                conflicts.add(abbrev)
                del mapping[abbrev]
        else:
            mapping[abbrev] = full

    # Preload lineups grouped by game_id (both teams combined — avoids home/away ID mismatch)
    lineup_index = {}   # game_id → [full_name, ...]
    for gid, player in conn.execute("SELECT game_id, player_raw FROM lineups"):
        lineup_index.setdefault(gid, []).append(player)

    # Match goal scorers / assisters
    for gid, scorer, assist in conn.execute(
        "SELECT game_id, scorer_raw, assist_raw FROM goals"
    ):
        names = lineup_index.get(gid, [])
        try_add(scorer, names)
        try_add(assist, names)

    # Match penalty players
    for gid, player in conn.execute(
        "SELECT game_id, player_raw FROM penalties WHERE player_raw IS NOT NULL"
    ):
        names = lineup_index.get(gid, [])
        try_add(player, names)

    conn.executemany(
        "INSERT OR REPLACE INTO name_map (abbrev_name, full_name) VALUES (?,?)",
        mapping.items()
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

    # name_map: full_name → abbrev_name  (so we key player_meta by abbreviated names)
    full_to_abbrev = {full: abbrev for abbrev, full in conn.execute(
        "SELECT abbrev_name, full_name FROM name_map"
    )}

    # Game IDs per full name, ordered by date
    player_game_ids = {}
    for full_name, game_id in conn.execute(
        "SELECT player_raw, game_id FROM lineups ORDER BY date, game_id"
    ):
        player_game_ids.setdefault(full_name, []).append(game_id)

    player_meta = {}
    for full_name in set(list(gp_total.keys()) + list(pos_map.keys())):
        key = full_to_abbrev.get(full_name, full_name)  # prefer abbreviated key
        player_meta[key] = {
            "pos":   pos_map.get(full_name),
            "gp":    gp_total.get(full_name, 0),
            "gp_s":  gp_seasons.get(full_name, {}),
            "full":  full_name if key != full_name else None,  # only set if different from key
            "gids":  player_game_ids.get(full_name, []),
        }

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

        for season in lg_cfg.get("seasons", SEASONS):
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
                # also fetch lineups for the new game
                time.sleep(SLEEP)
                for is_home, tid, tname in [(0, away_id, away_name), (1, home_id, home_name)]:
                    time.sleep(SLEEP)
                    fetch_and_store_lineup(conn, gid, tid, is_home, tname, season, iso_date)
                conn.commit()
                log.info(f"  ✓ {home_name} vs {away_name}  [{iso_date}]  {ng} goals  {np} pen")

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
    log.info(f"\n── Building name map…")
    build_name_map(conn)
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
