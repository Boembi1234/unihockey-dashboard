"""Fetch upcoming games (next 60 days) across ALL Swiss floorball competitions
and push to Supabase.

Three-stage fetch:
 1a. League sweep: `games?mode=list` gives the full league navigation as tabs
     (L-UPL, NLB, 1.-5. Liga inkl. KF, alle Junioren-Stufen, Senioren) with
     their groups. For every league/game_class/group combo we walk the round
     slider and collect game IDs whose date falls in the window. This is the
     complete source — `mode=current` alone is a curated selection and misses
     lower junior leagues entirely.
 1b. Current sweep: `mode=current` day-by-day as before. Cup competitions
     (Mobiliar Cup, Ligacup, Supercup-Spieltage) are not part of the league
     tabs, so this sweep is what surfaces them. Duplicates from 1a are
     skipped.
 2.  /api/games/<id> on each ID fills in the real team IDs, location with
     coordinates, accurate date/time and subtitle.

Venues: new venues are inserted with `on_conflict=name` +
`resolution=ignore-duplicates`. Requires the UNIQUE index on venues.name
(migration 20260729100000) — without an explicit conflict target PostgREST
checks only the auto-generated PK and inserts duplicates on every run.

Run daily via GitHub Actions or manually:
    SUPABASE_SERVICE_KEY=... python fetch_upcoming.py
"""

import os, re, sys, json, time, logging, requests
from datetime import date, timedelta, datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://api-v2.swissunihockey.ch/api"
SUPABASE_URL = "https://ibqwotgrzgrwvejtphnh.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SLEEP = 0.25
CURRENT_SEASON = 2026
DAYS_AHEAD = 60
MAX_ROUNDS_PER_COMBO = 40      # safety bound while walking the round slider
MAX_CURRENT_PAGES = 90         # day-by-day sweep upper bound (> DAYS_AHEAD)

# Normalise SU labels to the canonical names the app filters on.
LEAGUE_MAP = {
    "Herren L-UPL":                  "Herren NLA",
    "Herren SML":                    "Herren NLA",
    "Damen L-UPL":                   "Damen NLA",
    "Mobiliar Unihockey Cup Männer": "Mobiliar Cup Herren",
    "Mobiliar Unihockey Cup Frauen": "Mobiliar Cup Damen",
}


def combo_league_label(tab_label):
    """Map an SU tab label to the canonical league name used in fb_games.

    Tab labels look like 'L-UPL Men', 'HNLB', 'Herren Aktive GF 1. Liga',
    'Junioren U16 A', 'Junioren B  Regional' (yes, double space)."""
    label = " ".join((tab_label or "").split())    # collapse whitespace
    fixed = {
        "L-UPL Men":   "Herren NLA",
        "L-UPL Women": "Damen NLA",
        "HNLB":        "Herren NLB",
        "DNLB":        "Damen NLB",
    }
    if label in fixed:
        return fixed[label]
    m = re.match(r"^(Herren|Damen) Aktive (GF|KF) (.+)$", label)
    if m:
        base = f"{m.group(1)} {m.group(3)}"
        return base if m.group(2) == "GF" else f"{base} KF"
    return label


# ── HTTP ──────────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers["Accept"] = "application/json"


def api_get(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"  Attempt {attempt+1}/3 failed: {e}")
            time.sleep(1.5)
    return None


def api_data(endpoint, params=None):
    raw = api_get(endpoint, params)
    if not raw:
        return None
    return raw.get("data", raw) if isinstance(raw, dict) else None


# ── Helpers ───────────────────────────────────────────────────────────────────

def cell_text(cell, index=0):
    if not isinstance(cell, dict):
        return str(cell) if cell else ""
    t = cell.get("text", "")
    if isinstance(t, list):
        return t[index] if index < len(t) else (t[0] if t else "")
    return t or ""


def cell_link_ids(cell):
    link = (cell or {}).get("link") or {}
    return link.get("ids") or []


def parse_iso_date(s):
    """Accepts 'DD.MM.YYYY[ HH:MM]' or 'YYYY-MM-DD', returns (iso, weekday)."""
    if not s:
        return None, None
    s = s.strip().split(" ")[0]
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s, fmt).date()
            return d.isoformat(), d.strftime("%A")
        except ValueError:
            pass
    return None, None


def phase_from_label(label):
    if not label:
        return "Qualifikation"
    low = label.lower()
    if "cup" in low:
        return "Cup"
    if "final" in low and "viertel" in low:
        return "Playoff-Viertelfinal"
    if "halbfinal" in low or "semi" in low:
        return "Playoff-Halbfinal"
    if "final" in low:
        return "Playoff-Final"
    if "playoff" in low or "play-off" in low:
        return "Playoffs"
    return "Qualifikation"


def norm_league(name):
    return LEAGUE_MAP.get(name, name) if name else name


# ── Supabase ──────────────────────────────────────────────────────────────────

def sb_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",
    }


def sb_upsert(table, rows):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = SESSION.post(url, headers=sb_headers(), data=json.dumps(rows, default=str))
    if r.status_code not in (200, 201):
        log.error(f"  Supabase {table} failed [{r.status_code}]: {r.text[:300]}")
        r.raise_for_status()


def sb_insert_ignore(table, rows, conflict_col):
    """Insert, silently skipping rows whose `conflict_col` already exists.
    The explicit on_conflict target is essential: without it PostgREST
    resolves against the primary key (auto-generated) and every row inserts
    as a fresh duplicate."""
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflict_col}"
    headers = sb_headers().copy()
    headers["Prefer"] = "resolution=ignore-duplicates,return=minimal"
    r = SESSION.post(url, headers=headers, data=json.dumps(rows, default=str))
    if r.status_code not in (200, 201, 204):
        log.warning(f"  {table} insert returned {r.status_code}: {r.text[:200]}")


# ── Stage 1a: league sweep via mode=list tabs ────────────────────────────────

def discover_combos(season):
    """Walk the mode=list tab tree → [{label, league, game_class, groups}].

    Group names (Gruppe 1/2/…) sit as sub-entries of their league entry.
    A request without `group` returns only the first group, so every group
    has to be swept explicitly."""
    data = api_data("games", {"mode": "list", "season": season})
    if not data:
        return []

    combos = []

    def walk(entries):
        for e in entries or []:
            sic = ((e.get("link") or {}).get("set_in_context")
                   or e.get("set_in_context") or {})
            label = e.get("text")
            label = " ".join(label) if isinstance(label, list) else (label or "")
            if sic.get("league") is not None and sic.get("game_class") is not None:
                groups = []
                for sub in e.get("entries") or []:
                    g = (sub.get("set_in_context") or {}).get("group")
                    if g:
                        groups.append(g)
                combos.append({
                    "label":      combo_league_label(label),
                    "league":     sic["league"],
                    "game_class": sic["game_class"],
                    "groups":     groups or [None],
                })
            walk(e.get("entries"))

    walk(data.get("tabs"))
    # SU's internal test league produces junk rows.
    return [c for c in combos if "test" not in c["label"].lower()]


def row_game_id_and_date(row):
    """A mode=list row has no top-level id — the game id lives in the cells'
    game_detail links; the first cell text is 'DD.MM.YYYY HH:MM'."""
    gid = None
    game_date = None
    for cell in row.get("cells") or []:
        link = cell.get("link") or {}
        if link.get("page") == "game_detail" and link.get("ids"):
            gid = str(link["ids"][0])
        txt = cell_text(cell)
        if game_date is None and re.match(r"^\d{2}\.\d{2}\.\d{4}", txt or ""):
            game_date, _ = parse_iso_date(txt)
    return gid, game_date


def sweep_league_combo(season, combo, group, today, cutoff, seen_ids):
    """Walk one combo's round slider; return [(game_id, league_label)] within
    the date window. Rounds are chronological, so we stop as soon as a round
    lies entirely beyond the cutoff."""
    found = []
    params = {"mode": "list", "season": season,
              "league": combo["league"], "game_class": combo["game_class"]}
    if group:
        params["group"] = group

    round_id = None
    seen_rounds = set()
    for _ in range(MAX_ROUNDS_PER_COMBO):
        p = dict(params)
        if round_id is not None:
            p["round"] = round_id
        data = api_data("games", p)
        if not data:
            break

        rows = []
        for region in data.get("regions") or []:
            rows.extend(region.get("rows") or [])

        dates_on_page = []
        for row in rows:
            gid, gdate = row_game_id_and_date(row)
            if not gdate:
                continue
            dates_on_page.append(gdate)
            if gid and gid not in seen_ids and today <= gdate <= cutoff:
                seen_ids.add(gid)
                found.append((gid, combo["label"]))

        # Entire round beyond the window → later rounds are too.
        if dates_on_page and min(dates_on_page) > cutoff:
            break

        nxt = (((data.get("slider") or {}).get("next") or {})
               .get("set_in_context") or {}).get("round")
        if not nxt or nxt in seen_rounds:
            break
        seen_rounds.add(nxt)
        round_id = nxt
        time.sleep(SLEEP)

    return found


def sweep_leagues(season, days=DAYS_AHEAD):
    today  = date.today().isoformat()
    cutoff = (date.today() + timedelta(days=days)).isoformat()

    combos = discover_combos(season)
    n_targets = sum(len(c["groups"]) for c in combos)
    log.info(f"  {len(combos)} leagues / {n_targets} league+group targets discovered")

    ids = []
    seen_ids = set()
    for combo in combos:
        combo_found = []
        for group in combo["groups"]:
            combo_found.extend(
                sweep_league_combo(season, combo, group, today, cutoff, seen_ids))
            time.sleep(SLEEP)
        if combo_found:
            log.info(f"  {len(combo_found):>4}  {combo['label']}")
        ids.extend(combo_found)
    return ids, seen_ids


# ── Stage 1b: mode=current day sweep (cups etc.) ─────────────────────────────

def sweep_current(season, seen_ids, days=DAYS_AHEAD):
    """Walk mode=current forward day by day. The league sweep already covers
    regular leagues — this pass exists for competitions outside the league
    tabs (Mobiliar Cup, Ligacup, Supercup-Spieltage)."""
    today  = date.today()
    cutoff = today + timedelta(days=days)
    ids = []
    seen_dates = set()
    after_date = None

    for _ in range(MAX_CURRENT_PAGES):
        params = {"mode": "current", "season": season}
        if after_date:
            params["after_date"] = after_date

        data = api_data("games", params)
        if not data:
            break

        ctx = data.get("context", {}) or {}
        page_date = ctx.get("on_date") or after_date or today.isoformat()
        try:
            d = date.fromisoformat(page_date)
        except ValueError:
            break
        if d > cutoff:
            break
        if page_date in seen_dates:
            break
        seen_dates.add(page_date)

        page_count = 0
        for region in data.get("regions", []):
            region_label = (region.get("text") or region.get("title") or "").strip()
            for row in region.get("rows", []):
                gid = str(row.get("id") or "")
                if not gid or gid in seen_ids:
                    continue
                ids.append((gid, norm_league(region_label) or region_label))
                seen_ids.add(gid)
                page_count += 1

        if page_count:
            log.info(f"  {page_date}: {page_count} additional games")

        nxt = (data.get("slider") or {}).get("next", {}).get("set_in_context", {}).get("after_date")
        if not nxt:
            break
        # SU's slider echoes the current date as after_date; passing it back
        # returns the NEXT day with games. Only a repeat of the same page
        # (guarded via seen_dates above) means we're done.
        after_date = nxt
        time.sleep(SLEEP)

    return ids


# ── Stage 2: per-game detail → fb_games row ──────────────────────────────────

def fetch_game_detail(game_id, league_label, season):
    """Hit /games/<id> and build a row matching the fb_games schema.

    The detail endpoint declares cell order via `headers[*].key`, so we index
    by key instead of position — safe against future column reshuffles. Also
    pulls x/y from the location cell's map link for venue upsert."""
    data = api_data(f"games/{game_id}")
    if not data:
        return None

    headers = data.get("headers", []) or []
    keys = [(h.get("key") or "") for h in headers]
    regions = data.get("regions", []) or []
    if not regions or not regions[0].get("rows"):
        return None

    cells = regions[0]["rows"][0].get("cells", [])
    by_key = {k: cells[i] for i, k in enumerate(keys) if i < len(cells) and k}

    home_cell = by_key.get("home_name") or by_key.get("home_logo") or {}
    away_cell = by_key.get("away_name") or by_key.get("away_logo") or {}

    home_ids = cell_link_ids(home_cell)
    away_ids = cell_link_ids(away_cell)
    home_name = cell_text(by_key.get("home_name"))
    away_name = cell_text(by_key.get("away_name"))

    iso_date, weekday = parse_iso_date(cell_text(by_key.get("date")))
    if not iso_date:
        log.warning(f"  game {game_id}: no date, skipping")
        return None

    time_raw = cell_text(by_key.get("time")) or None
    result   = cell_text(by_key.get("result")) or None
    if result in ("", "-", "-:-"):
        result = None

    loc_cell = by_key.get("location") or {}
    location = cell_text(loc_cell) or None
    location_city = location.split()[-1] if location else None

    # SU's location cell carries a {type:'map', x: lon, y: lat} link.
    loc_link = loc_cell.get("link") if isinstance(loc_cell, dict) else None
    loc_lat = loc_lng = None
    if loc_link and loc_link.get("type") == "map":
        loc_lng = loc_link.get("x")
        loc_lat = loc_link.get("y")

    subtitle = (data.get("subtitle") or "").strip() or None
    phase = phase_from_label(subtitle or league_label)

    return {
        "game_id":       game_id,
        "home_team_id":  int(home_ids[0]) if home_ids else None,
        "away_team_id":  int(away_ids[0]) if away_ids else None,
        "home_team_raw": home_name,
        "away_team_raw": away_name,
        "date":          iso_date,
        "weekday":       weekday,
        "time":          time_raw,
        "season":        season,
        "result":        result,
        "location":      location,
        "location_city": location_city,
        "league_group":  league_label or None,
        "subtitle":      subtitle,
        "phase":         phase,
        "league":        league_label,
        # Private — stripped by split_venues() before the fb_games upsert.
        "_loc_lat":      loc_lat,
        "_loc_lng":      loc_lng,
    }


# ── Venues ────────────────────────────────────────────────────────────────────

def split_venues(games):
    """Pull (name, lat, lng, city) out of each game, dedupe by name, and
    strip the private _loc_* keys so the fb_games upsert stays
    schema-clean. Returns the venue list."""
    seen = set()
    venues = []
    for g in games:
        lat = g.pop("_loc_lat", None)
        lng = g.pop("_loc_lng", None)
        name = g.get("location")
        if not name or lat is None or lng is None:
            continue
        if name in seen:
            continue
        seen.add(name)
        venues.append({
            "name": name,
            "lat":  float(lat),
            "lng":  float(lng),
            "city": g.get("location_city"),
        })
    return venues


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — aborting.")
        sys.exit(1)

    log.info(f"Stage 1a: league sweep for season {CURRENT_SEASON}, next {DAYS_AHEAD} days…")
    id_pairs, seen_ids = sweep_leagues(CURRENT_SEASON)
    log.info(f"  {len(id_pairs)} games from leagues")

    log.info("Stage 1b: mode=current sweep (cup competitions)…")
    cup_pairs = sweep_current(CURRENT_SEASON, seen_ids)
    log.info(f"  {len(cup_pairs)} additional games from current sweep")
    id_pairs.extend(cup_pairs)

    if not id_pairs:
        log.info("Nothing to fetch.")
        return

    log.info(f"Stage 2: fetching details for {len(id_pairs)} games…")
    games = []
    skipped = 0
    for i, (gid, league_label) in enumerate(id_pairs, 1):
        row = fetch_game_detail(gid, league_label, CURRENT_SEASON)
        if row:
            games.append(row)
        else:
            skipped += 1
        if i % 25 == 0 or i == len(id_pairs):
            log.info(f"  {i}/{len(id_pairs)} fetched ({skipped} skipped)")
        time.sleep(SLEEP)

    # Per-league summary so the log is useful.
    by_league = {}
    for g in games:
        by_league[g["league"]] = by_league.get(g["league"], 0) + 1
    for label, n in sorted(by_league.items(), key=lambda x: (-x[1], x[0])):
        log.info(f"  {n:>4}  {label}")

    log.info(f"\nTotal: {len(games)} games to upsert ({skipped} skipped)")

    if games:
        venues = split_venues(games)
        if venues:
            sb_insert_ignore("venues", venues, conflict_col="name")
            log.info(f"  {len(venues)} venues sent (existing kept untouched)")

        sb_upsert("fb_games", games)
        log.info(f"Upserted {len(games)} games to Supabase fb_games")


if __name__ == "__main__":
    main()
