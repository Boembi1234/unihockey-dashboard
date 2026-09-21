"""Results pass: write the final result of every finished game to Supabase fb_games.

Why this exists: fetch_upcoming.py inserts the fixtures of ALL leagues, but only
looks at games from today on. fetch_daily.py imports finished games with goals
and penalties, but only the ones in live_games_cache — the app fills that from
`mode=current`, a curated selection (top leagues, U21, cups). Nothing ever went
back for the result of a 3. Liga or junior game. This pass does, for every league:

    fb_games rows up to today that still have no result
      → GET /api/games/<id>
      → final result            → fb_games.result
      → not played, new date    → fb_games.date / weekday / time   (postponed)
      → cancelled, still running, 0:0 → untouched, looked at again next run

Rules:
* Only a FINAL result is written. The API's result is a live score while a game
  runs, and the app scores Tipps against fb_games.result exactly once. A game of
  an earlier day is over; a game of today needs "Spielende" in its event feed.
* A row that has a result is closed. Only rows without one are read, and every
  PATCH carries `result=is.null`, so a finished game can never be rewritten —
  not even if someone entered the result by hand a second ago.
* A 0:0 is never written — every one so far was an API error (game_result.py).
  It is reported with what periods and event feed say, for a manual entry.

    SUPABASE_SERVICE_KEY=... python fetch_results.py [--dry-run] [--days 21] [--game-ids 1,2]
"""
import os, re, sys, json, time, logging, argparse
from datetime import datetime, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetch_upcoming import (
    SESSION, SUPABASE_URL, SUPABASE_SERVICE_KEY, SLEEP,
    api_data, cell_text, parse_iso_date,
)
from game_result import (
    parse_result_cell, check_result, feed_has_ended, last_feed_score,
    NONE, REVIEW,
)

log = logging.getLogger(__name__)

LOOKBACK_DAYS    = 21    # how long a game without result keeps being checked
MIN_GAME_MINUTES = 45    # no game is over sooner after kickoff — don't ask yet
PAGE             = 1000

_TIME_RE = re.compile(r"^\d{1,2}:\d{2}")


def swiss_now():
    """Game dates and kickoff times are Swiss local time; the runner is UTC."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Zurich")).replace(tzinfo=None)
    except Exception:
        return datetime.now()


# ── Supabase ──────────────────────────────────────────────────────────────────

def _sb_auth():
    return {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}


def sb_open_games(query):
    """fb_games rows WITHOUT a result matching `query`, all pages."""
    rows, offset = [], 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/fb_games"
               f"?select=game_id,date,time,league,home_team_raw,away_team_raw"
               f"&result=is.null&{query}&order=game_id&limit={PAGE}&offset={offset}")
        r = SESSION.get(url, headers=_sb_auth(), timeout=60)
        if r.status_code != 200:
            log.error(f"  fb_games read failed [{r.status_code}]: {r.text[:300]}")
            r.raise_for_status()
        page = r.json()
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        offset += PAGE


def sb_patch_open_game(game_id, fields):
    """PATCH one fb_games row, only while it has no result. Returns rows changed."""
    url = f"{SUPABASE_URL}/rest/v1/fb_games?game_id=eq.{game_id}&result=is.null"
    assert "result=is.null" in url and game_id.isdigit()
    headers = {**_sb_auth(), "Content-Type": "application/json", "Prefer": "return=representation"}
    r = SESSION.patch(url, headers=headers, data=json.dumps(fields), timeout=60)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"PATCH {game_id} failed [{r.status_code}]: {r.text[:300]}")
    return len(r.json()) if r.text else 0


# ── swiss unihockey API ───────────────────────────────────────────────────────

def fetch_detail(game_id):
    """→ {result_texts, date, weekday, time, date_raw} from /games/<id>, None if unreadable."""
    data = api_data(f"games/{game_id}")
    if not data:
        return None
    keys = [(h.get("key") or "") for h in data.get("headers") or []]
    regions = data.get("regions") or []
    if not regions or not regions[0].get("rows"):
        return None
    cells = regions[0]["rows"][0].get("cells") or []
    by_key = {k: cells[i] for i, k in enumerate(keys) if i < len(cells) and k}

    res_cell = by_key.get("result")
    texts = res_cell.get("text") if isinstance(res_cell, dict) else None
    if isinstance(texts, str):
        texts = [texts]

    date_raw = cell_text(by_key.get("date"))
    iso_date, weekday = parse_iso_date(date_raw)
    return {
        "result_texts": texts or [],
        "date":         iso_date,
        "weekday":      weekday,
        "time":         cell_text(by_key.get("time")) or None,
        "date_raw":     date_raw or "",
    }


def fetch_event_rows(game_id):
    time.sleep(SLEEP)
    data = api_data(f"game_events/{game_id}") or {}
    return [row for region in data.get("regions") or [] for row in region.get("rows") or []]


# ── One game ──────────────────────────────────────────────────────────────────

def kickoff(game):
    try:
        t = (game.get("time") or "")[:5]
        return datetime.strptime(f"{game['date']} {t}", "%Y-%m-%d %H:%M")
    except (ValueError, TypeError, KeyError):
        return None


def schedule_change(game, detail):
    """Fields to update when the API has another date/time than fb_games."""
    fields = {}
    if detail["date"] and detail["date"] != game.get("date"):
        fields["date"] = detail["date"]
        fields["weekday"] = detail["weekday"]
    api_time = detail["time"] or ""
    if _TIME_RE.match(api_time) and api_time[:5] != (game.get("time") or "")[:5]:
        fields["time"] = api_time
    return fields


def decide(game, detail, today, load_events):
    """→ (action, fields, note). Actions: result, postponed, review, running,
    cancelled, open."""
    texts = detail["result_texts"]
    cell = parse_result_cell(texts)
    result, status, note = check_result(texts)

    if status == NONE:
        if cell["headline"].startswith("-") or "abgesagt" in detail["date_raw"].lower():
            return "cancelled", {}, cell["headline"] or detail["date_raw"]
        change = schedule_change(game, detail)
        if change:
            return "postponed", change, None
        return "open", {}, None

    # Today's headline may be a live score. (A missing/odd API date counts as today.)
    if (detail["date"] or today) >= today and not feed_has_ended(load_events()):
        return "running", {}, cell["headline"]

    if status == REVIEW:
        _, _, note = check_result(texts, last_feed_score(load_events()))
        return "review", {}, f"API {texts}: {note}"

    return "result", {"result": result, **schedule_change(game, detail)}, None


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run=False, days=LOOKBACK_DAYS, game_ids=None):
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — aborting.")
        sys.exit(1)

    now = swiss_now()
    today = now.strftime("%Y-%m-%d")
    log.info(f"=== Results pass{' (DRY RUN — nothing is written)' if dry_run else ''} — {now:%Y-%m-%d %H:%M} Swiss time ===")

    if game_ids:
        games = sb_open_games(f"game_id=in.({','.join(game_ids)})")
        log.info(f"  {len(games)} of {len(game_ids)} requested games have no result yet")
    else:
        since = (now - timedelta(days=days)).strftime("%Y-%m-%d")
        games = sb_open_games(f"date=gte.{since}&date=lte.{today}")
        log.info(f"  {len(games)} games from {since} to {today} without a result")
    games.sort(key=lambda g: (g.get("date") or "", g.get("time") or "", g["game_id"]))

    stats = {k: 0 for k in ("result", "postponed", "open", "running", "cancelled",
                            "review", "too_early", "api_failed", "gone", "error")}
    reviews, by_league = [], {}

    for game in games:
        gid = str(game["game_id"])
        label = f"{gid} {game.get('date')} {game.get('league')}: {game.get('home_team_raw')} – {game.get('away_team_raw')}"

        kick = kickoff(game)
        if kick and now < kick + timedelta(minutes=MIN_GAME_MINUTES):
            stats["too_early"] += 1
            continue

        time.sleep(SLEEP)
        detail = fetch_detail(gid)
        if detail is None:
            stats["api_failed"] += 1
            log.warning(f"  ? {label} — no usable answer from the API")
            continue

        cache = {}
        def load_events():
            if "rows" not in cache:
                cache["rows"] = fetch_event_rows(gid)
            return cache["rows"]

        action, fields, note = decide(game, detail, today, load_events)
        stats[action] += 1

        if action in ("open", "running", "cancelled"):
            if action != "open":
                log.info(f"  · {label} — {action} ({note})")
            continue
        if action == "review":
            reviews.append(f"{label} — {note}")
            log.warning(f"  ! {label} — NOT written, needs a look: {note}")
            continue

        what = ", ".join(f"{k}={v}" for k, v in fields.items())
        if action == "result":
            by_league[game.get("league")] = by_league.get(game.get("league"), 0) + 1
        log.info(f"  {'→' if action == 'postponed' else '✓'} {label} — {what}")

        if dry_run:
            continue
        try:
            if sb_patch_open_game(gid, fields) == 0:
                stats["gone"] += 1
                log.info(f"    {gid}: row has a result by now — left alone")
        except Exception as e:
            stats["error"] += 1
            log.error(f"    {e}")

    log.info("\n── Summary ─────────────────")
    for league, n in sorted(by_league.items(), key=lambda x: (-x[1], str(x[0]))):
        log.info(f"  {n:>4}  {league}")
    log.info("  " + ", ".join(f"{k}: {v}" for k, v in stats.items() if v))
    if reviews:
        log.warning(f"  {len(reviews)} game(s) need a manual look — their result was NOT written:")
        for line in reviews:
            log.warning(f"    {line}")
            print(f"::warning title=Result needs review::{line}")
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a", encoding="utf-8") as f:
                f.write("### Results that need a manual look (not written)\n\n")
                f.writelines(f"- {line}\n" for line in reviews)

    if stats["error"]:
        sys.exit(1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    ap.add_argument("--days", type=int, default=LOOKBACK_DAYS, help="look back this many days")
    ap.add_argument("--game-ids", default="", help="comma-separated game ids instead of the date window")
    args = ap.parse_args()

    ids = [g.strip() for g in args.game_ids.split(",") if g.strip()]
    bad = [g for g in ids if not g.isdigit()]
    if bad:
        sys.exit(f"game ids must be numeric: {bad}")
    run(dry_run=args.dry_run, days=args.days, game_ids=ids)
