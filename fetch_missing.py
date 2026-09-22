"""Backfill: add the games of past seasons that fb_games never got.

fetch_upcoming.py used to find games only through the league tabs and their
round slider. Rounds without a tab — the promotion/relegation playoffs above
all, but also forfeits and the odd qualification game — were never fetched, and
the cups of the seasons before 2026 not at all (217 games of 2025/26 for the
182 teams in fb_games). `mode=team&team_id=…&season=…` lists a team's whole
season, so this script walks it for every team of the season:

    fb_games teams of the season → mode=team → game ids not in fb_games
      → /games/<id>            → fb_games row (league/group/phase from the
                                 subtitle, result if the game is over)
      → lineups, goals, penalties for leagues down to U14 (fetch_daily.import_game),
        synced to Supabase in batches — needs the SQLite DB from the release

    SUPABASE_SERVICE_KEY=... python fetch_missing.py --seasons 2025 [--dry-run] [--no-stats]

For the running season fetch_upcoming.py does the same every day (stage 1c).
"""
import os, sys, time, logging, argparse
from datetime import date, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetch_upcoming import (
    SUPABASE_SERVICE_KEY, SLEEP, DAYS_AHEAD, sb_rows, sb_season_games, sweep_teams,
    fetch_game_detail, split_venues, sb_insert_ignore, sb_upsert,
)
import fetch_daily
from fetch_daily import (
    canonical_league, wanted_league, FIXTURE_TO_STATS_LEAGUE, BATCH,
    get_db, ensure_sync_table, unsynced_games, import_game, sync_batch, DEFERRED,
)
from su_subtitle import parse_subtitle

log = logging.getLogger(__name__)


def find_missing(seasons, known_ids):
    """→ ([(game_id, None)], team_ids) over all seasons, one team sweep per
    season. Fixtures more than DAYS_AHEAD days ahead are left out, as in the
    daily run — only matters for the running season."""
    cutoff = (date.today() + timedelta(days=DAYS_AHEAD)).isoformat()
    seen = set()
    found, all_teams = [], set()
    for season in seasons:
        _, teams, _ = sb_season_games(season)
        all_teams |= teams
        log.info(f"Season {season}: {len(teams)} teams in fb_games — sweeping their game lists…")
        pairs = sweep_teams(season, teams, known_ids, seen, until=cutoff)
        log.info(f"  {len(pairs)} games of {season}/{str(season + 1)[2:]} not in fb_games")
        found.extend(pairs)
    return found, all_teams


def fetch_rows(pairs, today):
    rows, skipped = [], 0
    for i, (gid, _) in enumerate(pairs, 1):
        row = fetch_game_detail(gid, None, None, today=today)
        if row:
            rows.append(row)
        else:
            skipped += 1
        if i % 50 == 0 or i == len(pairs):
            log.info(f"  {i}/{len(pairs)} fetched ({skipped} skipped: cancelled or unknown competition)")
        time.sleep(SLEEP)
    return rows


def report(rows, today):
    by = {}
    for g in rows:
        key = (g["league"], g["phase"])
        by.setdefault(key, [0, 0])
        by[key][0] += 1
        by[key][1] += "result" in g
    log.info(f"\n  {'league':36} {'phase':14} games  with result")
    for (league, phase), (n, res) in sorted(by.items(), key=lambda x: (-x[1][0], x[0])):
        log.info(f"  {league:36} {phase:14} {n:5}  {res:5}")
    open_past = [g for g in rows if g["date"] < today and "result" not in g]
    if open_past:
        log.warning(f"  {len(open_past)} past games without a usable result (0:0 or none) — added without result:")
        for g in open_past:
            log.warning(f"    {g['game_id']} {g['date']} {g['league']}: {g['home_team_raw']} – {g['away_team_raw']}")


def import_stats(rows, max_minutes=0):
    """Lineups, goals and penalties for the backfilled games that are over and
    in a league we keep stats for. Same machinery as the daily refresh."""
    started = time.monotonic()
    wanted = []
    for g in rows:
        league = canonical_league(g["league"])
        if "result" in g and wanted_league(league):
            wanted.append({"id": str(g["game_id"]), "date": g["date"], "source": "fixture",
                           "league": FIXTURE_TO_STATS_LEAGUE.get(league, league)})
    wanted.sort(key=lambda g: (g["date"], g["id"]))
    log.info(f"\n── Stats for {len(wanted)} games (finished, leagues down to U14)…")

    conn = get_db()
    ensure_sync_table(conn)
    failed = []
    owed = unsynced_games(conn)
    if owed:
        log.info(f"  {len(owed)} games from an earlier run still to sync")
        for chunk in fetch_daily._batched(owed, BATCH):
            failed += sync_batch(conn, chunk)

    imported = skipped = deferred = resynced = 0
    goals = pens = 0
    batch = []
    for g in wanted:
        if max_minutes and (time.monotonic() - started) > max_minutes * 60:
            log.warning(f"  Stopped after {max_minutes} minutes — run again for the rest")
            break
        stored = conn.execute("SELECT subtitle FROM games WHERE game_id=?", (g["id"],)).fetchone()
        if stored:
            # Imported into SQLite before (April 2026: 74 games) but never
            # synced — fb_games has nothing of them. Sync what is there, with
            # the phase the current parser gives the stored subtitle.
            parsed = parse_subtitle(stored[0])
            conn.execute("UPDATE games SET phase = ?, league_group = ? WHERE game_id = ?",
                         (parsed["phase"], parsed["group"], g["id"]))
            conn.commit()
            resynced += 1
            batch.append(g["id"])
            continue
        result = import_game(conn, g)
        if result is None:
            skipped += 1
            continue
        if result == DEFERRED:
            deferred += 1
            continue
        goals += result[0]
        pens += result[1]
        imported += 1
        batch.append(g["id"])
        if len(batch) >= BATCH:
            log.info(f"\n── Syncing {len(batch)} games to Supabase…")
            failed += sync_batch(conn, batch)
            batch = []
    if batch:
        log.info(f"\n── Syncing {len(batch)} games to Supabase…")
        failed += sync_batch(conn, batch)
    conn.close()

    log.info(f"  Stats: {imported} games, {goals} goals, {pens} penalties imported "
             f"({skipped} failed and retried next time, {deferred} waiting for a lineup, "
             f"{resynced} already in SQLite and synced from there)")
    return failed


def run(seasons, dry_run=False, stats=True, max_minutes=0, both_teams_known=False):
    if not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — aborting.")
        sys.exit(1)
    today = date.today().isoformat()
    log.info(f"=== Backfill{' (DRY RUN — nothing is written)' if dry_run else ''}: seasons {seasons} ===")

    known_ids = {str(r["game_id"]) for r in sb_rows("game_id=not.is.null", "game_id")}
    log.info(f"  {len(known_ids)} games in fb_games altogether")

    pairs, teams = find_missing(seasons, known_ids)
    if not pairs:
        log.info("Nothing missing.")
        return

    log.info(f"\nFetching details for {len(pairs)} games…")
    rows = fetch_rows(pairs, today)
    if both_teams_known:
        n = len(rows)
        rows = [g for g in rows if g["home_team_id"] in teams and g["away_team_id"] in teams]
        log.info(f"  --both-teams-known: {len(rows)} of {n} games are between two teams already in fb_games")
    report(rows, today)
    if dry_run:
        log.info(f"\nDRY RUN — {len(rows)} games would be added to fb_games.")
        return

    venues = split_venues(rows)
    if venues:
        sb_insert_ignore("venues", venues, conflict_col="name")
    with_result = [g for g in rows if "result" in g]
    without = [g for g in rows if "result" not in g]
    for chunk in (without, with_result):
        for i in range(0, len(chunk), 500):
            sb_upsert("fb_games", chunk[i:i + 500])
    log.info(f"\n✓ {len(rows)} games added to fb_games ({len(with_result)} with result)")

    failed = import_stats(rows, max_minutes) if stats else []
    if failed:
        log.error(f"  {len(failed)} games could not be synced: {failed[:20]}")
        sys.exit(1)
    log.info("\n✓ Done!")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Add the games of past seasons that fb_games never got")
    ap.add_argument("--seasons", required=True, help="comma-separated start years, e.g. 2025 or 2024,2025")
    ap.add_argument("--dry-run", action="store_true", help="list what is missing, write nothing")
    ap.add_argument("--no-stats", action="store_true", help="fb_games rows only, no lineups/goals/penalties")
    ap.add_argument("--max-minutes", type=int, default=0, help="stop the stats import after this many minutes")
    ap.add_argument("--both-teams-known", action="store_true",
                    help="only games between two teams that are already in fb_games for that season")
    args = ap.parse_args()
    try:
        seasons = sorted({int(s) for s in args.seasons.split(",") if s.strip()})
        assert all(2000 <= s <= 2100 for s in seasons)
    except (ValueError, AssertionError):
        sys.exit(f"--seasons must be years like 2025 or 2024,2025, got {args.seasons!r}")
    run(seasons, dry_run=args.dry_run, stats=not args.no_stats, max_minutes=args.max_minutes,
        both_teams_known=args.both_teams_known)
