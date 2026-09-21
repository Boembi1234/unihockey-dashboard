"""Set season from the game date wherever SQLite holds a stale value.

fetch_daily.run() had a hard-coded `season = 2025` until 12.09.2026 (fixed in
e681733), so every game imported between 08.08. and 12.09.2026 is stored as
2025 in games / goals / penalties / lineups. A later sync of such a game
(repair_games.py did it for 1097674) pushes the 2025 into Supabase and the
game drops out of the fantasy scoring.

SQLite only — Supabase is not touched (no key needed, none used).

Usage:
  python fix_seasons.py --dry-run    # list only, writes nothing
  python fix_seasons.py              # apply
"""
import argparse, logging, os, sys
from collections import Counter
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetch_lupl import get_db
from fetch_daily import season_from_date

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("fix_seasons")

TABLES = ("games", "goals", "penalties", "lineups")


def find_wrong(conn):
    """→ [(game_id, date, league, stored_season, season_by_date)]"""
    wrong = []
    for g in conn.execute("SELECT game_id, date, league, season FROM games ORDER BY date, game_id"):
        try:
            datetime.strptime(g["date"], "%Y-%m-%d")
        except (ValueError, TypeError):
            log.warning(f"  {g['game_id']}: unparsable date {g['date']!r} — skipped")
            continue
        season = season_from_date(g["date"])
        if season != g["season"]:
            wrong.append((g["game_id"], g["date"], g["league"], g["season"], season))
    return wrong


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="list only, write nothing")
    args = ap.parse_args()

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    wrong = find_wrong(conn)
    log.info(f"=== Fix seasons{' (DRY RUN)' if args.dry_run else ''}: "
             f"{len(wrong)} of {total} games have a season that doesn't match their date ===")

    for gid, date, league, old, new in wrong:
        log.info(f"  {gid}  {date}  {league}: {old} → {new}")

    by_league = Counter((league, old, new) for _, _, league, old, new in wrong)
    log.info("\n── By league")
    for (league, old, new), n in sorted(by_league.items(), key=lambda kv: -kv[1]):
        log.info(f"  {n:>4}  {league}: {old} → {new}")

    # One transaction for everything: either all four tables agree afterwards or nothing changed.
    updated = Counter()
    for gid, _, _, _, new in wrong:
        for table in TABLES:
            cur = conn.execute(
                f"UPDATE {table} SET season = ? WHERE game_id = ? AND (season IS NULL OR season <> ?)",
                (new, gid, new))
            updated[table] += cur.rowcount

    log.info("\n── Rows " + ("that would be updated" if args.dry_run else "updated"))
    for table in TABLES:
        log.info(f"  {table}: {updated[table]}")

    if args.dry_run:
        conn.rollback()
        log.info("\nDry run — nothing written.")
    else:
        conn.commit()
        left = len(find_wrong(conn))
        mismatch = sum(conn.execute(
            f"SELECT COUNT(*) FROM {t} x JOIN games g ON g.game_id = x.game_id "
            f"WHERE COALESCE(x.season, -1) <> g.season").fetchone()[0] for t in TABLES[1:])
        log.info(f"\n✓ Done. Games still wrong: {left} · child rows disagreeing with their game: {mismatch}")
        if left or mismatch:
            sys.exit(1)
    conn.close()


if __name__ == "__main__":
    main()
