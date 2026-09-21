"""Re-import specific games: lineups (with roster fallback), goals, penalties.

Built for fantasy round 2 (18.–20.09.2026), where swiss unihockey published
no lineups: player IDs couldn't be resolved, so almost nobody scored.

Usage:
  python repair_games.py --dry-run             # round 2 + PENALTY_FIX, report only, writes nothing
  python repair_games.py                       # round 2 + PENALTY_FIX, apply
  python repair_games.py 1097599 1097600       # specific games
  python repair_games.py --fix-games-played    # also rewrite fb_players.games_played
                                               # for ALL players from SQLite

Per game (one SQLite transaction each):
  1. delete the game's lineups / goals / penalties in SQLite
  2. re-fetch lineups (team roster fills in if the lineup is missing)
  3. re-fetch game events (goals + penalties), resolve player IDs,
     then apply the MANUAL corrections
  4. sanity check — if the events call failed or returned no goals although
     the result has goals, roll back and keep the old data
Then, for all repaired games:
  5. delete their rows from fb_goals / fb_penalties in Supabase — goal_id and
     penalty_id are SQLite autoincrement IDs, a re-import gets new ones and
     the old rows would otherwise stay → every goal counted twice
  6. sync games + goals + penalties + players (fetch_daily.sync_games_to_supabase)

Safe to re-run: when swiss unihockey publishes the real lineups, run it again
for the same games and the roster rows are replaced by the real lineup.
After the sync, the fantasy gameweeks still have to be re-scored.
"""
import argparse, logging, re, sys, os, time

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from fetch_lupl import (
    get_db, build_lineup_map, fetch_and_store_goals, SLEEP,
    SUPABASE_URL, SUPABASE_SERVICE_KEY, FANTASY_LEAGUES,
    _sb_upsert, _batched,
)
from fetch_daily import sync_games_to_supabase

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("repair")

# Fantasy round 2: men 18.–20.09., women 19.–20.09.2026
ROUND_2 = [
    "1097076", "1097079", "1097080", "1097077", "1097078", "1097075", "1097069",  # Herren
    "1097598", "1097599", "1097601", "1097597", "1097600",                          # Damen
]
# Round 1 games that lost a 2'+2' penalty to the old parser (lineups were fine)
PENALTY_FIX = ["1097674"]  # Damen 12.09. UBO – Uri

# Events swiss unihockey can't disambiguate (same abbreviation, same team).
# Re-applied after every re-import of the game, so a re-run keeps them.
# (game_id, minute, role, name as in the events, player_id)
MANUAL = [
    ("1097077", "09:21", "scorer",  "R. Chiplunkar", 465906),  # Rohit Chiplunkar
    ("1097077", "09:21", "assist",  "R. Chiplunkar", 443950),  # Rahul Chiplunkar
    ("1097077", "38:12", "penalty", "R. Chiplunkar", 465906),  # Rohit, 2'+2'
]
_ROLE_COLS = {
    "scorer":  ("goals",     "scorer_id", "scorer_name", "scorer_raw"),
    "assist":  ("goals",     "assist_id", "assist_name", "assist_raw"),
    "penalty": ("penalties", "player_id", "player_name", "player_raw"),
}


def apply_manual(conn, gid):
    for g, minute, role, raw_name, pid in MANUAL:
        if g != gid:
            continue
        table, id_col, name_col, raw_col = _ROLE_COLS[role]
        row = conn.execute("SELECT player_raw FROM lineups WHERE game_id=? AND player_id=? LIMIT 1",
                           (gid, pid)).fetchone()
        if not row:
            log.warning(f"  {gid}: manual {role} {raw_name} → {pid} is not in this game's lineup "
                        f"(the fantasy only scores lineup players)")
        cur = conn.execute(
            f"UPDATE {table} SET {id_col}=?, {name_col}=COALESCE(?, {name_col}) "
            f"WHERE game_id=? AND minute=? AND {raw_col}=?",
            (pid, row[0] if row else None, gid, minute, raw_name))
        if cur.rowcount != 1:
            log.warning(f"  {gid}: manual {role} at {minute} matched {cur.rowcount} rows (expected 1)")


def expected_goals(result):
    m = re.match(r"^\s*(\d+)\s*:\s*(\d+)", result or "")
    return int(m.group(1)) + int(m.group(2)) if m else None


def repair_game(conn, gid):
    """Re-import one game inside a transaction. Returns a report dict, or None if skipped."""
    g = conn.execute("""
        SELECT game_id, home_team_id, away_team_id, home_team_raw, away_team_raw,
               date, weekday, season, league, result
        FROM games WHERE game_id = ?""", (gid,)).fetchone()
    if not g:
        log.warning(f"  {gid}: not in SQLite — skipped")
        return None

    roster_fb = g["league"] in FANTASY_LEAGUES
    for table in ("lineups", "goals", "penalties"):
        conn.execute(f"DELETE FROM {table} WHERE game_id = ?", (gid,))

    lineup_report = {}
    lineup_map = build_lineup_map(conn, gid, g["home_team_id"], g["away_team_id"],
                                  g["home_team_raw"], g["away_team_raw"], g["season"], g["date"],
                                  roster_fallback=roster_fb, report=lineup_report)
    time.sleep(SLEEP)
    res = fetch_and_store_goals(conn, gid, g["home_team_id"], g["away_team_id"],
                                g["home_team_raw"], g["away_team_raw"],
                                g["date"], g["weekday"], g["season"], lineup_map=lineup_map)
    if isinstance(res, tuple):
        apply_manual(conn, gid)

    exp = expected_goals(g["result"])
    if not isinstance(res, tuple):
        conn.rollback()
        log.error(f"  {gid}: game events fetch failed — rolled back, old data kept")
        return None
    if res[0] == 0 and exp:
        conn.rollback()
        log.error(f"  {gid}: events returned 0 goals for {g['result']} — rolled back, old data kept")
        return None

    q = lambda sql: conn.execute(sql, (gid,)).fetchone()[0]
    unresolved = [r[0] for r in conn.execute("""
        SELECT scorer_raw FROM goals WHERE game_id=? AND scorer_raw IS NOT NULL AND scorer_id IS NULL
        UNION ALL
        SELECT assist_raw FROM goals WHERE game_id=? AND assist_raw IS NOT NULL AND assist_id IS NULL
        UNION ALL
        SELECT player_raw FROM penalties WHERE game_id=? AND player_raw IS NOT NULL AND player_id IS NULL
    """, (gid, gid, gid))]

    return {
        "gid": gid,
        "match": f"{g['home_team_raw']} – {g['away_team_raw']} {g['result']}",
        "lineup": lineup_report,
        "goals": res[0], "expected": exp,
        "scorer_ok": q("SELECT COUNT(*) FROM goals WHERE game_id=? AND scorer_id IS NOT NULL"),
        "assists": q("SELECT COUNT(*) FROM goals WHERE game_id=? AND assist_raw IS NOT NULL"),
        "assist_ok": q("SELECT COUNT(*) FROM goals WHERE game_id=? AND assist_id IS NOT NULL"),
        "pens": res[1],
        "pen_ok": q("SELECT COUNT(*) FROM penalties WHERE game_id=? AND player_id IS NOT NULL"),
        "unresolved": unresolved,
    }


def print_report(rep):
    def side(s):
        out = f"{s['lineup']} lineup"
        if s["roster"]: out += f" + {s['roster']} roster"
        if s["pool"]:   out += f" + {s['pool']} pool"
        return out
    lu = rep["lineup"]
    goal_note = "" if rep["goals"] == rep["expected"] else f"  ⚠ result says {rep['expected']}"
    log.info(f"  {rep['gid']}  {rep['match']}")
    log.info(f"      home {side(lu['home'])} | away {side(lu['away'])}")
    log.info(f"      goals {rep['goals']}{goal_note} · scorer IDs {rep['scorer_ok']}/{rep['goals']} · "
             f"assist IDs {rep['assist_ok']}/{rep['assists']} · penalty IDs {rep['pen_ok']}/{rep['pens']}")
    if rep["unresolved"]:
        log.info(f"      unresolved: {', '.join(sorted(set(rep['unresolved'])))}")


def sb_delete(table, game_ids):
    ids = ",".join(f'"{g}"' for g in game_ids)
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}",
        params={"game_id": f"in.({ids})"},
        headers={"apikey": SUPABASE_SERVICE_KEY,
                 "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                 "Prefer": "return=representation"},
        timeout=30,
    )
    r.raise_for_status()
    return len(r.json())


def fix_games_played(conn):
    """Rewrite fb_players.games_played for every player from the full SQLite history."""
    rows = [{"player_id": r[0], "player_name": r[1], "games_played": r[2]} for r in conn.execute("""
        SELECT player_id, player_raw,
               SUM(CASE WHEN COALESCE(source, 'lineup') = 'lineup' THEN 1 ELSE 0 END)
        FROM lineups WHERE player_id IS NOT NULL GROUP BY player_id
    """)]
    for batch in _batched(rows, 500):
        _sb_upsert("fb_players", batch)
    log.info(f"  fb_players.games_played rewritten for {len(rows)} players")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("game_ids", nargs="*", help="default: fantasy round 2 + PENALTY_FIX")
    ap.add_argument("--dry-run", action="store_true", help="fetch + report, write nothing")
    ap.add_argument("--fix-games-played", action="store_true")
    args = ap.parse_args()
    game_ids = args.game_ids or ROUND_2 + PENALTY_FIX

    if not args.dry_run and not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — use --dry-run or set the key")
        sys.exit(1)

    conn = get_db()
    log.info(f"=== Repair {len(game_ids)} games{' (DRY RUN)' if args.dry_run else ''} ===")

    repaired = []
    for gid in game_ids:
        rep = repair_game(conn, gid)
        if not rep:
            continue
        print_report(rep)
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()
            repaired.append(gid)

    if args.dry_run:
        log.info("\nDry run — nothing written.")
        conn.close()
        return

    if repaired:
        log.info(f"\n── Supabase: replacing goals/penalties of {len(repaired)} games")
        log.info(f"    fb_goals deleted: {sb_delete('fb_goals', repaired)}")
        log.info(f"    fb_penalties deleted: {sb_delete('fb_penalties', repaired)}")
        sync_games_to_supabase(conn, repaired)

    if args.fix_games_played:
        fix_games_played(conn)

    conn.close()
    log.info(f"\n✓ Repaired {len(repaired)}/{len(game_ids)} games. Re-score the fantasy gameweeks next.")


if __name__ == "__main__":
    main()
