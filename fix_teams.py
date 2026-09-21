"""Re-credit goals and penalties that the old importer gave to the wrong team.

Until ce55095 fetch_and_store_goals compared the feed's CLUB name ("Tigers
Langnau") exactly with the game's TEAM name ("Tigers Langnau II") and fell back
to the away team. In such games every goal and most penalties sit on the away
side — in SQLite and in Supabase (fb_goals / fb_penalties).

This script fixes the stored rows with the same rules the importer uses now:
  goals      side from the score change (score_at_goal, in playing order).
             Steps that aren't clear from the stored scores (an own goal in
             between isn't stored) are looked up in the game_events feed —
             only for games that need fixing anyway. Still unclear → untouched.
  penalties  the player's lineup side, else the feed's club name (team_raw) as
             a prefix of exactly one team name. Lineup and name disagree, or
             neither decides → untouched.
Only the team columns change. Fixed goals get the full team names in
team_scored_raw / team_conceded_raw, like every other row written before ce55095.
A game is skipped if its team ids are missing or its last stored score doesn't
fit the result (max. one goal apart: shootout winner / final own goal).

Usage:
  python fix_teams.py --dry-run                 # report only, writes nothing
  python fix_teams.py                           # SQLite + Supabase
  python fix_teams.py --target sqlite|supabase  # one of them

Safe to re-run: rows that are already right are not touched.
"""
import argparse, logging, os, re, sys, time
from collections import Counter, defaultdict

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetch_lupl import (
    get_db, api_get, unwrap, cell_text, SLEEP, SESSION,
    SUPABASE_URL, SUPABASE_SERVICE_KEY,
    _goal_sides, _norm_team, _SCORE_RE,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("fix_teams")

PAGE = 1000
OTHER = {"home": "away", "away": "home"}


# ══════════════════════════════════════════════════════════════════════
# RULES (shared by SQLite and Supabase)
# ══════════════════════════════════════════════════════════════════════

def parse_score(s):
    m = re.fullmatch(r"\s*(\d+):(\d+)\s*", s or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def sides_from_scores(scores):
    """{score_at_goal: "home" | "away" | None} for one game, from the stored scores."""
    parsed = sorted(((parse_score(s), s) for s in set(scores) if parse_score(s)),
                    key=lambda t: (sum(t[0]), t[0]))
    sides, ph, pa = {}, 0, 0
    for (h, a), s in parsed:
        sides[s] = "home" if (h > ph and a == pa) else "away" if (a > pa and h == ph) else None
        ph, pa = h, a
    return sides


def result_fits(result, scores):
    m = re.match(r"\s*(\d+)\s*:\s*(\d+)", result or "")
    parsed = [p for p in map(parse_score, scores) if p]
    if not m or not parsed:
        return False
    H, A = int(m.group(1)), int(m.group(2))
    h, a = max(p[0] for p in parsed), max(p[1] for p in parsed)
    return h <= H and a <= A and (H - h) + (A - a) <= 1


def feed_sides(game_id):
    """{score: side} from the game_events feed — knows about own goals."""
    time.sleep(SLEEP)
    raw = api_get(f"game_events/{game_id}")
    if not raw:
        return {}
    rows = [row for region in unwrap(raw).get("regions", []) for row in region.get("rows", [])]
    out = {}
    for i, side in _goal_sides(rows).items():
        event = cell_text(rows[i]["cells"][1])
        if "Torschütze" in event and side:
            out[_SCORE_RE.search(event).group(0)] = side
    return out


def team_ids_ok(game):
    h, a = game["home_id"], game["away_id"]
    return bool(h) and bool(a) and h != a


def plan_goals(game, goal_rows, use_feed=True):
    """goal_rows: [(score_at_goal, team_scored_id)] → ({side: [scores]}, note | None)"""
    scores = [s for s, _ in goal_rows]
    sides = sides_from_scores(scores)
    want = {"home": game["home_id"], "away": game["away_id"]}
    wrong = [s for s, tid in goal_rows if sides.get(s) and tid != want[sides[s]]]
    if not wrong:
        return {}, None
    if not team_ids_ok(game):
        return {}, "team ids missing"
    if not result_fits(game["result"], scores):
        return {}, f"last stored score doesn't fit the result {game['result']!r}"
    note = None
    unclear = [s for s, side in sides.items() if side is None]
    if unclear and use_feed:
        feed = feed_sides(game["game_id"])
        for s in unclear:
            sides[s] = feed.get(s)
        left = [s for s in unclear if sides[s] is None]
        note = f"{len(unclear) - len(left)} unclear step(s) resolved from the feed" + \
               (f", {len(left)} left untouched" if left else "")
    fixes = defaultdict(list)
    for s, tid in goal_rows:
        if sides.get(s) and tid != want[sides[s]] and s not in fixes[sides[s]]:
            fixes[sides[s]].append(s)
    return dict(fixes), note


def penalty_side(game, team_raw, player_id):
    """→ ("home" | "away" | None, how)"""
    lineup = None
    if player_id is not None:
        in_home, in_away = player_id in game["home_players"], player_id in game["away_players"]
        if in_home != in_away:
            lineup = "home" if in_home else "away"
    name, feed = None, _norm_team(team_raw)
    if feed:
        is_home = _norm_team(game["home_name"]).startswith(feed)
        is_away = _norm_team(game["away_name"]).startswith(feed)
        if is_home != is_away:
            name = "home" if is_home else "away"
    if lineup and name and lineup != name:
        return None, "conflict"
    if lineup or name:
        return lineup or name, "lineup" if lineup else "name"
    return None, "undecided"


def plan_penalties(game, pen_rows, stats):
    """pen_rows: [(team_raw, player_id, team_id)] → {(team_raw, player_id): side}"""
    want = {"home": game["home_id"], "away": game["away_id"]}
    fixes = {}
    for team_raw, player_id, team_id in pen_rows:
        side, how = penalty_side(game, team_raw, player_id)
        stats[f"penalties decided by {how}" if side else f"penalties {how} (untouched)"] += 1
        if side and team_id != want[side] and team_ids_ok(game):
            fixes[(team_raw, player_id)] = side
    return fixes


def plan(games, goals, penalties, use_feed=True):
    """→ (goal_plan {gid: {side: [scores]}}, pen_plan {gid: {(team_raw, pid): side}}, stats, skipped)"""
    stats, skipped, goal_plan, pen_plan = Counter(), [], {}, {}
    for gid, rows in goals.items():
        game = games.get(gid)
        if not game:
            stats["goal games without a games row (untouched)"] += 1
            continue
        fixes, note = plan_goals(game, rows, use_feed)
        if fixes:
            goal_plan[gid] = fixes
            stats["goals to fix"] += sum(len(v) for v in fixes.values())
            if note:
                log.info(f"  {gid}: {note}")
        elif note:
            skipped.append((gid, note))
    for gid, rows in penalties.items():
        game = games.get(gid)
        if not game:
            continue
        fixes = plan_penalties(game, rows, stats)
        if fixes:
            pen_plan[gid] = fixes
            stats["penalty groups to fix"] += len(fixes)
    return goal_plan, pen_plan, stats, skipped


def report(label, games, goal_plan, pen_plan, stats, skipped):
    log.info(f"\n── {label}: {len(goal_plan)} games with wrongly credited goals, "
             f"{len(pen_plan)} games with wrongly credited penalties")
    for k, v in sorted(stats.items()):
        log.info(f"    {v:>7}  {k}")
    by_season = Counter(games[g]["season"] for g in goal_plan)
    log.info("    goal games by season: " + ", ".join(f"{s}: {n}" for s, n in sorted(by_season.items(), key=lambda kv: str(kv[0]))))
    suffix = re.compile(r"\s(II|III|IV|V|VI|VII)$")
    n_sfx = sum(1 for g in goal_plan if suffix.search(games[g]["home_name"] or ""))
    log.info(f"    home team ends in II/III/IV…: {n_sfx} · other name differences: {len(goal_plan) - n_sfx}")
    for gid in list(goal_plan)[:8]:
        g, fx = games[gid], goal_plan[gid]
        log.info(f"    e.g. {gid}  {g['home_name']} – {g['away_name']}  {g['result']}: "
                 f"→ home {len(fx.get('home', []))}, → away {len(fx.get('away', []))}")
    if skipped:
        log.info(f"    skipped {len(skipped)} games:")
        for gid, why in skipped[:15]:
            log.info(f"      {gid}  {games[gid]['home_name']} – {games[gid]['away_name']}: {why}")


# ══════════════════════════════════════════════════════════════════════
# SQLITE
# ══════════════════════════════════════════════════════════════════════

def load_sqlite(conn):
    games = {}
    for r in conn.execute("""SELECT game_id, home_team_id, away_team_id, home_team_raw,
                                    away_team_raw, result, season FROM games"""):
        games[r[0]] = {"game_id": r[0], "home_id": r[1], "away_id": r[2], "home_name": r[3],
                       "away_name": r[4], "result": r[5], "season": r[6],
                       "home_players": set(), "away_players": set()}
    # LEGACY STORAGE CONVENTION (see fetch_lupl.build_lineup_map):
    # lineups.team_id holds the OTHER team.
    for gid, tid, pid in conn.execute("SELECT game_id, team_id, player_id FROM lineups WHERE player_id IS NOT NULL"):
        g = games.get(gid)
        if g and tid == g["home_id"]:
            g["away_players"].add(pid)
        elif g and tid == g["away_id"]:
            g["home_players"].add(pid)
    goals, penalties = defaultdict(list), defaultdict(list)
    for gid, s, tid in conn.execute("SELECT DISTINCT game_id, score_at_goal, team_scored_id FROM goals"):
        goals[gid].append((s, tid))
    for gid, tr, pid, tid in conn.execute("SELECT DISTINCT game_id, team_raw, player_id, team_id FROM penalties"):
        penalties[gid].append((tr, pid, tid))
    return games, goals, penalties


def apply_sqlite(conn, games, goal_plan, pen_plan):
    n_goals = n_pens = 0
    for gid, fixes in goal_plan.items():
        g = games[gid]
        for side, scores in fixes.items():
            o = OTHER[side]
            for s in scores:
                n_goals += conn.execute(
                    """UPDATE goals SET team_scored_id=?, team_conceded_id=?,
                                        team_scored_raw=?, team_conceded_raw=?
                       WHERE game_id=? AND score_at_goal=?""",
                    (g[f"{side}_id"], g[f"{o}_id"], g[f"{side}_name"], g[f"{o}_name"], gid, s)).rowcount
    for gid, fixes in pen_plan.items():
        g = games[gid]
        for (team_raw, pid), side in fixes.items():
            n_pens += conn.execute(
                "UPDATE penalties SET team_id=? WHERE game_id=? AND team_raw IS ? AND player_id IS ?",
                (g[f"{side}_id"], gid, team_raw, pid)).rowcount
    return n_goals, n_pens


# ══════════════════════════════════════════════════════════════════════
# SUPABASE
# ══════════════════════════════════════════════════════════════════════

def _sb_auth():
    return {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}


def _sb_request(method, table, params, **kw):
    for attempt in range(4):
        try:
            r = SESSION.request(method, f"{SUPABASE_URL}/rest/v1/{table}", params=params,
                                timeout=90, **kw)
            if r.status_code < 500:
                break
        except Exception as e:
            log.warning(f"    {method} {table} attempt {attempt + 1}/4 failed: {e}")
        time.sleep(2 * (attempt + 1))
    if r.status_code >= 400:
        log.error(f"  Supabase {method} {table} [{r.status_code}]: {r.text[:300]}")
    r.raise_for_status()
    return r


def sb_get_all(table, select, order):
    """All rows of a table. Rows with equal `order` keys are identical in `select`,
    so offset paging can't lose anything that matters."""
    rows, total = [], None
    while total is None or len(rows) < total:
        r = _sb_request("GET", table, {"select": select, "order": order, "limit": PAGE, "offset": len(rows)},
                        headers={**_sb_auth(), **({"Prefer": "count=exact"} if total is None else {})})
        batch = r.json()
        if total is None:
            total = int(r.headers.get("Content-Range", "*/0").split("/")[-1] or 0)
        if not batch:
            break
        rows += batch
    log.info(f"    {table}: {len(rows)} rows read")
    return rows


def sb_patch(table, params, body):
    assert "game_id" in params and params["game_id"].startswith("eq."), "PATCH must be scoped to one game"
    r = _sb_request("PATCH", table, {**params, "select": "game_id"}, json=body,
                    headers={**_sb_auth(), "Prefer": "return=representation"})
    return len(r.json())


def load_supabase():
    games = {}
    for r in sb_get_all("fb_games", "game_id,home_team_id,away_team_id,home_team_raw,away_team_raw,"
                                    "result,season,home_lineup,away_lineup", "game_id"):
        # fb_games lineups are already the real sides (the sync swaps them back)
        games[r["game_id"]] = {"game_id": r["game_id"], "home_id": r["home_team_id"], "away_id": r["away_team_id"],
                               "home_name": r["home_team_raw"], "away_name": r["away_team_raw"],
                               "result": r["result"], "season": r["season"],
                               "home_players": set(r.get("home_lineup") or []),
                               "away_players": set(r.get("away_lineup") or [])}
    goals, penalties = defaultdict(set), defaultdict(set)
    for r in sb_get_all("fb_goals", "game_id,score_at_goal,team_scored_id", "game_id,score_at_goal,team_scored_id"):
        goals[r["game_id"]].add((r["score_at_goal"], r["team_scored_id"]))
    for r in sb_get_all("fb_penalties", "game_id,team_raw,player_id,team_id", "game_id,team_raw,player_id,team_id"):
        penalties[r["game_id"]].add((r["team_raw"], r["player_id"], r["team_id"]))
    return games, {k: sorted(v, key=str) for k, v in goals.items()}, {k: sorted(v, key=str) for k, v in penalties.items()}


def apply_supabase(games, goal_plan, pen_plan):
    n_goals = n_pens = 0
    for i, (gid, fixes) in enumerate(goal_plan.items(), 1):
        g = games[gid]
        for side, scores in fixes.items():
            o = OTHER[side]
            n_goals += sb_patch("fb_goals",
                                {"game_id": f"eq.{gid}",
                                 "score_at_goal": "in.(" + ",".join(f'"{s}"' for s in scores) + ")"},
                                {"team_scored_id": g[f"{side}_id"], "team_conceded_id": g[f"{o}_id"],
                                 "team_scored_raw": g[f"{side}_name"], "team_conceded_raw": g[f"{o}_name"]})
        if i % 200 == 0:
            log.info(f"    goals: {i}/{len(goal_plan)} games")
    for gid, fixes in pen_plan.items():
        g = games[gid]
        for (team_raw, pid), side in fixes.items():
            n_pens += sb_patch("fb_penalties",
                               {"game_id": f"eq.{gid}",
                                "team_raw": "is.null" if team_raw is None else f"eq.{team_raw}",
                                "player_id": "is.null" if pid is None else f"eq.{pid}"},
                               {"team_id": g[f"{side}_id"]})
    return n_goals, n_pens


# ══════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    ap.add_argument("--target", choices=["both", "sqlite", "supabase"], default="both")
    args = ap.parse_args()
    if args.target != "sqlite" and not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_SERVICE_KEY not set — use --target sqlite or set the key")
        sys.exit(1)
    log.info(f"=== Fix team credits ({args.target}){' (DRY RUN)' if args.dry_run else ''} ===")
    left = 0

    if args.target in ("both", "sqlite"):
        conn = get_db()
        games, goals, penalties = load_sqlite(conn)
        goal_plan, pen_plan, stats, skipped = plan(games, goals, penalties)
        report("SQLite", games, goal_plan, pen_plan, stats, skipped)
        if not args.dry_run:
            n_goals, n_pens = apply_sqlite(conn, games, goal_plan, pen_plan)
            conn.commit()
            log.info(f"    SQLite updated: {n_goals} goals, {n_pens} penalties")
            again = plan(*load_sqlite(conn), use_feed=False)
            left += len(again[0]) + len(again[1])
        conn.close()

    if args.target in ("both", "supabase"):
        log.info("\n── Supabase: reading fb_games / fb_goals / fb_penalties")
        games, goals, penalties = load_supabase()
        goal_plan, pen_plan, stats, skipped = plan(games, goals, penalties)
        report("Supabase", games, goal_plan, pen_plan, stats, skipped)
        if not args.dry_run:
            n_goals, n_pens = apply_supabase(games, goal_plan, pen_plan)
            log.info(f"    Supabase updated: {n_goals} goals, {n_pens} penalties")

    if args.dry_run:
        log.info("\nDry run — nothing written.")
    else:
        log.info(f"\n✓ Done. SQLite games still wrong after the fix: {left}")
        if left:
            sys.exit(1)


if __name__ == "__main__":
    main()
