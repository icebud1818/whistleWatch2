#!/usr/bin/env python3
"""
WhistleWatch - Premier League Referee Bias Analyzer
Run: python3 app.py
Then open: http://localhost:8080

Cron job (once daily): restart this process — it will auto-sync any new matches.
"""

import json
import time
import threading
import ssl
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed

import firebase_admin
from firebase_admin import credentials, firestore

ssl._create_default_https_context = ssl._create_unverified_context

# ─── Config ───────────────────────────────────────────────────────────────────

HEADERS = {
    "Origin": "https://www.premierleague.com",
    "Referer": "https://www.premierleague.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

COMPETITION_ID = 8
# All seasons to track. Add future seasons here each year.
# On startup, completed seasons are skipped instantly if already in DB.
# Only seasons with missing matches trigger API calls.
ALL_SEASONS    = [2022, 2023, 2024, 2025]
FETCH_WORKERS  = 20

# Minimum game thresholds for inclusion in analytics
MIN_REF_GAMES      = 3   # ref must have officiated at least this many games overall
MIN_REF_TEAM_GAMES = 1   # ref×team pair must have at least this many games
MIN_TEAM_GAMES     = 1   # team must have at least this many games
FIREBASE_KEY   = "firebase_key.json"
PROJECT_ID     = "whistle-watch"

TEAM_MAPPING = {
    3: "Arsenal", 7: "Aston Villa", 91: "Bournemouth", 94: "Brentford",
    36: "Brighton", 90: "Burnley", 8: "Chelsea", 31: "Crystal Palace",
    11: "Everton", 54: "Fulham", 2: "Leeds United", 14: "Liverpool",
    43: "Man City", 1: "Man United", 4: "Newcastle", 17: "Nottm Forest",
    56: "Sunderland", 6: "Tottenham", 21: "West Ham", 39: "Wolves",
    13: "Leicester", 35: "West Brom", 49: "Sheffield Utd", 45: "Norwich",
    57: "Watford", 20: "Southampton", 102: "Luton", 40: "Ipswich"
}

# ─── Data Store ───────────────────────────────────────────────────────────────

cache = {
    "matches": [],
    "last_updated": None,
    "status": "idle",
    "progress": 0,
    "total": 0,
    "error": None
}

db = None

# ─── Firebase ─────────────────────────────────────────────────────────────────

def init_firebase():
    global db
    if not os.path.exists(FIREBASE_KEY):
        raise FileNotFoundError(
            f"'{FIREBASE_KEY}' not found. Download your service account key from "
            f"Firebase console → Project Settings → Service Accounts and save it as '{FIREBASE_KEY}'."
        )
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
    db = firestore.client()
    print("  Firebase connected!")


def backfill_seasons():
    """One-time migration: add season field to existing Firestore docs that lack it."""
    print("  Checking for docs missing season field...")
    # Build matchId -> season map from PL API id ranges
    # We'll query Firestore for docs where season is missing and update them in batches
    needs_update = []
    for doc in db.collection("matches").stream():
        d = doc.to_dict()
        if d.get("season") is None:
            needs_update.append(doc.id)

    if not needs_update:
        print("  All docs have season field — no migration needed")
        return

    print(f"  Backfilling season on {len(needs_update)} docs...")
    # Re-fetch season->ids mapping to know which matchId belongs to which season
    season_map = {}
    for season in ALL_SEASONS:
        ids = fetch_match_ids_for_season(season)
        for mid in ids:
            season_map[str(mid)] = season

    batch_count = 0
    for i in range(0, len(needs_update), 400):
        batch = db.batch()
        for doc_id in needs_update[i:i+400]:
            season = season_map.get(doc_id)
            if season:
                ref = db.collection("matches").document(doc_id)
                batch.update(ref, {"season": season})
        batch.commit()
        batch_count += 1
    print(f"  Backfill complete ({batch_count} batches)")

def get_stored_match_ids():
    docs = db.collection("matches").select(["matchId"]).stream()
    return {doc.id for doc in docs}

def save_matches_to_firestore(matches):
    if not matches:
        return
    for i in range(0, len(matches), 400):
        batch = db.batch()
        for m in matches[i:i + 400]:
            ref = db.collection("matches").document(str(m["matchId"]))
            batch.set(ref, m)
        batch.commit()
        print(f"    Saved {min(i+400, len(matches))}/{len(matches)} to Firestore")

def load_all_from_firestore():
    print("  Loading all matches from Firestore...")
    matches = [doc.to_dict() for doc in db.collection("matches").stream()]
    print(f"  Loaded {len(matches)} matches")
    return matches

# ─── PL API ───────────────────────────────────────────────────────────────────

def fetch_json(url):
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())

def fetch_match_ids_for_season(season):
    ids = []
    for mw in range(1, 39):
        try:
            url = (f"https://sdp-prem-prod.premier-league-prod.pulselive.com"
                   f"/api/v1/competitions/{COMPETITION_ID}/seasons/{season}"
                   f"/matchweeks/{mw}/matches")
            data = fetch_json(url)
            matches = data.get("data") or data.get("content") or data.get("matches") or []
            mw_ids = [m.get("matchId") or m.get("id") for m in matches
                      if m.get("matchId") or m.get("id")]
            if not mw_ids:
                break
            ids.extend(mw_ids)
            print(f"    MW{mw:02d}: {len(mw_ids)} matches")
        except HTTPError as e:
            if e.code in (400, 404):
                break
            print(f"    MW{mw:02d}: HTTP {e.code}")
        except Exception as e:
            print(f"    MW{mw:02d}: ERROR — {e}")
        time.sleep(0.1)
    return ids

def fetch_match_detail(match_id, season=None):
    try:
        stats     = fetch_json(f"https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v3/matches/{match_id}/stats")
        officials = fetch_json(f"https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v1/matches/{match_id}/officials")

        if isinstance(stats, dict):
            stats = stats.get("data") or stats.get("stats") or stats.get("teamStats") or stats.get("content") or []
        if not isinstance(stats, list) or not stats:
            return None

        home = next((s for s in stats if str(s.get("side","")).lower() == "home"), None)
        away = next((s for s in stats if str(s.get("side","")).lower() == "away"), None)
        if not home or not away:
            return None

        if isinstance(officials, dict):
            officials_list = officials.get("officials") or officials.get("matchOfficials") or officials.get("data") or []
        else:
            officials_list = officials if isinstance(officials, list) else []

        ref_entry = next((o for o in officials_list if str(o.get("type","")).lower() == "referee"), None)
        if ref_entry:
            off = ref_entry.get("official") or {}
            referee = off.get("name") or off.get("shortName") or "Unknown"
        else:
            referee = "Unknown"

        def stat(side, key):
            try: return int(side.get("stats", {}).get(key) or 0)
            except: return 0

        def tid(s):
            try: return int(s.get("teamId", 0))
            except: return 0

        hg, ag = stat(home, "goals"), stat(away, "goals")
        if hg > ag:   hr, ar = "win", "loss"
        elif hg < ag: hr, ar = "loss", "win"
        else:         hr = ar = "draw"
        def pts(r): return 3 if r == "win" else (1 if r == "draw" else 0)

        return {
            "matchId":  match_id,
            "season":   season,
            "referee":  referee,
            "homeTeam": {
                "teamId": tid(home), "teamName": TEAM_MAPPING.get(tid(home), f"Team {tid(home)}"),
                "goals": hg, "redCards": stat(home,"totalRedCard"),
                "yellowCards": stat(home,"totalYelCard"), "fouls": stat(home,"fkFoulLost"),
                "result": hr, "points": pts(hr)
            },
            "awayTeam": {
                "teamId": tid(away), "teamName": TEAM_MAPPING.get(tid(away), f"Team {tid(away)}"),
                "goals": ag, "redCards": stat(away,"totalRedCard"),
                "yellowCards": stat(away,"totalYelCard"), "fouls": stat(away,"fkFoulLost"),
                "result": ar, "points": pts(ar)
            }
        }
    except Exception:
        return None

def fetch_and_store_missing(match_ids, season, label=""):
    if not match_ids:
        return []
    print(f"  Fetching {len(match_ids)} new matches {label}...")
    results = []
    failed = [0]
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_match_detail, mid, season): mid for mid in match_ids}
        for future in as_completed(futures):
            result = future.result()
            with lock:
                if result:
                    results.append(result)
                else:
                    failed[0] += 1
                done = len(results) + failed[0]
                if done % 100 == 0:
                    print(f"    {done}/{len(match_ids)} ({len(results)} ok)")

    print(f"  Done: {len(results)} fetched, {failed[0]} failed")
    save_matches_to_firestore(results)
    return results

# ─── Main Data Load ───────────────────────────────────────────────────────────

def load_data():
    global cache
    cache["status"] = "loading"
    cache["matches"] = []
    cache["error"] = None

    try:
        print("\n  Checking Firestore...")
        backfill_seasons()
        stored_ids = get_stored_match_ids()
        print(f"  {len(stored_ids)} matches already in DB")

        for season in ALL_SEASONS:
            print(f"\n  Season {season}...")
            season_ids = fetch_match_ids_for_season(season)
            if not season_ids:
                print(f"  No matches found for {season} — season may not have started yet")
                continue
            missing = [mid for mid in season_ids if str(mid) not in stored_ids]
            if not missing:
                print(f"  All {len(season_ids)} matches already in DB — skipping")
            else:
                print(f"  {len(missing)} new matches to fetch")
                fetched = fetch_and_store_missing(missing, season, f"(season {season})")
                stored_ids.update(str(m["matchId"]) for m in fetched)

        all_matches = load_all_from_firestore()
        cache["matches"]      = all_matches
        cache["last_updated"] = time.time()
        cache["progress"]     = len(all_matches)
        cache["total"]        = len(all_matches)
        cache["status"]       = "ready"
        print(f"\n  Ready! {len(all_matches)} matches loaded.")

    except Exception as e:
        import traceback
        print(f"\n  Load failed: {e}")
        traceback.print_exc()
        cache["status"] = "error"
        cache["error"]  = str(e)

# ─── Analytics ────────────────────────────────────────────────────────────────

def compute_analytics(seasons=None):
    all_matches = cache["matches"]
    matches = [m for m in all_matches if seasons is None or m.get("season") in seasons]

    refs = {}
    for m in matches:
        r = m["referee"]
        if r == "Unknown": continue
        if r not in refs:
            refs[r] = {"name": r, "games": 0,
                       "totalFouls": 0, "totalYellow": 0, "totalRed": 0,
                       "homeFouls": 0, "homeYellow": 0, "homeRed": 0,
                       "awayFouls": 0, "awayYellow": 0, "awayRed": 0}
        s = refs[r]; s["games"] += 1
        ht, at = m["homeTeam"], m["awayTeam"]
        s["homeFouls"]   += ht["fouls"];   s["homeYellow"] += ht["yellowCards"]; s["homeRed"] += ht["redCards"]
        s["awayFouls"]   += at["fouls"];   s["awayYellow"] += at["yellowCards"]; s["awayRed"] += at["redCards"]
        s["totalFouls"]  += ht["fouls"] + at["fouls"]
        s["totalYellow"] += ht["yellowCards"] + at["yellowCards"]
        s["totalRed"]    += ht["redCards"] + at["redCards"]

    ref_list = []
    for s in refs.values():
        g = s["games"]
        if g < MIN_REF_GAMES: continue
        fy = s["totalFouls"] / s["totalYellow"] if s["totalYellow"] else 0
        ref_list.append({**s,
            "foulesPerGame": round(s["totalFouls"]/g,2), "yellowPerGame": round(s["totalYellow"]/g,2),
            "redPerGame": round(s["totalRed"]/g,2), "homeFoulsPerGame": round(s["homeFouls"]/g,2),
            "awayFoulsPerGame": round(s["awayFouls"]/g,2), "homeYellowPerGame": round(s["homeYellow"]/g,2),
            "awayYellowPerGame": round(s["awayYellow"]/g,2), "homeRedPerGame": round(s["homeRed"]/g,2),
            "awayRedPerGame": round(s["awayRed"]/g,2), "foulToYellowRatio": round(fy,2),
        })

    teams = {}
    for m in matches:
        for side in ["homeTeam", "awayTeam"]:
            t = m[side]; name = t["teamName"]
            if name not in teams:
                teams[name] = {"name": name, "games": 0,
                               "totalFouls": 0, "totalYellow": 0, "totalRed": 0,
                               "points": 0, "refGames": {}}
            s = teams[name]; s["games"] += 1
            s["totalFouls"] += t["fouls"]; s["totalYellow"] += t["yellowCards"]
            s["totalRed"] += t["redCards"]; s["points"] += t["points"]
            ref = m["referee"]
            if ref != "Unknown":
                if ref not in s["refGames"]:
                    s["refGames"][ref] = {"games":0,"points":0,"fouls":0,"yellow":0,"red":0}
                rs = s["refGames"][ref]
                rs["games"]+=1; rs["points"]+=t["points"]; rs["fouls"]+=t["fouls"]
                rs["yellow"]+=t["yellowCards"]; rs["red"]+=t["redCards"]

    team_list = []
    for s in teams.values():
        g = s["games"]
        if g < MIN_TEAM_GAMES: continue
        rb = {}
        for ref, rs in s["refGames"].items():
            if rs["games"] < MIN_REF_TEAM_GAMES: continue
            rb[ref] = {"games": rs["games"],
                       "pointsPerGame": round(rs["points"]/rs["games"],2),
                       "foulesPerGame": round(rs["fouls"]/rs["games"],2),
                       "yellowPerGame": round(rs["yellow"]/rs["games"],2),
                       "redPerGame":    round(rs["red"]/rs["games"],2)}
        team_list.append({"name": s["name"], "games": g,
            "foulesPerGame": round(s["totalFouls"]/g,2), "yellowPerGame": round(s["totalYellow"]/g,2),
            "redPerGame": round(s["totalRed"]/g,2), "pointsPerGame": round(s["points"]/g,2),
            "refBreakdown": rb})

    ref_team = {}
    for m in matches:
        r = m["referee"]
        if r == "Unknown": continue
        for side in ["homeTeam", "awayTeam"]:
            t = m[side]; key = f"{r}||{t['teamName']}"
            if key not in ref_team:
                ref_team[key] = {"referee":r,"team":t["teamName"],"games":0,"fouls":0,"yellow":0,"red":0,"points":0}
            rs = ref_team[key]
            rs["games"]+=1; rs["fouls"]+=t["fouls"]; rs["yellow"]+=t["yellowCards"]
            rs["red"]+=t["redCards"]; rs["points"]+=t["points"]

    ref_team_list = []
    for rs in ref_team.values():
        if rs["games"] < MIN_REF_TEAM_GAMES: continue
        ref_team_list.append({**rs,
            "foulesPerGame": round(rs["fouls"]/rs["games"],2),
            "yellowPerGame": round(rs["yellow"]/rs["games"],2),
            "redPerGame":    round(rs["red"]/rs["games"],2),
            "pointsPerGame": round(rs["points"]/rs["games"],2)})

    return {
        "refs":        sorted(ref_list, key=lambda x: x["games"], reverse=True),
        "teams":       sorted(team_list, key=lambda x: x["name"]),
        "refTeam":     ref_team_list,
        "matchCount":  len(matches),
        "lastUpdated": cache["last_updated"]
    }

# ─── HTTP Server ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            try:
                with open("index.html", "rb") as f: body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", len(body))
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self.send_json({"error": "index.html not found"}, 404)

        elif path == "/api/status":
            self.send_json({"status": cache["status"], "progress": cache["progress"],
                            "total": cache["total"], "lastUpdated": cache["last_updated"],
                            "matchCount": len(cache["matches"]), "error": cache["error"]})

        elif path == "/api/data":
            if cache["status"] != "ready":
                self.send_json({"error": f"Data not ready (status: {cache['status']})"}, 503)
            else:
                seasons_param = params.get("seasons", [None])[0]
                selected = [int(s) for s in seasons_param.split(",")] if seasons_param else None
                self.send_json(compute_analytics(selected))

        elif path == "/api/debug":
            self.send_json({"status": cache["status"], "matchCount": len(cache["matches"]),
                            "error": cache["error"],
                            "sampleMatch": cache["matches"][0] if cache["matches"] else None})
        else:
            self.send_json({"error": "Not found"}, 404)

    def do_POST(self): self.do_GET()

# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("WhistleWatch - Premier League Referee Bias Analyzer")
    print("=" * 55)
    print(f"  Seasons tracked: {ALL_SEASONS}")
    print(f"  Open http://localhost:8080")
    print("=" * 55)

    print("\nInitializing Firebase...")
    init_firebase()

    threading.Thread(target=load_data, daemon=True).start()

    server = HTTPServer(("0.0.0.0", 8080), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")