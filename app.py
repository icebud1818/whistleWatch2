#!/usr/bin/env python3
"""
WhistleWatch - Premier League Referee Bias Analyzer
Run: python3 app.py  →  open http://localhost:8080

Startup flow:
  1. Load pre-computed analytics from Firestore (instant)
  2. In background: check PL API for new matches, fetch any missing ones,
     recompute analytics, update Firestore
  3. Frontend gets served immediately from step 1 while step 2 runs quietly
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
ALL_SEASONS    = [2022, 2023, 2024, 2025]
FETCH_WORKERS  = 20

MIN_REF_GAMES      = 3
MIN_REF_TEAM_GAMES = 1
MIN_TEAM_GAMES     = 1

FIREBASE_KEY = "firebase_key.json"
PROJECT_ID   = "whistle-watch"

# Firestore document IDs for stored analytics
ANALYTICS_DOC   = "analytics/computed"      # full all-seasons analytics
ANALYTICS_PREFIX = "analytics/season_"      # per-season: analytics/season_2024

TEAM_MAPPING = {
    3: "Arsenal", 7: "Aston Villa", 91: "Bournemouth", 94: "Brentford",
    36: "Brighton", 90: "Burnley", 8: "Chelsea", 31: "Crystal Palace",
    11: "Everton", 54: "Fulham", 2: "Leeds United", 14: "Liverpool",
    43: "Man City", 1: "Man United", 4: "Newcastle", 17: "Nottm Forest",
    56: "Sunderland", 6: "Tottenham", 21: "West Ham", 39: "Wolves",
    13: "Leicester", 35: "West Brom", 49: "Sheffield Utd", 45: "Norwich",
    57: "Watford", 20: "Southampton", 102: "Luton", 40: "Ipswich"
}

# ─── In-Memory Cache ──────────────────────────────────────────────────────────
# We store pre-computed analytics per season + combined, not raw matches.

cache = {
    # analytics[None] = all seasons combined
    # analytics[2022] = just 22/23, etc.
    "analytics": {},
    "last_updated": None,
    "status": "idle",   # idle | ready | syncing | error
    "error": None,
    "matchCount": 0,
}

db = None

# ─── Firebase ─────────────────────────────────────────────────────────────────

def init_firebase():
    global db
    if not os.path.exists(FIREBASE_KEY):
        raise FileNotFoundError(
            f"'{FIREBASE_KEY}' not found. Download from Firebase console → "
            f"Project Settings → Service Accounts → Generate new private key."
        )
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
    db = firestore.client()
    print("  Firebase connected!")

# ─── Stored Analytics ─────────────────────────────────────────────────────────

def load_analytics_from_firestore():
    """Load pre-computed analytics from Firestore. Returns dict keyed by season (or None for all)."""
    result = {}
    # All-seasons combined
    doc = db.document(ANALYTICS_DOC).get()
    if doc.exists:
        result[None] = doc.to_dict()
        print(f"  Loaded combined analytics from Firestore ({result[None].get('matchCount',0)} matches)")
    # Per-season
    for season in ALL_SEASONS:
        doc = db.document(f"analytics/season_{season}").get()
        if doc.exists:
            result[season] = doc.to_dict()
            print(f"  Loaded season {season} analytics ({result[season].get('matchCount',0)} matches)")
    return result

def save_analytics_to_firestore(analytics_by_season):
    """Store pre-computed analytics back to Firestore."""
    # Save combined
    if None in analytics_by_season:
        db.document(ANALYTICS_DOC).set(analytics_by_season[None])
    # Save per-season
    for season, data in analytics_by_season.items():
        if season is not None:
            db.document(f"analytics/season_{season}").set(data)
    print("  Analytics saved to Firestore")

# ─── Match Storage ────────────────────────────────────────────────────────────

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
    print(f"  Saved {len(matches)} matches to Firestore")

def load_matches_from_firestore(seasons=None):
    """Load raw matches from Firestore, optionally filtered by season list."""
    if seasons:
        # Firestore 'in' queries support up to 10 values
        docs = db.collection("matches").where("season", "in", seasons).stream()
    else:
        docs = db.collection("matches").stream()
    return [doc.to_dict() for doc in docs]

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
            if e.code in (400, 404): break
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
            "matchId": match_id, "season": season, "referee": referee,
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
                if result: results.append(result)
                else: failed[0] += 1
                done = len(results) + failed[0]
                if done % 100 == 0:
                    print(f"    {done}/{len(match_ids)} ({len(results)} ok)")

    print(f"  Fetched {len(results)}/{len(match_ids)}")
    save_matches_to_firestore(results)
    return results

# ─── Analytics Computation ────────────────────────────────────────────────────

def compute_analytics_from_matches(matches):
    """Compute analytics dict from a list of match objects."""
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
        "lastUpdated": time.time()
    }

def rebuild_all_analytics():
    """Load all matches from Firestore, compute analytics for each season + combined, store back."""
    print("  Computing analytics...")

    # Load all raw matches once
    all_matches = load_matches_from_firestore()
    print(f"  Loaded {len(all_matches)} total matches for computation")

    analytics = {}

    # Combined (all seasons)
    analytics[None] = compute_analytics_from_matches(all_matches)

    # Per season
    for season in ALL_SEASONS:
        season_matches = [m for m in all_matches if m.get("season") == season]
        if season_matches:
            analytics[season] = compute_analytics_from_matches(season_matches)
            print(f"  Season {season}: {len(season_matches)} matches → analytics computed")

    # Firestore can't have None as a key — convert for storage
    firestore_data = {}
    if None in analytics:
        firestore_data["combined"] = analytics[None]
    for s, data in analytics.items():
        if s is not None:
            firestore_data[f"season_{s}"] = data

    # Save as single document to minimise reads on next startup
    db.document("analytics/precomputed").set(firestore_data)
    print("  Analytics saved to Firestore (analytics/precomputed)")
    return analytics

# ─── Main Load ────────────────────────────────────────────────────────────────

def load_data():
    global cache
    cache["error"] = None

    try:
        # ── Step 1: Load pre-computed analytics instantly ──
        print("\n  Loading pre-computed analytics from Firestore...")
        doc = db.document("analytics/precomputed").get()
        if doc.exists:
            stored = doc.to_dict()
            analytics = {}
            if "combined" in stored:
                analytics[None] = stored["combined"]
            for season in ALL_SEASONS:
                key = f"season_{season}"
                if key in stored:
                    analytics[season] = stored[key]
            cache["analytics"]    = analytics
            cache["last_updated"] = analytics.get(None, {}).get("lastUpdated")
            cache["matchCount"]   = analytics.get(None, {}).get("matchCount", 0)
            cache["status"]       = "ready"
            print(f"  Ready immediately! {cache['matchCount']} matches in analytics.")
        else:
            print("  No pre-computed analytics found — will compute after sync.")
            cache["status"] = "loading"

        # ── Step 2: Background sync — check for new matches ──
        threading.Thread(target=sync_new_matches, daemon=True).start()

    except Exception as e:
        import traceback
        print(f"\n  Load failed: {e}")
        traceback.print_exc()
        cache["status"] = "error"
        cache["error"]  = str(e)

def sync_new_matches():
    """Background: check PL API for new matches, fetch missing ones, recompute analytics."""
    global cache
    try:
        print("\n  [sync] Checking for new matches...")
        stored_ids = get_stored_match_ids()
        print(f"  [sync] {len(stored_ids)} matches in DB")

        found_new = False
        for season in ALL_SEASONS:
            print(f"\n  [sync] Season {season}...")
            season_ids = fetch_match_ids_for_season(season)
            if not season_ids:
                print(f"  [sync] No matches yet for {season}")
                continue
            missing = [mid for mid in season_ids if str(mid) not in stored_ids]
            if not missing:
                print(f"  [sync] All {len(season_ids)} matches already in DB")
            else:
                print(f"  [sync] {len(missing)} new matches to fetch")
                fetched = fetch_and_store_missing(missing, season, f"(season {season})")
                stored_ids.update(str(m["matchId"]) for m in fetched)
                if fetched:
                    found_new = True

        if found_new or cache["status"] != "ready":
            print("\n  [sync] New matches found — recomputing analytics...")
            analytics = rebuild_all_analytics()
            cache["analytics"]    = analytics
            cache["last_updated"] = time.time()
            cache["matchCount"]   = analytics.get(None, {}).get("matchCount", 0)
            cache["status"]       = "ready"
            print(f"  [sync] Done! {cache['matchCount']} total matches.")
        else:
            print("\n  [sync] No new matches — analytics are up to date.")
            cache["status"] = "ready"

    except Exception as e:
        import traceback
        print(f"\n  [sync] Error: {e}")
        traceback.print_exc()
        # Don't overwrite status if we already have good data
        if cache["status"] != "ready":
            cache["status"] = "error"
            cache["error"]  = str(e)

# ─── Request Handler ──────────────────────────────────────────────────────────

def get_analytics(seasons=None):
    """Return analytics for the requested seasons from in-memory cache."""
    a = cache["analytics"]
    if not seasons:
        return a.get(None) or {}

    if len(seasons) == 1:
        return a.get(seasons[0]) or {}

    # Multiple seasons selected — merge them on the fly
    # Collect all raw match data for just those seasons from Firestore
    # (rare case, acceptable to be slightly slower)
    matches = load_matches_from_firestore(seasons=list(seasons))
    return compute_analytics_from_matches(matches)

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
        path   = parsed.path
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
            self.send_json({
                "status":      cache["status"],
                "matchCount":  cache["matchCount"],
                "lastUpdated": cache["last_updated"],
                "error":       cache["error"]
            })

        elif path == "/api/data":
            if not cache["analytics"]:
                self.send_json({"error": f"Data not ready (status: {cache['status']})"}, 503)
            else:
                seasons_param = params.get("seasons", [None])[0]
                selected = [int(s) for s in seasons_param.split(",")] if seasons_param else None
                self.send_json(get_analytics(selected))

        elif path == "/api/debug":
            self.send_json({
                "status":     cache["status"],
                "matchCount": cache["matchCount"],
                "error":      cache["error"],
                "seasons":    list(cache["analytics"].keys()),
            })

        else:
            self.send_json({"error": "Not found"}, 404)

    def do_POST(self): self.do_GET()

# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("WhistleWatch - Premier League Referee Bias Analyzer")
    print("=" * 55)
    print(f"  Seasons: {ALL_SEASONS}")
    print(f"  Open http://localhost:8080")
    print("=" * 55)

    print("\nInitializing Firebase...")
    init_firebase()

    # Start load (serves immediately from stored analytics, syncs in background)
    threading.Thread(target=load_data, daemon=True).start()

    server = HTTPServer(("0.0.0.0", 8080), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")