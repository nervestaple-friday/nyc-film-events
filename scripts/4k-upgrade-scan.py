#!/usr/bin/env python3
"""
4K Upgrade Scanner
==================
Checks movies in the Radarr library against TMDB to detect newly available
4K UHD releases for films currently only downloaded in HD or lower.

Flow:
  1. Get all movies with files from Radarr (via arr-proxy)
  2. For each sub-4K movie, check TMDB for 4K release dates
  3. If a 4K release exists and we haven't notified about it yet → alert Jim
  4. On approval: trigger Radarr search (quality upgrade handled by profile preferences)

Without --apply: dry-run / detect only, no Radarr changes.
With    --apply: triggers search for approved movies.
With    --check: detection pass only, outputs JSON of new 4K candidates.

Usage:
  python3 4k-upgrade-scan.py [--apply] [--check] [--limit N]
"""

import os, sys, json, urllib.request, urllib.parse, datetime, time, argparse, random

PROXY_URL  = os.environ.get("ARR_PROXY_URL", "http://192.168.4.94:7879")
PROXY_KEY  = os.environ.get("ARR_PROXY_KEY", "orRkC573vbA4cepg4TV_kdtLoy-AaaM8uuyBloWQzT4")
TMDB_TOKEN = os.environ.get("TMDB_TOKEN", "")  # Bearer token (read access token)

STATE_FILE = os.path.join(os.path.dirname(__file__), "../memory/4k-state.json")
CHECK_COOLDOWN_DAYS = 30  # re-check TMDB at most once per month per movie


def proxy_req(method, path, body=None):
    url = PROXY_URL + path
    data = json.dumps(body).encode() if body else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"X-Proxy-Key": PROXY_KEY,
                                        "Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=30) as resp:
        return json.loads(resp.read())


def tmdb_req(path, params=None):
    if not TMDB_TOKEN:
        raise RuntimeError("TMDB_TOKEN not set")
    base = "https://api.themoviedb.org/3"
    url = base + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    r = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {TMDB_TOKEN}",
    })
    with urllib.request.urlopen(r, timeout=15) as resp:
        return json.loads(resp.read())


def has_4k_release(tmdb_id):
    """
    Returns (has_4k: bool, release_date: str|None).
    Checks TMDB release_dates for a Digital/Physical 4K UHD release.
    TMDB doesn't directly flag 4K, so we check:
      - release_type 4 (Digital) or 5 (Physical) in US
      - note field contains '4K' or 'UHD'
    Fallback: check if video is flagged as 4K in TMDB videos.
    """
    try:
        data = tmdb_req(f"/movie/{tmdb_id}/release_dates")
        for country in data.get("results", []):
            if country.get("iso_3166_1") != "US":
                continue
            for rel in country.get("release_dates", []):
                note = (rel.get("note") or "").upper()
                rtype = rel.get("type")
                if rtype in (4, 5) and ("4K" in note or "UHD" in note):
                    return True, rel.get("release_date", "")[:10]
        # Secondary: check videos for 4K mentions
        videos = tmdb_req(f"/movie/{tmdb_id}/videos", {"language": "en-US"})
        for v in videos.get("results", []):
            if "4K" in (v.get("name") or "").upper():
                return True, None
    except Exception:
        pass
    return False, None


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "checked": {},       # tmdbId → {"checkedAt": iso, "has4K": bool, "releaseDate": str}
            "notified": [],      # tmdbIds already notified
            "upgraded": {},      # movieId → {"upgradedAt": iso, "title": str}
            "lastScan": None,
        }


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply",  action="store_true", help="Switch profile + trigger search for approved movies")
    parser.add_argument("--check",  action="store_true", help="Detection pass only, print JSON candidates")
    parser.add_argument("--limit",  type=int, default=200, help="Max TMDB lookups per run (default 200)")
    parser.add_argument("--upgrade", type=int, nargs="+", metavar="MOVIE_ID",
                        help="Immediately trigger a search for specific Radarr movie IDs")
    args = parser.parse_args()

    # --- Direct upgrade mode ---
    if args.upgrade:
        for movie_id in args.upgrade:
            print(f"Triggering search for movie {movie_id}...")
            proxy_req("POST", f"/movies/{movie_id}/search")
            print(f"  Done.")
        return

    if not TMDB_TOKEN:
        print("Error: TMDB_TOKEN environment variable not set.", file=sys.stderr)
        sys.exit(1)

    print("Fetching library...")
    movies = {m["id"]: m for m in proxy_req("GET", "/movies")}
    print("Fetching file quality info...")
    files  = {f["movieId"]: f for f in proxy_req("GET", "/movies/files")}

    state        = load_state()
    now          = datetime.datetime.now()
    current_year = now.year

    # Build candidate list: movies with files but not 4K
    # Exclude films too new for a 4K disc release (less than ~8 months old)
    cutoff_year = current_year - 1  # films from last year or older only
    candidates = []
    for movie_id, movie in movies.items():
        if not movie.get("hasFile"):
            continue
        file_info  = files.get(movie_id, {})
        resolution = file_info.get("resolution", 0) or 0
        if resolution >= 2160:
            continue  # already 4K
        tmdb_id = movie.get("tmdbId")
        if not tmdb_id:
            continue
        year = movie.get("year", 0) or 0
        if year > cutoff_year:
            continue  # too new — no disc release yet
        candidates.append((movie_id, movie, file_info, tmdb_id))

    print(f"  {len(candidates)} movies with sub-4K files to check (up to {cutoff_year})")

    # Shuffle so each run samples across all years, not just newest/oldest
    random.shuffle(candidates)

    # Filter by cooldown
    to_check = []
    for movie_id, movie, file_info, tmdb_id in candidates:
        prev = state["checked"].get(str(tmdb_id))
        if prev:
            days = (now - datetime.datetime.fromisoformat(prev["checkedAt"])).days
            if days < CHECK_COOLDOWN_DAYS:
                continue
        to_check.append((movie_id, movie, file_info, tmdb_id))

    to_check = to_check[:args.limit]
    print(f"  {len(to_check)} due for TMDB check (limit {args.limit}/run)")

    newly_found = []
    for i, (movie_id, movie, file_info, tmdb_id) in enumerate(to_check):
        has_4k, release_date = has_4k_release(tmdb_id)
        state["checked"][str(tmdb_id)] = {
            "checkedAt": now.isoformat(),
            "has4K": has_4k,
            "releaseDate": release_date,
        }
        if has_4k and str(tmdb_id) not in [str(x) for x in state.get("notified", [])]:
            newly_found.append({
                "movieId": movie_id,
                "tmdbId": tmdb_id,
                "title": movie["title"],
                "year": movie.get("year"),
                "currentQuality": file_info.get("quality"),
                "currentSizeGB": file_info.get("sizeGB"),
                "releaseDate": release_date,
            })
        if i > 0 and i % 20 == 0:
            print(f"  ... {i}/{len(to_check)}")
        time.sleep(0.25)  # respect TMDB rate limit

    state["lastScan"] = now.isoformat()
    save_state(state)

    if args.check:
        print(json.dumps(newly_found, indent=2))
        return

    if not newly_found:
        print("\nNo new 4K releases found for your library.")
        save_state(state)
        return

    print(f"\n🎬 New 4K releases available for {len(newly_found)} movies in your library:\n")
    for m in newly_found:
        rel = f" (released {m['releaseDate']})" if m['releaseDate'] else ""
        print(f"  [{m['movieId']}] {m['title']} ({m['year']}){rel}")
        print(f"         Currently: {m['currentQuality']} @ {m['currentSizeGB']}GB")
        # Mark as notified so we don't re-alert
        state["notified"].append(m["tmdbId"])
    save_state(state)
    print(f"\nTo trigger a search for any of these, run:")
    print(f"  python3 4k-upgrade-scan.py --upgrade <MOVIE_ID> [MOVIE_ID ...]")
    print(f"Radarr will grab the 4K automatically based on your quality preferences.")


if __name__ == "__main__":
    main()
