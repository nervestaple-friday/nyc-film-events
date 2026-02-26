#!/usr/bin/env python3
"""
Plex watch status tracker.

Surfaces:
  - Shows Jim started but hasn't watched in 14+ days (stalled)
  - Monthly show recommendation (--show-rec mode)
  - Summary of recently watched movies (--movies mode)

Run: python3 scripts/plex-watch-status.py [--stalled] [--show-rec] [--movies]
"""

import os, sys, json, urllib.request, urllib.parse, datetime, argparse

WORKSPACE  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(WORKSPACE, 'memory', 'show-recs-state.json')
PLEX       = 'http://192.168.4.121:32400'
PLEX_TOKEN = 'XRRDNdQVeCjRumgE9mUy'
ARR_URL    = 'http://192.168.4.94:7879'
ARR_KEY    = 'orRkC573vbA4cepg4TV_kdtLoy-AaaM8uuyBloWQzT4'

STALE_DAYS    = 14   # shows not touched in this many days get flagged
MAX_STALLED   = 4    # max stalled shows to surface per check


def load_openai_key():
    try:
        with open('/home/claw/.openclaw/openclaw.json') as f:
            return json.load(f)['skills']['entries']['openai-whisper-api']['apiKey']
    except Exception:
        return os.environ.get('OPENAI_API_KEY', '')


def plex_req(path):
    sep = '&' if '?' in path else '?'
    url = f"{PLEX}{path}{sep}X-Plex-Token={PLEX_TOKEN}"
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {'show_recommended': [], 'last_show_rec': None}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def get_stalled_shows():
    """Find shows Jim started but hasn't watched in STALE_DAYS+."""
    now    = datetime.datetime.now()
    cutoff = now - datetime.timedelta(days=STALE_DAYS)

    data  = plex_req('/library/sections/2/all?type=2')
    shows = data['MediaContainer'].get('Metadata', [])

    stalled = []
    for s in shows:
        viewed = s.get('viewedLeafCount', 0)
        total  = s.get('leafCount', 0)
        last_ts = s.get('lastViewedAt', 0)
        if viewed == 0 or viewed >= total or not last_ts:
            continue
        last = datetime.datetime.fromtimestamp(last_ts)
        if last < cutoff:
            days_ago  = (now - last).days
            remaining = total - viewed
            stalled.append({
                'title':     s.get('title'),
                'year':      s.get('year'),
                'watched':   viewed,
                'total':     total,
                'remaining': remaining,
                'days_ago':  days_ago,
                'last':      last.strftime('%b %d'),
            })

    # Sort by most recently watched first (most likely to re-engage with)
    stalled.sort(key=lambda x: x['days_ago'])
    return stalled


def get_in_progress_context():
    """Get all in-progress shows for GPT context."""
    data  = plex_req('/library/sections/2/all?type=2')
    shows = data['MediaContainer'].get('Metadata', [])
    result = []
    for s in shows:
        viewed = s.get('viewedLeafCount', 0)
        total  = s.get('leafCount', 0)
        if 0 < viewed < total:
            result.append(f"{s.get('title')} ({s.get('year')}) — {viewed}/{total} eps")
    return result


def get_completed_shows():
    """Shows fully watched."""
    data  = plex_req('/library/sections/2/all?type=2')
    shows = data['MediaContainer'].get('Metadata', [])
    return [
        f"{s.get('title')} ({s.get('year')})"
        for s in shows
        if s.get('viewedLeafCount', 0) >= s.get('leafCount', 1) > 0
    ]


def get_recently_watched_movies(n=30):
    data = plex_req(f'/library/sections/1/all?type=1&sort=lastViewedAt:desc&limit={n}')
    return [
        f"{m.get('title')} ({m.get('year')})"
        for m in data['MediaContainer'].get('Metadata', [])
        if m.get('lastViewedAt')
    ]


def gpt_show_rec(in_progress, completed, past_recs, openai_key):
    import json as _json
    data = _json.dumps({
        'model': 'gpt-4o-mini',
        'messages': [{'role': 'user', 'content': f"""You are recommending a TV show for Jim, a cinephile in Brooklyn.

Jim's taste: smart, well-crafted TV. Loves shows like The Wire, The Shield, Veep, Peacemaker.
Appreciates dry humor, crime, political satire, prestige drama. Not into procedurals or reality TV.

Shows already in his library (do NOT recommend these):
{chr(10).join(f'- {s}' for s in in_progress[:30])}
{chr(10).join(f'- {s}' for s in completed[:30])}

Recently recommended (do NOT repeat):
{chr(10).join(f'- {r}' for r in past_recs)}

Suggest exactly 1 show. Write a specific 2-3 sentence pitch explaining why Jim in particular would like it.

Respond with JSON only:
{{
  "title": "show title",
  "year": 2019,
  "pitch": "specific pitch for Jim",
  "seasons": 3
}}"""}],
        'temperature': 0.8,
        'response_format': {'type': 'json_object'},
    }).encode()
    req = urllib.request.Request(
        'https://api.openai.com/v1/chat/completions', data=data,
        headers={'Authorization': f'Bearer {openai_key}', 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return _json.loads(_json.loads(r.read())['choices'][0]['message']['content'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stalled',  action='store_true', help='Check stalled shows')
    parser.add_argument('--show-rec', action='store_true', help='Monthly show recommendation')
    parser.add_argument('--movies',   action='store_true', help='Recently watched movies')
    args = parser.parse_args()

    if not any([args.stalled, args.show_rec, args.movies]):
        args.stalled = True  # default

    state = load_state()

    # ── Stalled shows ──────────────────────────────────────────────────────────
    if args.stalled:
        stalled = get_stalled_shows()
        if not stalled:
            print("No stalled shows.")
        else:
            lines = [f"📺 Shows you've left hanging ({len(stalled)} total, top {MAX_STALLED}):\n"]
            for s in stalled[:MAX_STALLED]:
                lines.append(
                    f"• {s['title']} ({s['year']}) — {s['watched']}/{s['total']} eps"
                    f" · {s['remaining']} left · last watched {s['days_ago']}d ago ({s['last']})"
                )
            print('\n'.join(lines))

    # ── Monthly show recommendation ────────────────────────────────────────────
    if args.show_rec:
        openai_key = load_openai_key()
        in_progress = get_in_progress_context()
        completed   = get_completed_shows()
        past_recs   = state.get('show_recommended', [])

        rec = gpt_show_rec(in_progress, completed, past_recs, openai_key)
        title   = rec.get('title', '')
        year    = rec.get('year', '')
        pitch   = rec.get('pitch', '')
        seasons = rec.get('seasons', '?')

        print(f"📺 Show of the month:\n")
        print(f"• {title} ({year}) — {seasons} season{'s' if seasons != 1 else ''}")
        print(f"  {pitch}")

        state['show_recommended'] = past_recs[-12:] + [f"{title} ({year})"]
        state['last_show_rec']    = datetime.date.today().isoformat()
        save_state(state)

    # ── Recently watched movies ────────────────────────────────────────────────
    if args.movies:
        movies = get_recently_watched_movies(20)
        print("🎬 Recently watched movies:\n")
        for m in movies:
            print(f"  • {m}")


if __name__ == '__main__':
    main()
