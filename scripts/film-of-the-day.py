#!/usr/bin/env python3
"""
Daily film recommendation for Jim.

- Pulls recent watch history from Plex for taste context
- Uses GPT to suggest 2 films matched to Jim's taste profile
- Checks each against Plex library and Radarr (already owned / already queued)
- Tracks past recommendations to avoid repeats (memory/film-recs-state.json)

Run: python3 scripts/film-of-the-day.py [--dry-run]
"""

import os, sys, json, urllib.request, urllib.parse, datetime, argparse, time

WORKSPACE  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(WORKSPACE, 'memory', 'film-recs-state.json')
PLEX       = 'http://192.168.4.121:32400'
PLEX_TOKEN = 'XRRDNdQVeCjRumgE9mUy'
ARR_URL    = 'http://192.168.4.94:7879'
ARR_KEY    = 'orRkC573vbA4cepg4TV_kdtLoy-AaaM8uuyBloWQzT4'

TASTE_PROFILE = """
Jim's film taste:
- Directors he loves: Frederick Wiseman, Claire Denis, Ralph Bakshi, Lars von Trier,
  Josh Safdie, Benny Safdie, Alfred Hitchcock
- Genres/styles: international cinema, documentary, genre films with real craft,
  slow cinema, crime, neo-noir, midnight movies, arthouse
- Recent watches (context): Fallen Leaves (Kaurismäki), Barry Lyndon (Kubrick),
  Party Girl (1995), Judgment at Nuremberg, AUM: The Cult at the End of the World
- Hard skip: franchise films, superhero, generic blockbusters
- Preferred eras: all eras welcome, strong appreciation for 60s-90s cinema
- Location: Brooklyn, NY — appreciates NYC-set films
"""


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


def arr_req(path):
    url = f"{ARR_URL}{path}"
    req = urllib.request.Request(url, headers={'X-Proxy-Key': ARR_KEY, 'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def get_plex_library_titles():
    """Return set of (title_lower, year) tuples for all movies in Plex."""
    data = plex_req('/library/sections/1/all?type=1')
    titles = set()
    for m in data['MediaContainer'].get('Metadata', []):
        titles.add((m.get('title', '').lower(), m.get('year')))
    return titles


def get_recently_watched(n=20):
    """Get Jim's most recently watched movies."""
    data = plex_req(f'/library/sections/1/all?type=1&sort=lastViewedAt:desc&limit={n}')
    watched = []
    for m in data['MediaContainer'].get('Metadata', []):
        if m.get('lastViewedAt'):
            watched.append(f"{m.get('title')} ({m.get('year')})")
    return watched


def get_radarr_titles():
    """Return set of title_lower for all monitored Radarr movies. Skips gracefully if unreachable."""
    try:
        movies = arr_req('/movies')
        return {m.get('title', '').lower() for m in movies}
    except Exception:
        return set()


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {'recommended': [], 'last_run': None}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def gpt(prompt, openai_key):
    data = json.dumps({
        'model': 'gpt-4o-mini',
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': 0.8,
        'response_format': {'type': 'json_object'},
    }).encode()
    req = urllib.request.Request(
        'https://api.openai.com/v1/chat/completions', data=data,
        headers={'Authorization': f'Bearer {openai_key}', 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(json.loads(r.read())['choices'][0]['message']['content'])


def check_in_library(title, year, plex_titles, radarr_titles):
    """Check if a film is in Plex or Radarr."""
    t = title.lower()
    in_plex   = any(pt == t or (pt == t and py == year) for pt, py in plex_titles)
    in_radarr = t in radarr_titles
    return in_plex, in_radarr


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    openai_key = load_openai_key()
    if not openai_key:
        print("No OpenAI key", file=sys.stderr); sys.exit(1)

    state          = load_state()
    recently_seen  = get_recently_watched(25)
    plex_titles    = get_plex_library_titles()
    radarr_titles  = get_radarr_titles()
    past_recs      = state.get('recommended', [])[-60:]  # avoid repeats for ~2 months

    prompt = f"""You are recommending films for Jim, a cinephile in Brooklyn.

{TASTE_PROFILE}

Recently watched (do NOT recommend these):
{chr(10).join(f'- {f}' for f in recently_seen)}

Already recommended recently (do NOT repeat):
{chr(10).join(f'- {r}' for r in past_recs[-30:])}

Today's date: {datetime.date.today().isoformat()}

Suggest exactly 2 films. Rules:
- Must be real films that exist
- No franchise films, no superhero films
- Avoid films from {datetime.date.today().year} (too new for physical releases)
- Mix it up — don't always pick the same era or style
- Write a genuine, specific pitch (2-3 sentences) — not generic. Tell him WHY this specific film.

Respond with JSON only:
{{
  "recommendations": [
    {{
      "title": "exact film title",
      "year": 1984,
      "director": "Director Name",
      "pitch": "2-3 sentence pitch specific to Jim's taste"
    }},
    ...
  ]
}}"""

    result = gpt(prompt, openai_key)
    recs   = result.get('recommendations', [])

    if not recs:
        print("No recommendations generated.")
        return

    lines = ["🎬 Film recommendations for today:\n"]
    new_recs = []

    for r in recs:
        title    = r.get('title', '')
        year     = r.get('year')
        director = r.get('director', '')
        pitch    = r.get('pitch', '')

        in_plex, in_radarr = check_in_library(title, year, plex_titles, radarr_titles)

        if in_plex:
            status = "✅ In your library"
        elif in_radarr:
            status = "📥 Already in Radarr"
        else:
            status = "➕ Not in library"

        lines.append(f"• {title} ({year}) — {director}")
        lines.append(f"  {pitch}")
        lines.append(f"  {status}")
        lines.append("")
        new_recs.append(f"{title} ({year})")

    report = '\n'.join(lines).strip()
    print(report)

    state['recommended'] = past_recs + new_recs
    state['last_run']    = datetime.date.today().isoformat()
    save_state(state)


if __name__ == '__main__':
    main()
