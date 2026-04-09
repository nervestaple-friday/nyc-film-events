#!/usr/bin/env python3
"""
NYC Movie Events Digest
=======================
Scrapes upcoming special screenings and events from:
  - Metrograph
  - IFC Center
  - Film Forum
  - Anthology Film Archives
  - Nitehawk Cinema (Williamsburg + Prospect Park)
  - BAM
  - Spectacle Theater
  - Syndicated BK
  - Film at Lincoln Center   (best-effort, Cloudflare-protected)
  - Museum of the Moving Image (best-effort, Cloudflare-protected)

Filters out mainstream Hollywood wide releases.
Run weekly for a digest, or with --test to preview without updating state.
"""

import os, sys, json, hashlib, re, time, math, requests, html as _html, difflib
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE  = os.path.dirname(SCRIPT_DIR)
STATE_FILE = os.path.join(WORKSPACE, 'memory', 'events-state.json')
TEST_MODE  = '--test' in sys.argv
PUSH       = '--push' in sys.argv or (not TEST_MODE)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

FLARESOLVERR = 'http://downloader:8191/v1'

MIN_DAYS, MAX_DAYS = 0, 21

# Mainstream studio titles to skip (case-insensitive substring match)
MAINSTREAM_BLOCKLIST = [
    'avatar', 'marvel', 'avengers', 'spider-man', 'batman', 'superman',
    'star wars', 'fast &', 'fast and furious', 'jurassic', 'transformers',
    'mission: impossible', 'james bond', '007', 'minions', 'despicable me',
    'toy story', 'frozen', 'lion king', 'little mermaid', 'moana',
    'captain america', 'black panther', 'thor', 'doctor strange',
    'guardians of the galaxy', 'ant-man', 'black widow',
]

# Keywords suggesting repertory / special programming (boosts inclusion)
SPECIAL_KEYWORDS = [
    'q&a', 'qa', 'discussion', 'special', 'retrospective', 'series',
    'anniversary', 'tribute', 'in person', 'premiere', 'festival',
    'restoration', 'new print', 'double feature', 'marathon',
    'director', 'actor', 'conversation', 'introduction', 'intro',
    '35mm', '16mm', 'archive', 'rare', 'one night', 'one-night',
    'shorts', 'documentary', 'new wave', 'classic', 'retrospective',
]


# ── helpers ────────────────────────────────────────────────────────────────

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            data = json.load(f)
        # Migrate old list format to {hash: first_seen_date} dict
        if isinstance(data.get('seen'), list):
            data['seen'] = {h: '2026-01-01' for h in data['seen']}
        return data
    return {'seen': {}}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def event_id(venue, title, date_str=''):
    raw = f"{venue}:{title}:{date_str}".lower()
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def clean_title(title):
    title = _html.unescape(title)
    title = re.sub(r'&(\d+);', lambda m: chr(int(m.group(1))), title)  # fix malformed &#NNN; missing #
    title = re.sub(r'^[,\s]*\d{1,2}:\d{2}\s*(AM|PM)\s*', '', title).strip()
    title = re.sub(r'\s*(Q&A?|More Info|Series Schedule|Watch Trailer|Tickets|Buy Tickets)\s*$',
                   '', title, flags=re.IGNORECASE).strip()
    title = re.sub(r'Q&\s*$', '', title).strip()
    # Strip "by Director Name" suffix — require at least first+last name
    # so "Stand By Me" is not truncated to "Stand"
    title = re.sub(r'\s*[Bb]y\s+[A-Z][a-zé\-]+(?:[\s\-]+[A-Z][a-zé\-]+)+(?:\s*In\s+.*)?$', '', title).strip()
    # Handle no-space case: "TITLEby Director"
    title = re.sub(r'(?<=[a-z\)])by\s+[A-Z].*$', '', title).strip()
    # Strip orphaned ". Directed" or ". Written and directed" fragments
    title = re.sub(r'\s*\.?\s*(?:Directed|Written and directed)\s*$', '', title, flags=re.IGNORECASE).strip()
    # Strip "preceded by [short film title]" suffix
    title = re.sub(r'\s+preceded\s+by\s+.*$', '', title, flags=re.IGNORECASE).strip()
    title = re.sub(r'\s+preceded$', '', title, flags=re.IGNORECASE).strip()
    # Strip language suffixes
    title = re.sub(r'\s*In\s+(?:English|French|Spanish|German|Italian|Japanese|Korean)\s+(?:and|with)\s+.*$', '', title, flags=re.IGNORECASE).strip()
    return ' '.join(title.split())


def clean_title_for_display(title):
    """Clean display titles: strip director prefixes, 'X in TITLE' prefixes, format suffixes."""
    t = title.strip()
    # Normalize smart quotes
    t = t.replace('\u2019', "'").replace('\u2018', "'")
    # Strip "Director's TITLE" prefix: "Satyajit Ray's DAYS AND NIGHTS..."
    t = re.sub(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*'s?\s+", '', t)
    # Handle "Giuseppe De Santis' BITTER RICE" with particles
    t = re.sub(r"^[A-Z][a-z]+(?:\s+(?:De|di|del|von|van|Le|La)\s+)?[A-Z][a-z]+'s?\s+", '', t)
    # Handle "Martin and Lewis in TITLE" → "TITLE"
    t = re.sub(r'^.+?\s+in\s+(?=[A-Z])', '', t)
    # Strip trailing " in 3-D!" or " in 3D"
    t = re.sub(r'\s+in\s+3-?D!?\s*$', '', t, flags=re.IGNORECASE)
    t = ' '.join(t.split()).strip()
    return t if len(t) >= 3 else title

def is_special(title, description=''):
    text = (title + ' ' + description).lower()
    return any(kw in text for kw in SPECIAL_KEYWORDS)

def is_mainstream(title):
    t = title.lower()
    return any(kw in t for kw in MAINSTREAM_BLOCKLIST)

def _retry(fn, retries=3, backoff=1):
    """Retry a callable up to *retries* times with exponential backoff.
    Returns the result on success, or None after all attempts fail."""
    last_err = None
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                print(f"  [retry {attempt+1}/{retries}] {e} — retrying in {wait}s", file=sys.stderr)
                time.sleep(wait)
    print(f"  [retry] all {retries} attempts failed: {last_err}", file=sys.stderr)
    return None

def fetch(url, timeout=12):
    def _do():
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    result = _retry(_do)
    if result is None:
        print(f"  [fetch error] {url}: all retries exhausted", file=sys.stderr)
    return result

def fetch_cf(url, timeout=35):
    """Fetch via FlareSolverr for Cloudflare-protected pages."""
    def _do():
        r = requests.post(FLARESOLVERR, json={
            'cmd': 'request.get',
            'url': url,
            'maxTimeout': timeout * 1000,
        }, timeout=timeout + 5)
        r.raise_for_status()
        data = r.json()
        if data.get('status') != 'ok':
            raise RuntimeError(f"FlareSolverr error: {data.get('message')}")
        # Return a simple object with .text attribute
        class _Resp:
            text = data['solution']['response']
            status_code = data['solution']['status']
        return _Resp()
    result = _retry(_do)
    if result is None:
        print(f"  [flaresolverr error] {url}: all retries exhausted", file=sys.stderr)
    return result

def parse_date_loose(text):
    try:
        from dateutil import parser as dp
        dt = dp.parse(text, fuzzy=True)
        # Strip timezone info to keep all dates naive for consistent comparison
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None

def make_event(venue, title, link='', date=None, date_str='', special=None, showtimes=None):
    title = clean_title(title)
    if not title or len(title) < 3 or is_mainstream(title):
        return None
    ev = {
        'venue': venue,
        'title': title,
        'link': link,
        'date': date,
        'date_str': date_str,
        'special': special if special is not None else is_special(title),
    }
    if showtimes:
        ev['showtimes'] = showtimes
    return ev


# ── scrapers ───────────────────────────────────────────────────────────────

def scrape_metrograph():
    """Metrograph — scrape the NYC calendar page for upcoming screenings."""
    events = []
    r = fetch('https://metrograph.com/nyc/')
    if not r:
        return events
    try:
        soup = BeautifulSoup(r.text, 'html.parser')
        seen = set()
        for day_div in soup.find_all('div', class_='calendar-list-day'):
            # Date is encoded in the id: "calendar-list-day-2026-03-12"
            day_id = day_div.get('id', '')
            m = re.search(r'calendar-list-day-(\d{4}-\d{2}-\d{2})', day_id)
            if not m:
                continue
            date = datetime.strptime(m.group(1), '%Y-%m-%d')
            date_str = date.strftime('%b %d')
            for item in day_div.find_all('div', class_='item'):
                h4 = item.find('h4')
                if not h4:
                    continue
                a = h4.find('a', href=True)
                if not a:
                    continue
                title = a.get_text(strip=True)
                href = a['href']
                link = 'https://metrograph.com' + href if href.startswith('/') else href
                key = (title, m.group(1))
                if key in seen:
                    continue
                seen.add(key)
                # Extract showtimes from the sibling showtimes div
                show_times = []
                st_div = item.find('div', class_='showtimes')
                if st_div:
                    for st_a in st_div.find_all('a'):
                        t = st_a.get_text(strip=True)
                        # Normalize "4:00pm" → "4:00 PM"
                        tm = re.match(r'(\d{1,2}:\d{2})\s*(am|pm)', t, re.IGNORECASE)
                        if tm:
                            show_times.append(f"{tm.group(1)} {tm.group(2).upper()}")
                e = make_event('Metrograph', title, link, date=date, date_str=date_str,
                               showtimes=show_times if show_times else None)
                if e:
                    events.append(e)
    except Exception as ex:
        print(f"  [Metrograph] {ex}", file=sys.stderr)
    return events


def _spectacle_parse_screening_dates(text):
    """Extract screening dates from Spectacle event page/RSS body text.

    Looks for patterns like 'TUESDAY, APRIL 7th 10PM' or 'FRIDAY, APRIL 3rd – MIDNIGHT'.
    Returns list of (datetime, time_str) tuples.
    """
    days = r'(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)'
    months = (r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|'
              r'SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)')
    pat = (
        rf'({days}),?\s+'
        rf'({months})\s+(\d{{1,2}})(?:st|nd|rd|th)?'
        rf'(?:\s*[–—-]?\s*((?:\d{{1,2}}(?::\d{{2}})?\s*(?:AM|PM))|MIDNIGHT|NOON))?'
    )
    results = []
    now = datetime.now()
    for m in re.finditer(pat, text, re.IGNORECASE):
        month_str = m.group(2)
        day_str = m.group(3)
        time_str = (m.group(4) or '').strip()
        try:
            # Try current year first, then next year if date is in the past
            for year in (now.year, now.year + 1):
                dt = parse_date_loose(f"{month_str} {day_str} {year}")
                if dt and dt >= now - timedelta(days=1):
                    results.append((dt, time_str))
                    break
        except Exception:
            continue
    return results


def scrape_spectacle():
    """Spectacle Theater — RSS feed with screening dates parsed from event pages."""
    events = []
    r = fetch('https://www.spectacletheater.com/feed/')
    if not r:
        return events
    try:
        root = ET.fromstring(r.text)
        for item in root.findall('.//item'):
            title = item.findtext('title', '').strip()
            link  = item.findtext('link', '').strip()

            # Try content:encoded from RSS first (saves an HTTP request)
            screening_dates = []
            content_encoded = item.findtext(
                '{http://purl.org/rss/1.0/modules/content/}encoded', ''
            )
            if content_encoded:
                screening_dates = _spectacle_parse_screening_dates(content_encoded)

            # Fall back to fetching the event page
            if not screening_dates and link:
                time.sleep(0.5)
                page = fetch(link)
                if page:
                    screening_dates = _spectacle_parse_screening_dates(page.text)

            if screening_dates:
                for dt, time_str in screening_dates:
                    date_str = dt.strftime('%b %d')
                    if time_str:
                        date_str += f' {time_str}'
                    e = make_event('Spectacle Theater', title, link,
                                   date=dt, date_str=date_str, special=True)
                    if e:
                        events.append(e)
            else:
                # No screening dates found — keep event with no date so it still appears
                e = make_event('Spectacle Theater', title, link, special=True)
                if e:
                    events.append(e)
    except Exception as ex:
        print(f"  [Spectacle] {ex}", file=sys.stderr)
    return events


def scrape_syndicated():
    """Syndicated BK — uses Veezi ticketing, scrape the sessions page."""
    events = []
    url = 'https://ticketing.useast.veezi.com/sessions/?siteToken=dxdq5wzbef6bz2sjqt83ytzn1c'
    r = fetch(url)
    if not r:
        return events
    soup = BeautifulSoup(r.text, 'html.parser')
    now = datetime.now()
    seen_titles = set()
    for date_div in soup.find_all('div', class_='date'):
        date_h3 = date_div.find('h3', class_='date-title')
        if not date_h3:
            continue
        date_text = date_h3.get_text(strip=True)
        try:
            from dateutil import parser as dp
            current_date = dp.parse(date_text + f" {now.year}", fuzzy=True)
        except Exception:
            continue
        delta = (current_date - now).days
        if not (MIN_DAYS <= delta <= MAX_DAYS):
            continue
        date_str = date_text
        for film_div in date_div.find_all('div', class_='film'):
            title_el = film_div.find('h3', class_='title')
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title or title in seen_titles:
                continue
            if title.lower() in ('show future dates', 'showtimes', 'buy tickets'):
                continue
            # Extract showtimes from <time> elements
            show_times = []
            for time_el in film_div.find_all('time'):
                t = time_el.get_text(strip=True)
                if t:
                    show_times.append(t)
            e = make_event('Syndicated BK', title, url,
                           date=current_date, date_str=date_str,
                           showtimes=show_times if show_times else None)
            if e:
                seen_titles.add(title)
                events.append(e)
    return events


def scrape_ifc():
    """IFC Center — apitap read extracts clean markdown from the homepage.
    Parse title/link pairs from rendered markdown instead of raw HTML soup."""
    import subprocess as _sp
    events = []
    try:
        result = _sp.run(
            ['apitap', 'read', 'https://www.ifccenter.com/'],
            capture_output=True, text=True, timeout=30,
        )
        md = result.stdout
    except Exception as ex:
        print(f"  [IFC/apitap] fallback to requests: {ex}", file=sys.stderr)
        r = fetch('https://www.ifccenter.com/')
        if not r:
            return events
        md = r.text  # raw HTML fallback — regex still works on href/h1 patterns

    seen = set()
    # Match markdown links: [TITLE](https://www.ifccenter.com/films/...) or /series/
    for m in re.finditer(
        r'#\s+([^\n\r]+)\n[^\[]*\[?[^\]]*\]?\(?(?:https://www\.ifccenter\.com)?((?:/films/|/series/)[\w\-/]+)\)?',
        md,
    ):
        title = m.group(1).strip()
        path  = m.group(2).strip().rstrip(')')
        link  = f"https://www.ifccenter.com{path}"
        # Strip markdown formatting and trailing context
        title = re.sub(r'[#\*\[\]]', '', title).strip()
        # Strip markdown link syntax from title
        title = re.sub(r'\]\(https?://[^\)]+\)\s*$', '', title).strip()
        title = re.sub(r'\(https?://[^\)]+\)\s*$', '', title).strip()
        title = re.split(r'Q&A|Filmmaker|Director|Screening on|Opens|Academy Award', title)[0].strip()
        if not title or len(title) < 3 or link in seen:
            continue
        seen.add(link)
        note_block = md[m.end():m.end()+400]
        note = re.search(r'\n([^\n#]{10,120})', note_block)
        special = is_special(title) or bool(note and is_special(note.group(1)))
        # Extract date from nearby text: "Mon, Mar 3 at 7:00" or "Opens Fri, Mar 20"
        date = None
        date_str = ''
        dm = re.search(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+'
            r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2})',
            note_block,
        )
        if dm:
            date_str = dm.group(1)
            date = parse_date_loose(date_str + f" {datetime.now().year}")
        e = make_event('IFC Center', title, link, date=date, date_str=date_str, special=special)
        if e:
            events.append(e)

    # Enrich with showtimes from the HTML sidebar (apitap read strips it)
    if events:
        r_html = fetch('https://www.ifccenter.com/')
        if r_html:
            ifc_soup = BeautifulSoup(r_html.text, 'html.parser')
            slug_times = {}
            for sched in ifc_soup.find_all('div', class_='daily-schedule'):
                for li in sched.find_all('li'):
                    details = li.find('div', class_='details')
                    if not details:
                        continue
                    h3 = details.find('h3')
                    if not h3 or not h3.find('a', href=True):
                        continue
                    href = h3.find('a')['href']
                    slug_m = re.search(r'/films/([\w-]+)', href)
                    if not slug_m:
                        continue
                    times_ul = details.find('ul', class_='times')
                    if not times_ul:
                        continue
                    times = []
                    for time_a in times_ul.find_all('a'):
                        t = time_a.get_text(strip=True)
                        tm = re.match(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', t, re.IGNORECASE)
                        if tm:
                            times.append(tm.group(1).strip())
                    if times:
                        slug_times.setdefault(slug_m.group(1), []).extend(times)
            # Dedupe times per slug
            for s in slug_times:
                slug_times[s] = list(dict.fromkeys(slug_times[s]))
            for e in events:
                if e.get('showtimes'):
                    continue
                link_slug = re.search(r'/films/([\w-]+)', e.get('link', ''))
                if link_slug and link_slug.group(1) in slug_times:
                    e['showtimes'] = slug_times[link_slug.group(1)]

    return events[:15]


def scrape_film_forum():
    events = []
    r = fetch('https://filmforum.org/films')
    if not r:
        return events
    soup = BeautifulSoup(r.text, 'html.parser')
    now = datetime.now()
    # First pass: build href→date map from img alt text
    # Film Forum puts dates in img alt: "February 27 - March 12  TWO WEEKS\nSATYAJIT RAY'S..."
    href_dates = {}
    for img in soup.find_all('img', alt=True):
        parent_a = img.parent
        if parent_a and parent_a.name == 'a' and '/film/' in parent_a.get('href', ''):
            alt = img.get('alt', '')
            alt_stripped = alt.strip().upper()
            # Search entire alt text for month+day pattern (not just start)
            dm = re.search(
                r'((?:January|February|March|April|May|June|July|August|September|October|November|December)'
                r'\s+\d{1,2})',
                alt, re.IGNORECASE,
            )
            if dm:
                href_dates[parent_a['href']] = dm.group(1).title()
            elif alt_stripped.startswith('NOW PLAYING') or alt_stripped.startswith('HELD OVER'):
                href_dates[parent_a['href']] = 'Now Playing'

    seen_titles = set()
    for a in soup.find_all('a', href=True):
        href = a.get('href', '')
        if '/film/' in href or '/films/' in href:
            title = a.get_text(separator=' ', strip=True)
            title = clean_title_for_display(title)
            if title and 3 < len(title) < 100 and title not in seen_titles:
                seen_titles.add(title)
                link = f"https://filmforum.org{href}" if href.startswith('/') else href
                date = None
                date_str = ''
                # Try img alt date first
                raw_date = href_dates.get(href, '') or href_dates.get(link, '')
                if raw_date and raw_date != 'Now Playing':
                    date_str = raw_date
                    date = parse_date_loose(date_str + f" {now.year}")
                elif raw_date == 'Now Playing':
                    date_str = 'Now Playing'
                e = make_event('Film Forum', title, link, date=date, date_str=date_str)
                if e:
                    events.append(e)

    # For events with no date, try fetching the individual film page
    _month_pat = (r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*'
                  r'((?:January|February|March|April|May|June|July|August|September|October|November|December)'
                  r'\s+\d{1,2})')
    for e in events:
        if e.get('date_str') or e.get('date'):
            continue
        link = e.get('link', '')
        if not link:
            continue
        time.sleep(0.5)
        pr = fetch(link)
        if not pr:
            continue
        page_text = pr.text
        # Find all screening dates on the page
        page_dates = re.findall(_month_pat, page_text, re.IGNORECASE)
        if page_dates:
            # Pick the earliest upcoming date
            best = None
            for pd_str in page_dates:
                dt = parse_date_loose(pd_str.title() + f" {now.year}")
                if dt and dt >= now.replace(hour=0, minute=0, second=0, microsecond=0):
                    if best is None or dt < best[1]:
                        best = (pd_str.title(), dt)
            if best:
                e['date_str'] = best[0]
                e['date'] = best[1]

    # Extract showtimes from sidebar day-of-week tab structure
    link_times = {}
    for tab_div in soup.find_all('div', id=re.compile(r'^tabs-\d+')):
        for a in tab_div.find_all('a', href=True):
            href = a.get('href', '')
            if '/film/' not in href:
                continue
            link = f"https://filmforum.org{href}" if href.startswith('/') else href
            container = a.find_parent('p')
            if not container:
                continue
            parent_text = container.get_text(' ', strip=True)
            times = re.findall(r'\b(\d{1,2}:\d{2})\b', parent_text)
            if times and len(times) <= 10:
                link_times.setdefault(link, []).extend(times)
    for e in events:
        if e.get('showtimes') or not e.get('link'):
            continue
        times = link_times.get(e['link'])
        if times:
            times = list(dict.fromkeys(times))
            show_times = []
            for t in times:
                h, mn = t.split(':')
                h = int(h)
                if 1 <= h <= 9:
                    show_times.append(f"{h}:{mn} PM")
                elif h == 10 or h == 11:
                    show_times.append(f"{h}:{mn} AM")
                elif h == 12:
                    show_times.append(f"12:{mn} PM")
            if show_times:
                e['showtimes'] = show_times

    return events[:20]


def scrape_anthology():
    events = []
    now = datetime.now()
    for month_offset in [0, 1]:
        month = (now.month + month_offset - 1) % 12 + 1
        year  = now.year + (1 if now.month + month_offset > 12 else 0)
        url   = (f"https://anthologyfilmarchives.org/film_screenings/calendar"
                 f"?view=list&month={month:02d}&year={year}")
        r = fetch(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, 'html.parser')

        # Structured extraction: film-showing divs with film-title spans and showing-* anchors
        showings = soup.find_all('div', class_='film-showing')
        if showings:
            for showing in showings:
                prev_h3 = showing.find_previous('h3')
                if not prev_h3:
                    continue
                date_text = prev_h3.get_text(strip=True)
                try:
                    from dateutil import parser as dp
                    current_date = dp.parse(date_text + f" {year}", fuzzy=True)
                except Exception:
                    continue
                delta = (current_date - now).days
                if not (MIN_DAYS <= delta <= MAX_DAYS):
                    continue
                # Extract showtimes from <a name="showing-..."> elements
                show_times = []
                for a in showing.find_all('a', attrs={'name': re.compile(r'^showing-')}):
                    t = a.get_text(strip=True).rstrip(',').strip()
                    tm = re.match(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', t, re.IGNORECASE)
                    if tm:
                        show_times.append(tm.group(1))
                # Extract title from <span class="film-title">
                title_span = showing.find('span', class_='film-title')
                if title_span:
                    title = title_span.get_text(strip=True)
                else:
                    text = showing.get_text(strip=True)
                    clean = re.sub(r'^(?:[,\s]*\d{1,2}:\d{2}\s*(?:AM|PM)[,\s]*)+', '', text).strip()
                    title = re.split(r'\s+by\s+|\s+Share\s+|In \w+ with|\d{4},\s*\d+\s*min|Share \+', clean)[0].strip()
                title = re.sub(r'^EC:\s*', '', title).strip()
                title = clean_title_for_display(title)
                if title and 5 < len(title) < 100:
                    e = make_event('Anthology Film Archives', title, url,
                                   date=current_date,
                                   date_str=current_date.strftime('%b %d'),
                                   special=True,
                                   showtimes=show_times if show_times else None)
                    if e:
                        events.append(e)
        else:
            # Fallback: original h3 sibling walking (no showtime extraction)
            current_date = None
            for h3 in soup.find_all('h3'):
                date_text = h3.get_text(strip=True)
                try:
                    from dateutil import parser as dp
                    current_date = dp.parse(date_text + f" {year}", fuzzy=True)
                except Exception:
                    pass
                for sib in h3.next_siblings:
                    if hasattr(sib, 'name') and sib.name == 'h3':
                        break
                    text = sib.get_text(strip=True) if hasattr(sib, 'get_text') else ''
                    if not text or len(text) < 5:
                        continue
                    clean = re.sub(r'^[,\s]*\d{1,2}:\d{2}\s*(AM|PM)\s*', '', text).strip()
                    title = re.split(r'\s+by\s+|\s+Share\s+|In \w+ with|\d{4},\s*\d+\s*min|Share \+', clean)[0].strip()
                    title = re.sub(r'^EC:\s*', '', title).strip()
                    title = clean_title_for_display(title)
                    if title and 5 < len(title) < 100 and current_date:
                        delta = (current_date - now).days
                        if MIN_DAYS <= delta <= MAX_DAYS:
                            e = make_event('Anthology Film Archives', title, url,
                                           date=current_date,
                                           date_str=current_date.strftime('%b %d'),
                                           special=True)
                            if e:
                                events.append(e)
    seen = set()
    return [e for e in events if e['title'] not in seen and not seen.add(e['title'])][:20]


def scrape_nitehawk():
    """Nitehawk — apitap-style: regex series index -> movie slug -> /nj/v1/show API.
    No BeautifulSoup, no h1 guessing. Two lightweight HTML fetches per location
    (index page + series page), one API call per film for structured data."""
    import urllib.request as _ur, urllib.error as _ue
    now = datetime.now()

    SKIP_SLUGS = {
        'adults-with-infants', 'adults-with-infants-2',
        'brunch', 'brunch-screenings', 'brunch-screenings-2',
        'family-friendly', 'family-friendly-screenings', 'family-friendly-screenings-2',
        'spoons-toons', 'spoons-toons-booze-2', 'all-ages-2',
        'shorts', 'shorts-2',  # short programs, not features
    }
    LOCATIONS = [
        ('Williamsburg',  'https://nitehawkcinema.com/williamsburg/film-series/'),
        ('Prospect Park', 'https://nitehawkcinema.com/prospectpark/film-series-2/'),
    ]
    NJ_API = 'https://nitehawkcinema.com/wp-json/nj/v1/show'

    def nj_fetch(slug):
        """Fetch structured show data from Nitehawk's WP REST API by movie slug."""
        try:
            req = _ur.Request(
                f'{NJ_API}?slug={slug}&_fields=id,title,link,excerpt,director,_start_date,_start_date_display,showtimes',
                headers={'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'},
            )
            with _ur.urlopen(req, timeout=8) as resp:
                data = json.load(resp)
                return data[0] if data else None
        except Exception:
            return None

    events = []
    seen_slugs = set()

    for location, index_url in LOCATIONS:
        r = fetch(index_url)
        if not r:
            continue

        # Extract series hrefs with regex — no BS4 needed
        series_hrefs = re.findall(
            r'href="(https://nitehawkcinema\.com/(?:williamsburg|prospectpark)/film-series[^"]*)"',
            r.text,
        )
        series_hrefs = list(dict.fromkeys(series_hrefs))  # dedupe, preserve order

        for series_href in series_hrefs[:25]:
            slug = series_href.rstrip('/').split('/')[-1]
            if slug in SKIP_SLUGS or slug in ('film-series', 'film-series-2'):
                continue

            # Fetch series page, extract ALL /movies/<slug> links
            r2 = fetch(series_href)
            if not r2:
                continue
            # Parse date-to-movie mapping from showtime blocks on series page
            from bs4 import BeautifulSoup as _BS
            soup2 = _BS(r2.text, 'html.parser')
            slug_dates = {}  # movie_slug -> earliest date string
            for block in soup2.find_all(class_=re.compile('showtime|schedule|screening')):
                block_html = str(block)
                block_text = block.get_text(' ', strip=True)
                movie_m = re.search(r'/movies/([\w-]+)', block_html)
                date_m = re.search(
                    r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+'
                    r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
                    block_text,
                )
                if movie_m and date_m:
                    ms = movie_m.group(1)
                    if ms not in slug_dates:
                        slug_dates[ms] = date_m.group(1)

            movie_slugs = list(dict.fromkeys(re.findall(r'/movies/([\w\-]+)', r2.text)))
            if not movie_slugs:
                # Fallback: extract film title from h1 tags (venue, series, film)
                # Only include if we can find a date on the series page
                from bs4 import BeautifulSoup as _BS
                soup2 = _BS(r2.text, 'html.parser')
                h1s = [h.get_text(strip=True) for h in soup2.find_all('h1')]
                film_title = h1s[2] if len(h1s) >= 3 else (h1s[1] if len(h1s) == 2 else None)
                if film_title:
                    film_title = re.sub(r'\s*\(\d{4}\)\s*$', '', film_title).strip()
                    # Try to find a date on the page
                    page_text = soup2.get_text(' ', strip=True)
                    date_m = re.search(
                        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+'
                        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
                        page_text,
                    )
                    fb_date = None
                    fb_date_str = ''
                    if date_m:
                        fb_date_str = date_m.group(1)
                        fb_date = parse_date_loose(fb_date_str + f" {now.year}")
                    if fb_date or fb_date_str:
                        e = make_event(f'Nitehawk ({location})', film_title, series_href,
                                       date=fb_date, date_str=fb_date_str, special=True)
                        if e:
                            events.append(e)
                continue

            for movie_slug in movie_slugs:
                if movie_slug in seen_slugs:
                    continue
                seen_slugs.add(movie_slug)

                # Get structured data from API
                show = nj_fetch(movie_slug)
                date = None
                date_str = ''
                if show:
                    title   = show['title']['rendered']
                    link    = show.get('link', series_href)
                    excerpt = re.sub(r'<[^>]+>', '', show.get('excerpt', {}).get('rendered', '')).strip()
                    dirs    = [d['name'] for d in show.get('director', [])]
                    special_note = excerpt or (f"Dir. {', '.join(dirs)}" if dirs else None)
                    # Extract showtimes from API
                    show_times = []
                    if show.get('showtimes') and isinstance(show['showtimes'], list):
                        for st in show['showtimes']:
                            if isinstance(st, dict):
                                st_raw = st.get('time', '') or st.get('start', '') or st.get('date', '')
                                if st_raw:
                                    tm = re.search(r'(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm))', st_raw)
                                    if tm:
                                        show_times.append(tm.group(1).strip().upper())
                                    else:
                                        # Try parsing ISO datetime for time
                                        pd = parse_date_loose(st_raw)
                                        if pd and pd.hour:
                                            show_times.append(pd.strftime('%I:%M %p').lstrip('0'))
                            elif isinstance(st, str):
                                tm = re.search(r'(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm))', st)
                                if tm:
                                    show_times.append(tm.group(1).strip().upper())
                    # Dedupe while preserving order
                    show_times = list(dict.fromkeys(show_times))
                    # Extract date from API fields
                    start = show.get('_start_date', '') or ''
                    start_display = show.get('_start_date_display', '') or ''
                    if start:
                        date = parse_date_loose(start)
                        date_str = start_display or (date.strftime('%b %d') if date else '')
                    elif show.get('showtimes'):
                        first_st = show['showtimes'][0] if isinstance(show['showtimes'], list) else None
                        if first_st and isinstance(first_st, dict):
                            st_date = first_st.get('date', '') or first_st.get('start', '')
                            if st_date:
                                date = parse_date_loose(st_date)
                                if date:
                                    date_str = date.strftime('%b %d')
                    # Fallback: use date from series page HTML parsing
                    if not date and movie_slug in slug_dates:
                        date_str = slug_dates[movie_slug]
                        date = parse_date_loose(date_str + f" {now.year}")
                    # Skip "Not Currently Showing" — no date from API or series page
                    if not start and not date:
                        continue
                else:
                    title = movie_slug.replace('-', ' ').title()
                    link  = f'https://nitehawkcinema.com/movies/{movie_slug}/'
                    special_note = None
                    # Use date from series page HTML parsing
                    if movie_slug in slug_dates:
                        date_str = slug_dates[movie_slug]
                        date = parse_date_loose(date_str + f" {now.year}")
                    else:
                        # No API data and no date from series page — skip
                        continue

                e = make_event(f'Nitehawk ({location})', title, link,
                               date=date, date_str=date_str, special=True,
                               showtimes=show_times if show_times else None)
                if e:
                    if special_note:
                        e['special_note'] = special_note
                    events.append(e)

    # Enrich events with showtimes from the listings API
    # (The per-movie /nj/v1/show endpoint returns empty showtimes;
    #  the location-scoped /showtime/listings endpoint has all data.)
    LISTINGS_URLS = {
        'Williamsburg': 'https://nitehawkcinema.com/williamsburg/wp-json/nj/v1/showtime/listings',
        'Prospect Park': 'https://nitehawkcinema.com/prospectpark/wp-json/nj/v1/showtime/listings',
    }
    for location, listings_url in LISTINGS_URLS.items():
        venue_name = f'Nitehawk ({location})'
        try:
            req = _ur.Request(listings_url, headers={
                'Accept': 'application/json',
                'User-Agent': HEADERS['User-Agent'],
            })
            with _ur.urlopen(req, timeout=10) as resp:
                listings = json.load(resp)
        except Exception:
            continue
        # Build movie_id → normalized name
        id_to_norm = {}
        for mv in listings.get('movies', []):
            id_to_norm[mv['movie_id']] = re.sub(r'[^a-z0-9]', '', mv['movie_name'].lower())
        # Build (norm_name, date_key) → [time_str, ...]
        name_date_times = {}
        for st in listings.get('showtimes', []):
            norm_name = id_to_norm.get(st['movie_id'])
            if not norm_name:
                continue
            dt_raw = st.get('datetime', '')
            if len(dt_raw) < 14:
                continue
            try:
                dt = datetime.strptime(dt_raw, '%Y%m%d%H%M%S')
                date_key = dt.strftime('%Y-%m-%d')
                time_str = dt.strftime('%I:%M %p').lstrip('0')
                name_date_times.setdefault((norm_name, date_key), []).append(time_str)
            except Exception:
                pass
        # Match showtimes to events
        for e in events:
            if e['venue'] != venue_name or e.get('showtimes'):
                continue
            enorm = re.sub(r'[^a-z0-9]', '', e['title'].lower())
            date_key = e['date'].strftime('%Y-%m-%d') if e.get('date') else ''
            times = name_date_times.get((enorm, date_key), [])
            if not times:
                # Try matching by name across all dates
                for (n, d), t in name_date_times.items():
                    if n == enorm:
                        times = t
                        break
            if times:
                e['showtimes'] = list(dict.fromkeys(times))

    # Filter out ghost films with no actual screening date
    events = [e for e in events if e.get('date') or e.get('date_str')]

    seen_titles = set()
    deduped = [e for e in events if e['title'] not in seen_titles and not seen_titles.add(e['title'])]
    return deduped[:50]


def scrape_flc():
    """Film at Lincoln Center — apitap-captured REST API at api.filmlinc.org/showtimes.
    Returns structured film objects with title, slug, dates, and ticket URLs.
    No FlareSolverr needed."""
    import urllib.request as _ur
    events = []
    try:
        req = _ur.Request(
            'https://api.filmlinc.org/showtimes',
            headers={'Accept': 'application/json', 'Origin': 'https://www.filmlinc.org'},
        )
        with _ur.urlopen(req, timeout=15) as resp:
            data = json.load(resp)
    except Exception as ex:
        print(f"  [FLC/api] {ex}", file=sys.stderr)
        return events

    now = datetime.now()
    seen = set()
    for film in data.get('films', []):
        title = film.get('title', '').strip()
        slug  = film.get('slug', '')
        link  = f"https://www.filmlinc.org/films/{slug}/" if slug else 'https://www.filmlinc.org/calendar/'
        if not title or title in seen or len(title) < 4:
            continue
        # Filter to upcoming showtimes within window
        showtimes = film.get('showtimes', [])
        def _flc_date(s):
            """Parse FLC dateTimeET to naive date for comparison."""
            raw = s.get('dateTimeET', '')
            if not raw:
                return None
            try:
                # Strip tz suffix → naive datetime for delta calc
                return datetime.fromisoformat(raw.split('T')[0])
            except Exception:
                return None

        upcoming = [
            s for s in showtimes
            if _flc_date(s) and MIN_DAYS <= (_flc_date(s) - now).days <= MAX_DAYS
        ] if showtimes else []
        # Include if it has upcoming showtimes or no dates at all (always show current programming)
        if showtimes and not upcoming:
            continue
        seen.add(title)
        date = None
        date_str = ''
        show_times = []
        if upcoming:
            try:
                dt_str = upcoming[0]['dateTimeET'].split('T')[0]
                date   = datetime.fromisoformat(dt_str)
                date_str = date.strftime('%b %d') if date else ''
            except Exception:
                pass
            # Extract showtimes from all upcoming screenings
            for s in upcoming:
                raw = s.get('dateTimeET', '')
                if 'T' in raw:
                    try:
                        t = datetime.fromisoformat(raw.replace('Z', '+00:00'))
                        show_times.append(t.strftime('%I:%M %p').lstrip('0'))
                    except Exception:
                        pass
            show_times = list(dict.fromkeys(show_times))  # dedupe
        e = make_event('Film at Lincoln Center', title, link, date=date, date_str=date_str,
                       showtimes=show_times if show_times else None)
        if e:
            events.append(e)

    return events[:20]


def scrape_momi():
    """Museum of the Moving Image — cascading fetch strategy.
    movingimage.org is behind Cloudflare and blocks direct requests, so we try:
    1. apitap read (readability extraction)
    2. Google web cache
    3. Wayback Machine
    """
    import subprocess as _sp

    MOMI_URL = 'https://movingimage.org/whats-on/screenings-and-series/'
    SKIP = {'screenings and series', 'screenings', 'events', 'calendar',
            'see all', 'rentals', 'museum of the moving image', 'whats on',
            'screenings this week', 'ongoing series', 'keep exploring',
            'plan yourvisit', 'plan your visit', 'tours & workshops',
            'watch/read/listen', 'special screenings'}

    _DATE_RE = re.compile(
        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+'
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2})'
        r'(?:[,\s].*)?$'
    )
    _NEARBY_DATE_RE = re.compile(
        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+'
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2})'
    )

    def _extract_date_str(text):
        """Extract a date_str like 'Apr 02' from text containing day+month+day."""
        m = _DATE_RE.search(text) or _NEARBY_DATE_RE.search(text)
        if not m:
            return ''
        dt = parse_date_loose(m.group(1) + f" {datetime.now().year}")
        return dt.strftime('%b %d') if dt else ''

    def _parse_html(raw_html, source_label):
        """Parse HTML from any source and extract film titles."""
        soup = BeautifulSoup(raw_html, 'html.parser')
        main = soup.find('main') or soup
        found = []
        seen = set()

        # Strategy 1: event-entry divs with <time> elements (main MoMI structure)
        for entry in main.find_all('div', class_='event-entry'):
            time_el = entry.find('time', attrs={'datetime': True})
            link_el = entry.find('a', href=True)
            if not link_el:
                continue
            # Prefer dedicated event-title div (avoids concatenating subtitle/date)
            raw_title = None
            title_div = entry.find('div', class_='event-title')
            if title_div:
                raw_title = title_div.get_text(strip=True)
            # Fallback: <a title="Learn More About ..."> attribute
            if not raw_title:
                a_title = link_el.get('title', '')
                if a_title.startswith('Learn More About '):
                    raw_title = a_title[len('Learn More About '):]
            # Final fallback: iterate children to get first meaningful text
            if not raw_title:
                for child in link_el.children:
                    if isinstance(child, str):
                        s = child.strip()
                        if s:
                            raw_title = s
                            break
                    elif hasattr(child, 'get_text'):
                        s = child.get_text(strip=True)
                        if s:
                            raw_title = s
                            break
            if not raw_title:
                raw_title = link_el.get_text(strip=True)
            if not raw_title or len(raw_title) < 4:
                continue
            # Extract date from <time datetime="..."> attribute
            date_str = ''
            date = None
            if time_el:
                try:
                    dt = datetime.fromisoformat(time_el['datetime'])
                    date = dt.replace(tzinfo=None)
                    date_str = date.strftime('%b %d')
                except Exception:
                    date_str = _extract_date_str(time_el.get_text(strip=True))
                    date = parse_date_loose(date_str + f" {datetime.now().year}") if date_str else None
            # Strip date from title text
            title = re.sub(r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*$', '', raw_title).strip()
            title = re.sub(r'\s*[—–-]\s*Presented\s+by\s+.*$', '', title).strip()
            if not title or len(title) < 4 or title.lower() in SKIP or title in seen:
                continue
            seen.add(title)
            href = link_el['href']
            if href.startswith('http'):
                link = href
            elif href.startswith('/'):
                link = f"https://movingimage.org{href}"
            else:
                link = MOMI_URL
            e = make_event('Museum of the Moving Image', title, link,
                           date=date, date_str=date_str, special=True)
            if e:
                found.append(e)

        # Strategy 2: h2/h3/h4 headings (fallback for other HTML sources)
        for el in main.find_all(['h2', 'h3', 'h4']):
            raw_title = el.get_text(' ', strip=True)
            # Collapse multiple spaces that get_text(' ') may produce
            raw_title = re.sub(r'\s+', ' ', raw_title).strip()
            if not raw_title or len(raw_title) < 4 or len(raw_title) > 100:
                continue
            if raw_title.lower() in SKIP:
                continue
            # Extract date from title before stripping it
            date_str = _extract_date_str(raw_title)
            # Strip date suffixes: "Mulholland DriveFri, Mar 6, 6:30 pm"
            title = re.sub(r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*$', '', raw_title).strip()
            # Strip "—Presented by ..." suffix
            title = re.sub(r'\s*[—–-]\s*Presented\s+by\s+.*$', '', title).strip()
            if not title or len(title) < 4 or title.lower() in SKIP or title in seen:
                continue
            seen.add(title)
            # If no date in title, check nearby sibling/parent elements
            if not date_str:
                for sib in el.find_next_siblings(limit=3):
                    sib_text = sib.get_text(strip=True)
                    date_str = _extract_date_str(sib_text)
                    if date_str:
                        break
                if not date_str and el.parent:
                    date_str = _extract_date_str(el.parent.get_text(strip=True))
            link_el = el.find('a', href=True) or (el.parent and el.parent.find('a', href=True))
            href = link_el['href'] if link_el else ''
            if href.startswith('http'):
                link = href
            elif href.startswith('/'):
                link = f"https://movingimage.org{href}"
            else:
                link = MOMI_URL
            date = parse_date_loose(date_str + f" {datetime.now().year}") if date_str else None
            e = make_event('Museum of the Moving Image', title, link,
                           date=date, date_str=date_str, special=True)
            if e:
                found.append(e)
        if found:
            print(f"  [MoMI] {source_label}: found {len(found)} events", file=sys.stderr)
        return found

    def _parse_markdown(md):
        """Parse apitap markdown output for film titles."""
        found = []
        seen = set()
        lines = md.splitlines()
        for i, line in enumerate(lines):
            m = re.match(
                r'(?:#{1,3}\s+|\*\*)\[?([^\n\]\*]{4,80})\]?\(?(?:https://movingimage\.org)?(/[^\s\)\"]{5,100})?\)?',
                line,
            )
            if not m:
                continue
            raw_title = m.group(1).strip().strip('*').strip()
            path  = (m.group(2) or '').strip()
            link  = f"https://movingimage.org{path}" if path else MOMI_URL
            # Extract date from title text
            date_str = _extract_date_str(raw_title)
            # Strip date from title
            title = re.sub(r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*$', '', raw_title).strip()
            if not title or title.lower() in SKIP or title in seen:
                continue
            # If no date in title, check next few lines for date info
            if not date_str:
                for j in range(i + 1, min(i + 4, len(lines))):
                    date_str = _extract_date_str(lines[j])
                    if date_str:
                        break
            seen.add(title)
            date = parse_date_loose(date_str + f" {datetime.now().year}") if date_str else None
            e = make_event('Museum of the Moving Image', title, link,
                           date=date, date_str=date_str, special=True)
            if e:
                found.append(e)
        if found:
            print(f"  [MoMI] apitap markdown: found {len(found)} events", file=sys.stderr)
        return found

    # --- Strategy 1: apitap read ---
    try:
        result = _sp.run(
            ['apitap', 'read', MOMI_URL],
            capture_output=True, text=True, timeout=30,
        )
        md = result.stdout
        if md and len(md.strip()) >= 100:
            events = _parse_markdown(md)
            if events:
                return events[:20]
            # apitap returned content but no events parsed — try HTML parse
            events = _parse_html(md, 'apitap-html')
            if events:
                return events[:20]
        print(f"  [MoMI] apitap returned insufficient content", file=sys.stderr)
    except Exception as ex:
        print(f"  [MoMI/apitap] {ex}", file=sys.stderr)

    # --- Strategy 2: Google web cache ---
    try:
        cache_url = f'https://webcache.googleusercontent.com/search?q=cache:movingimage.org/whats-on/screenings-and-series/'
        r = requests.get(cache_url, headers=HEADERS, timeout=20)
        if r.status_code == 200 and len(r.text) > 500:
            events = _parse_html(r.text, 'Google cache')
            if events:
                return events[:20]
        print(f"  [MoMI] Google cache: status {r.status_code}", file=sys.stderr)
    except Exception as ex:
        print(f"  [MoMI/Google cache] {ex}", file=sys.stderr)

    # --- Strategy 3: Wayback Machine ---
    try:
        wb_url = f'https://web.archive.org/web/2026/{MOMI_URL}'
        r = requests.get(wb_url, headers=HEADERS, timeout=25, allow_redirects=True)
        if r.status_code == 200 and len(r.text) > 500:
            events = _parse_html(r.text, 'Wayback Machine')
            if events:
                return events[:20]
        print(f"  [MoMI] Wayback Machine: status {r.status_code}", file=sys.stderr)
    except Exception as ex:
        print(f"  [MoMI/Wayback] {ex}", file=sys.stderr)

    # --- Strategy 4: Google search results scrape ---
    try:
        search_url = 'https://www.google.com/search'
        params = {'q': 'site:movingimage.org screenings 2026', 'num': '20'}
        r = requests.get(search_url, headers=HEADERS, params=params, timeout=15)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            seen = set()
            events = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                # Google wraps links in /url?q=... redirects
                if '/url?q=' in href:
                    href = href.split('/url?q=')[1].split('&')[0]
                if 'movingimage.org' not in href:
                    continue
                # Skip non-screening pages
                if any(skip in href for skip in ['/about', '/visit', '/education', '/donate', '/membership', '/support']):
                    continue
                title = a.get_text(strip=True)
                # Strip date suffixes and site name
                title = re.sub(r'\s*[-|–—].*Museum of the Moving Image.*$', '', title).strip()
                title = re.sub(r'\s*[-|–—]\s*MoMI\s*$', '', title).strip()
                if not title or len(title) < 4 or len(title) > 100:
                    continue
                if title.lower() in SKIP or title in seen:
                    continue
                seen.add(title)
                link = href if href.startswith('http') else MOMI_URL
                e = make_event('Museum of the Moving Image', title, link, special=True)
                if e:
                    events.append(e)
            if events:
                print(f"  [MoMI] Google search: found {len(events)} events", file=sys.stderr)
                return events[:20]
        print(f"  [MoMI] Google search: no results extracted", file=sys.stderr)
    except Exception as ex:
        print(f"  [MoMI/Google search] {ex}", file=sys.stderr)

    print(f"  [MoMI] WARNING: all fetch strategies failed, returning empty", file=sys.stderr)
    return []


def scrape_moma():
    """MoMA Film — direct fetch of the calendar page with Films filter.
    The /calendar/?happening_filter=Films endpoint bypasses Cloudflare and returns
    individual film events with dates. Falls back to apitap/FlareSolverr."""
    events = []
    now = datetime.now()

    SKIP = {'upcoming showtimes', 'upcoming exhibitions', 'view all showtimes',
            'upcoming events, films, workshops, and more',
            'film series', 'moma film', 'moma', 'modern mondays',
            'read, watch, and listen from wherever you are.',
            'visit moma ps1 in queens'}

    # --- Primary: direct calendar fetch with Films filter ---
    r = fetch('https://www.moma.org/calendar/?happening_filter=Films&location=both')
    if r and len(r.text) > 10000:
        soup = BeautifulSoup(r.text, 'html.parser')

        # Build day→date mapping from h2 headers ("Wed, Mar 11")
        current_date = None
        current_date_str = ''
        seen = set()

        # Walk through all elements in order to track current date
        for el in soup.find_all(['h2', 'a']):
            if el.name == 'h2':
                text = el.get_text(strip=True)
                dm = re.match(r'((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+'
                              r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2})', text)
                if dm:
                    current_date_str = dm.group(1)
                    current_date = parse_date_loose(current_date_str + f" {now.year}")
                continue

            # Process event links
            href = el.get('href', '')
            if '/calendar/events/' not in href:
                continue
            raw_text = el.get_text(' ', strip=True)
            if not raw_text or len(raw_text) < 5:
                continue

            # Extract title: strip year, director, time, venue info
            # Format: "TITLE . YEAR. Directed by DIRECTOR TIME p.m. MoMA, Floor..."
            # Or: "Series Name TIME p.m. MoMA..."
            title = raw_text
            # Extract showtime before stripping it
            show_times = []
            for tm in re.finditer(r'(\d{1,2}:\d{2})\s*([ap])\.?m\.?', raw_text, re.IGNORECASE):
                hour_min = tm.group(1)
                ampm = tm.group(2).upper() + 'M'
                show_times.append(f"{hour_min} {ampm}")
            show_times = list(dict.fromkeys(show_times))
            # Strip time and venue suffix: "4:00 p.m. MoMA..." or "4:30–8:00 p.m...."
            title = re.sub(r'\s*\d{1,2}:\d{2}\s*[\u2013\-–]?\s*(?:\d{1,2}:\d{2}\s*)?[ap]\.?m\.?\s*.*$', '', title, flags=re.IGNORECASE).strip()
            # Strip "Followed by a conversation..." suffix
            title = re.sub(r'\s*Followed by\s+.*$', '', title).strip()
            # Strip " . YEAR. Directed by DIRECTOR" or " YEAR. Directed by DIRECTOR"
            # Require leading space so "Dr." abbreviation dot isn't consumed
            title = re.sub(r'\s+\.?\s*\d{4}\.\s*(?:Directed|Written and directed)\s+by\s+.*$', '', title).strip()
            # Strip ". Directed by DIRECTOR" without year prefix
            title = re.sub(r'\s*\.\s*(?:Directed|Written and directed)\s+by\s+.*$', '', title, flags=re.IGNORECASE).strip()
            # Strip trailing standalone dots (not abbreviations like "Dr.")
            title = re.sub(r'(?<![A-Z][a-z])\.\s*$', '', title).strip()
            # Handle double features: "Film1 . 1973. Dir... Film2 . 1952. Dir..." → keep first
            double_m = re.match(r'^(.+?)\s+\.\s+\d{4}', title)
            if double_m:
                title = double_m.group(1).strip()

            if not title or len(title) < 4:
                continue
            if title.lower() in SKIP:
                continue

            # Deduplicate by normalized title
            norm = re.sub(r'[^a-z0-9 ]+', '', title.lower()).strip()
            if norm in seen:
                continue
            seen.add(norm)

            link = f"https://www.moma.org{href}" if href.startswith('/') else href
            date = current_date
            date_str = current_date_str

            e = make_event('MoMA', title, link, date=date, date_str=date_str, special=True,
                          showtimes=show_times if show_times else None)
            if e:
                events.append(e)

        if events:
            return events[:30]

    # --- Fallback: apitap read ---
    import subprocess as _sp
    print(f"  [MoMA] direct fetch failed, trying apitap", file=sys.stderr)
    try:
        result = _sp.run(
            ['apitap', 'read', 'https://www.moma.org/calendar/film'],
            capture_output=True, text=True, timeout=30,
        )
        md = result.stdout
    except Exception as ex:
        print(f"  [MoMA/apitap] {ex}", file=sys.stderr)
        md = ''

    if not md or len(md.strip()) < 100:
        print(f"  [MoMA] apitap empty, trying FlareSolverr", file=sys.stderr)
        r = fetch_cf('https://www.moma.org/calendar/film')
        if r:
            soup = BeautifulSoup(r.text, 'html.parser')
            main = soup.find('main') or soup
            seen = set()
            for el in main.find_all(['h2', 'h3']):
                raw_title = el.get_text(strip=True)
                if not raw_title or len(raw_title) < 4 or len(raw_title) > 100:
                    continue
                if re.match(r'^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)', raw_title):
                    continue
                if raw_title.lower() in SKIP:
                    continue
                title = re.sub(r'^(.{4,40})\1$', r'\1', raw_title).strip()
                title = re.sub(r'\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d.*$', '', title).strip()
                title = re.sub(r'^MoMA Presents:\s*', '', title).strip()
                title = re.sub(r"^[A-Z][a-zé]+(?:\s+[A-Z][a-zé]+)*['\u2019]s?\s*(?=[A-Z])", '', title).strip()
                title = re.sub(r'^An Evening (?:Celebrating|with)\s+', '', title).strip()
                if not title or len(title) < 4 or title.lower() in SKIP or title in seen:
                    continue
                seen.add(title)
                link_el = el.find('a', href=True) or (el.parent and el.parent.find('a', href=True))
                href = link_el['href'] if link_el else ''
                link = href if href.startswith('http') else (f"https://www.moma.org{href}" if href.startswith('/') else 'https://www.moma.org/calendar/film')
                e = make_event('MoMA', title, link, special=True)
                if e:
                    events.append(e)
            return events[:20]

    seen = set()
    for m in re.finditer(
        r'###\s+\[?([^\n\]\#]{4,100})\]?\(?(https://www\.moma\.org/calendar/film/[^\s\)\"]+)?\)?',
        md,
    ):
        title = m.group(1).strip()
        link  = m.group(2) or 'https://www.moma.org/calendar/film'
        title = re.sub(r'\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d.*$', '', title).strip()
        title = re.sub(r'\s*(Ongoing|Now Playing|Coming Soon)\s*$', '', title, flags=re.IGNORECASE).strip()
        if not title or title.lower() in SKIP or title in seen or len(title) < 4:
            continue
        seen.add(title)
        e = make_event('MoMA', title, link, special=True)
        if e:
            events.append(e)

    return events[:20]


def scrape_paris():
    """Paris Theater NYC — Strapi CMS API at cms.ntflxthtrs.com.
    Fetches structured film data with dates, slugs, and titles.
    Falls back to HTML scraping if the API is unavailable."""
    events = []
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    max_date = (now + timedelta(days=MAX_DAYS)).strftime('%Y-%m-%d')

    # --- Primary: Strapi API ---
    try:
        r = requests.get('https://cms.ntflxthtrs.com/api/films', params={
            'pagination[limit]': 100,
            'sort': 'OpeningDate:asc',
            'filters[OpeningDate][$lte]': max_date,
            'filters[ClosingDate][$gte]': today,
            'populate': 'events',
        }, headers={'User-Agent': HEADERS['User-Agent']}, timeout=15)
        r.raise_for_status()
        data = r.json()
        films = data.get('data', [])
    except Exception as ex:
        print(f"  [Paris/api] {ex}, falling back to HTML", file=sys.stderr)
        films = []

    if films:
        seen = set()
        for item in films:
            a = item.get('attributes', {})
            name = (a.get('FilmName') or '').strip()
            slug = (a.get('Slug') or '').strip()
            opening = a.get('OpeningDate', '')
            closing = a.get('ClosingDate', '')

            if not name or len(name) < 3:
                continue
            # Skip Egyptian Theater screenings (LA venue, not NYC)
            if slug.endswith('-egyptian'):
                continue

            # Deduplicate by normalized title
            norm = re.sub(r'[^a-z0-9 ]+', '', name.lower()).strip()
            if norm in seen:
                continue
            seen.add(norm)

            link = f"https://www.paristheaternyc.com/film/{slug}" if slug else 'https://www.paristheaternyc.com'
            date = parse_date_loose(opening) if opening else None
            date_str = date.strftime('%b %d') if date else ''

            # Extract showtimes from CMS events relation (EventTime field)
            show_times = []
            for ev in a.get('events', {}).get('data', []):
                ea = ev.get('attributes', {})
                assoc = ea.get('Association', [])
                if assoc and 'Paris' not in assoc:
                    continue
                et = (ea.get('EventTime') or '').strip()
                if et:
                    raw = et
                    if ':' not in raw:
                        raw = re.sub(r'(\d+)\s*(AM|PM)', lambda m: f"{m.group(1)}:00 {m.group(2).upper()}", raw, flags=re.IGNORECASE)
                    else:
                        raw = re.sub(r'(am|pm)', lambda m: m.group(1).upper(), raw, flags=re.IGNORECASE)
                    show_times.append(raw)
            # Also extract from CMS text fields (special events)
            if not show_times:
                for field in ('FeaturedFilmSubtextOverride', 'RedLabelOverride', 'HeroDetails'):
                    val = a.get(field) or ''
                    for tm in re.finditer(r'(\d{1,2}(?::\d{2})?\s*(?:AM|PM))', val, re.IGNORECASE):
                        raw = tm.group(1).strip()
                        # Normalize "7 PM" → "7:00 PM", "3:30 PM" stays
                        if ':' not in raw:
                            raw = re.sub(r'(\d+)\s*(AM|PM)', lambda m: f"{m.group(1)}:00 {m.group(2).upper()}", raw, flags=re.IGNORECASE)
                        else:
                            raw = re.sub(r'(am|pm)', lambda m: m.group(1).upper(), raw, flags=re.IGNORECASE)
                        show_times.append(raw)
            show_times = list(dict.fromkeys(show_times))

            e = make_event('Paris Theater', name, link, date=date, date_str=date_str,
                           showtimes=show_times if show_times else None)
            if e:
                events.append(e)

        # Enrich from homepage embedded data: slug → Time field
        try:
            import urllib.request as _ur2
            req = _ur2.Request('https://www.paristheaternyc.com',
                               headers={'User-Agent': HEADERS['User-Agent']})
            with _ur2.urlopen(req, timeout=12) as resp:
                page_html = resp.read().decode('utf-8', errors='replace')
            # Extract Slug→Time pairs from embedded JSON
            slug_times = {}
            for m in re.finditer(r'Slug[\\]*"[\\]*:[\\]*"([^"\\]+)', page_html):
                slug_val = m.group(1)
                ahead = page_html[m.end():m.end()+1000]
                tm = re.search(r'Time[\\]*"[\\]*:[\\]*"([^"\\]{1,20})', ahead)
                if tm and re.search(r'\d', tm.group(1)):
                    raw = tm.group(1).strip()
                    if ':' not in raw:
                        raw = re.sub(r'(\d+)\s*(AM|PM)', lambda mx: f"{mx.group(1)}:00 {mx.group(2).upper()}", raw, flags=re.IGNORECASE)
                    else:
                        raw = re.sub(r'(am|pm)', lambda mx: mx.group(1).upper(), raw, flags=re.IGNORECASE)
                    slug_times[slug_val] = raw
            # Match to events
            for e in events:
                if e.get('showtimes'):
                    continue
                e_slug = re.search(r'/film/(.+?)/?$', e.get('link', ''))
                if e_slug and e_slug.group(1) in slug_times:
                    e['showtimes'] = [slug_times[e_slug.group(1)]]
        except Exception:
            pass

        return events[:25]

    # --- Fallback: HTML scraping (original approach) ---
    import urllib.request as _ur
    try:
        req = _ur.Request(
            'https://www.paristheaternyc.com',
            headers={'User-Agent': HEADERS['User-Agent']},
        )
        with _ur.urlopen(req, timeout=12) as resp:
            html = resp.read().decode('utf-8', errors='replace')
    except Exception as ex:
        print(f"  [Paris/html] {ex}", file=sys.stderr)
        return events

    seen = set()
    SKIP_TITLES = {'fall preview', 'coming soon', 'paris theater', 'special event',
                   'special engagements', 'series and events', 'sign up', 'about',
                   'read more', 'tickets', 'buy tickets', 'learn more'}

    def _norm_key(t):
        return re.sub(r'[^a-z0-9 ]+', '', t.lower()).strip()

    def _add(title, link, date=None, date_str=''):
        title = title.title() if title == title.upper() and len(title) > 4 else title
        norm = _norm_key(title)
        if not title or len(title) < 3 or norm in SKIP_TITLES or norm in seen:
            return
        for s in list(seen):
            if norm in s or s in norm:
                return
        seen.add(norm)
        e = make_event('Paris Theater', title, link, date=date, date_str=date_str)
        if e:
            events.append(e)

    # DisplayTitle + OpeningDate from double-escaped JSON
    title_dates = {}
    for script_m in re.finditer(r'<script[^>]*>([^<]*DisplayTitle[^<]*)</script>', html):
        blob = script_m.group(1)
        unesc = blob.replace('\\"', '"').replace('\\\\', '\\')
        for fm in re.finditer(
            r'DisplayTitle":"([^"]+)".*?OpeningDate":"(\d{4}-\d{2}-\d{2})"',
            unesc,
        ):
            raw_title = fm.group(1).replace('\\u003c', '<').replace('\\u003e', '>')
            raw_title = re.sub(r'\s*-\s*<Paris>\s*$', '', raw_title).strip()
            if raw_title and len(raw_title) >= 3:
                title_dates[raw_title] = fm.group(2)

    for m in re.finditer(r'DisplayTitle[^:]*?:.*?"([^"\\]{3,80})', html):
        title = m.group(1).strip().rstrip(' -').strip()
        title = re.sub(r'\s*-\s*\\?u003c?Paris\\?u003e?\s*$', '', title).strip()
        date = None
        date_str = ''
        opening = title_dates.get(title, '')
        if not opening:
            for t, d in title_dates.items():
                if title.lower() in t.lower() or t.lower() in title.lower():
                    opening = d
                    break
        if opening:
            date = parse_date_loose(opening)
            if date:
                date_str = date.strftime('%b %d')
        slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
        _add(title, f"https://www.paristheaternyc.com/film/{slug}", date=date, date_str=date_str)

    # SlideTitle + SlideLink from homepage slider data
    unesc_html = html.replace('\\"', '"').replace('\\u003c', '<').replace('\\u003e', '>')
    for sm in re.finditer(
        r'"SlideTitle"\s*:\s*"([^"]{3,100})"\s*,\s*"SlideSubtext"\s*:\s*"[^"]*"\s*,\s*"SlideLink"\s*:\s*"([^"]+)"',
        unesc_html,
    ):
        slide_title = sm.group(1).strip()
        slide_link = sm.group(2).strip()
        if any(skip in slide_title.lower() for skip in ('nominated', 'preview', 'coming soon', 'sign up', 'academy award')):
            continue
        _add(slide_title, slide_link)

    # /film/ URL slugs
    for fm in re.finditer(r'paristheaternyc\.com/film/([a-z0-9][a-z0-9-]+[a-z0-9])', unesc_html):
        slug = fm.group(1)
        raw = slug.replace('-', ' ')
        raw = re.sub(r'\s+paris\s*$', '', raw).strip()
        if not raw or len(raw) < 3:
            continue
        _add(raw.title(), f"https://www.paristheaternyc.com/film/{slug}")

    return events[:25]


def clean_bam_title(title):
    """Strip presenter/series prefixes from BAM titles for cleaner display and TMDB matching.
    e.g. 'Cinema Tropical at 25: Silvia Prieto' → 'Silvia Prieto'
         'MUBI Notebook Presents: I Am Love' → 'I Am Love'
         'ArteEast Presents Prince of Nothingwood' → 'Prince of Nothingwood'"""
    # "{Presenter} Presents: {Film}" or "{Presenter} presents: {Film}"
    m = re.match(r'^.+?\s+[Pp]resents?:\s+(.+)$', title)
    if m:
        return m.group(1).strip()
    # "{Presenter} Presents {Film}" (no colon)
    m = re.match(r'^.+?\s+[Pp]resents?\s+(.+)$', title)
    if m and len(m.group(1)) > 3:
        return m.group(1).strip()
    # "{Series} at {N}: {Film}"
    m = re.match(r'^.+?\s+at\s+\d+:\s+(.+)$', title)
    if m:
        return m.group(1).strip()
    return title


def scrape_bam():
    """BAM — direct HTML fetch of listing page for titles/dates/links,
    then fetch individual film pages for showtimes via JSON-LD."""
    events = []
    r = fetch('https://www.bam.org/film')
    if not r:
        return events

    soup = BeautifulSoup(r.text, 'html.parser')
    now = datetime.now()

    BAM_SKIP = {'BAM Film 2026', 'BAM Film', 'Film', 'More', 'See All', 'See all',
                 'View All', 'Buy Tickets', 'Learn More', 'NEW RELEASE', 'FILM SERIES',
                 'Now Playing'}
    seen = set()

    # Build href → (title, date, date_str) from h3 elements near film links
    film_data = {}  # href → (title, date, date_str)
    for h3 in soup.find_all('h3'):
        text = h3.get_text(strip=True)
        if not text or len(text) < 4 or len(text) > 100 or text in BAM_SKIP:
            continue
        if re.match(r'^\d{4}$', text):
            continue
        # Walk up to find a parent with a /film/YEAR/slug link
        parent = h3.parent
        href = None
        for _ in range(6):
            if parent is None:
                break
            link_el = parent.find('a', href=re.compile(r'/film/\d{4}/'))
            if link_el:
                href = link_el['href'].split('#')[0]
                break
            parent = parent.parent
        if not href or href in film_data:
            continue

        # Extract date from nearby text
        container = h3.find_parent('div', class_=True)
        date = None
        date_str = ''
        if container:
            ct = container.get_text(' ', strip=True)
            dm = re.search(r'Opens?\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2})', ct)
            if dm:
                date_str = dm.group(1)
                date = parse_date_loose(date_str + f" {now.year}")
            elif re.search(r'Now\s+Playing', ct):
                date_str = 'Now Playing'
            else:
                dm2 = re.search(r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2})', ct)
                if dm2:
                    date_str = dm2.group(1)
                    date = parse_date_loose(date_str + f" {now.year}")

        film_data[href] = (text, date, date_str)

    # Fetch detail pages for showtimes (JSON-LD) — only for films within date window
    for href, (raw_title, date, date_str) in film_data.items():
        if raw_title in seen:
            continue
        seen.add(raw_title)

        link = f"https://www.bam.org{href}"
        title = clean_bam_title(raw_title)

        # Check if within date window (allow undated "Now Playing")
        if date:
            delta = (date - now).days
            if delta > MAX_DAYS:
                continue
        elif date_str != 'Now Playing':
            continue

        # Fetch detail page for showtimes
        show_times = []
        r2 = fetch(link)
        if r2:
            for ld_m in re.finditer(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                r2.text, re.DOTALL,
            ):
                try:
                    ld = json.loads(ld_m.group(1))
                    ld_events = ld.get('graph', []) if isinstance(ld, dict) else []
                    for ev in ld_events:
                        if ev.get('@type') != 'Event':
                            continue
                        start = ev.get('startDate', '')
                        if not start:
                            continue
                        try:
                            dt = datetime.fromisoformat(start)
                            delta = (dt.replace(tzinfo=None) - now).days
                            if MIN_DAYS <= delta <= MAX_DAYS:
                                t = dt.strftime('%I:%M %p').lstrip('0')
                                show_times.append(t)
                                # Use earliest screening date if none yet
                                if not date or date_str == 'Now Playing':
                                    date = dt.replace(tzinfo=None)
                                    date_str = date.strftime('%b %d')
                        except Exception:
                            pass
                except (json.JSONDecodeError, AttributeError):
                    pass
            show_times = list(dict.fromkeys(show_times))

        e = make_event('BAM', title, link, date=date, date_str=date_str,
                       showtimes=show_times if show_times else None)
        if e:
            events.append(e)

    return events[:15]


# ── TMDB enrichment ────────────────────────────────────────────────────────

TMDB_CACHE_FILE = os.path.join(WORKSPACE, 'memory', 'tmdb-cache.json')

def _clean_title_for_tmdb(title):
    """Strip director prefixes, 'by' suffixes, years, and formatting for better TMDB matching."""
    t = title.strip()
    # Normalize smart quotes to ASCII
    t = t.replace('\u2019', "'").replace('\u2018', "'").replace('\u201c', '"').replace('\u201d', '"')
    # Strip "by Director Name" suffix — require first+last name
    # Also strip trailing language notes: "In English and French with..."
    t = re.sub(r'\s*[Bb]y\s+[A-Z][a-zé\-]+(?:[\s\-]+[A-Z][a-zé\-]+)+(?:\s*In\s+.*)?$', '', t)
    # Strip "Director's TITLE" prefix: "Satyajit Ray's DAYS AND NIGHTS..."
    t = re.sub(r"^[A-ZÀ-Ý][a-zà-ÿ]+(?:\s+[A-ZÀ-Ý][a-zà-ÿ]+)*'s?\s+", '', t)
    # Also handle "Giuseppe De Santis' BITTER RICE", "Ida Lupino's THE BIGAMIST"
    t = re.sub(r"^[A-ZÀ-Ý][a-zà-ÿ]+(?:\s+(?:De|di|del|von|van|Le|La)\s+)?[A-ZÀ-Ý][a-zà-ÿ]+'s?\s+", '', t)
    # Handle "Martin and Lewis in TITLE in 3-D!" → "MONEY FROM HOME"
    t = re.sub(r'^.+?\s+in\s+(?=[A-Z])', '', t)
    # Strip trailing " in 3-D!" or " in 3D"
    t = re.sub(r'\s+in\s+3-?D!?\s*$', '', t, flags=re.IGNORECASE)
    # Strip "PRESENTER PRESENTS" prefix (case-insensitive)
    t = re.sub(r'^.+?\s+[Pp][Rr][Ee][Ss][Ee][Nn][Tt][Ss]?:?\s+', '', t)
    # Strip "(35mm!)" and similar tags
    t = re.sub(r'\s*\((?:35mm|16mm|DCP|FREE)[^)]*\)\s*$', '', t, flags=re.IGNORECASE)
    # Strip "[35mm]", "[16mm]", "[DCP]" etc. in square brackets
    t = re.sub(r'\s*\[(?:35mm|16mm|DCP|4K|70mm)[^\]]*\]', '', t, flags=re.IGNORECASE)
    # Strip quoted titles: '"HUSBANDS"' → 'HUSBANDS'
    t = re.sub(r'^"(.+)"$', r'\1', t)
    # Strip "Presented by Name" suffix
    t = re.sub(r'\s+[Pp]resented\s+by\s+.*$', '', t)
    # Strip trailing year: "Jaws 1975" → "Jaws"
    t = re.sub(r'\s+\d{4}\s*$', '', t)
    # Strip trailing URL fragments: "title(https://...)"
    t = re.sub(r'\(https?://[^\)]+\)\s*$', '', t)
    # Strip "In English and French with..." suffixes
    t = re.sub(r'\s*In\s+\w+\s+and\s+\w+\s+with.*$', '', t)
    # Strip "Nth Anniversary Screening" and everything after it
    t = re.sub(r':?\s*\d+(?:st|nd|rd|th)\s+Anniversary\s+Screening\b.*$', '', t, flags=re.IGNORECASE)
    # Strip "+ Q&A", "+ Book Event", "+ Discussion" etc.
    t = re.sub(r'\s*\+\s+.*$', '', t)
    # Strip "(Singalong Version)", "(Restored Edition)" etc.
    t = re.sub(r'\s*\([^)]*(?:Version|Edition|Cut)\)', '', t, flags=re.IGNORECASE)
    # Strip "(Ep. 1-3)" episode notations
    t = re.sub(r'\s*\(Ep\.?\s*\d+(?:\s*[-–]\s*\d+)?\)', '', t, flags=re.IGNORECASE)
    # Strip "with Person Name in person..." suffix
    t = re.sub(r'\s+with\s+[A-ZÀ-Ý][a-zà-ÿ]+(?:\s+[A-ZÀ-Ý][a-zà-ÿ]+)+\s+in\s+person.*$', '', t, flags=re.IGNORECASE)
    # Strip ": Episodes X-Y" suffix (Dekalog-style)
    t = re.sub(r':\s*[Ee]pisodes?\s+\d+(?:\s*[-–]\s*\d+)?$', '', t)
    # Strip "preceded by [short film title]" suffix
    t = re.sub(r'\s+preceded\s+by\s+.*$', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s+preceded$', '', t, flags=re.IGNORECASE)
    # Strip trailing colon or dash left after suffix removal
    t = re.sub(r'\s*[:–—-]\s*$', '', t)
    # Collapse whitespace
    t = ' '.join(t.split()).strip()
    return t if len(t) >= 3 else title

def _extract_year_from_title(title):
    """Extract a 4-digit year from title if present, for TMDB year filtering."""
    m = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    return int(m.group(1)) if m else None

# Titles that are compilations/events and won't match on TMDB
_TMDB_SKIP_PATTERNS = [
    r'oscar.nominated.*shorts', r'academy awards', r'oscar.*shorts',
    r'late night favorites', r'spoons toons', r'sunday on fire',
    r'ultra.mega oscars', r'video store gems', r'super mario',
    r'\bpgm\s+\d', r'\bprogram\s+[a-z]:', r'doc\s+program',
    r'making visible.*free screening', r'four films by',
    r'defiant and playful', r'doc nyc selects',
    r'crossroads of dreams.*four', r'wrong number.*films by',
    r'sock sock', r'rockuary', r'travel companions',
    r'let the dead bury', r'listen up!', r'an evening with',
    r'in conversation', r'selections from',
    r'caribbean film series', r'o.neill.*richter.*sharits',
    r'sidney peterson$', r'mabou mines',
    r'roots of rhythm remain', r'faust lsc$',
    r'may your eyes be blessed', r'bakri family',
    r'kevin geeks out', r'snowy bing bongs',
    r'spoons toons.*booze', r'sundays on fire',
    r'kino-pravda', r'family time:.*vintage',
    r'earthly delights:.*historic',
]

def _should_skip_tmdb(title):
    t = title.lower()
    return any(re.search(p, t) for p in _TMDB_SKIP_PATTERNS)

# Manual TMDB ID overrides for titles that consistently match the wrong film.
# Key = exact event title, Value = TMDB movie ID.
_TMDB_OVERRIDES = {
    'Stalker': 1398,           # Tarkovsky 1979
    'Living in Oblivion': 9071,   # DiCillo 1995
    'Point Blank': 26039,      # Boorman 1967
    'The Vanishing': 1575,     # Sluizer 1988 (Spoorloos)
    "You're Next": 83899,      # Wingard 2013
    'Dekalog: Episodes 1-4': 118663,   # Kieslowski – use Decalogue III as representative
    'Dekalog: Episodes 5-7': 118663,
    'Dekalog: Episodes 8-10': 118663,
    'Tahar Cheriaa: Under the Shadow of the Baobab': 504647,  # 2014 documentary
    'LIVING THE LAND': 1421153,
    'REBEL TIME: YOUNG SOUL REBELS': 31785,
    'The MisconceivedOpening Night': 1580569,
    'SURVIVING TIME: A LITANY FOR SURVIVAL': 327115,
    "STORY TIME: GOD'S GIFT / WEND KUUNI": 181083,
    "BLACK/QUEER TIME: BLACK IS...BLACK AIN'T": 77341,
    'Fantastic Planet': 16306,
    'Present.Perfect.': 573675,
    'Snowy Bing Bongs and Friends': 458189,
    'The Pied Piper & The Vanished World of Gloves': 262054,
    'THE WHOLE SHEBANG: TWO WRENCHING DEPARTURES': 272681,
    'Undertone': 1480387,
    'KINO-PRAVDA, NOS. 1-6': 462750,
    'Big Mistakes | Episodes 1 & 2': 291506,       # TV series
    'BEEF | Season 2 | Episodes 1 & 2': 154385,    # TV series
    'Cold Metal': 1493018,
    'Frío metal (Cold Metal) . Directed': 1493018,
    '(ANTI) COLONIAL TIME': None,                   # Anthology program, no single TMDB entry
    'MASTERS OF INDONESIAN EXPLOITATION: H. TJUT DJALIL': None,  # Program, no single entry
    'The Silence': 490,            # Bergman 1963 (not 2019 horror)
    'The Hand': 100592,            # Trnka 1965 (not The Handmaiden 2016)
    'Alice': 18917,                # Švankmajer 1988 (not Woody Allen 1990)
}


def enrich_with_tmdb(events_by_venue):
    """Look up each unique title on TMDB and attach poster, overview, year, rating."""
    creds_path = os.path.join(WORKSPACE, 'credentials.json')
    try:
        with open(creds_path) as f:
            tmdb_token = json.load(f)['tmdb']['token']
    except Exception as e:
        print(f"  [tmdb] could not load token: {e}", file=sys.stderr)
        return

    # Load cache
    cache = {}
    if os.path.exists(TMDB_CACHE_FILE):
        try:
            with open(TMDB_CACHE_FILE) as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    # Re-search no-match entries (no overview AND no poster) only after 7 days
    now_iso = datetime.now().isoformat()
    seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
    stale_keys = [k for k, v in cache.items()
                  if not v.get('overview') and not v.get('poster')
                  and v.get('cached_at', '') < seven_days_ago]
    for k in stale_keys:
        del cache[k]
    if stale_keys:
        print(f"  [tmdb] cleared {len(stale_keys)} stale no-match cache entries (>7 days) for re-search", file=sys.stderr)

    # Collect unique titles with venue info for venue-aware TMDB matching
    from collections import defaultdict as _dds
    title_venues = _dds(set)
    for venue, events in events_by_venue.items():
        for e in events:
            title_venues[e['title']].add(venue)
    titles = set(title_venues.keys())

    # New-film venues primarily show recent/new films
    NEW_FILM_VENUES = {'Film at Lincoln Center', 'BAM', 'IFC Center', 'MoMA', 'Museum of the Moving Image'}

    # Clear cache for new-film venue titles only if they had no match and are older than 3 days
    three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
    cleared_new = [t for t in title_venues if t in cache
                   and (title_venues[t] & NEW_FILM_VENUES)
                   and not cache[t].get('poster') and not cache[t].get('overview')
                   and cache[t].get('cached_at', '') < three_days_ago]
    for t in cleared_new:
        del cache[t]
    if cleared_new:
        print(f"  [tmdb] cleared {len(cleared_new)} no-match new-film venue entries (>3 days) for re-search", file=sys.stderr)

    def _search_tmdb(query, year=None, search_type='movie'):
        """Search TMDB movie or TV, return all results."""
        endpoint = f'https://api.themoviedb.org/3/search/{search_type}'
        params = {'query': query}
        if year:
            params['year' if search_type == 'movie' else 'first_air_date_year'] = year
        r = requests.get(endpoint, params=params,
                         headers={'Authorization': f'Bearer {tmdb_token}'}, timeout=10)
        r.raise_for_status()
        return r.json().get('results', [])

    def _pick_best(results, prefer_recent, query_title=''):
        """Pick best TMDB result using title similarity, popularity, and vote count.

        Uses log-scaled absolute scores for popularity/votes instead of relative
        normalization, so a single mega-popular wrong match can't crush the correct
        film's score. Adds bonuses for near-exact title matches with meaningful
        votes, and penalizes very recent low-vote films at repertory venues.
        """
        if not results:
            return None
        query_lower = query_title.lower().strip()
        if not query_lower:
            return results[0]

        any_high_pop = any(r.get('popularity', 0) > 50 for r in results)

        best, best_score = None, -1
        for r in results:
            # Title similarity: best of title and original_title
            title = (r.get('title') or r.get('name') or '').lower().strip()
            orig = (r.get('original_title') or r.get('original_name') or '').lower().strip()
            sim = difflib.SequenceMatcher(None, query_lower, title).ratio()
            if orig and orig != title:
                sim = max(sim, difflib.SequenceMatcher(None, query_lower, orig).ratio())

            if sim < 0.4:
                continue

            pop = r.get('popularity', 0)
            votes = r.get('vote_count', 0)

            # Log-scaled absolute scores instead of relative normalization.
            # This prevents a single mega-popular wrong match (e.g. The Blind Side
            # when searching for Blind Chance) from making the correct film's
            # normalized score near-zero.
            vote_score = min(1.0, math.log10(max(votes, 1)) / 4)  # saturates at 10,000
            pop_score = min(1.0, math.log10(max(pop, 1)) / 3)     # saturates at 1,000

            score = sim * 0.5 + pop_score * 0.15 + vote_score * 0.35

            # Near-exact title match with meaningful votes — likely the correct film
            if sim > 0.9 and votes > 100:
                score += 0.15

            # Well-known films get a bonus (absolute vote thresholds)
            if votes > 500:
                score += 0.1
            if votes > 2000:
                score += 0.05

            # Penalize very low popularity when much more popular alternatives exist
            if any_high_pop and pop < 5:
                score -= 0.15

            year_str = (r.get('release_date', '') or r.get('first_air_date', ''))[:4]

            # For repertory/classic venues, penalize very recent low-vote films
            if not prefer_recent:
                if year_str in ('2023', '2024', '2025', '2026') and votes < 500:
                    score -= 0.3

            # Recent film boost for new-film venues
            if prefer_recent:
                if year_str in ('2023', '2024', '2025', '2026'):
                    score += 0.15

            if score > best_score:
                best, best_score = r, score

        return best

    # Search TMDB for uncached titles
    api_calls = 0
    for title in sorted(titles):
        if title in cache:
            continue
        # Check manual overrides BEFORE skip patterns — overrides take priority
        if title in _TMDB_OVERRIDES:
            tmdb_id = _TMDB_OVERRIDES[title]
            if tmdb_id is None:
                # Explicitly marked as no TMDB match (program/compilation)
                cache[title] = {'poster': '', 'overview': '', 'year': '', 'rating': 0, 'skipped': True, 'cached_at': now_iso}
                continue
            try:
                if api_calls > 0:
                    time.sleep(0.25)
                # Try movie endpoint first, fall back to TV for TV series IDs
                r = requests.get(f'https://api.themoviedb.org/3/movie/{tmdb_id}',
                                 headers={'Authorization': f'Bearer {tmdb_token}'}, timeout=10)
                if r.status_code == 404:
                    # Try TV endpoint
                    r = requests.get(f'https://api.themoviedb.org/3/tv/{tmdb_id}',
                                     headers={'Authorization': f'Bearer {tmdb_token}'}, timeout=10)
                r.raise_for_status()
                api_calls += 1
                hit = r.json()
                poster_path = hit.get('poster_path') or ''
                release_date = hit.get('release_date') or hit.get('first_air_date', '')
                raw_rating = hit.get('vote_average', 0)
                cache[title] = {
                    'poster': f'https://image.tmdb.org/t/p/w300{poster_path}' if poster_path else '',
                    'overview': hit.get('overview', ''),
                    'year': release_date[:4] if len(release_date) >= 4 else '',
                    'rating': round(raw_rating, 1) if raw_rating else 0,
                    'cached_at': now_iso,
                }
                continue
            except Exception as ex:
                print(f"  [tmdb] override fetch failed for '{title}' (id={tmdb_id}): {ex}", file=sys.stderr)

        if _should_skip_tmdb(title):
            cache[title] = {'poster': '', 'overview': '', 'year': '', 'rating': 0, 'skipped': True, 'cached_at': now_iso}
            continue

        if api_calls > 0:
            time.sleep(0.25)
        try:
            cleaned = _clean_title_for_tmdb(title)
            year = _extract_year_from_title(title)
            hit = None

            # Determine if we should prefer recent films for this title
            venues_for_title = title_venues.get(title, set())
            prefer_recent = bool(venues_for_title & NEW_FILM_VENUES)

            # Try movie search with cleaned title
            results = _search_tmdb(cleaned, year=year, search_type='movie')
            api_calls += 1

            # If no result, try without year filter
            if not results and year:
                time.sleep(0.25)
                results = _search_tmdb(cleaned, search_type='movie')
                api_calls += 1

            # If still no result, try TV search
            if not results:
                time.sleep(0.25)
                results = _search_tmdb(cleaned, year=year, search_type='tv')
                api_calls += 1

            hit = _pick_best(results, prefer_recent, query_title=cleaned)
            if hit:
                poster_path = hit.get('poster_path') or ''
                # TV uses first_air_date, movies use release_date
                release_date = hit.get('release_date', '') or hit.get('first_air_date', '')
                raw_rating = hit.get('vote_average', 0)
                cache[title] = {
                    'poster': f'https://image.tmdb.org/t/p/w300{poster_path}' if poster_path else '',
                    'overview': hit.get('overview', ''),
                    'year': release_date[:4] if len(release_date) >= 4 else '',
                    'rating': round(raw_rating, 1) if raw_rating else 0,
                    'cached_at': now_iso,
                }
            else:
                cache[title] = {'poster': '', 'overview': '', 'year': '', 'rating': 0, 'cached_at': now_iso}
        except Exception as ex:
            print(f"  [tmdb] search failed for '{title}': {ex}", file=sys.stderr)
            cache[title] = {'poster': '', 'overview': '', 'year': '', 'rating': 0, 'cached_at': now_iso}

    # Save cache
    try:
        os.makedirs(os.path.dirname(TMDB_CACHE_FILE), exist_ok=True)
        with open(TMDB_CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as ex:
        print(f"  [tmdb] cache write failed: {ex}", file=sys.stderr)

    # Attach fields to events
    for events in events_by_venue.values():
        for e in events:
            info = cache.get(e['title'], {})
            e['poster'] = info.get('poster', '')
            e['overview'] = info.get('overview', '')
            e['year'] = info.get('year', '')
            e['rating'] = info.get('rating', 0)

    print(f"  [tmdb] enriched {len(titles)} titles ({api_calls} API calls)", file=sys.stderr)


# ── filtering + formatting ─────────────────────────────────────────────────

def filter_by_date(events):
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    result = []
    for e in events:
        if e['date']:
            edate = e['date'].replace(hour=0, minute=0, second=0, microsecond=0)
            delta = (edate - today).days
            if MIN_DAYS <= delta <= MAX_DAYS:
                result.append(e)
        else:
            # For undated events, try to parse date_str as a fallback.
            # If it resolves to a past date, exclude it.
            ds = (e.get('date_str') or '').strip()
            if ds and ds != 'Now Playing':
                fallback = parse_date_loose(ds + f" {now.year}")
                if fallback:
                    delta = (fallback.replace(hour=0, minute=0, second=0, microsecond=0) - today).days
                    if MIN_DAYS <= delta <= MAX_DAYS:
                        result.append(e)
                    # else: parseable past/far-future date — skip
                else:
                    result.append(e)  # unparseable date_str — keep
            else:
                result.append(e)  # no date_str or "Now Playing" — keep
    return result


def format_digest(events_by_venue):
    now   = datetime.now()
    lines = [f"🎬 *NYC Movie Events — Week of {now.strftime('%B %d')}*\n"]
    total = 0
    for venue, events in events_by_venue.items():
        if not events:
            continue
        lines.append(f"*{venue}*")
        for e in events:
            date_part = f" — {e['date'].strftime('%b %d')}" if e['date'] else ""
            lines.append(f"• {e['title']}{date_part}")
            if e.get('link'):
                lines.append(f"  {e['link']}")
            total += 1
        lines.append('')
    if total == 0:
        return None
    return '\n'.join(lines).strip()




def push_to_github(events_by_venue):
    """Push events.json to nervestaple-friday/nyc-film-events for GitHub Pages."""
    import base64
    try:
        creds_path = os.path.join(WORKSPACE, 'credentials.json')
        with open(creds_path) as f:
            token = json.load(f)['github']['friday_org']
    except Exception as e:
        print(f"  [github] could not load token: {e}", file=sys.stderr)
        return False

    payload = {
        'updated': datetime.now(tz=timezone.utc).isoformat(),
        'venues': {
            venue: {
                'url': VENUE_URLS.get(venue, ''),
                'events': [
                    {k: v for k, v in [
                        ('title', e['title']),
                        ('link', e.get('link', '')),
                        ('date_str', e.get('date_str', '')),
                        ('showtimes', e.get('showtimes')),
                        ('also_at', e.get('also_at')),
                        ('poster', e.get('poster', '')),
                        ('overview', e.get('overview', '')),
                        ('year', e.get('year', '')),
                        ('rating', e.get('rating', 0)),
                        ('first_seen', e.get('first_seen', '')),
                    ] if v}
                    for e in events
                ]
            }
            for venue, events in events_by_venue.items()
            if events
        }
    }
    content = json.dumps(payload, indent=2, ensure_ascii=False)
    encoded = base64.b64encode(content.encode()).decode()

    api = 'https://api.github.com/repos/nervestaple-friday/nyc-film-events/contents/events.json'
    headers = {'Authorization': f'token {token}', 'Content-Type': 'application/json'}

    # Get current SHA if file exists
    sha = None
    r = requests.get(api, headers=headers)
    if r.status_code == 200:
        sha = r.json().get('sha')

    body = {'message': f'Update events {datetime.now().strftime("%Y-%m-%d")}',
            'content': encoded}
    if sha:
        body['sha'] = sha

    r = requests.put(api, headers=headers, json=body)
    if r.status_code in (200, 201):
        print(f"  [github] pushed events.json ✓", file=sys.stderr)
        return True
    else:
        print(f"  [github] push failed: {r.status_code} {r.text[:200]}", file=sys.stderr)
        return False


# ── main ───────────────────────────────────────────────────────────────────

VENUES_ORDER = [
    'Metrograph', 'Film Forum', 'IFC Center', 'Film at Lincoln Center',
    'Anthology Film Archives', 'Museum of the Moving Image', 'MoMA',
    'Nitehawk (Williamsburg)', 'Nitehawk (Prospect Park)',
    'Spectacle Theater', 'Syndicated BK', 'Paris Theater', 'BAM',
]

VENUE_URLS = {
    'Metrograph':                  'https://metrograph.com/calendar/',
    'Film Forum':                  'https://filmforum.org/films',
    'IFC Center':                  'https://www.ifccenter.com/',
    'Film at Lincoln Center':      'https://www.filmlinc.org/calendar/',
    'Anthology Film Archives':     'https://anthologyfilmarchives.org/film_screenings/calendar',
    'Museum of the Moving Image':  'https://movingimage.org/whats-on/screenings-and-series/',
    'MoMA':                        'https://www.moma.org/calendar/film',
    'Nitehawk (Williamsburg)':     'https://nitehawkcinema.com/williamsburg/',
    'Nitehawk (Prospect Park)':    'https://nitehawkcinema.com/prospectpark/',
    'Spectacle Theater':           'https://www.spectacletheater.com/',
    'Syndicated BK':               'https://syndicatedbk.com/',
    'Paris Theater':               'https://paristheaternyc.com',
    'BAM':                         'https://www.bam.org/film',
}

SCRAPERS = [
    scrape_metrograph,
    scrape_spectacle,
    scrape_syndicated,
    scrape_ifc,
    scrape_film_forum,
    scrape_anthology,
    scrape_nitehawk,
    scrape_flc,
    scrape_momi,
    scrape_moma,
    scrape_paris,
    scrape_bam,
]


def main():
    print("Scraping NYC movie events...", file=sys.stderr)
    state    = load_state()
    seen_ids = set(state.get('seen', {}).keys())
    today    = datetime.now().strftime('%Y-%m-%d')
    all_events = []
    for scraper in SCRAPERS:
        name = scraper.__name__.replace('scrape_', '').replace('_', ' ').title()
        print(f"  Fetching {name}...", file=sys.stderr)
        events = _retry(scraper)
        if events is not None:
            print(f"    {len(events)} items", file=sys.stderr)
            all_events.extend(events)
        else:
            print(f"    Skipping {name}: all retries failed", file=sys.stderr)

    # Remove events with no date info (series headers, not individual screenings)
    all_events = [e for e in all_events if e.get('date_str') or e.get('date')]

    filtered  = filter_by_date(all_events)
    new_events, new_ids = [], []
    for e in filtered:
        eid = event_id(e['venue'], e['title'], e.get('date_str', ''))
        if eid not in seen_ids:
            new_events.append(e)
            new_ids.append(eid)

    # Build digest from NEW events only (for the printed summary)
    new_by_venue = {v: [] for v in VENUES_ORDER}
    for e in new_events:
        if e['venue'] in new_by_venue:
            new_by_venue[e['venue']].append(e)
        else:
            new_by_venue[e['venue']] = new_by_venue.get(e['venue'], []) + [e]

    digest = format_digest(new_by_venue)
    print(digest if digest else "No new events found.")

    if not TEST_MODE and new_ids:
        seen_dict = state.get('seen', {})
        for eid in new_ids:
            seen_dict[eid] = today
        # Keep only the most recent 300 entries
        if len(seen_dict) > 300:
            sorted_items = sorted(seen_dict.items(), key=lambda x: x[1])
            seen_dict = dict(sorted_items[-300:])
        state['seen']    = seen_dict
        state['lastRun'] = datetime.now().isoformat()
        save_state(state)
        print(f"\n[State updated: {len(new_ids)} new events]", file=sys.stderr)

    # Build FULL event list for events.json (all filtered events, not just new)
    events_by_venue = {v: [] for v in VENUES_ORDER}
    for e in filtered:
        if e['venue'] in events_by_venue:
            events_by_venue[e['venue']].append(e)
        else:
            events_by_venue[e['venue']] = events_by_venue.get(e['venue'], []) + [e]

    # Purge stale past events (dates before today)
    today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    purged = 0
    for venue in list(events_by_venue):
        before = len(events_by_venue[venue])
        events_by_venue[venue] = [
            e for e in events_by_venue[venue]
            if not e.get('date') or e['date'].replace(hour=0, minute=0, second=0, microsecond=0) >= today_midnight
        ]
        purged += before - len(events_by_venue[venue])
    if purged:
        print(f"\n[Purged {purged} stale past events]", file=sys.stderr)

    # Per-venue title deduplication
    for venue in events_by_venue:
        seen = set()
        deduped = []
        for e in events_by_venue[venue]:
            norm_title = re.sub(r'[^a-z0-9 ]', '', e['title'].lower()).strip()
            if norm_title not in seen:
                seen.add(norm_title)
                deduped.append(e)
        events_by_venue[venue] = deduped

    # Cross-venue deduplication: add also_at field
    from collections import defaultdict
    title_venues = defaultdict(list)
    norm = lambda t: re.sub(r'[^a-z0-9 ]', '', t.lower()).strip()
    for venue, evts in events_by_venue.items():
        for e in evts:
            title_venues[norm(e['title'])].append(venue)
    for venue, evts in events_by_venue.items():
        for e in evts:
            others = [v for v in title_venues[norm(e['title'])] if v != venue]
            if others:
                e['also_at'] = others

    total_events = sum(len(evts) for evts in events_by_venue.values())
    venues_with_events = sum(1 for evts in events_by_venue.values() if evts)
    print(f"\n[Full dataset: {total_events} events across {venues_with_events} venues]", file=sys.stderr)

    enrich_with_tmdb(events_by_venue)

    # Annotate each event with first_seen date from state
    seen_dict = state.get('seen', {})
    for venue, evts in events_by_venue.items():
        for e in evts:
            eid = event_id(e['venue'], e['title'], e.get('date_str', ''))
            e['first_seen'] = seen_dict.get(eid, today)

    if PUSH:
        if total_events == 0:
            print('[ABORT] 0 events after filtering — refusing to push empty dataset', file=sys.stderr)
        elif total_events < 20:
            print(f'[WARN] Only {total_events} events — suspiciously low, refusing to push', file=sys.stderr)
        else:
            push_to_github(events_by_venue)


if __name__ == '__main__':
    try:
        from dateutil import parser as _
    except ImportError:
        import subprocess
        subprocess.run(['pip3', 'install', 'python-dateutil', '--break-system-packages', '-q'])
    main()
