#!/usr/bin/env python3
"""
Weekly recruiter email scanner.
Scans Gmail for inbound recruiter emails from the past 7 days, evaluates each
opportunity against Jim's criteria using GPT, and sends a Telegram summary only
if there are genuinely exceptional roles worth knowing about.

Criteria for "exceptional":
  - High comp or equity signals
  - Founding / early team roles
  - Fully remote
  - Interesting company / problem space

Skip:
  - Pure crypto / web3 (soft avoid)
  - "AI for X" generic wrappers (soft avoid, not hard rule)
  - Mass recruiter blasts with no specifics
  - LinkedIn/Greenhouse/ATS autoblasts

Run: python3 scripts/recruiter-scan.py [--days 7] [--dry-run]
"""

import os, sys, json, urllib.request, urllib.parse, datetime, re, time, argparse

WORKSPACE  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOKEN_FILE = os.path.join(WORKSPACE, 'gmail_token.json')
CREDS_FILE = os.path.join(WORKSPACE, 'gmail_credentials.json')
STATE_FILE = os.path.join(WORKSPACE, 'memory', 'recruiter-state.json')

OPENAI_KEY = None  # loaded from openclaw.json

RECRUITER_KEYWORDS = [
    'opportunity', 'role', 'position', 'hiring', 'join our team',
    'excited to connect', 'reaching out', 'open to', 'exploring',
    'engineer', 'frontend', 'full stack', 'fullstack', 'react', 'typescript',
    'founding', 'staff', 'principal', 'vp of engineering', 'head of',
]

SKIP_SENDERS = [
    'linkedin.com', 'greenhouse.io', 'lever.co', 'ashbyhq.com',
    'workday.com', 'jobvite', 'smartrecruiters',
]


def load_openai_key():
    try:
        path = '/home/claw/.openclaw/openclaw.json'
        with open(path) as f:
            cfg = json.load(f)
        return cfg['skills']['entries']['openai-whisper-api']['apiKey']
    except Exception:
        return os.environ.get('OPENAI_API_KEY', '')


def load_token():
    with open(TOKEN_FILE) as f:
        return json.load(f)


def save_token(t):
    with open(TOKEN_FILE, 'w') as f:
        json.dump(t, f, indent=2)


def refresh_token(token):
    with open(CREDS_FILE) as f:
        creds = json.load(f)['installed']
    data = urllib.parse.urlencode({
        'client_id': creds['client_id'], 'client_secret': creds['client_secret'],
        'refresh_token': token['refresh_token'], 'grant_type': 'refresh_token',
    }).encode()
    req = urllib.request.Request(creds['token_uri'], data=data,
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=15) as r:
        new = json.loads(r.read())
    token['access_token'] = new['access_token']
    save_token(token)
    return token


def gmail(path, token, params=None):
    for attempt in range(2):
        try:
            url = f'https://gmail.googleapis.com/gmail/v1/users/me{path}'
            if params:
                url += '?' + urllib.parse.urlencode(params)
            req = urllib.request.Request(url, headers={'Authorization': f"Bearer {token['access_token']}"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read()), token
        except urllib.error.HTTPError as e:
            if e.code == 401 and attempt == 0:
                token = refresh_token(token)
            else:
                raise


def get_body(payload):
    """Recursively extract plain text body from message payload."""
    if payload.get('mimeType') == 'text/plain':
        data = payload.get('body', {}).get('data', '')
        if data:
            import base64
            return base64.urlsafe_b64decode(data + '==').decode('utf-8', errors='ignore')
    for part in payload.get('parts', []):
        text = get_body(part)
        if text:
            return text
    return ''


def evaluate_opportunity(subject, sender, body_preview, openai_key):
    """Use GPT to evaluate whether this is an exceptional opportunity."""
    prompt = f"""You are evaluating a recruiter email for Jim, a senior frontend/fullstack engineer in Brooklyn.

Jim's preferences:
- INTERESTED IN: high comp/equity, founding or early-team roles, fully remote, interesting problem spaces, companies with real traction
- SOFT AVOID: pure crypto/web3, generic "AI for X" wrappers, low-signal mass blasts
- NOT a dealbreaker: crypto/AI if the company is genuinely compelling

Email:
From: {sender}
Subject: {subject}
Body preview: {body_preview[:800]}

Respond with JSON only:
{{
  "is_recruiter": true/false,
  "is_exceptional": true/false,
  "score": 0-10,
  "role": "title or null",
  "company": "name or null",
  "comp_signal": "salary/equity mention or null",
  "remote": "remote/hybrid/onsite/unknown",
  "red_flags": ["list of concerns"],
  "summary": "one sentence why this is or isn't worth Jim's time"
}}"""

    data = json.dumps({
        'model': 'gpt-4o-mini',
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': 0.2,
        'response_format': {'type': 'json_object'},
    }).encode()

    req = urllib.request.Request(
        'https://api.openai.com/v1/chat/completions',
        data=data,
        headers={
            'Authorization': f'Bearer {openai_key}',
            'Content-Type': 'application/json',
        }
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        result = json.loads(r.read())

    content = result['choices'][0]['message']['content']
    return json.loads(content)


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {'seen': []}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    global OPENAI_KEY
    OPENAI_KEY = load_openai_key()
    if not OPENAI_KEY:
        print("No OpenAI key found.", file=sys.stderr)
        sys.exit(1)

    token   = load_token()
    state   = load_state()
    after   = int((datetime.datetime.now() - datetime.timedelta(days=args.days)).timestamp())

    # Search inbox for potential recruiter emails
    query = f'in:inbox after:{after} (opportunity OR "reaching out" OR "open to" OR "hiring" OR "role" OR "position")'
    results, token = gmail('/messages', token, {'q': query, 'maxResults': 50})
    msgs = results.get('messages', [])

    print(f"Found {len(msgs)} candidate emails in last {args.days} days")

    exceptional = []
    evaluated   = 0

    for m in msgs:
        msg_id = m['id']
        if msg_id in state['seen']:
            continue

        # Fast pass: metadata + snippet only
        meta, token = gmail(
            f"/messages/{msg_id}?format=metadata&metadataHeaders=From&metadataHeaders=Subject",
            token
        )
        hmap    = {h['name']: h['value'] for h in meta.get('payload', {}).get('headers', [])}
        sender  = hmap.get('From', '')
        subject = hmap.get('Subject', '')
        snippet = meta.get('snippet', '')

        # Skip ATS/automated senders
        if any(s in sender.lower() for s in SKIP_SENDERS):
            state['seen'].append(msg_id)
            continue

        # Quick keyword filter on subject + snippet
        combined = (subject + ' ' + snippet).lower()
        if not any(kw in combined for kw in RECRUITER_KEYWORDS):
            state['seen'].append(msg_id)
            continue

        # Promising — fetch full body for GPT evaluation
        detail, token = gmail(f"/messages/{msg_id}?format=full", token)
        body = get_body(detail.get('payload', {})) or snippet

        try:
            eval_result = evaluate_opportunity(subject, sender, body, OPENAI_KEY)
        except Exception as e:
            print(f"  GPT error for {subject[:40]}: {e}")
            continue

        state['seen'].append(msg_id)
        evaluated += 1

        if not eval_result.get('is_recruiter'):
            continue

        if eval_result.get('is_exceptional') and eval_result.get('score', 0) >= 7:
            exceptional.append({**eval_result, 'subject': subject, 'sender': sender})

        time.sleep(0.3)

    save_state(state)

    print(f"Evaluated {evaluated} recruiter emails. {len(exceptional)} exceptional.")

    if not exceptional:
        print("Nothing worth reporting this week.")
        return

    # Build report
    lines = [f"📋 Weekly recruiter scan — {len(exceptional)} worth a look:\n"]
    for e in sorted(exceptional, key=lambda x: -x.get('score', 0)):
        role    = e.get('role') or 'Unknown role'
        company = e.get('company') or 'Unknown company'
        remote  = {'remote': '🌐 Remote', 'hybrid': '🏢 Hybrid', 'onsite': '📍 Onsite'}.get(e.get('remote', ''), '')
        comp    = e.get('comp_signal') or ''
        comp    = '' if comp in ('null', 'None', 'N/A') else comp
        flags   = ', '.join(e.get('red_flags', [])) or None
        score   = e.get('score', '?')

        lines.append(f"• {role} @ {company} [{score}/10] {remote}")
        lines.append(f"  {e.get('summary', '')}")
        if comp:
            lines.append(f"  💰 {comp}")
        if flags:
            lines.append(f"  ⚠️ {flags}")
        lines.append('')

    report = '\n'.join(lines).strip()

    if args.dry_run:
        print(report)
    else:
        print(report)
        # Send via Telegram
        tg_data = json.dumps({'action': 'send', 'channel': 'telegram', 'target': '573228387', 'message': report}).encode()
        print("(Would send to Telegram in production — wire up message tool or openclaw send)")


if __name__ == '__main__':
    main()
