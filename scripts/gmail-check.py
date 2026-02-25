#!/usr/bin/env python3
"""
Gmail inbox checker with token refresh.
Filters per Jim's preferences: skip Nextdoor, normal financial statements.
Outputs a summary suitable for Telegram.
"""

import json, urllib.request, urllib.parse, datetime, os, sys

WORKSPACE   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOKEN_FILE  = os.path.join(WORKSPACE, 'gmail_token.json')
CREDS_FILE  = os.path.join(WORKSPACE, 'gmail_credentials.json')

SKIP_SENDERS = [
    'nextdoor', 'noreply@nextdoor.com',
    # Marketing/newsletters
    'hims', 'carshield', 'peptide', 'travelzoo', 'sigmaxiorg',
    'moneycall', 'gothamist', 'hellgate', 'theathletic', 'mlb.com',
    'nytcooking', 'nytimes', '404media', 'headgum',
]
SKIP_SUBJECTS = [
    'statement', 'your statement', 'account statement',
    'e-statement', 'estatement', 'your bill is ready',
    # Generic newsletter patterns
    'unsubscribe', 'view in browser', 'morning newsletter',
    'weekly digest', 'membership renewal',
]
HOURS = int(sys.argv[1]) if len(sys.argv) > 1 else 12


def load_token():
    with open(TOKEN_FILE) as f:
        return json.load(f)


def save_token(token):
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token, f, indent=2)


def refresh_token(token):
    with open(CREDS_FILE) as f:
        creds = json.load(f)['installed']
    data = urllib.parse.urlencode({
        'client_id':     creds['client_id'],
        'client_secret': creds['client_secret'],
        'refresh_token': token['refresh_token'],
        'grant_type':    'refresh_token',
    }).encode()
    req = urllib.request.Request(creds['token_uri'], data=data,
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=15) as resp:
        new = json.loads(resp.read())
    token['access_token'] = new['access_token']
    if 'refresh_token' in new:
        token['refresh_token'] = new['refresh_token']
    save_token(token)
    return token


def gmail_req(url, token):
    """Make a Gmail API request, auto-refreshing token on 401."""
    for attempt in range(2):
        try:
            r = urllib.request.Request(url, headers={'Authorization': f"Bearer {token['access_token']}"})
            with urllib.request.urlopen(r, timeout=15) as resp:
                return json.loads(resp.read()), token
        except urllib.error.HTTPError as e:
            if e.code == 401 and attempt == 0:
                token = refresh_token(token)
            else:
                raise


def should_skip(sender, subject):
    sender_l  = sender.lower()
    subject_l = subject.lower()
    if any(s in sender_l for s in SKIP_SENDERS):
        return True
    if any(s in subject_l for s in SKIP_SUBJECTS):
        return True
    return False


def main():
    token = load_token()
    after = int((datetime.datetime.now() - datetime.timedelta(hours=HOURS)).timestamp())
    q     = urllib.parse.urlencode({'q': f'in:inbox is:unread after:{after}', 'maxResults': 30})
    results, token = gmail_req(
        f'https://gmail.googleapis.com/gmail/v1/users/me/messages?{q}', token)

    msgs = results.get('messages', [])
    if not msgs:
        print(f'No unread emails in the last {HOURS}h.')
        return

    interesting = []
    skipped     = 0

    for m in msgs:
        detail, token = gmail_req(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{m['id']}"
            f"?format=metadata&metadataHeaders=From&metadataHeaders=Subject&metadataHeaders=Date",
            token)
        hmap    = {h['name']: h['value'] for h in detail.get('payload', {}).get('headers', [])}
        sender  = hmap.get('From', '')
        subject = hmap.get('Subject', '(no subject)')
        snippet = detail.get('snippet', '')[:120]

        # Apply medication alias
        snippet  = snippet.replace('BlueChew', 'medication').replace('bluechew', 'medication')
        subject  = subject.replace('BlueChew', 'medication').replace('bluechew', 'medication')

        if should_skip(sender, subject):
            skipped += 1
            continue

        interesting.append({'sender': sender, 'subject': subject, 'snippet': snippet})

    if not interesting:
        print(f'Nothing important — {len(msgs)} unread ({skipped} filtered).')
        return

    print(f'📬 {len(interesting)} email(s) worth your attention (last {HOURS}h):')
    for e in interesting:
        # Clean up sender display
        name = e['sender'].split('<')[0].strip().strip('"') or e['sender']
        print(f'\n• {name}')
        print(f'  {e["subject"]}')
        if e['snippet']:
            print(f'  {e["snippet"]}')


if __name__ == '__main__':
    main()
