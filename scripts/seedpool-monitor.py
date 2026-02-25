#!/usr/bin/env python3
"""
SeedPool open-registration monitor.

Modes:
  (default)  Check if registration is open. If so, notify Jim and schedule auto-signup.
  --signup   Attempt registration via headless browser.

Cron: twice daily. If open → messages Jim immediately, then a 1-hour cron fires --signup.
"""

import os, sys, json, urllib.request, subprocess, argparse, time

WORKSPACE   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE  = os.path.join(WORKSPACE, 'memory', 'seedpool-state.json')
CREDS_FILE  = os.path.join(WORKSPACE, 'credentials.json')
REGISTER_URL = 'https://seedpool.org/register'
CLOSED_TEXT  = 'Registration Is Disabled'


def load_creds():
    with open(CREDS_FILE) as f:
        d = json.load(f)
    return d['seedpool']


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def is_open():
    req = urllib.request.Request(
        REGISTER_URL,
        headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        html = r.read().decode('utf-8', errors='ignore')
    return CLOSED_TEXT not in html, html


def notify_jim(message):
    """Send a Telegram message via openclaw CLI."""
    subprocess.run(
        ['openclaw', 'send', '--channel', 'telegram', '--to', '573228387', '--message', message],
        capture_output=True
    )


def schedule_signup():
    """Schedule --signup to run in 55 minutes (buffer before 1h window closes)."""
    script = os.path.abspath(__file__)
    result = subprocess.run([
        'openclaw', 'cron', 'add',
        '--name', 'SeedPool auto-signup',
        '--at', '55m',
        '--session', 'main',
        '--system-event', f'Run: python3 {script} --signup',
        '--wake', 'now',
        '--delete-after-run',
    ], capture_output=True, text=True)
    return result.returncode == 0


def attempt_signup(html):
    """Use Playwright to fill out the registration form."""
    import importlib.util
    spec = importlib.util.find_spec('playwright')
    if not spec:
        return False, "Playwright not installed"

    creds = load_creds()
    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = '/home/linuxbrew/.linuxbrew/lib'

    script = f"""
import asyncio
from playwright.async_api import async_playwright

async def signup():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox',
                  '--enable-unsafe-swiftshader', '--disable-dev-shm-usage']
        )
        page = await browser.new_page()
        await page.goto('{REGISTER_URL}', wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(2)

        # Check still open
        content = await page.content()
        if '{CLOSED_TEXT}' in content:
            print('CLOSED')
            await browser.close()
            return

        # Fill fields — common field names on Gazelle/custom trackers
        for selector in ['#username', 'input[name="username"]', 'input[placeholder*="sername"]']:
            try:
                await page.fill(selector, '{creds["username"]}', timeout=2000)
                break
            except: pass

        for selector in ['#email', 'input[name="email"]', 'input[type="email"]']:
            try:
                await page.fill(selector, '{creds["email"]}', timeout=2000)
                break
            except: pass

        for selector in ['#password', 'input[name="password"]', 'input[type="password"]']:
            try:
                await page.fill(selector, '{creds["password"]}', timeout=2000)
                break
            except: pass

        # Confirm password
        for selector in ['#password_confirmation', 'input[name="password_confirm"]',
                         'input[name="confirm_password"]', 'input[name="password2"]']:
            try:
                await page.fill(selector, '{creds["password"]}', timeout=2000)
                break
            except: pass

        # Check for CAPTCHA
        has_captcha = await page.query_selector('.h-captcha, .g-recaptcha, [data-sitekey]')
        if has_captcha:
            print('CAPTCHA')
            await browser.close()
            return

        # Submit
        for selector in ['button[type="submit"]', 'input[type="submit"]', '#register-btn']:
            try:
                await page.click(selector, timeout=3000)
                break
            except: pass

        await asyncio.sleep(3)
        final = await page.content()
        if 'success' in final.lower() or 'confirm' in final.lower() or 'email' in final.lower():
            print('SUCCESS')
        else:
            print('UNKNOWN')
        await browser.close()

asyncio.run(signup())
"""

    result = subprocess.run(
        ['python3', '-c', script],
        capture_output=True, text=True, timeout=60, env=env
    )
    output = (result.stdout + result.stderr).strip()

    if 'CLOSED' in output:
        return False, "closed by the time signup ran"
    elif 'CAPTCHA' in output:
        return False, "captcha"
    elif 'SUCCESS' in output:
        return True, "success"
    else:
        return False, f"unknown result: {output[:200]}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--signup', action='store_true', help='Attempt registration')
    parser.add_argument('--force-check', action='store_true', help='Check even if recently checked')
    args = parser.parse_args()

    state = load_state()

    if args.signup:
        # Attempt signup
        print("Attempting SeedPool signup...")
        open_now, html = is_open()
        if not open_now:
            print("Closed by the time signup ran.")
            notify_jim("⏰ SeedPool: registration closed again before I could sign up. I'll keep watching.")
            state['signup_attempted'] = True
            state['signup_result'] = 'closed_before_signup'
            save_state(state)
            return

        ok, reason = attempt_signup(html)
        if ok:
            notify_jim(
                f"✅ SeedPool: registered successfully!\n"
                f"Username: {load_creds()['username']}\n"
                f"Email: {load_creds()['email']}\n"
                f"Check your inbox to confirm."
            )
            state['registered'] = True
            state['signup_result'] = 'success'
        elif reason == 'captcha':
            notify_jim(
                f"🔒 SeedPool is open but hit a CAPTCHA — couldn't auto-register.\n"
                f"Sign up manually at {REGISTER_URL}\n"
                f"Username: {load_creds()['username']} | Email: {load_creds()['email']}\n"
                f"Password: in 1Password as 'SeedPool'"
            )
            state['signup_result'] = 'captcha'
        else:
            notify_jim(
                f"⚠️ SeedPool signup attempted but result unclear ({reason}).\n"
                f"Check {REGISTER_URL} manually."
            )
            state['signup_result'] = reason

        save_state(state)
        return

    # -- Monitor mode --
    if state.get('registered'):
        print("Already registered. Monitor disabled.")
        return

    print(f"Checking {REGISTER_URL}...")
    try:
        open_now, html = is_open()
    except Exception as e:
        print(f"Check failed: {e}")
        return

    import time as time_mod
    state['last_check'] = int(time_mod.time())

    if not open_now:
        print("Closed.")
        save_state(state)
        return

    print("OPEN! Notifying Jim and scheduling signup...")
    state['open_detected'] = int(time_mod.time())
    save_state(state)

    notify_jim(
        f"🚨 SeedPool open registration detected!\n"
        f"Signing up automatically in 1 hour if you don't respond.\n"
        f"Reply 'skip seedpool' to cancel."
    )

    schedule_signup()


if __name__ == '__main__':
    main()
