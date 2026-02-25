#!/bin/bash
# Syncs OpenClaw config files into the encrypted workspace backup.
# Run after any config changes, or call from a cron/heartbeat.

set -e
WORKSPACE="$(cd "$(dirname "$0")/.." && pwd)"
OPENCLAW="$HOME/.openclaw"

mkdir -p "$WORKSPACE/config"

cp "$OPENCLAW/openclaw.json"                                  "$WORKSPACE/config/"
cp "$OPENCLAW/agents/main/agent/auth-profiles.json"           "$WORKSPACE/config/"
cp "$OPENCLAW/agents/main/agent/auth.json"                    "$WORKSPACE/config/"
cp "$OPENCLAW/credentials/telegram-default-allowFrom.json"    "$WORKSPACE/config/"
cp "$OPENCLAW/credentials/telegram-pairing.json"              "$WORKSPACE/config/"
cp "$OPENCLAW/cron/jobs.json"                                 "$WORKSPACE/config/"
cp "$OPENCLAW/identity/device.json"                           "$WORKSPACE/config/"

cd "$WORKSPACE"
git add config/
git diff --cached --quiet && echo "No config changes." && exit 0
git commit -m "chore: sync openclaw config backup"
git push github master
echo "Config backed up to GitHub."
