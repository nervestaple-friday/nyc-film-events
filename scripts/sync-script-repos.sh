#!/usr/bin/env bash
# Syncs Friday's automation scripts to their individual GitHub repos.
# Runs as part of the heartbeat backup cycle.

set -euo pipefail

WORKSPACE="$(cd "$(dirname "$0")/.." && pwd)"
MIRRORS="/tmp/friday-mirrors"
FRIDAY_TOKEN=$(python3 -c "
import json
with open('$WORKSPACE/credentials.json') as f:
    d = json.load(f)
print(d['github']['friday_org'])
")

# Map: repo-name → script filename
declare -A REPOS=(
  [gmail-check]="gmail-check.py"
  [recruiter-scan]="recruiter-scan.py"
  [4k-upgrade-scan]="4k-upgrade-scan.py"
  [seedpool-monitor]="seedpool-monitor.py"
  [film-of-the-day]="film-of-the-day.py"
  [plex-watch-status]="plex-watch-status.py"
)

any_pushed=false

for repo in "${!REPOS[@]}"; do
  script="${REPOS[$repo]}"
  src="$WORKSPACE/scripts/$script"
  mirror="$MIRRORS/$repo"

  # Clone mirror if it doesn't exist locally
  if [ ! -d "$mirror/.git" ]; then
    rm -rf "$mirror"
    git clone -q "https://$FRIDAY_TOKEN@github.com/nervestaple-friday/$repo.git" "$mirror"
    git -C "$mirror" remote set-url origin "https://$FRIDAY_TOKEN@github.com/nervestaple-friday/$repo.git"
  fi

  # Copy latest script
  cp "$src" "$mirror/$script"

  # Check for changes
  if git -C "$mirror" diff --quiet HEAD -- "$script" 2>/dev/null && \
     git -C "$mirror" diff --cached --quiet HEAD -- "$script" 2>/dev/null; then
    continue
  fi

  git -C "$mirror" add "$script"
  git -C "$mirror" commit -q -m "Sync from workspace"
  git -C "$mirror" push -q
  echo "✓ Pushed $repo"
  any_pushed=true
done

if [ "$any_pushed" = false ]; then
  echo "Script repos up to date."
fi
