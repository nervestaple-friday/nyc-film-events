# HEARTBEAT.md

## Disk Space Monitor
- Check `df /` on every heartbeat
- **Alert immediately** if available < 20GB
- **Alert immediately** if used space grew by >3GB since last check
- Save current state to `memory/disk-state.json` after each check

## Config Backup
- Run `scripts/backup-config.sh` to sync OpenClaw config to GitHub

## 4K Upgrade Scan
- Run `python3 scripts/4k-upgrade-scan.py` (auto-loads token from credentials.json)
- Checks up to 75 movies/run (~20s), random sample across library
- Pre-2000 films rechecked every 14 days (active remaster era); others every 30 days
- If new 4K releases found → message Jim: title, Radarr movie ID, current quality
- Jim approves → run `--upgrade <ID> [ID...]` to trigger Radarr search

## Email Check
- Run `python3 scripts/gmail-check.py 6` (last 6 hours)
- Skip: Nextdoor, newsletters, normal financial statements
- Call BlueChew "medication"
- If anything actionable found → message Jim with distilled summary
