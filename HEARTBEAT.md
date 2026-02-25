# HEARTBEAT.md

## Disk Space Monitor
- Check `df /` on every heartbeat
- **Alert immediately** if available < 20GB
- **Alert immediately** if used space grew by >3GB since last check
- Save current state to `memory/disk-state.json` after each check

## Config Backup
- Run `scripts/backup-config.sh` to sync OpenClaw config to GitHub

## 4K Upgrade Scan
- Run `TMDB_TOKEN=<token> python3 scripts/4k-upgrade-scan.py` (token in credentials.json)
- Checks up to 150 movies/run against TMDB, newest first
- If new 4K releases found → message Jim with titles and movie IDs
- Jim replies with IDs to upgrade → run `--upgrade <ID>` to trigger Radarr search

## Email Check
- Check Gmail inbox (a few times/day)
- Skip: Nextdoor, normal financial statements
- Call BlueChew "medication"
