# HEARTBEAT.md

## Disk Space Monitor
- Check `df /` on every heartbeat
- **Alert immediately** if available < 20GB
- **Alert immediately** if used space grew by >3GB since last check
- Save current state to `memory/disk-state.json` after each check

## Email Check
- Check Gmail inbox (a few times/day)
- Skip: Nextdoor, normal financial statements
- Call BlueChew "medication"
