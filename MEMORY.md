# MEMORY.md - Friday's Long-Term Memory

_Started fresh after data loss on 2026-02-25. Rebuilt from Telegram history dump._

---

## About Jim
- Name: Jim
- Location: Brooklyn, NY (Kensington area)
- Timezone: America/New_York
- GitHub: nervestaple
- Gmail: nervestaple@gmail.com

## Preferences & Quirks
- "Tomorrow before bed" means **starting the next morning** (e.g., reminders)
- BlueChew/medication-related: call it **"medication"** in any summaries
- **Nextdoor**: useless, skip entirely
- **Financial statements**: skip unless something is abnormal
- **Film suggestions**: ALWAYS verify not already in Plex/Radarr library before suggesting
- Film taste: Wiseman docs, Claire Denis, Bakshi, Trier, Safdie, Hitchcock, international cinema, genre with real craft. **Ignore franchise/blockbuster films** (obvious).
- Daily suggestions: propose 1-2 films + 1 show per day for addition

---

## Infrastructure

### VM
- IP: 192.168.4.222
- MAC: 00:a0:98:16:9a:a8
- Networking: macvtap (@eno1 on TrueNAS host)
- Gateway (Eero): 192.168.4.1
- TrueNAS host (truenas-24.local): ~192.168.4.194 area

### SMTP Blocking (security)
- Enforced via nftables **netdev egress hook** on eno1 (TrueNAS host)
- Blocks ports 25, 465, 587 outbound from my MAC
- Lives outside the VM — I cannot modify it
- Persistence: TrueNAS Init/Shutdown Scripts (Post Init)
- Script at: `/home/claw/.openclaw/workspace/` (nftables netdev)

---

## Integrations

### Gmail (read-only)
- Account: nervestaple@gmail.com
- Scope: gmail.readonly (enforced by Google)
- Google Cloud Project: "Friday"
- OAuth credentials stored in workspace
- Token stored in OpenClaw config
- Email summaries: check a few times/day; filter Nextdoor, financial statements (unless abnormal)

### GitHub
- **nervestaple/jim-wtf** — full control token (personal site)
- **nervestaple/jimandhallie-biz** — read/write token (wedding site, reference for patterns)
- **nervestaple-friday** org — Friday's sandbox for creating repos freely
  - arr-proxy: https://github.com/nervestaple-friday/arr-proxy

### Philips Hue
- Bridge IP: 192.168.4.39
- Light: "plant lamp"
- Sleep mode: activate "Sleep" scene in Living Room

### Plex
- Server: jim-media-server at 192.168.4.121:32400
- Token: XRRDNdQVeCjRumgE9mUy
- Libraries: Movies, TV Shows, 4K TV Shows
- ~4,291 movies, ~698 shows
- Media deletion **disabled** in Plex settings (security)

### arr-proxy (Radarr + Sonarr gateway)
- Running at: 192.168.4.94:7879
- Proxy key: orRkC573vbA4cepg4TV_kdtLoy-AaaM8uuyBloWQzT4 (header: `X-Proxy-Key`)
- Repo: https://github.com/nervestaple-friday/arr-proxy
- Allowed: list, search, add, toggle monitored
- Blocked: DELETE (enforced in proxy code)
- Radarr direct URL: http://192.168.4.94:7878

---

## Active Projects

### jim.wtf (personal site)
- Repo: nervestaple/jim-wtf
- Stack: React Three Fiber + Next.js
- Current state: main page returns null (blank), has HeadFlow component (3D monograms), TorturePit/RagdollBody built but unwired
- PR #222 open: deps bump to latest (Next 16, React 19, R3F 9, etc.)
- **Creative vision**: starts as vintage 1997 HTML (GeoCities aesthetic), progressively corrupts/glitches as you navigate deeper, with 3D R3F effects bleeding through
- Reference site: nervestaple/jimandhallie-biz (wedding site, more modern patterns)
- Note: using --webpack flag (Turbopack doesn't support file-loader for .glb imports — separate PR needed)

### arr-proxy
- Deployed on Radarr machine (192.168.4.94)
- Docker compose, restart: unless-stopped
- Proxies both Radarr (/movies/*) and Sonarr (/series/*)

---

## Heartbeat Tasks
- Check email (Gmail) — urgent/missed items, filter noise
- Include Plex recently added
- Daily film/show suggestions (verified against library)
- Track: email, calendar, plex in heartbeat-state.json
