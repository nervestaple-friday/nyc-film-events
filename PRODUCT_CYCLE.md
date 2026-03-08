# NYC Film Events — Daily Product Development Cycle

## Overview
A daily automated cycle that improves the app incrementally. Runs as a cron job, spawns a coding agent, and includes a mandatory assessment gate.

## Cycle Steps

### 1. Scrape Fresh Data
- Run `scraper.py` to pull latest events from all venues
- Commit updated `events.json`

### 2. Product Development
- Review current state of `index.html` and `scraper.py`
- Pick 1-2 improvements from the backlog (or identify new ones)
- Implement changes
- Commit with descriptive message

### 3. Assessment Gate (MANDATORY)
After changes are committed but BEFORE pushing:
- **Visual check**: Serve `index.html` locally and screenshot it
- **Data integrity**: Verify event counts per venue, no empty/broken entries
- **UX regression check**: Compare against known-good baseline
  - Does search still work?
  - Do filters work?
  - Do posters load?
  - Is the page responsive?
- **Rollback if needed**: `git reset --hard` to pre-change state if assessment fails

### 4. Push & Report
- Push to GitHub (auto-deploys via Pages)
- Log what was done in `memory/YYYY-MM-DD.md`

## Backlog (pick from here)
- [ ] Paris Theater: only 1 event scraping (likely has more)
- [ ] Add showtime information where available
- [ ] Venue website links (click venue name → their site)
- [ ] "Tonight" / "This Weekend" quick filters
- [ ] Dark/light theme toggle
- [ ] Last updated timestamp more prominent
- [ ] Mobile swipe between venue filters
- [ ] Event count badges on venue filter tabs
- [ ] Sort by: date, rating, venue
- [ ] "New this week" badge for recently added events
- [ ] Better empty state when no events match search
- [ ] Favicon
- [ ] Social meta tags (og:image, etc.)
- [ ] PWA manifest for "add to home screen"
- [ ] Scraper resilience: retry on failure, better error handling
- [ ] Cache TMDB data to reduce API calls
- [ ] Accessibility: ARIA labels, keyboard navigation
- [ ] Performance: lazy load posters below fold

## Anti-Regression Baselines
- All 13 venues should have events
- Total events should be > 20
- Search should filter by title
- Venue filter tabs should work
- Posters should render (TMDB URLs valid)
- Page should load in < 2s on broadband
