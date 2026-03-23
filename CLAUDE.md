# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KFS Scraper — scrapes Polish powiatowe urzędy pracy (county employment offices) websites for active KFS (Krajowy Fundusz Szkoleniowy) grant application announcements. Articles are classified using Gemini 2.0 Flash API, and results are rendered as an interactive HTML report with a Leaflet map of Poland's counties.

The project is in Polish. All prompts, variable names, UI text, and comments are in Polish.

## Running

```bash
# Main production pipeline (scrape → classify → HTML report with map)
# Runs on GitHub Actions every 5h, or manually
python run.py

# Older standalone scripts (test/dev variants):
python test_scraper.py      # scrape only → test_articles.json
python test_classify.py     # classify test_articles.json → test_results.json
python test_report.py       # generate test_output.html from test_results.json

# Post-processing (operate on test_results.json):
python rescan_tak.py        # re-classify TAK results + retry rate-limited entries
python rescan_dedup.py      # retry 429 errors + deduplicate TAK per county
python rescan_enrich.py     # fetch full article pages for TAK entries missing termin/kwota
```

## Environment

- Requires `GEMINI_API_KEYS` env var (comma-separated) or falls back to hardcoded keys
- Older `scraper.py` uses `GEMINI_API_KEY` (single key) via `google.generativeai` SDK
- Dependencies: `requests`, `beautifulsoup4`, `google-generativeai` (only in scraper.py)
- No requirements.txt — install manually: `pip install requests beautifulsoup4`

## Architecture

**`run.py`** is the unified production pipeline (replaces the separate test_*.py scripts). It:
1. Loads office list from `urzedy.json`
2. Scrapes news pages + KFS pages (sequential, with detail page fetching)
3. Classifies articles via Gemini API (multithreaded, with URL-keyed cache)
4. Generates `index.html` with interactive Leaflet map + searchable card list

**Data flow:** `urzedy.json` → scrape → classify (Gemini) → `results.json` + `cache.json` → `index.html`

**Classification logic:** Gemini receives a structured prompt asking TAK/NIE for KFS grant announcements. Response is parsed for `WYNIK`, `POWOD`, `TERMIN`, `KWOTA` fields. Cache is keyed by article URL. Articles with KFS keywords in title but cached as NIE get reclassified (cache override).

**Key data files:**
- `urzedy.json` — list of ~340 offices with `name`, `homepage`, `aktualnosci_url`, `kfs_url`, `base_url`
- `cache.json` — URL→classification cache (persists across runs)
- `results.json` / `errors.json` — latest run output
- `powiaty.geojson` — county boundaries for map rendering
- `urzad_to_powiat.json` — maps office names to GeoJSON county names
- `urzad_to_woj.json` — maps office names to voivodeships

**Scraping targets Liferay CMS** — extraction strategies look for `journal-content-article`, `asset-content`, `portlet-body`, etc. Detail pages use smart truncation that preserves date/amount patterns when content exceeds 3000 chars.

**Rescan scripts** (`rescan_*.py`) are post-processing tools that operate on `test_results.json` to fix API errors, deduplicate, and enrich results. They call Gemini API directly via REST (not the SDK).

**Two Gemini API calling patterns exist:**
- `scraper.py`: uses `google.generativeai` SDK (`genai.GenerativeModel`)
- All other files: direct REST calls to `generativelanguage.googleapis.com` with multiple API key rotation and retry logic

**Report generation** embeds all data inline in the HTML (no external JS/CSS except Leaflet CDN). The map colors counties green (has KFS announcements) or gray (none), with click popups showing details.
