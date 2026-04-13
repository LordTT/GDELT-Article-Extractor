# GDELT Article Extractor

Query GDELT 2.0 events and articles via an interactive web app or CLI tool. Filter by date range, event type (CAMEO), location, and topic keywords. Get results as a map, table, or CSV export.

## Backends

### BigQuery (`gdelt-bq.gdeltv2.*`)
- **Pros**: Fast, no local storage needed, can handle large date ranges
- **Cons**: Requires Google Cloud setup, uses quota, costs money on large queries
- **Tables**:  
  - `gdelt-bq.gdeltv2.events` — event records (GLOBALEVENTID, date, location, event code)
  - `gdelt-bq.gdeltv2.eventmentions` — article mentions per event
  - `gdelt-bq.gdeltv2.gkg` — themes/topics extracted from articles

### Raw Files (`data.gdeltproject.org`)
- **Pros**: Free, no quota, works offline after download, deterministic
- **Cons**: Slower (extracts ZIPs locally), limited to ~90 days of cached data
- **Source**: Downloads 15-minute rolling ZIP files from GDELT v2 exports; auto-caches to `.cache/gdeltv2/`

## Setup

### 1. Clone/extract and create Python environment

```bash
python -m venv .venv
. .venv/Scripts/activate  # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Google Cloud setup (Optional for BigQuery)

If you want to use **BigQuery** backend, authenticate:

#### Option A: Application Default Credentials
```bash
gcloud auth application-default login
```

#### Option B: Service account key file
**Windows (Command Prompt)**
```bat
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account.json
```

**Windows (PowerShell)**
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service-account.json"
```

**macOS/Linux**
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Alternatively, place a `project-*.json` file in the project root and it will auto-discover it.

### 3. Run

## Web UI

```bash
streamlit run app.py
```

Opens at `http://localhost:8501`.

**Sidebar options:**
- **Data source**: Select `Auto` (tries BigQuery → raw fallback), `BigQuery` only, or `Raw files` only
- **Date range**: Start and end dates (defaults to today)
- **Event location countries**: GDELT 2-letter country codes (leave empty for all)
- **Event root codes**: Broad CAMEO families (e.g., `19` = Protest)
- **Exact event subtypes**: Fine-grained CAMEO codes (e.g., `191` = Peaceful protest)
- **Quick topic presets**: Pre-curated keywords (Health, Conflict, Economics, etc.)
- **Custom topic keywords**: Comma-separated keywords (e.g., `DISEASE,EPIDEMIC`)
- **Maximum results**: Row limit (default 10,000)
- **Remove duplicate URLs**: Deduplicate by article URL (enabled by default)

**Main view:**
- **Metrics**: Total rows, unique events, unique article URLs
- **Map**: Aggregated event bubbles (size = event count per location); click to select cluster
- **Selected Event card**: Shows event details, article title, matched topics, CAMEO code meaning
- **Results table**: All rows with columns including Event Code, Event Code Meaning, Article Title, Topics, URL
- **Export**: Download visible results as CSV

## CLI Tool

Use `cli.py` to batch-export queries without opening the web UI. Ideal for colleagues, scheduled exports, or analysis pipelines.

### Usage

```bash
# Last 7 days, US events only
python cli.py --output results.csv --days 7 --countries US

# Specific date range with topics
python cli.py --output health.csv --start-date 2026-04-01 --end-date 2026-04-13 --topics HEALTH,DISEASE

# Event code search
python cli.py --output protests.csv --days 3 --event-codes 141,142,143 --countries US

# Use raw files (no BigQuery)
python cli.py --output results.csv --backend raw --days 7

# Force BigQuery
python cli.py --output results.csv --backend bigquery --days 7

# Disable deduplication
python cli.py --output results.csv --no-deduplicate --days 7

# Show all options
python cli.py --help
```

### Arguments

| Argument | Type | Required | Default | Example |
|----------|------|----------|---------|---------|
| `--output`, `-o` | str | ✓ | — | `results.csv` |
| `--days` | int | (date)* | — | `7` |
| `--start-date` | str | (date)* | — | `2026-04-01` |
| `--end-date` | str | (required w/ start) | — | `2026-04-13` |
| `--countries` | str | | "" | `US,UK,FR` |
| `--event-codes` | str | | "" | `141,142,143` |
| `--event-roots` | str | | "" | `14,18` |
| `--topics` | str | | "" | `HEALTH,DISEASE` |
| `--limit` | int | | 10000 | `5000` |
| `--backend` | `auto` \| `bigquery` \| `raw` | | `auto` | `raw` |
| `--no-deduplicate` | flag | | (deduplicate) | — |

*Date range: either `--days` OR `--start-date + --end-date`  
**Empty value (omit the argument)** means no filter applied (searches all countries/events/topics)

### Output

Exports CSV with 14 columns (same schema as web UI):
1. **GLOBALEVENTID** — unique event ID
2. **SQLDATE** — event date (YYYYMMDD format)
3. **EventCode** — exact CAMEO code (3-4 digits)
4. **EventRootCode** — root CAMEO family (2 digits)
5. **ActionGeo_CountryCode** — 2-letter country code
6. **ActionGeo_ADM1Code** — admin-1 region code (state/province)
7. **ActionGeo_FullName** — place name (e.g., "New York, United States")
8. **ActionGeo_Lat** — latitude
9. **ActionGeo_Long** — longitude
10. **MentionTimeDate** — article publication date/time
11. **MentionSourceName** — news source (e.g., "Reuters")
12. **ArticleTitle** — extracted article title (if topics searched)
13. **MatchedTopics** — GKG themes matching search keywords
14. **ArticleURL** — URL to full article

## Project Structure

```
.
├── app.py                          # Streamlit web UI
├── cli.py                          # Command-line export tool
├── requirements.txt                # Python dependencies
├── backend/
│   ├── bigquery_service.py        # BigQuery query logic
│   ├── raw_service.py             # Raw file download & query logic
│   ├── constants.py               # Presets, URLs, config
│   ├── lookups.py                 # Country & CAMEO code mappings
│   ├── theme_catalog.py           # GKG theme catalog
│   └── utils.py                   # Shared helpers
├── .cache/gdeltv2/                # (auto-created) Downloaded raw files
└── project-*.json                 # (optional) Google service account key
```

## Common Queries

### Extract protest events from last 3 days
```bash
python cli.py --output protests.csv --days 3 --event-roots 19 --countries US
```

### Extract health-related news globally for a week
```bash
python cli.py --output health.csv --days 7 --topics HEALTH,DISEASE,MEDICAL
```

### Extract exact event codes for a specific event type

First, find the codes you need in the CAMEO lookup (reference at bottom). Then:

```bash
python cli.py --output events.csv --start-date 2026-03-01 --end-date 2026-04-13 --event-codes 191,192,193 --limit 50000
```

Replace `191,192,193` with codes for your use case. See [CAMEO lookup](https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt) for full code definitions.

## Troubleshooting

### "BigQuery quotaExceeded" or "Quota exceeded"

BigQuery quota was exhausted. Switch to raw files or wait for quota to reset (typically next calendar month).

**In web UI:** Select `Raw files` in Data source dropdown.

**In CLI:** Add `--backend raw`.

### "Query failed: 'utf-8' codec can't decode byte..."

GDELT raw files use Latin-1 encoding. This is normally handled, but if it recurs:
```bash
rm -r .cache/gdeltv2/
```
Then retry (will redownload and re-parse with correct encoding).

### No results returned

Check:
1. **Date range**: Ensure start_date ≤ end_date and both are reasonable (GDELT goes back ~15 years)
2. **Country codes**: Use valid 2-letter codes (e.g., `US`, `UK`, `FR`)
3. **Event codes**: Verify codes exist in CAMEO taxonomy (run `python cli.py --help` for examples)
4. **Topics**: Keywords are searched via regex; ensure they match GKG theme names

### Streamlit app is slow

- Use shorter date ranges or smaller row limits
- Deduplication adds slight overhead; use `--no-deduplicate` if unwanted
- Raw files are inherently slower; try BigQuery if quota available

### Google credentials not found

Set `GOOGLE_APPLICATION_CREDENTIALS` env var or place service account JSON in project root (auto-discovered).

## Resources

- **GDELT 2.0 docs**: https://www.gdeltproject.org/
- **CAMEO event codes**: https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt
- **GKG themes**: http://data.gdeltproject.org/api/v2/guides/LOOKUP-GKGTHEMES.TXT
- **Country codes (FIPS)**: https://www.gdeltproject.org/data/lookups/FIPS.country.txt

## Notes

- Country filtering uses **event location** via `ActionGeo_CountryCode` (FIPS GEO country codes), not actor country.
- For region-level filtering, use the region filter / `ActionGeo_ADM1Code` prefix.
- Topic filtering uses **GKG themes + article titles + URLs** from the GDELT Global Knowledge Graph.
- CAMEO filtering supports both:
  - exact event codes (`EventCode`) like `190, 194`
  - root codes (`EventRootCode`) like `19`
