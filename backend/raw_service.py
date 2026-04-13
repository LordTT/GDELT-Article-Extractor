"""Raw GDELT file backend for offline-friendly querying without BigQuery."""

from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
import re
import time as pytime
from typing import List

import pandas as pd
import requests
import streamlit as st

from .utils import build_keyword_regex, infer_title_from_url, to_sql_date_int

MASTER_FILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
CACHE_DIR = Path(".cache/gdeltv2")
PARSED_CACHE_DIR = CACHE_DIR / "parsed"
MASTERFILELIST_CACHE_PATH = CACHE_DIR / "masterfilelist.txt"
MASTERFILELIST_TTL_SECONDS = 60 * 30

EVENT_COLS = [0, 1, 26, 28, 52, 53, 54, 56, 57, 60]
EVENT_COL_NAMES = [
  "GLOBALEVENTID",
  "SQLDATE",
  "EventCode",
  "EventRootCode",
  "ActionGeo_FullName",
  "ActionGeo_CountryCode",
  "ActionGeo_ADM1Code",
  "ActionGeo_Lat",
  "ActionGeo_Long",
  "SOURCEURL",
]

MENTION_COLS = [0, 2, 4, 5]
MENTION_COL_NAMES = [
  "GLOBALEVENTID",
  "MentionTimeDate",
  "MentionSourceName",
  "MentionIdentifier",
]

GKG_COLS = [4, 7, 26]
GKG_COL_NAMES = [
  "DocumentIdentifier",
  "Themes",
  "Extras",
]


def normalize_url(value: object) -> str:
  """Normalizes URLs to maximize join/match consistency.

  Args:
    value: URL value.

  Returns:
    Normalized URL string.
  """
  if value is None or pd.isna(value):
    return ""

  normalized = str(value).strip()
  if not normalized:
    return ""

  normalized = normalized.split("#", 1)[0]
  normalized = re.sub(r"^https?://", "", normalized, flags=re.IGNORECASE)
  normalized = re.sub(r"^www\.", "", normalized, flags=re.IGNORECASE)
  return normalized.rstrip("/").lower()


def combine_unique_strings(values: pd.Series, max_items: int = 6) -> str:
  """Combines distinct string values into a short preview string.

  Args:
    values: Input string series.
    max_items: Maximum number of unique values to include.

  Returns:
    Pipe-separated preview string.
  """
  seen: list[str] = []
  for value in values.fillna("").astype(str):
    cleaned = value.strip()
    if cleaned and cleaned not in seen:
      seen.append(cleaned)
    if len(seen) >= max_items:
      break
  return " | ".join(seen)


@st.cache_data(show_spinner=False, ttl=MASTERFILELIST_TTL_SECONDS)
def fetch_masterfilelist() -> str:
  """Fetches and caches GDELT master file list.

  Returns:
    Raw text content of the GDELT 2.0 master file list.
  """
  CACHE_DIR.mkdir(parents=True, exist_ok=True)

  if MASTERFILELIST_CACHE_PATH.exists():
    age_seconds = pytime.time() - MASTERFILELIST_CACHE_PATH.stat().st_mtime
    if age_seconds < MASTERFILELIST_TTL_SECONDS:
      return MASTERFILELIST_CACHE_PATH.read_text(encoding="utf-8", errors="ignore")

  response = requests.get(MASTER_FILELIST_URL, timeout=120)
  response.raise_for_status()
  MASTERFILELIST_CACHE_PATH.write_text(response.text, encoding="utf-8")
  return response.text


def parse_masterfilelist(
  master_text: str,
  start_date: date,
  end_date: date,
  max_files: int,
) -> tuple[List[str], List[str], List[str]]:
  """Parses master file list and returns URLs for export/mentions/gkg files.

  Args:
    master_text: Full master file list text.
    start_date: Inclusive start date.
    end_date: Inclusive end date.
    max_files: Maximum number of 15-minute export/mentions pairs to include.

  Returns:
    Tuple of URL lists: (event_urls, mention_urls, gkg_urls).
  """
  start_dt = datetime.combine(start_date, time.min)
  end_dt = datetime.combine(end_date, time.max)

  urls_by_ts: dict[str, dict[str, str]] = {}
  url_re = re.compile(r"https?://\S+")
  complete_ts: list[str] = []

  for line in reversed(master_text.splitlines()):
    match = url_re.search(line)
    if not match:
      continue

    url = match.group(0)
    filename = url.rsplit("/", 1)[-1]
    if not (
      filename.endswith(".export.CSV.zip")
      or filename.endswith(".mentions.CSV.zip")
      or filename.endswith(".gkg.csv.zip")
    ):
      continue

    ts_str = filename[:14]
    if len(ts_str) != 14 or not ts_str.isdigit():
      continue

    ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
    if ts > end_dt:
      continue
    if ts < start_dt:
      break

    entry = urls_by_ts.setdefault(ts_str, {})
    if filename.endswith(".export.CSV.zip"):
      entry["export"] = url
    elif filename.endswith(".mentions.CSV.zip"):
      entry["mentions"] = url
    elif filename.endswith(".gkg.csv.zip"):
      entry["gkg"] = url

    if "export" in entry and "mentions" in entry and ts_str not in complete_ts:
      complete_ts.append(ts_str)
      if len(complete_ts) >= max_files:
        break

  selected_ts = complete_ts[:max_files]
  event_urls = [urls_by_ts[ts]["export"] for ts in selected_ts]
  mention_urls = [urls_by_ts[ts]["mentions"] for ts in selected_ts]
  gkg_urls = [urls_by_ts[ts]["gkg"] for ts in selected_ts if "gkg" in urls_by_ts[ts]]
  return event_urls, mention_urls, gkg_urls


def download_cached(url: str) -> Path:
  """Downloads a raw file to local cache if needed.

  Args:
    url: Source URL for the file.

  Returns:
    Local cached file path.
  """
  CACHE_DIR.mkdir(parents=True, exist_ok=True)
  target = CACHE_DIR / url.rsplit("/", 1)[-1]
  if target.exists() and target.stat().st_size > 0:
    return target

  response = requests.get(url, timeout=180)
  response.raise_for_status()
  target.write_bytes(response.content)
  return target


def parsed_cache_path(source_path: Path, dataset_name: str) -> Path:
  """Builds parsed-cache path for a downloaded raw ZIP file.

  Args:
    source_path: Path to cached raw ZIP file.
    dataset_name: Logical dataset name (events, mentions, or gkg).

  Returns:
    Target Parquet cache path.
  """
  PARSED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
  return PARSED_CACHE_DIR / f"{source_path.name}.{dataset_name}.parquet"


def can_use_parsed_cache(source_path: Path, cache_path: Path) -> bool:
  """Checks whether a parsed cache file can be reused.

  Args:
    source_path: Path to source ZIP file.
    cache_path: Path to parsed Parquet cache.

  Returns:
    True if parsed cache exists, is non-empty, and is at least as new as source.
  """
  if not cache_path.exists() or cache_path.stat().st_size <= 0:
    return False
  return cache_path.stat().st_mtime >= source_path.stat().st_mtime


@st.cache_data(show_spinner=False, ttl=86400)
def load_events(paths: List[Path]) -> pd.DataFrame:
  """Loads event rows from raw ZIP files.

  Args:
    paths: Paths to event ZIP files.

  Returns:
    Normalized events DataFrame.
  """
  frames: List[pd.DataFrame] = []
  for path in paths:
    cache_path = parsed_cache_path(path, "events")
    if can_use_parsed_cache(path, cache_path):
      df = pd.read_parquet(cache_path)
    else:
      df = pd.read_csv(
        path,
        sep="\t",
        header=None,
        compression="zip",
        usecols=EVENT_COLS,
        dtype=str,
        encoding="latin-1",
        on_bad_lines="skip",
        low_memory=False,
      )
      df.columns = EVENT_COL_NAMES
      df.to_parquet(cache_path, index=False)
    frames.append(df)

  if not frames:
    return pd.DataFrame(columns=EVENT_COL_NAMES)

  events = pd.concat(frames, ignore_index=True)
  events["ActionGeo_Lat"] = pd.to_numeric(events["ActionGeo_Lat"], errors="coerce")
  events["ActionGeo_Long"] = pd.to_numeric(events["ActionGeo_Long"], errors="coerce")
  events["SQLDATE"] = pd.to_numeric(events["SQLDATE"], errors="coerce")
  return events


@st.cache_data(show_spinner=False, ttl=86400)
def load_mentions(paths: List[Path]) -> pd.DataFrame:
  """Loads mentions rows from raw ZIP files.

  Args:
    paths: Paths to mentions ZIP files.

  Returns:
    Normalized mentions DataFrame.
  """
  frames: List[pd.DataFrame] = []
  for path in paths:
    cache_path = parsed_cache_path(path, "mentions")
    if can_use_parsed_cache(path, cache_path):
      df = pd.read_parquet(cache_path)
    else:
      df = pd.read_csv(
        path,
        sep="\t",
        header=None,
        compression="zip",
        usecols=MENTION_COLS,
        dtype=str,
        encoding="latin-1",
        on_bad_lines="skip",
        low_memory=False,
      )
      df.columns = MENTION_COL_NAMES
      df.to_parquet(cache_path, index=False)
    frames.append(df)

  if not frames:
    return pd.DataFrame(columns=MENTION_COL_NAMES)

  mentions = pd.concat(frames, ignore_index=True)
  mentions["MentionTimeDate"] = pd.to_numeric(mentions["MentionTimeDate"], errors="coerce")
  mentions = mentions.sort_values("MentionTimeDate", ascending=False)
  return mentions


@st.cache_data(show_spinner=False, ttl=86400)
def load_gkg(paths: List[Path]) -> pd.DataFrame:
  """Loads GKG rows and builds searchable text fields.

  Args:
    paths: Paths to GKG ZIP files.

  Returns:
    DataFrame with normalized URL and topic search columns.
  """
  frames: List[pd.DataFrame] = []
  for path in paths:
    cache_path = parsed_cache_path(path, "gkg")
    if can_use_parsed_cache(path, cache_path):
      df = pd.read_parquet(cache_path)
    else:
      df = pd.read_csv(
        path,
        sep="\t",
        header=None,
        compression="zip",
        usecols=GKG_COLS,
        dtype=str,
        encoding="latin-1",
        on_bad_lines="skip",
        low_memory=False,
      )
      df.columns = GKG_COL_NAMES
      df.to_parquet(cache_path, index=False)
    frames.append(df)

  if not frames:
    return pd.DataFrame(columns=["URLNorm", "Themes", "PageTitle", "SearchText"])

  gkg = pd.concat(frames, ignore_index=True)
  gkg["PageTitle"] = (
    gkg["Extras"]
    .fillna("")
    .str.extract(r"<PAGE_TITLE>(.*?)</PAGE_TITLE>", expand=False)
    .fillna("")
  )
  gkg["URLNorm"] = gkg["DocumentIdentifier"].map(normalize_url)
  gkg["SearchText"] = (
    gkg["Themes"].fillna("")
    + " "
    + gkg["PageTitle"].fillna("")
    + " "
    + gkg["DocumentIdentifier"].fillna("")
  ).str.upper()
  gkg = gkg[gkg["URLNorm"] != ""]
  return gkg[["URLNorm", "Themes", "PageTitle", "SearchText"]]


@st.cache_data(show_spinner=False, ttl=3600)
def run_query(
  start_date: date,
  end_date: date,
  country_codes: List[str],
  adm1_prefix: str,
  event_codes: List[str],
  event_root_codes: List[str],
  topic_keywords: List[str],
  max_files: int | None = 288,
  row_limit: int = 3000,
  project_id: str = "",
) -> pd.DataFrame:
  """Runs query over raw GDELT files and returns normalized output schema.

  Args:
    start_date: Inclusive start date.
    end_date: Inclusive end date.
    country_codes: Optional event-location country filters.
    adm1_prefix: Optional region code prefix filter.
    event_codes: Optional exact CAMEO event code filters.
    event_root_codes: Optional CAMEO root code filters.
    topic_keywords: Optional topic/theme keyword filters.
    max_files: Maximum 15-minute batches to scan.
    row_limit: Maximum rows to return.
    project_id: Unused in raw mode; accepted for signature compatibility.

  Returns:
    Query result DataFrame with the shared frontend schema.
  """
  del project_id
  max_files = max_files or 288

  master_text = fetch_masterfilelist()
  event_urls, mention_urls, gkg_urls = parse_masterfilelist(master_text, start_date, end_date, max_files)
  if not event_urls:
    return pd.DataFrame()

  event_paths = [download_cached(url) for url in event_urls]
  mention_paths = [download_cached(url) for url in mention_urls]

  events = load_events(event_paths)
  mentions = load_mentions(mention_paths)

  if events.empty:
    return pd.DataFrame()

  start_int = to_sql_date_int(start_date)
  end_int = to_sql_date_int(end_date)

  filtered = events[(events["SQLDATE"] >= start_int) & (events["SQLDATE"] <= end_int)]

  if country_codes:
    filtered = filtered[filtered["ActionGeo_CountryCode"].isin(country_codes)]

  if adm1_prefix:
    filtered = filtered[filtered["ActionGeo_ADM1Code"].fillna("").str.startswith(adm1_prefix)]

  if event_codes or event_root_codes:
    code_mask = filtered["EventCode"].isin(event_codes) if event_codes else False
    root_mask = filtered["EventRootCode"].isin(event_root_codes) if event_root_codes else False
    if isinstance(code_mask, bool):
      code_mask = pd.Series([False] * len(filtered), index=filtered.index)
    if isinstance(root_mask, bool):
      root_mask = pd.Series([False] * len(filtered), index=filtered.index)
    filtered = filtered[code_mask | root_mask]

  filtered = filtered[filtered["ActionGeo_Lat"].notna() & filtered["ActionGeo_Long"].notna()]
  if filtered.empty:
    return pd.DataFrame()

  joined = filtered.merge(mentions, on="GLOBALEVENTID", how="left")
  joined["ArticleURL"] = joined["MentionIdentifier"].fillna(joined["SOURCEURL"])
  joined["MatchedTopics"] = ""
  joined["ArticleTitle"] = ""

  if topic_keywords:
    gkg_paths = [download_cached(url) for url in gkg_urls]
    gkg = load_gkg(gkg_paths)
    if gkg.empty:
      return pd.DataFrame()

    topic_pattern = build_keyword_regex(topic_keywords)
    matched_gkg = gkg[gkg["SearchText"].str.contains(topic_pattern, regex=True, na=False)].copy()
    if matched_gkg.empty:
      return pd.DataFrame()

    matched_gkg = matched_gkg.groupby("URLNorm", as_index=False).agg(
      {
        "Themes": lambda s: combine_unique_strings(s),
        "PageTitle": lambda s: combine_unique_strings(s, max_items=1),
      }
    )
    theme_map = matched_gkg.set_index("URLNorm")["Themes"].to_dict()
    title_map = matched_gkg.set_index("URLNorm")["PageTitle"].to_dict()

    joined["SourceURLNorm"] = joined["SOURCEURL"].map(normalize_url)
    joined["ArticleURLNorm"] = joined["ArticleURL"].map(normalize_url)
    joined["MatchedTopics"] = joined["ArticleURLNorm"].map(theme_map).fillna(joined["SourceURLNorm"].map(theme_map)).fillna("")
    joined["ArticleTitle"] = joined["ArticleURLNorm"].map(title_map).fillna(joined["SourceURLNorm"].map(title_map)).fillna("")
    joined = joined[(joined["MatchedTopics"] != "") | (joined["ArticleTitle"] != "")]

  joined = joined.sort_values(["SQLDATE", "MentionTimeDate"], ascending=[False, False])
  joined = joined.drop_duplicates(subset=["GLOBALEVENTID", "ArticleURL"])
  joined = joined.head(row_limit)

  empty_title_mask = joined["ArticleTitle"].fillna("").astype(str).str.strip() == ""
  if empty_title_mask.any():
    joined.loc[empty_title_mask, "ArticleTitle"] = joined.loc[empty_title_mask, "ArticleURL"].map(infer_title_from_url)

  return joined[
    [
      "GLOBALEVENTID",
      "SQLDATE",
      "EventCode",
      "EventRootCode",
      "ActionGeo_CountryCode",
      "ActionGeo_ADM1Code",
      "ActionGeo_FullName",
      "ActionGeo_Lat",
      "ActionGeo_Long",
      "MentionTimeDate",
      "MentionSourceName",
      "ArticleTitle",
      "MatchedTopics",
      "ArticleURL",
    ]
  ]
