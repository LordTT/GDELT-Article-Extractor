"""Theme catalog loaders for topic browsing without BigQuery scans."""

from __future__ import annotations

import pandas as pd
import requests
import streamlit as st

from .constants import (
  GKG_THEMES_LOOKUP_URL,
  STATIC_GKG_THEME_CATALOG,
  STATIC_GKG_THEME_CATALOG_DATE,
)
from .utils import classify_gkg_supertheme


def _fallback_static_catalog() -> tuple[pd.DataFrame, str]:
  """Builds the local fallback static theme catalog.

  Returns:
    Tuple of fallback DataFrame and source label.
  """
  df = pd.DataFrame(STATIC_GKG_THEME_CATALOG).copy()
  if "Count" not in df.columns:
    df["Count"] = pd.NA
  return df[["Topic Group", "Topic Label", "Count"]], f"Local static snapshot ({STATIC_GKG_THEME_CATALOG_DATE})"


@st.cache_data(show_spinner=False, ttl=86400)
def load_official_theme_catalog() -> tuple[pd.DataFrame, str]:
  """Loads the official GDELT theme lookup file with automatic fallback.

  Returns:
    Tuple of:
      - DataFrame with columns Topic Group, Topic Label, Count
      - Source description string for UI display
  """
  response_text = None
  request_urls = [
    GKG_THEMES_LOOKUP_URL,
    GKG_THEMES_LOOKUP_URL.replace("https://", "http://", 1),
  ]
  headers = {"User-Agent": "gdelt-article-extractor/1.0"}
  for url in request_urls:
    try:
      response = requests.get(url, timeout=60, headers=headers)
      response.raise_for_status()
      response_text = response.text
      break
    except Exception:
      continue

  if not response_text:
    return _fallback_static_catalog()

  rows: list[dict[str, object]] = []
  for line in response_text.splitlines():
    parts = line.strip().split()
    if len(parts) != 2:
      continue
    theme, count_raw = parts
    if not theme:
      continue
    try:
      count = int(count_raw)
    except ValueError:
      continue

    rows.append(
      {
        "Topic Group": classify_gkg_supertheme(theme),
        "Topic Label": theme,
        "Count": count,
      }
    )

  if not rows:
    return _fallback_static_catalog()

  df = pd.DataFrame(rows)
  df = df.sort_values(["Topic Group", "Count", "Topic Label"], ascending=[True, False, True], kind="stable")
  return df, "Official GDELT LOOKUP-GKGTHEMES.TXT"
