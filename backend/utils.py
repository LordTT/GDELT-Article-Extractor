"""Utility helpers used by both backend services and frontend UI."""

from __future__ import annotations

from datetime import date
import re
from typing import List

import pandas as pd


def parse_csv_input(raw: str) -> List[str]:
  """Parses a comma-separated text input into normalized uppercase tokens.

  Args:
    raw: User-provided comma-separated values.

  Returns:
    List of uppercase values without empty entries.
  """
  return [v.strip().upper() for v in raw.split(",") if v.strip()]


def to_sql_date_int(value: date) -> int:
  """Converts a Python date to GDELT/BigQuery YYYYMMDD integer format.

  Args:
    value: Date to convert.

  Returns:
    Integer date representation, e.g. 20260413.
  """
  return int(value.strftime("%Y%m%d"))


def build_keyword_regex(keywords: List[str]) -> str:
  """Builds a BigQuery RE2-safe pattern for topic keyword matching.

  Args:
    keywords: Topic words or phrases to match.

  Returns:
    A regex string using non-alphanumeric boundaries.
  """
  escaped = [re.escape(keyword.strip().upper()) for keyword in keywords if keyword.strip()]
  if not escaped:
    return ""
  joined = "|".join(sorted(escaped, key=len, reverse=True))
  return rf"(?:^|[^A-Z0-9])(?:{joined})(?:$|[^A-Z0-9])"


def classify_gkg_supertheme(theme: str) -> str:
  """Maps a raw GKG theme to a broader, user-friendly topic group.

  Args:
    theme: Raw GKG theme token.

  Returns:
    Name of the high-level supertheme bucket.
  """
  t = (theme or "").upper()

  if any(key in t for key in ["HEALTH", "MEDICAL", "DISEASE", "EPIDEMIC", "PANDEMIC", "SANITATION"]):
    return "Health & Disease"
  if any(key in t for key in ["ARMEDCONFLICT", "MILITARY", "KILL", "CEASEFIRE", "WAR", "VIOLENCE", "PEACE_OPERATIONS"]):
    return "Conflict & Security"
  if any(key in t for key in ["DISASTER", "EARTHQUAKE", "FLOOD", "DROUGHT", "HURRICANE", "WILDFIRE", "CRISISLEX_C"]):
    return "Disasters & Crisis"
  if any(key in t for key in ["ECON_", "EPU_", "PRICE", "TRADE", "JOBS", "DEBT", "FINANCIAL", "STOCKMARKET"]):
    return "Economy & Markets"
  if any(key in t for key in ["GOVERNMENT", "POLITIC", "ELECTION", "LEGISLATION", "JUSTICE", "GOVERNANCE", "PUBLIC_SECTOR"]):
    return "Politics & Governance"
  if any(key in t for key in ["MIGRATION", "REFUG", "HUMAN_RIGHTS", "TRAFFICKING", "INEQUALITY", "POVERTY"]):
    return "Society & Humanitarian"
  if any(key in t for key in ["EDUCATION", "SCHOOL", "UNIVERSITY", "STUDENT"]):
    return "Education"
  if any(key in t for key in ["TRANSPORT", "ROADS", "RAIL", "AVIATION", "MARITIME", "INFRASTRUCTURE", "ENERGY"]):
    return "Infrastructure & Environment"
  if any(key in t for key in ["MEDIA", "SOCIAL_MEDIA", "DIGITAL", "ICT", "BROADCAST"]):
    return "Media & Information"
  if any(key in t for key in ["CRIME", "DRUG", "ARREST", "TRIAL", "POLICE", "PRISON"]):
    return "Crime & Law"
  if t.startswith("TAX_"):
    return "Taxonomies & Entities"
  if t.startswith("WB_"):
    return "World Bank Topics"
  if t.startswith("CRISISLEX_"):
    return "CrisisLex"
  if t.startswith("USPEC_"):
    return "USPEC"
  return "Other"


def extract_selected_map_row_index(chart_state: object) -> int | None:
  """Extracts the selected row index from Streamlit PyDeck selection payload.

  Args:
    chart_state: Object returned by st.pydeck_chart when on_select is enabled.

  Returns:
    The selected row index when present, otherwise None.
  """

  def walk(value: object) -> int | None:
    if isinstance(value, dict):
      for key in ("row_index", "__row_index__", "index"):
        candidate = value.get(key)
        if candidate is not None and not pd.isna(candidate):
          try:
            return int(candidate)
          except (TypeError, ValueError):
            pass
      for nested in value.values():
        found = walk(nested)
        if found is not None:
          return found
    elif isinstance(value, list):
      for item in value:
        found = walk(item)
        if found is not None:
          return found
    elif hasattr(value, "to_dict"):
      try:
        return walk(value.to_dict())
      except Exception:
        return None
    return None

  return walk(chart_state)
