"""Lookup loaders for countries and CAMEO event codes."""

from __future__ import annotations

import pandas as pd
import requests
import streamlit as st

from .constants import CAMEO_LOOKUP_URL, COUNTRY_LOOKUP_URL


@st.cache_data(show_spinner=False, ttl=86400)
def fetch_lookup_text(url: str) -> str:
  """Downloads lookup text and caches it for one day.

  Args:
    url: Public URL hosting a lookup text file.

  Returns:
    Raw response text.
  """
  response = requests.get(url, timeout=60)
  response.raise_for_status()
  return response.text


@st.cache_data(show_spinner=False, ttl=86400)
def load_country_lookup() -> pd.DataFrame:
  """Loads and normalizes the GDELT country lookup table.

  Returns:
    DataFrame with columns: code, name.
  """
  rows: list[dict[str, str]] = []
  for line in fetch_lookup_text(COUNTRY_LOOKUP_URL).splitlines():
    parts = line.strip().split("\t", 1)
    if len(parts) != 2:
      continue
    code, name = parts[0].strip().upper(), " ".join(parts[1].split())
    if len(code) == 2 and name:
      rows.append({"code": code, "name": name})

  return pd.DataFrame(rows).drop_duplicates(subset=["code"]).sort_values("name", kind="stable")


@st.cache_data(show_spinner=False, ttl=86400)
def load_cameo_lookup() -> pd.DataFrame:
  """Loads and normalizes CAMEO event code descriptions.

  Returns:
    DataFrame with columns: code, description sorted numerically.
  """
  rows: list[dict[str, str]] = []
  for line in fetch_lookup_text(CAMEO_LOOKUP_URL).splitlines():
    parts = line.strip().split("\t", 1)
    if len(parts) != 2:
      continue
    code, description = parts[0].strip(), " ".join(parts[1].split())
    if code.upper() == "CAMEOEVENTCODE":
      continue
    if code.isdigit() and description:
      rows.append({"code": code, "description": description})

  df = pd.DataFrame(rows).drop_duplicates(subset=["code"])
  df["sort_key"] = pd.to_numeric(df["code"], errors="coerce")
  return df.sort_values(["sort_key", "code"], kind="stable").drop(columns=["sort_key"])
