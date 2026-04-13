"""BigQuery data access layer for querying GDELT events, mentions, and GKG topics."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

from .utils import build_keyword_regex, classify_gkg_supertheme, infer_title_from_url, to_sql_date_int


def _find_local_service_account_file() -> Path | None:
  """Finds a service-account JSON file in the project root.

  Returns:
    Path to a discovered service-account file, or None if not found.
  """
  project_root = Path(__file__).resolve().parent.parent
  candidates = sorted(project_root.glob("*.json"))
  for candidate in candidates:
    name = candidate.name.lower()
    if name.startswith("project-") and "google" not in name:
      return candidate
  return None


BQ_BASE_QUERY = """
WITH filtered_events AS (
  SELECT
    e.GLOBALEVENTID,
    CAST(e.SQLDATE AS INT64) AS SQLDATE,
    CAST(e.EventCode AS STRING) AS EventCode,
    CAST(e.EventRootCode AS STRING) AS EventRootCode,
    e.ActionGeo_CountryCode,
    e.ActionGeo_ADM1Code,
    e.ActionGeo_FullName,
    e.ActionGeo_Lat,
    e.ActionGeo_Long,
    e.SOURCEURL
  FROM `gdelt-bq.gdeltv2.events` e
  WHERE CAST(e.SQLDATE AS INT64) BETWEEN @start_date_int AND @end_date_int
    AND (@country_filter_off OR e.ActionGeo_CountryCode IN UNNEST(@country_codes))
    AND (@adm1_filter_off OR STARTS_WITH(COALESCE(e.ActionGeo_ADM1Code, ''), @adm1_prefix))
    AND (
      @event_filter_off
      OR CAST(e.EventCode AS STRING) IN UNNEST(@event_codes)
      OR CAST(e.EventRootCode AS STRING) IN UNNEST(@event_root_codes)
    )
    AND e.ActionGeo_Lat IS NOT NULL
    AND e.ActionGeo_Long IS NOT NULL
)
SELECT
  fe.GLOBALEVENTID,
  fe.SQLDATE,
  fe.EventCode,
  fe.EventRootCode,
  fe.ActionGeo_CountryCode,
  fe.ActionGeo_ADM1Code,
  fe.ActionGeo_FullName,
  fe.ActionGeo_Lat,
  fe.ActionGeo_Long,
  fm.MentionTimeDate,
  fm.MentionSourceName,
  '' AS ArticleTitle,
  '' AS MatchedTopics,
  COALESCE(fm.MentionIdentifier, fe.SOURCEURL) AS ArticleURL
FROM filtered_events fe
LEFT JOIN `gdelt-bq.gdeltv2.eventmentions` fm
USING (GLOBALEVENTID)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY fe.GLOBALEVENTID, COALESCE(fm.MentionIdentifier, fe.SOURCEURL)
  ORDER BY fm.MentionTimeDate DESC
) = 1
ORDER BY fe.SQLDATE DESC
LIMIT @row_limit
"""


BQ_TOPIC_QUERY = """
WITH filtered_events AS (
  SELECT
    e.GLOBALEVENTID,
    CAST(e.SQLDATE AS INT64) AS SQLDATE,
    CAST(e.EventCode AS STRING) AS EventCode,
    CAST(e.EventRootCode AS STRING) AS EventRootCode,
    e.ActionGeo_CountryCode,
    e.ActionGeo_ADM1Code,
    e.ActionGeo_FullName,
    e.ActionGeo_Lat,
    e.ActionGeo_Long,
    e.SOURCEURL
  FROM `gdelt-bq.gdeltv2.events` e
  WHERE CAST(e.SQLDATE AS INT64) BETWEEN @start_date_int AND @end_date_int
    AND (@country_filter_off OR e.ActionGeo_CountryCode IN UNNEST(@country_codes))
    AND (@adm1_filter_off OR STARTS_WITH(COALESCE(e.ActionGeo_ADM1Code, ''), @adm1_prefix))
    AND (
      @event_filter_off
      OR CAST(e.EventCode AS STRING) IN UNNEST(@event_codes)
      OR CAST(e.EventRootCode AS STRING) IN UNNEST(@event_root_codes)
    )
    AND e.ActionGeo_Lat IS NOT NULL
    AND e.ActionGeo_Long IS NOT NULL
),
filtered_topics AS (
  SELECT
    g.DocumentIdentifier,
    COALESCE(g.V2Themes, g.Themes, '') AS Themes,
    REGEXP_EXTRACT(g.Extras, r'<PAGE_TITLE>(.*?)</PAGE_TITLE>') AS ArticleTitle
  FROM `gdelt-bq.gdeltv2.gkg` g
  WHERE CAST(SUBSTR(CAST(g.`DATE` AS STRING), 1, 8) AS INT64) BETWEEN @start_date_int AND @end_date_int
    AND REGEXP_CONTAINS(
      UPPER(CONCAT(
        COALESCE(g.V2Themes, ''), ' ',
        COALESCE(g.Themes, ''), ' ',
        COALESCE(g.Extras, ''), ' ',
        COALESCE(g.DocumentIdentifier, '')
      )),
      @topic_pattern
    )
)
SELECT
  fe.GLOBALEVENTID,
  fe.SQLDATE,
  fe.EventCode,
  fe.EventRootCode,
  fe.ActionGeo_CountryCode,
  fe.ActionGeo_ADM1Code,
  fe.ActionGeo_FullName,
  fe.ActionGeo_Lat,
  fe.ActionGeo_Long,
  fm.MentionTimeDate,
  fm.MentionSourceName,
  COALESCE(ft1.ArticleTitle, ft2.ArticleTitle, '') AS ArticleTitle,
  COALESCE(ft1.Themes, ft2.Themes, '') AS MatchedTopics,
  COALESCE(fm.MentionIdentifier, fe.SOURCEURL) AS ArticleURL
FROM filtered_events fe
LEFT JOIN `gdelt-bq.gdeltv2.eventmentions` fm
USING (GLOBALEVENTID)
LEFT JOIN filtered_topics ft1
  ON ft1.DocumentIdentifier = fm.MentionIdentifier
LEFT JOIN filtered_topics ft2
  ON ft2.DocumentIdentifier = fe.SOURCEURL
WHERE ft1.DocumentIdentifier IS NOT NULL OR ft2.DocumentIdentifier IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY fe.GLOBALEVENTID, COALESCE(fm.MentionIdentifier, fe.SOURCEURL)
  ORDER BY fm.MentionTimeDate DESC
) = 1
ORDER BY fe.SQLDATE DESC
LIMIT @row_limit
"""


def get_bigquery_client(project_id: str = ""):
  """Creates a BigQuery client for the active credentials.

  Args:
    project_id: Optional project override.

  Returns:
    An authenticated BigQuery client.

  Raises:
    RuntimeError: If BigQuery dependencies are not installed.
  """
  try:
    from google.cloud import bigquery
    from google.auth import exceptions as ga_exceptions
    from google.oauth2 import service_account
  except ImportError as exc:
    raise RuntimeError(
      "BigQuery support is not available in this environment. Run 'pip install -r requirements.txt' "
      "and make sure 'google-cloud-bigquery' is installed."
    ) from exc

  try:
    return bigquery.Client(project=project_id or None)
  except ga_exceptions.DefaultCredentialsError as default_exc:
    local_key = _find_local_service_account_file()
    if local_key is None:
      raise RuntimeError(
        "Google credentials were not found. Set GOOGLE_APPLICATION_CREDENTIALS or place your service-account JSON in the project root."
      ) from default_exc

    credentials = service_account.Credentials.from_service_account_file(str(local_key))
    return bigquery.Client(project=project_id or credentials.project_id, credentials=credentials)


@st.cache_data(show_spinner=False, ttl=3600)
def load_recent_gkg_theme_summary(sample_file_pairs: int = 4, project_id: str = "") -> pd.DataFrame:
  """Builds a recent, grouped list of GKG themes for browsing in the UI.

  Args:
    sample_file_pairs: Controls sample size (used as a rough scale factor).
    project_id: Optional BigQuery billing project.

  Returns:
    DataFrame with columns: Supertheme, Theme, Count.
  """
  client = get_bigquery_client(project_id)
  sample_rows = max(sample_file_pairs * 5000, 5000)
  recent_start = to_sql_date_int(date.today() - timedelta(days=1))
  sample_percent = max(0.03, min(0.5, sample_file_pairs * 0.03))

  query = f"""
  SELECT COALESCE(V2Themes, Themes, '') AS Themes
  FROM `gdelt-bq.gdeltv2.gkg` TABLESAMPLE SYSTEM ({sample_percent:.3f} PERCENT)
  WHERE CAST(SUBSTR(CAST(`DATE` AS STRING), 1, 8) AS INT64) >= @recent_start
    AND COALESCE(V2Themes, Themes, '') != ''
  LIMIT @sample_rows
  """

  from google.cloud import bigquery
  from google.api_core import exceptions as api_exceptions

  job_config = bigquery.QueryJobConfig(
    maximum_bytes_billed=1_000_000_000,
    query_parameters=[
      bigquery.ScalarQueryParameter("recent_start", "INT64", recent_start),
      bigquery.ScalarQueryParameter("sample_rows", "INT64", sample_rows),
    ]
  )
  try:
    gkg = client.query(query, job_config=job_config).result().to_dataframe(create_bqstorage_client=False)
  except api_exceptions.Forbidden as exc:
    message = str(exc)
    if "quotaExceeded" in message or "Quota exceeded" in message:
      raise RuntimeError(
        "Live topic catalog uses a broad GKG scan and your free BigQuery quota is currently exceeded. "
        "Try again later or lower the 'Recent topic-catalog samples' slider."
      ) from exc
    raise

  if gkg.empty:
    return pd.DataFrame(columns=["Supertheme", "Theme", "Count"])

  counts: dict[str, int] = {}
  for themes in gkg["Themes"].fillna(""):
    for theme in str(themes).split(";"):
      cleaned = theme.strip()
      if cleaned:
        counts[cleaned] = counts.get(cleaned, 0) + 1

  rows = [
    {
      "Supertheme": classify_gkg_supertheme(theme),
      "Theme": theme,
      "Count": count,
    }
    for theme, count in counts.items()
  ]
  if not rows:
    return pd.DataFrame(columns=["Supertheme", "Theme", "Count"])

  df = pd.DataFrame(rows)
  return df.sort_values(["Supertheme", "Count", "Theme"], ascending=[True, False, True], kind="stable")


@st.cache_data(show_spinner=False, ttl=3600)
def run_query(
  start_date: date,
  end_date: date,
  country_codes: List[str],
  adm1_prefix: str,
  event_codes: List[str],
  event_root_codes: List[str],
  topic_keywords: List[str],
  max_files: int | None = None,
  row_limit: int = 3000,
  project_id: str = "",
) -> pd.DataFrame:
  """Executes the main GDELT query against BigQuery.

  Args:
    start_date: Inclusive start date.
    end_date: Inclusive end date.
    country_codes: Optional event-location country filters.
    adm1_prefix: Optional region code prefix.
    event_codes: Optional exact CAMEO codes.
    event_root_codes: Optional CAMEO root codes.
    topic_keywords: Optional topic/theme keywords.
    max_files: Legacy compatibility argument, unused in BigQuery mode.
    row_limit: Maximum rows returned.
    project_id: Optional BigQuery billing project.

  Returns:
    Normalized query result DataFrame used by the UI.
  """
  del max_files

  client = get_bigquery_client(project_id)
  start_int = to_sql_date_int(start_date)
  end_int = to_sql_date_int(end_date)

  from google.cloud import bigquery

  query_parameters = [
    bigquery.ScalarQueryParameter("start_date_int", "INT64", start_int),
    bigquery.ScalarQueryParameter("end_date_int", "INT64", end_int),
    bigquery.ArrayQueryParameter("country_codes", "STRING", country_codes),
    bigquery.ScalarQueryParameter("country_filter_off", "BOOL", len(country_codes) == 0),
    bigquery.ScalarQueryParameter("adm1_prefix", "STRING", adm1_prefix),
    bigquery.ScalarQueryParameter("adm1_filter_off", "BOOL", adm1_prefix == ""),
    bigquery.ArrayQueryParameter("event_codes", "STRING", event_codes),
    bigquery.ArrayQueryParameter("event_root_codes", "STRING", event_root_codes),
    bigquery.ScalarQueryParameter(
      "event_filter_off",
      "BOOL",
      len(event_codes) == 0 and len(event_root_codes) == 0,
    ),
    bigquery.ScalarQueryParameter("row_limit", "INT64", row_limit),
  ]

  query = BQ_BASE_QUERY
  if topic_keywords:
    query = BQ_TOPIC_QUERY
    query_parameters.append(
      bigquery.ScalarQueryParameter("topic_pattern", "STRING", build_keyword_regex(topic_keywords))
    )

  job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
  df = client.query(query, job_config=job_config).result().to_dataframe(create_bqstorage_client=False)
  if not df.empty and "ArticleTitle" in df.columns and "ArticleURL" in df.columns:
    empty_mask = df["ArticleTitle"].fillna("").astype(str).str.strip() == ""
    if empty_mask.any():
      df.loc[empty_mask, "ArticleTitle"] = df.loc[empty_mask, "ArticleURL"].map(infer_title_from_url)
  return df
