from __future__ import annotations

from datetime import date, timedelta
import re
from typing import Any

import pandas as pd
import pydeck as pdk
import streamlit as st

from backend.bigquery_service import (
  run_query as run_query_backend,
)
from backend.raw_service import run_query as run_query_raw_backend
from backend.constants import STATIC_GKG_THEME_CATALOG, STATIC_GKG_THEME_CATALOG_DATE, TOPIC_PRESETS
from backend.lookups import load_cameo_lookup, load_country_lookup
from backend.theme_catalog import load_official_theme_catalog
from backend.utils import extract_selected_map_row_index, parse_csv_input


st.set_page_config(page_title="GDELT Article Extractor", page_icon="🌍", layout="wide")


def run_query(*args: Any, **kwargs: Any) -> pd.DataFrame:
  """Compatibility wrapper around backend query execution.

  Args:
    *args: Positional arguments passed through to backend.run_query.
    **kwargs: Keyword arguments passed through to backend.run_query.

  Returns:
    DataFrame containing normalized GDELT query results.
  """
  return run_query_backend(*args, **kwargs)


def render_sidebar() -> dict[str, Any]:
  """Renders sidebar controls and returns normalized query parameters.

  Returns:
    Dictionary of query parameters consumed by backend.run_query.
  """
  st.sidebar.header("Filters")
  st.sidebar.caption("Use the searchable filters below. Leave a field empty if you do not want to restrict by it.")

  data_source_label = st.sidebar.selectbox(
    "Data source",
    options=["Auto (BigQuery -> Raw files)", "BigQuery", "Raw files"],
    index=0,
    help="Auto first tries BigQuery and falls back to raw files if BigQuery fails (quota/credentials).",
  )

  start_date = st.sidebar.date_input(
    "Start date",
    value=date.today(),
    help="Select the beginning of the time window to search. Very large ranges or topic-heavy searches can take longer to run.",
  )
  end_date = st.sidebar.date_input(
    "End date",
    value=date.today(),
    help="Select the end of the time window to search. Very large ranges or topic-heavy searches can take longer to run.",
  )

  lookup_error = None
  country_codes: list[str] = []
  event_codes: list[str] = []
  event_root_codes: list[str] = []

  try:
    country_df = load_country_lookup()
    cameo_df = load_cameo_lookup()
  except Exception as exc:
    lookup_error = str(exc)
    country_df = pd.DataFrame(columns=["code", "name"])
    cameo_df = pd.DataFrame(columns=["code", "description"])

  if lookup_error:
    st.sidebar.warning("Official lookup lists could not be loaded, so manual code entry is enabled.")

    country_codes_raw = st.sidebar.text_input(
      "Event location countries (manual FIPS codes)",
      value="US, UK",
      help="Use 2-letter GDELT event-location country codes such as US, UK, FR, BR. This filters where the event happened, not who was involved.",
    )
    event_codes_raw = st.sidebar.text_input(
      "Exact event codes (manual CAMEO codes)",
      value="190, 194",
      help="Use this only for very specific event subtypes such as 194 = artillery/tank fighting.",
    )
    event_root_codes_raw = st.sidebar.text_input(
      "Event types (manual CAMEO root codes)",
      value="19",
      help="Use broad event families such as 14 = Protest, 19 = Fight, 07 = Provide aid.",
    )

    country_codes = parse_csv_input(country_codes_raw)
    event_codes = parse_csv_input(event_codes_raw)
    event_root_codes = parse_csv_input(event_root_codes_raw)
  else:
    country_df = country_df.copy()
    country_df["label"] = country_df["code"] + " — " + country_df["name"]
    country_options = country_df["label"].tolist()
    country_label_to_code = dict(zip(country_df["label"], country_df["code"]))
    default_country_labels = [
      label for label in country_options if label.startswith("UK —") or label.startswith("US —")
    ]

    selected_country_labels = st.sidebar.multiselect(
      "Event location countries",
      options=country_options,
      default=default_country_labels,
      help="Choose where the event happened. Leave empty to include all countries.",
    )
    country_codes = [country_label_to_code[label] for label in selected_country_labels]

    root_df = cameo_df[cameo_df["code"].str.len() == 2].copy()
    root_df["label"] = root_df["code"] + " — " + root_df["description"].str.title()
    root_options = root_df["label"].tolist()
    root_label_to_code = dict(zip(root_df["label"], root_df["code"]))
    default_root_labels = [label for label in root_options if label.startswith("19 —")]

    selected_root_labels = st.sidebar.multiselect(
      "Event types",
      options=root_options,
      default=default_root_labels,
      help="Broad event families. Good default choice if you want general categories like Protest, Fight, or Aid.",
    )
    event_root_codes = [root_label_to_code[label] for label in selected_root_labels]

    exact_df = cameo_df[cameo_df["code"].str.len() > 2].copy()
    exact_df["label"] = exact_df["code"] + " — " + exact_df["description"]
    exact_options = exact_df["label"].tolist()
    exact_label_to_code = dict(zip(exact_df["label"], exact_df["code"]))
    selected_exact_labels = st.sidebar.multiselect(
      "Exact event subtypes",
      options=exact_options,
      default=[],
      help="Optional fine-grained event subtypes. Use this only when you need a very specific subtype inside a broader event family.",
    )
    event_codes = [exact_label_to_code[label] for label in selected_exact_labels]

  selected_topic_presets = st.sidebar.multiselect(
    "Quick topic presets",
    options=list(TOPIC_PRESETS.keys()),
    default=[],
    help="Ready-made topic bundles for common needs like diseases, disasters, conflict, aid, and migration. Use these when you want a fast starting point.",
  )
  topic_keywords_raw = st.sidebar.text_input(
    "Custom topic keywords",
    value="",
    help="Type your own words or phrases to find what the article is about, for example: cholera, vaccination, hospital strike, food insecurity.",
  )
  topic_keywords = sorted(
    {
      keyword.strip().upper()
      for preset in selected_topic_presets
      for keyword in TOPIC_PRESETS[preset]
    }
    | set(parse_csv_input(topic_keywords_raw))
  )

  adm1_prefix = ""

  row_limit = st.sidebar.slider(
    "Maximum results to return",
    min_value=100,
    max_value=20000,
    value=10000,
    step=100,
    help="Caps the number of rows shown and exported. Higher values can take longer to display.",
  )

  deduplicate_urls = st.sidebar.checkbox(
    "Remove duplicate URLs",
    value=True,
    help="If enabled, keeps only the first occurrence of each article URL and removes duplicates.",
  )

  selected_days = max((end_date - start_date).days + 1, 1)
  max_files = selected_days * 96

  return {
    "data_source": data_source_label,
    "start_date": start_date,
    "end_date": end_date,
    "country_codes": country_codes,
    "adm1_prefix": adm1_prefix,
    "event_codes": event_codes,
    "event_root_codes": event_root_codes,
    "topic_keywords": topic_keywords,
    "max_files": max_files,
    "row_limit": row_limit,
    "deduplicate_urls": deduplicate_urls,
  }


def render_supported_values() -> None:
  """Renders lookup/reference tables for countries, CAMEO codes, and static themes."""
  with st.expander("Supported filter values", expanded=False):
    st.markdown("Use the searchable sidebar dropdowns, or browse the official and live-derived lookup tables below.")
    try:
      country_lookup = load_country_lookup()
      cameo_lookup = load_cameo_lookup()
    except Exception as exc:
      st.warning(f"Lookup tables are temporarily unavailable: {exc}")
      return

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
      "Countries",
      "CAMEO root codes",
      "All CAMEO codes",
      "Quick topic presets",
      "Topic catalog",
    ])

    with tab1:
      st.dataframe(
        country_lookup.rename(columns={"code": "Country Code", "name": "Country"}),
        use_container_width=True,
        hide_index=True,
      )

    with tab2:
      root_lookup = cameo_lookup[cameo_lookup["code"].str.len() == 2].copy()
      st.dataframe(
        root_lookup.rename(columns={"code": "Root Code", "description": "Description"}),
        use_container_width=True,
        hide_index=True,
      )

    with tab3:
      st.dataframe(
        cameo_lookup.rename(columns={"code": "CAMEO Code", "description": "Description"}),
        use_container_width=True,
        hide_index=True,
      )

    with tab4:
      st.caption("These are quick shortcuts for common monitoring needs. For niche topics, use the custom topic keywords field.")
      preset_df = pd.DataFrame(
        [{"Preset": name, "Keywords": ", ".join(keywords)} for name, keywords in TOPIC_PRESETS.items()]
      )
      st.dataframe(preset_df, use_container_width=True, hide_index=True)

    with tab5:
      theme_catalog, source_label = load_official_theme_catalog()
      st.caption(f"Topic labels source: {source_label}")
      if source_label.startswith("Local static"):
        st.caption(f"Fallback date: {STATIC_GKG_THEME_CATALOG_DATE}")
      else:
        st.caption("Lookup file date: 2021-11-20")

      group_options = sorted(theme_catalog["Topic Group"].unique().tolist())
      selected_groups = st.multiselect(
        "Filter topic groups",
        options=group_options,
        default=group_options,
        key="static_gkg_group_filter",
      )
      theme_search = st.text_input(
        "Search topic labels",
        value="",
        key="static_gkg_theme_search",
        help="Search for disease, disaster, governance, conflict, and other topic labels.",
      ).strip().upper()

      filtered_catalog = theme_catalog[theme_catalog["Topic Group"].isin(selected_groups)]
      if theme_search:
        filtered_catalog = filtered_catalog[
          filtered_catalog["Topic Label"].str.upper().str.contains(re.escape(theme_search), regex=True, na=False)
        ]

      st.dataframe(filtered_catalog, use_container_width=True, hide_index=True)


def render_results(df: pd.DataFrame) -> None:
  """Renders metrics, map interactions, focused record, and export controls.

  Args:
    df: Query result DataFrame.
  """
  st.success(f"Fetched {len(df):,} rows")

  try:
    cameo_lookup = load_cameo_lookup()
    code_to_description = dict(zip(cameo_lookup["code"].astype(str), cameo_lookup["description"].astype(str)))
  except Exception:
    code_to_description = {}

  def format_event_code(code: object) -> str:
    code_str = str(code).strip()
    description = code_to_description.get(code_str, "")
    return f"{code_str} - {description}" if description else code_str

  col1, col2, col3 = st.columns(3)
  col1.metric("Rows", f"{len(df):,}")
  col2.metric("Unique events", f"{df['GLOBALEVENTID'].nunique():,}")
  col3.metric("Unique article URLs", f"{df['ArticleURL'].nunique():,}")

  map_rows_df = df[
    [
      "GLOBALEVENTID",
      "ActionGeo_Lat",
      "ActionGeo_Long",
      "EventCode",
      "ActionGeo_FullName",
      "ArticleTitle",
      "ArticleURL",
    ]
  ].copy().reset_index(drop=True)
  map_rows_df.columns = ["event_id", "lat", "lon", "event_code", "location", "article_title", "article_url"]
  map_rows_df["row_index"] = map_rows_df.index.astype(int)

  def preview_titles(values: pd.Series, max_items: int = 3) -> str:
    titles: list[str] = []
    for value in values.fillna("").astype(str):
      cleaned = value.strip()
      if cleaned and cleaned not in titles:
        titles.append(cleaned)
      if len(titles) >= max_items:
        break
    return "<br/>".join(titles)

  def top_event_codes(values: pd.Series, max_items: int = 5) -> str:
    counts = values.fillna("").astype(str).str.strip()
    counts = counts[counts != ""].value_counts().head(max_items)
    if counts.empty:
      return "N/A"
    return "<br/>".join([f"{format_event_code(code)}: {count}" for code, count in counts.items()])

  def parse_row_index_list(serialized: str) -> list[int]:
    values: list[int] = []
    for part in str(serialized).split(","):
      cleaned = part.strip()
      if not cleaned:
        continue
      try:
        values.append(int(cleaned))
      except ValueError:
        continue
    return values

  def extract_selected_cluster_row_indices(chart_state: object) -> list[int]:
    def walk(value: object) -> list[int] | None:
      if isinstance(value, dict):
        if "cluster_row_indices" in value:
          parsed = parse_row_index_list(value.get("cluster_row_indices", ""))
          if parsed:
            return parsed
        for nested in value.values():
          found = walk(nested)
          if found:
            return found
      elif isinstance(value, list):
        for item in value:
          found = walk(item)
          if found:
            return found
      elif hasattr(value, "to_dict"):
        try:
          return walk(value.to_dict())
        except Exception:
          return None
      return None

    return walk(chart_state) or []

  map_df = (
    map_rows_df.groupby(["lat", "lon"], as_index=False)
    .agg(
      location=("location", "first"),
      top_event_codes=("event_code", top_event_codes),
      row_index=("row_index", "first"),
      event_count=("event_id", "count"),
      title_preview=("article_title", preview_titles),
      cluster_row_indices=("row_index", lambda s: ",".join(str(v) for v in s.tolist())),
    )
  )
  map_df["radius"] = map_df["event_count"].clip(upper=50).pow(0.5) * 12000
  map_df["radius"] = map_df["radius"].clip(lower=12000, upper=60000)

  selected_row_index = st.session_state.get("selected_map_row_index")
  if not isinstance(selected_row_index, int) or not 0 <= selected_row_index < len(map_rows_df):
    selected_row_index = None
  selected_row_indices = st.session_state.get("selected_map_row_indices", [])
  selected_row_indices = [idx for idx in selected_row_indices if isinstance(idx, int) and 0 <= idx < len(map_rows_df)]

  st.subheader("Event Locations")
  st.caption("Points are aggregated by coordinates. Larger bubbles indicate more events at that location.")

  map_view = pdk.ViewState(
    latitude=float(map_df["lat"].mean()),
    longitude=float(map_df["lon"].mean()),
    zoom=1,
    pitch=0,
  )
  map_layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_df,
    id="event-points",
    get_position="[lon, lat]",
    get_fill_color=[43, 104, 196, 180],
    get_radius="radius",
    pickable=True,
    auto_highlight=True,
  )
  map_event = st.pydeck_chart(
    pdk.Deck(
      map_style=None,
      initial_view_state=map_view,
      layers=[map_layer],
      tooltip={
        "html": "<b>Event location:</b> {location}<br/><b>Events here:</b> {event_count}<br/><b>Top event codes:</b><br/>{top_event_codes}<br/><b>Sample articles:</b><br/>{title_preview}",
        "style": {"backgroundColor": "#0f172a", "color": "white"},
      },
    ),
    on_select="rerun",
    selection_mode="single-object",
    key="event_map",
  )

  clicked_row_index = extract_selected_map_row_index(map_event)
  clicked_cluster_indices = extract_selected_cluster_row_indices(map_event)
  if clicked_cluster_indices:
    st.session_state["selected_map_row_indices"] = clicked_cluster_indices
    st.session_state["selected_map_row_index"] = clicked_cluster_indices[0]
    selected_row_indices = clicked_cluster_indices
    selected_row_index = clicked_cluster_indices[0]
  elif clicked_row_index is not None and 0 <= clicked_row_index < len(df):
    st.session_state["selected_map_row_index"] = clicked_row_index
    st.session_state["selected_map_row_indices"] = [clicked_row_index]
    selected_row_indices = [clicked_row_index]
    selected_row_index = clicked_row_index

  st.subheader("Matched Records")
  display_df = df.rename(
    columns={
      "GLOBALEVENTID": "Event ID",
      "SQLDATE": "Event Date",
      "EventCode": "Event Code",
      "EventRootCode": "Event Root Code",
      "ActionGeo_CountryCode": "Event Country",
      "ActionGeo_ADM1Code": "Event Region Code",
      "ActionGeo_FullName": "Event Location",
      "ActionGeo_Lat": "Latitude",
      "ActionGeo_Long": "Longitude",
      "MentionTimeDate": "Mention Timestamp",
      "MentionSourceName": "Source Name",
      "ArticleTitle": "Article Title",
      "MatchedTopics": "Matched Topics",
      "ArticleURL": "Article URL",
    }
  ).reset_index(drop=True)
  display_df.insert(
    display_df.columns.get_loc("Event Code") + 1,
    "Event Code Meaning",
    display_df["Event Code"].astype(str).map(code_to_description).fillna(""),
  )
  display_df.insert(0, "Focus", "")

  if selected_row_indices:
    display_df.loc[selected_row_indices, "Focus"] = "👉"
    if len(selected_row_indices) > 1:
      cluster_df = display_df.iloc[selected_row_indices].copy()

      def short_text(value: object, limit: int = 60) -> str:
        text = str(value or "").strip()
        if not text:
          return ""
        return text if len(text) <= limit else text[: limit - 1] + "..."

      cluster_labels = []
      for i, (_, row) in enumerate(cluster_df.iterrows()):
        base = f"{i + 1}. {row['Event Date']} | {format_event_code(row['Event Code'])} | {row['Source Name'] or 'Unknown'}"
        title = short_text(row.get("Article Title", ""), limit=80)
        topics = short_text(row.get("Matched Topics", ""), limit=80)
        if title:
          base += f" | Title: {title}"
        if topics:
          base += f" | Topics: {topics}"
        cluster_labels.append(base)

      default_label = cluster_labels[0]
      if selected_row_index in selected_row_indices:
        default_label = cluster_labels[selected_row_indices.index(selected_row_index)]

      chosen_label = st.selectbox(
        "Selected Event in cluster",
        options=cluster_labels,
        index=cluster_labels.index(default_label),
        key="cluster_selected_event",
        help="This cluster contains multiple events. Pick one to show in the Selected Event card.",
      )
      selected_row_index = selected_row_indices[cluster_labels.index(chosen_label)]
      st.session_state["selected_map_row_index"] = selected_row_index

    selected_record = display_df.iloc[selected_row_index]
    st.info(
      f"Focused cluster: {len(selected_row_indices)} article(s) at {selected_record['Event Location']}"
    )

    with st.container():
      st.markdown("### Selected Event")
      detail_col1, detail_col2 = st.columns(2)
      with detail_col1:
        st.markdown(
          f"**Event ID:** `{selected_record['Event ID']}`  \n"
          f"**Event location:** {selected_record['Event Location']}  \n"
          f"**Event country:** {selected_record['Event Country']}"
        )
      with detail_col2:
        st.markdown(
          f"**Event date:** `{selected_record['Event Date']}`  \n"
          f"**Event code:** `{selected_record['Event Code']}`  \n"
          f"**Event code meaning:** {selected_record['Event Code Meaning'] or 'N/A'}  \n"
          f"**Source:** {selected_record['Source Name'] or 'Unknown'}"
        )

      article_title = str(selected_record["Article Title"]).strip()
      matched_topics = str(selected_record["Matched Topics"]).strip()
      st.markdown(f"**Article title:** {article_title or 'N/A'}")
      st.markdown(f"**Matched topics:** {matched_topics or 'N/A'}")
      article_url = str(selected_record["Article URL"]).strip()
      if article_url:
        st.markdown(f"**Article URL:** [{article_url}]({article_url})")

    selected_block = display_df.iloc[selected_row_indices]
    remaining_block = display_df.drop(index=selected_row_indices)
    display_df = pd.concat([selected_block, remaining_block], ignore_index=True)
  else:
    st.caption("Click a point on the map to show that event's details and article link here.")

  st.dataframe(display_df, use_container_width=True, hide_index=True)

  csv_data = display_df.to_csv(index=False).encode("utf-8")
  st.download_button(
    "Download CSV",
    data=csv_data,
    file_name="gdelt_filtered_articles.csv",
    mime="text/csv",
  )


def main() -> None:
  """Runs the Streamlit frontend workflow for search, map, and results."""
  st.title("GDELT Article Extractor")
  st.caption(
    "Queries GDELT by date, event type, location, and topic, then joins article mentions for map and table views."
  )

  params = render_sidebar()
  render_supported_values()

  if params["start_date"] > params["end_date"]:
    st.error("Start date must be before or equal to end date.")
    st.stop()

  run = st.button("Run Query", type="primary")
  if run:
    with st.spinner("Querying GDELT..."):
      query_params = {k: v for k, v in params.items() if k not in ("data_source", "deduplicate_urls")}
      selected_source = params["data_source"]
      used_source = ""

      try:
        if selected_source == "BigQuery":
          df = run_query_backend(**query_params)
          used_source = "BigQuery"
        elif selected_source == "Raw files":
          df = run_query_raw_backend(**query_params)
          used_source = "Raw files"
        else:
          try:
            df = run_query_backend(**query_params)
            used_source = "BigQuery"
          except Exception as bq_exc:
            st.warning(f"BigQuery unavailable ({bq_exc}). Falling back to Raw files.")
            df = run_query_raw_backend(**query_params)
            used_source = "Raw files"
      except Exception as exc:
        st.error(f"Query failed: {exc}")
        st.stop()

      # Apply URL deduplication if enabled
      if params["deduplicate_urls"] and "ArticleURL" in df.columns:
        before_count = len(df)
        df = df.drop_duplicates(subset=["ArticleURL"], keep="first")
        after_count = len(df)
        removed_count = before_count - after_count
        if removed_count > 0:
          st.caption(f"Data source used: {used_source} | Removed {removed_count} duplicate URL(s)")
        else:
          st.caption(f"Data source used: {used_source}")
      else:
        st.caption(f"Data source used: {used_source}")

    st.session_state["last_results_df"] = df
    st.session_state["selected_map_row_index"] = None
    st.session_state["selected_map_row_indices"] = []
  elif "last_results_df" not in st.session_state:
    st.info("Set your filters and click Run Query.")
    st.stop()
  else:
    df = st.session_state["last_results_df"]
    st.caption("Showing the most recent query results. Click `Run Query` after changing filters.")

  if df.empty:
    st.warning(
      "No records matched the current filters. Try clearing some country or event filters, shortening the date range, or removing topic keywords."
    )
    st.stop()

  render_results(df)


if __name__ == "__main__":
  main()
