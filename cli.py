#!/usr/bin/env python3
"""Command-line interface for GDELT article extraction.

Export GDELT events, mentions, and article metadata to CSV for analysis.

Examples:
  # Extract US events from the last 7 days, save to CSV
  python cli.py --output results.csv --days 7 --countries US

  # Extract health-related events from specific date range
  python cli.py --output health.csv --start-date 2026-04-01 --end-date 2026-04-13 --topics HEALTH

  # Extract protest events by event code
  python cli.py --output protests.csv --event-codes 141,142,143 --countries US

  # Use raw files backend (no BigQuery)
  python cli.py --output results.csv --backend raw --days 3

  # Disable URL deduplication
  python cli.py --output results.csv --no-deduplicate --days 7
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from backend.bigquery_service import run_query as run_query_bigquery
from backend.raw_service import run_query as run_query_raw


def parse_args() -> argparse.Namespace:
  """Parses command-line arguments."""
  parser = argparse.ArgumentParser(
    description="Extract GDELT events and articles to CSV.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=__doc__,
  )

  # Output
  parser.add_argument(
    "-o",
    "--output",
    required=True,
    type=str,
    help="Output CSV file path.",
  )

  # Date range
  date_group = parser.add_mutually_exclusive_group(required=True)
  date_group.add_argument(
    "--days",
    type=int,
    help="Number of days back from today (e.g., --days 7 for last 7 days).",
  )
  date_group.add_argument(
    "--start-date",
    type=str,
    help="Start date (YYYY-MM-DD). Must also specify --end-date.",
  )

  parser.add_argument(
    "--end-date",
    type=str,
    help="End date (YYYY-MM-DD). Only used with --start-date.",
  )

  # Filters
  parser.add_argument(
    "--countries",
    type=str,
    default="",
    help="Comma-separated country codes (e.g., US,UK,FR). Leave empty for all.",
  )

  parser.add_argument(
    "--event-codes",
    type=str,
    default="",
    help="Comma-separated exact CAMEO codes (e.g., 141,142,143). Leave empty for all.",
  )

  parser.add_argument(
    "--event-roots",
    type=str,
    default="",
    help="Comma-separated root CAMEO codes (e.g., 14,18). Leave empty for all.",
  )

  parser.add_argument(
    "--topics",
    type=str,
    default="",
    help="Comma-separated topic keywords (e.g., HEALTH,DISEASE). Leave empty for all.",
  )

  # Limits
  parser.add_argument(
    "--limit",
    type=int,
    default=10000,
    help="Maximum number of rows to return. Default: 10000.",
  )

  # Backend
  parser.add_argument(
    "--backend",
    choices=["auto", "bigquery", "raw"],
    default="auto",
    help="Data source: auto (BigQuery→Raw fallback), bigquery (BigQuery only), or raw (raw files only). Default: auto.",
  )

  # Deduplication
  parser.add_argument(
    "--no-deduplicate",
    action="store_true",
    help="Disable URL deduplication. By default, duplicate URLs are removed.",
  )

  return parser.parse_args()


def parse_date(date_str: str) -> date:
  """Parses YYYY-MM-DD string to date object."""
  try:
    return date.fromisoformat(date_str)
  except ValueError as exc:
    raise ValueError(f"Invalid date format '{date_str}'. Use YYYY-MM-DD.") from exc


def parse_csv_codes(codes_str: str) -> list[str]:
  """Parses comma-separated codes."""
  if not codes_str.strip():
    return []
  return [code.strip().upper() for code in codes_str.split(",")]


def main() -> None:
  """Main CLI entrypoint."""
  args = parse_args()

  # Resolve date range
  if args.days:
    end_date = date.today()
    start_date = end_date - timedelta(days=args.days - 1)
  else:
    if not args.start_date or not args.end_date:
      parser = argparse.ArgumentParser()
      parser.error("--start-date and --end-date are required when not using --days.")
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

  if start_date > end_date:
    print("Error: start_date must be before or equal to end_date.", file=sys.stderr)
    sys.exit(1)

  # Parse filters
  country_codes = parse_csv_codes(args.countries)
  event_codes = parse_csv_codes(args.event_codes)
  event_root_codes = parse_csv_codes(args.event_roots)
  topic_keywords = parse_csv_codes(args.topics)

  print(
    f"Querying GDELT: {start_date} to {end_date}",
    f"| Countries: {country_codes or 'all'}",
    f"| Event codes: {event_codes or 'all'}",
    f"| Event roots: {event_root_codes or 'all'}",
    f"| Topics: {topic_keywords or 'all'}",
    sep=" ",
  )

  # Calculate max_files for raw backend
  selected_days = max((end_date - start_date).days + 1, 1)
  max_files = selected_days * 96

  # Query backend
  query_kwargs = {
    "start_date": start_date,
    "end_date": end_date,
    "country_codes": country_codes,
    "adm1_prefix": "",
    "event_codes": event_codes,
    "event_root_codes": event_root_codes,
    "topic_keywords": topic_keywords,
    "max_files": max_files,
    "row_limit": args.limit,
  }

  try:
    if args.backend == "bigquery":
      print("Using BigQuery backend...")
      df = run_query_bigquery(**query_kwargs)
    elif args.backend == "raw":
      print("Using Raw files backend...")
      df = run_query_raw(**query_kwargs)
    else:  # auto
      print("Using Auto backend (BigQuery → Raw files fallback)...")
      try:
        df = run_query_bigquery(**query_kwargs)
        print("✓ BigQuery succeeded.")
      except Exception as bq_exc:
        print(f"✗ BigQuery failed: {bq_exc}")
        print("Falling back to Raw files backend...")
        df = run_query_raw(**query_kwargs)
        print("✓ Raw files succeeded.")
  except Exception as exc:
    print(f"Error: Query failed: {exc}", file=sys.stderr)
    sys.exit(1)

  if df.empty:
    print("Warning: Query returned no results.", file=sys.stderr)

  # Apply deduplication
  if not args.no_deduplicate and "ArticleURL" in df.columns:
    before_count = len(df)
    df = df.drop_duplicates(subset=["ArticleURL"], keep="first")
    after_count = len(df)
    removed_count = before_count - after_count
    if removed_count > 0:
      print(f"Deduplication: removed {removed_count} duplicate URL(s). ({before_count} → {after_count} rows)")

  # Write CSV
  output_path = Path(args.output)
  output_path.parent.mkdir(parents=True, exist_ok=True)

  df.to_csv(output_path, index=False)
  print(f"✓ Exported {len(df):,} rows to {output_path}")


if __name__ == "__main__":
  main()
