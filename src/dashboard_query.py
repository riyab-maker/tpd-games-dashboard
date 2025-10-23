"""
Dashboard data fetch and processing script.

- Loads DB credentials from environment (.env)
- Connects via SQLAlchemy
- Runs parameterized, safe SQL queries
- Processes results into tidy data frames for visualizations
- Exports prepared datasets to the outputs/ directory

Adapt the SQL in build_base_query() to your schema.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL


OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class DateRange:
	start_date: str  # ISO date string YYYY-MM-DD
	end_date: str    # ISO date string YYYY-MM-DD


def load_environment() -> None:
	"""Load environment variables from .env if present."""
	load_dotenv(override=False)


def get_database_url() -> str:
	"""Construct a SQLAlchemy URL from environment variables.

	Expected env vars:
	- DB_DIALECT: postgresql | mysql | mssql
	- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
	- Optional (MSSQL): DB_DRIVER (e.g., "ODBC Driver 18 for SQL Server")
	"""
	dialect = os.getenv("DB_DIALECT", "postgresql").strip().lower()
	host = os.environ["DB_HOST"].strip()
	port = os.environ["DB_PORT"].strip()
	database = os.environ["DB_NAME"].strip()
	username = os.environ["DB_USER"].strip()
	password = os.environ["DB_PASSWORD"].strip()

	if dialect == "postgresql":
		return str(
			URL.create(
				drivername="postgresql+psycopg",
				host=host,
				port=port,
				database=database,
				username=username,
				password=password,
			)
		)
	elif dialect == "mysql":
		return str(
			URL.create(
				drivername="mysql+pymysql",
				host=host,
				port=port,
				database=database,
				username=username,
				password=password,
			)
		)
	elif dialect == "mssql":
		driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
		query = {"driver": driver}
		return str(
			URL.create(
				drivername="mssql+pyodbc",
				host=host,
				port=port,
				database=database,
				username=username,
				password=password,
				query=query,
			)
		)
	else:
		raise ValueError(f"Unsupported DB_DIALECT: {dialect}")


def build_engine() -> Engine:
	"""Create a SQLAlchemy Engine using env credentials."""
	database_url = get_database_url()
	return create_engine(database_url, pool_pre_ping=True)


def build_base_query() -> str:
	"""Define the core SQL query for the dashboard.

	Replace the FROM/JOIN/WHERE with your real tables/columns.
	Use named parameters (:start_date, :end_date) for safe binding.
	"""
	return (
		"""
		SELECT
			order_date::date AS order_date,
			customer_id,
			region,
			category,
			SUM(order_amount) AS revenue,
			COUNT(*) AS order_count
		FROM sales_orders
		WHERE order_date >= :start_date AND order_date < (:end_date::date + INTERVAL '1 day')
		GROUP BY 1,2,3,4
		"""
	)


def run_query(engine: Engine, sql: str, date_range: DateRange) -> pd.DataFrame:
	"""Execute the parameterized SQL and return a DataFrame."""
	with engine.connect() as conn:
		result = conn.execute(
			text(sql),
			{"start_date": date_range.start_date, "end_date": date_range.end_date},
		)
		df = pd.DataFrame(result.fetchall(), columns=result.keys())
	return df


def process_for_visualizations(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
	"""Produce common dashboard datasets.

	Returns tuple of:
	- daily_timeseries: revenue and orders per day
	- top_regions: top regions by revenue
	- category_mix: revenue share by category
	"""
	if df.empty:
		# Return empty frames with expected columns
		daily = pd.DataFrame(columns=["order_date", "revenue", "order_count"])
		top_regions = pd.DataFrame(columns=["region", "revenue"]) 
		category_mix = pd.DataFrame(columns=["category", "revenue", "share"])
		return daily, top_regions, category_mix

	# Ensure correct dtypes
	if "order_date" in df.columns:
		df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

	# Daily time series
	daily = (
		df.groupby("order_date", as_index=False)[["revenue", "order_count"]]
		.sum()
		.sort_values("order_date")
	)

	# Top regions
	top_regions = (
		df.groupby("region", as_index=False)["revenue"].sum()
		.sort_values("revenue", ascending=False)
	)

	# Category mix with share
	category_totals = df.groupby("category", as_index=False)["revenue"].sum()
	total_revenue = float(category_totals["revenue"].sum()) or 1.0
	category_totals["share"] = category_totals["revenue"] / total_revenue

	return daily, top_regions, category_totals


def export_outputs(
	daily: pd.DataFrame,
	top_regions: pd.DataFrame,
	category_mix: pd.DataFrame,
	date_range: DateRange,
) -> None:
	"""Save prepared datasets to outputs/ as CSV and Parquet (if available)."""
	prefix = f"{date_range.start_date}_to_{date_range.end_date}"

	outputs = {
		f"{prefix}_daily_timeseries.csv": daily,
		f"{prefix}_top_regions.csv": top_regions,
		f"{prefix}_category_mix.csv": category_mix,
	}

	for name, frame in outputs.items():
		csv_path = OUTPUT_DIR / name
		frame.to_csv(csv_path, index=False)

	# Parquet export if pyarrow available
	try:
		import pyarrow  # noqa: F401
		for name, frame in outputs.items():
			pq_path = OUTPUT_DIR / name.replace(".csv", ".parquet")
			frame.to_parquet(pq_path, index=False)
	except Exception:
		# Non-fatal; CSVs are still written
		pass


def resolve_date_range() -> DateRange:
	"""Get the query date range from env or default to the current month-to-date."""
	start = os.getenv("START_DATE")
	end = os.getenv("END_DATE")
	if start and end:
		return DateRange(start, end)

	# Fallback: current month-to-date
	import datetime as dt
	today = dt.date.today()
	first = today.replace(day=1)
	return DateRange(first.isoformat(), today.isoformat())


def main() -> None:
	load_environment()
	date_range = resolve_date_range()
	engine = build_engine()

	sql = build_base_query()
	raw_df = run_query(engine, sql, date_range)

	daily, top_regions, category_mix = process_for_visualizations(raw_df)
	export_outputs(daily, top_regions, category_mix, date_range)

	print("Data exported to:", OUTPUT_DIR.resolve())
	print("Rows:", {
		"raw": len(raw_df),
		"daily": len(daily),
		"top_regions": len(top_regions),
		"category_mix": len(category_mix),
	})


if __name__ == "__main__":
	main()

