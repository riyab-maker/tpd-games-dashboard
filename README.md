## Hybrid Dashboard Data Pipeline (Python)

This scaffold securely loads DB credentials from environment variables, runs a parameterized SQL query, and prepares/export datasets for visualizations.

### 1) Setup (Windows PowerShell)
```powershell
python -m venv .venv
. .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2) Configure credentials
Copy `.env.example` to `.env` and edit the values:
```ini
DB_DIALECT=postgresql   # or mysql | mssql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=app_db
DB_USER=app_user
DB_PASSWORD=supersecret
# For MSSQL only (example):
# DB_DRIVER=ODBC Driver 18 for SQL Server

# Optional query dates (YYYY-MM-DD)
# START_DATE=2025-01-01
# END_DATE=2025-01-31
```

Supported drivers (install only what you need):
- PostgreSQL: `psycopg[binary]`
- MySQL/MariaDB: `PyMySQL`
- SQL Server: `pyodbc` (requires Microsoft ODBC driver installed)

### 3) Customize the SQL
Edit `src/dashboard_query.py` → `build_base_query()` with your real tables/columns. Keep the named parameters `:start_date` and `:end_date` so the script binds safely.

### 4) Run the query and export
```powershell
python .\src\dashboard_query.py
```
Outputs will be saved to the `outputs/` directory as CSVs (and Parquet if `pyarrow` is available):
- `*_daily_timeseries.csv`
- `*_top_regions.csv`
- `*_category_mix.csv`

### 5) Next steps
- Load the exported datasets into your dashboard tool (e.g., Power BI, Tableau, Plotly Dash, Streamlit).
- Or import `process_for_visualizations()` directly to feed pandas dataframes into plotting code.

### Troubleshooting
- `pyodbc` on Windows requires the appropriate Microsoft ODBC driver (`DB_DRIVER`).
- SSL or network errors often stem from firewall/VPN or cloud DB settings.
- Test connectivity with a minimal query before heavy processing.

