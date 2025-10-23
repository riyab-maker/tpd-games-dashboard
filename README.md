## Hybrid Dashboard - Matomo Events Analytics

A comprehensive Streamlit dashboard for analyzing Matomo events data with support for multiple deployment platforms.

### 🚀 Deployment Options

#### Option 1: Streamlit Community Cloud (Recommended for Free Hosting)
- **File**: `streamlit_dashboard_cloud.py`
- **Platform**: [share.streamlit.io](https://share.streamlit.io)
- **Memory Optimized**: <1GB usage for free tier
- **Auto-deployment**: From GitHub repository

#### Option 2: Render (Production Hosting)
- **File**: `streamlit_dashboard.py`
- **Platform**: [render.com](https://render.com)
- **Configuration**: Uses `render.yaml`
- **Environment**: Production-ready with custom domains

## 📋 Quick Start

### Local Development
```bash
# Clone the repository
git clone https://github.com/riyab-maker/hybrid_dashboard.git
cd hybrid_dashboard

# Install dependencies
pip install -r requirements.txt

# Run locally (Streamlit Community Cloud version)
streamlit run src/streamlit_dashboard_cloud.py

# Or run Render version
streamlit run src/streamlit_dashboard.py
```

## ☁️ Streamlit Community Cloud Deployment

### Step 1: Prepare Your Repository
1. **Push your code to GitHub** (already done)
2. **Ensure `streamlit_dashboard_cloud.py` is in the `src/` directory**
3. **Verify `requirements.txt` includes all dependencies**

### Step 2: Deploy to Streamlit Cloud
1. **Go to [share.streamlit.io](https://share.streamlit.io)**
2. **Sign in with your GitHub account**
3. **Click "New app"**
4. **Fill in the deployment form:**
   - **Repository**: `riyab-maker/hybrid_dashboard`
   - **Branch**: `main`
   - **Main file path**: `src/streamlit_dashboard_cloud.py`
   - **App URL**: Choose your custom URL (e.g., `matomo-dashboard`)

### Step 3: Configure Secrets
In your Streamlit Cloud app settings, add these secrets:
```
DB_HOST = a9c3c220991da47c895500da385432b6-1807075149.ap-south-1.elb.amazonaws.com
DB_PORT = 3310
DB_NAME = live
DB_USER = techadmin
DB_PASSWORD = Rl@ece@1234
```

### Step 4: Deploy
- **Click "Deploy!"**
- **Wait for the build to complete**
- **Your dashboard will be live at**: `https://matomo-dashboard.streamlit.app`

## 🏗️ Render Deployment (Production)

### Step 1: Configure Environment Variables
In your Render dashboard, add these environment variables:
```
DB_HOST = a9c3c220991da47c895500da385432b6-1807075149.ap-south-1.elb.amazonaws.com
DB_PORT = 3310
DB_NAME = live
DB_USER = techadmin
DB_PASSWORD = Rl@ece@1234
```

### Step 2: Deploy
- **Connect your GitHub repository**
- **Select the repository**: `riyab-maker/hybrid_dashboard`
- **Render will automatically use `render.yaml` configuration**
- **Your dashboard will be live at**: `https://your-app-name.onrender.com`

## 🔧 Local Development Setup

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

