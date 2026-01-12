# TPD Games Dashboard - Creation Summary

## ✅ Completed Tasks

### 1. Preprocessing Script (`preprocess_data_tpd.py`)
- ✅ Updated to use `tpd_conversion_funnel.csv` instead of `conversion_funnel.csv`
- ✅ Replaced TIME_SERIES_QUERY with TPD version:
  - Filters: `custom_dimension_2 IN ('149','150','160','166')`
  - Date: `server_time >= '2025-01-03'`
  - Pattern: `%game_started%` / `%game_completed%` (no 'hybrid_' prefix)
  - Direct CASE statement for game name mapping
  - No game tables or unmapped mappings
- ✅ Replaced repeatability query with TPD version (same filters)
- ✅ Updated date filter in time series preprocessing (2025-01-03 instead of 2025-07-02)
- ✅ Handles missing game_code column (TPD query doesn't return it)

### 2. Dashboard Application (`src/streamlit_dashboard_tpd.py`)
- ✅ Updated title to "TPD Games Dashboard"
- ✅ Updated to load `tpd_conversion_funnel.csv` instead of `conversion_funnel.csv`
- ✅ All visualization functions remain the same
- ✅ All sections work the same way (scores, question correctness, parent poll unchanged)

### 3. Query Files
- ✅ `TPD_TIME_SERIES_QUERY.sql` - TPD time series query
- ✅ `TPD_REPEATABILITY_QUERY.sql` - TPD repeatability query
- ✅ `HYBRID_DASHBOARD_REPEATABILITY_QUERY.sql` - Reference query for comparison

### 4. Documentation
- ✅ `TPD_DASHBOARD_SETUP.md` - Setup and deployment instructions
- ✅ `QUERY_COMPARISON_SUMMARY.md` - Comparison between Hybrid and TPD queries

## 📋 Next Steps

### For Local Testing:
1. Ensure `tpd_conversion_funnel.csv` exists in the root directory
2. Run preprocessing: `python preprocess_data_tpd.py --all`
3. Run dashboard: `streamlit run src/streamlit_dashboard_tpd.py`

### For GitHub Repository:
1. Create a new repository (e.g., `tpd-games-dashboard`)
2. Copy TPD-specific files:
   - `preprocess_data_tpd.py`
   - `src/streamlit_dashboard_tpd.py`
   - `tpd_conversion_funnel.csv`
   - `requirements.txt`
   - `render.yaml` (if using Render)
   - All supporting files (from `src/` directory)
3. Push to GitHub

### For Render Deployment:
1. Create new Render service
2. Connect to TPD GitHub repository
3. Set environment variables (same as Hybrid Dashboard)
4. Set start command: `streamlit run src/streamlit_dashboard_tpd.py --server.port=$PORT --server.address=0.0.0.0`

## 🔍 Verification Checklist

- [ ] `preprocess_data_tpd.py` uses TPD queries
- [ ] `preprocess_data_tpd.py` loads from `tpd_conversion_funnel.csv`
- [ ] `src/streamlit_dashboard_tpd.py` has title "TPD Games Dashboard"
- [ ] `src/streamlit_dashboard_tpd.py` loads from `tpd_conversion_funnel.csv`
- [ ] Time series query filters by custom_dimension_2 IN (149,150,160,166)
- [ ] Time series query uses date >= 2025-01-03
- [ ] Repeatability query uses same filters
- [ ] Game name mapping works correctly
- [ ] Hybrid Dashboard files remain unchanged

## 📝 Important Notes

- **Hybrid Dashboard is NOT modified** - all changes are in separate TPD files
- Both dashboards can coexist independently
- TPD dashboard uses same RM active users CSV
- Scores, question correctness, and parent poll sections are unchanged
- Time series and repeatability use TPD-specific queries only
