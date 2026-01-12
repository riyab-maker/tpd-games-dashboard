# TPD Games Dashboard - Deployment Checklist

Use this checklist to ensure everything is set up correctly before and after deployment.

## Pre-Deployment Checklist

### Local Setup
- [ ] `tpd_conversion_funnel.csv` exists in root directory
- [ ] Environment variables configured in `.env` file
- [ ] Preprocessing script runs successfully: `python preprocess_data_tpd.py --all`
- [ ] All data files generated in `data/` directory:
  - [ ] `summary_data.csv`
  - [ ] `game_conversion_numbers.csv`
  - [ ] `time_series_data.csv`
  - [ ] `repeatability_data.csv`
  - [ ] `score_distribution_data.csv`
  - [ ] `question_correctness_data.csv`
  - [ ] `poll_responses_data.csv`
  - [ ] `video_viewership_data.csv`
  - [ ] `metadata.json`
- [ ] Dashboard runs locally: `streamlit run src/streamlit_dashboard_tpd.py`
- [ ] All sections display correctly:
  - [ ] Conversion Funnel
  - [ ] Time Series Analysis
  - [ ] Repeatability
  - [ ] Score Distribution
  - [ ] Question Correctness
  - [ ] Parent Poll
  - [ ] Video Viewership

### Code Verification
- [ ] `preprocess_data_tpd.py` uses TPD queries (date: 2026-01-03)
- [ ] `src/streamlit_dashboard_tpd.py` loads from `tpd_conversion_funnel.csv`
- [ ] Dashboard title is "TPD Games Dashboard"
- [ ] Queries filter by custom_dimension_2 IN (149, 150, 160, 166)
- [ ] Date filters set to 2026-01-03

### Files Ready for Repository
- [ ] `preprocess_data_tpd.py`
- [ ] `src/streamlit_dashboard_tpd.py`
- [ ] `render_tpd.yaml`
- [ ] `README_TPD.md`
- [ ] `requirements.txt`
- [ ] `.gitignore` (excludes .env, __pycache__, etc.)
- [ ] `tpd_conversion_funnel.csv` (or plan to add separately)
- [ ] All `data/*.csv` files
- [ ] `data/metadata.json`

## GitHub Repository Checklist

- [ ] Repository created on GitHub
- [ ] Repository name: `tpd-games-dashboard` (or chosen name)
- [ ] Git initialized locally
- [ ] All TPD files added and committed
- [ ] `.env` file NOT committed (in .gitignore)
- [ ] Remote repository connected
- [ ] Files pushed to GitHub
- [ ] Verify all files visible on GitHub
- [ ] README displays correctly

## Render Deployment Checklist

### Service Configuration
- [ ] Render account created
- [ ] GitHub account connected to Render
- [ ] New Web Service created
- [ ] Correct repository selected
- [ ] Service name: `tpd-games-dashboard`
- [ ] Environment: Python 3
- [ ] Build command: `pip install -r requirements.txt`
- [ ] Start command: `streamlit run src/streamlit_dashboard_tpd.py --server.port=$PORT --server.address=0.0.0.0`

### Environment Variables
- [ ] `REDSHIFT_HOST` set
- [ ] `REDSHIFT_DATABASE` set
- [ ] `REDSHIFT_PORT` set (5439)
- [ ] `REDSHIFT_USER` set
- [ ] `REDSHIFT_PASSWORD` set
- [ ] `PYTHON_VERSION` set (3.11.0)

### Deployment
- [ ] Service created successfully
- [ ] Build completed without errors
- [ ] Service started successfully
- [ ] Dashboard URL accessible
- [ ] No errors in logs

## Post-Deployment Verification

### Dashboard Functionality
- [ ] Dashboard loads without errors
- [ ] All sections display:
  - [ ] Summary statistics
  - [ ] Conversion Funnel
  - [ ] Time Series Analysis
  - [ ] Repeatability
  - [ ] Score Distribution
  - [ ] Question Correctness
  - [ ] Parent Poll
  - [ ] Video Viewership
- [ ] Global filters work correctly
- [ ] Date range filters work (if applicable)
- [ ] Charts render properly
- [ ] Data appears correct

### Data Verification
- [ ] Time series shows data from 2026-01-03 onwards
- [ ] Only TPD games appear (149, 150, 160, 166)
- [ ] Game names mapped correctly:
  - [ ] 166 → significance_of_early_years_2
  - [ ] 150 → significance_of_early_years_1
  - [ ] 160 → language_development_1
  - [ ] 149 → redirected to emotional_development
- [ ] Conversion funnel uses TPD data
- [ ] Repeatability shows correct game counts

### Performance
- [ ] Dashboard loads in reasonable time (< 10 seconds)
- [ ] No memory errors
- [ ] Charts render smoothly
- [ ] Filters respond quickly

## Maintenance Checklist

### Regular Updates
- [ ] Preprocessing runs successfully
- [ ] Data files updated
- [ ] Changes committed to GitHub
- [ ] Render redeploys automatically
- [ ] Dashboard reflects latest data

### Monitoring
- [ ] Check Render logs regularly
- [ ] Monitor service health
- [ ] Review error logs
- [ ] Check data freshness
- [ ] Verify query performance

## Troubleshooting Reference

### Common Issues
- **"No data available"**: Run preprocessing, check data files exist
- **Build fails**: Check requirements.txt, verify all dependencies
- **Service won't start**: Check start command, verify port configuration
- **Database errors**: Verify environment variables, check Redshift access
- **Missing files**: Ensure all files committed to repository

### Quick Fixes
1. **Clear cache**: Delete `__pycache__` directories
2. **Reinstall dependencies**: `pip install -r requirements.txt --force-reinstall`
3. **Redeploy**: Push empty commit or trigger manual deploy
4. **Check logs**: Review Render logs for specific errors

## Notes

- Keep Hybrid Dashboard and TPD Dashboard repositories separate
- Never commit `.env` file with credentials
- Test locally before deploying
- Keep data files updated regularly
- Monitor Render service health

## Sign-Off

- [ ] All pre-deployment items completed
- [ ] GitHub repository set up
- [ ] Render deployment successful
- [ ] Dashboard verified and working
- [ ] Documentation reviewed

**Deployment Date**: _______________
**Deployed By**: _______________
**Dashboard URL**: _______________
