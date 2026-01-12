# TPD Games Dashboard - Quick Start Guide

## 🚀 Quick Setup (5 Steps)

### 1. Prepare Data Locally
```bash
# Ensure tpd_conversion_funnel.csv exists
# Run preprocessing
python preprocess_data_tpd.py --all
```

### 2. Create GitHub Repository
- Go to GitHub → New Repository
- Name: `tpd-games-dashboard`
- Don't initialize with README
- Copy repository URL

### 3. Push to GitHub
```bash
git init
git add preprocess_data_tpd.py src/streamlit_dashboard_tpd.py render_tpd.yaml README_TPD.md requirements.txt tpd_conversion_funnel.csv data/*.csv
git commit -m "Initial commit: TPD Games Dashboard"
git remote add origin https://github.com/YOUR_USERNAME/tpd-games-dashboard.git
git push -u origin main
```

### 4. Deploy to Render
1. Go to https://render.com
2. New + → Web Service
3. Connect GitHub → Select `tpd-games-dashboard`
4. Settings:
   - Build: `pip install -r requirements.txt`
   - Start: `streamlit run src/streamlit_dashboard_tpd.py --server.port=$PORT --server.address=0.0.0.0`
5. Add Environment Variables:
   - REDSHIFT_HOST, REDSHIFT_DATABASE, REDSHIFT_PORT, REDSHIFT_USER, REDSHIFT_PASSWORD
6. Create Web Service

### 5. Access Dashboard
- URL: `https://tpd-games-dashboard.onrender.com`
- Test all sections

## 📚 Detailed Guides

- **GitHub Setup**: See `GITHUB_SETUP_TPD.md`
- **Render Setup**: See `RENDER_SETUP_TPD.md`
- **Full Documentation**: See `README_TPD.md`
- **Deployment Checklist**: See `TPD_DEPLOYMENT_CHECKLIST.md`

## ✅ Verification

After deployment, verify:
- [ ] Dashboard loads
- [ ] All sections display data
- [ ] Time series shows data from 2026-01-03
- [ ] Only TPD games (149, 150, 160, 166) appear
- [ ] Game names mapped correctly

## 🆘 Need Help?

- Check `TPD_DEPLOYMENT_CHECKLIST.md` for troubleshooting
- Review Render logs for errors
- Verify environment variables are set correctly
