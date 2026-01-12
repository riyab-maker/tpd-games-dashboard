# GitHub Repository Setup for TPD Games Dashboard

## Step-by-Step Guide

### Step 1: Create New GitHub Repository

1. Go to https://github.com and log in
2. Click the "+" icon in the top right → "New repository"
3. Repository settings:
   - **Repository name**: `tpd-games-dashboard` (or your preferred name)
   - **Description**: "TPD Games Dashboard - Analytics dashboard for TPD Games data"
   - **Visibility**: Private (recommended) or Public
   - **DO NOT** check "Initialize this repository with a README"
   - **DO NOT** add .gitignore or license
4. Click "Create repository"

### Step 2: Prepare Files for Repository

Create a new directory for the TPD dashboard files (optional, or work directly in current directory):

```bash
# Create a list of files to include
# TPD-specific files:
- preprocess_data_tpd.py
- src/streamlit_dashboard_tpd.py
- render_tpd.yaml
- README_TPD.md
- tpd_conversion_funnel.csv
- requirements.txt
- .env.template (if exists)
- data/*.csv (all processed data files)
- data/metadata.json
```

### Step 3: Initialize Git Repository

```bash
# Navigate to your project directory
cd "C:\Cursor\Hybird Dashboard"

# Initialize git (if not already initialized)
git init

# Create .gitignore if it doesn't exist
# Add this content to .gitignore:
cat > .gitignore << EOF
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
.venv

# Environment variables
.env
.env.local

# Data files (optional - you may want to track these)
# Uncomment if you don't want to track data files:
# data/*.csv
# *.csv

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log
output.txt
*.txt
!requirements.txt
EOF
```

### Step 4: Add and Commit TPD Files

```bash
# Add TPD-specific files
git add preprocess_data_tpd.py
git add src/streamlit_dashboard_tpd.py
git add render_tpd.yaml
git add README_TPD.md
git add requirements.txt
git add .gitignore

# Add TPD conversion funnel CSV (if exists)
if (Test-Path "tpd_conversion_funnel.csv") {
    git add tpd_conversion_funnel.csv
}

# Add data files (if you want to track them)
# Note: These can be large, consider using Git LFS or excluding them
git add data/*.csv
git add data/metadata.json

# Add .env.template if it exists
if (Test-Path ".env.template") {
    git add .env.template
}

# Commit
git commit -m "Initial commit: TPD Games Dashboard"
```

### Step 5: Connect to GitHub and Push

```bash
# Add remote repository (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/tpd-games-dashboard.git

# Rename branch to main (if needed)
git branch -M main

# Push to GitHub
git push -u origin main
```

### Step 6: Verify Repository

1. Go to your GitHub repository page
2. Verify all files are present:
   - ✅ `preprocess_data_tpd.py`
   - ✅ `src/streamlit_dashboard_tpd.py`
   - ✅ `render_tpd.yaml`
   - ✅ `README_TPD.md`
   - ✅ `requirements.txt`
   - ✅ `tpd_conversion_funnel.csv` (if included)
   - ✅ `data/` directory with CSV files

### Step 7: Optional - Add GitHub Actions for Auto-Updates

Create `.github/workflows/update-tpd-data.yml`:

```yaml
name: Update TPD Dashboard Data

on:
  schedule:
    - cron: '0 2 * * *'  # Run daily at 2 AM UTC
  workflow_dispatch:  # Allow manual trigger

jobs:
  update-data:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install psycopg2-binary
      
      - name: Run preprocessing
        env:
          REDSHIFT_HOST: ${{ secrets.REDSHIFT_HOST }}
          REDSHIFT_DATABASE: ${{ secrets.REDSHIFT_DATABASE }}
          REDSHIFT_PORT: ${{ secrets.REDSHIFT_PORT }}
          REDSHIFT_USER: ${{ secrets.REDSHIFT_USER }}
          REDSHIFT_PASSWORD: ${{ secrets.REDSHIFT_PASSWORD }}
        run: |
          python preprocess_data_tpd.py --all
      
      - name: Commit and push changes
        run: |
          git config --local user.email "action@github.com"
          git config --local user.name "GitHub Action"
          git add data/*.csv
          git diff --staged --quiet || (git commit -m "Auto-update TPD dashboard data [skip ci]" && git push)
```

Add secrets in GitHub repository settings:
- Go to Settings → Secrets and variables → Actions
- Add: REDSHIFT_HOST, REDSHIFT_DATABASE, REDSHIFT_PORT, REDSHIFT_USER, REDSHIFT_PASSWORD

## Important Notes

1. **Do NOT include Hybrid Dashboard files** in the TPD repository
2. **Keep repositories separate** - TPD and Hybrid dashboards should be in different repos
3. **Data files can be large** - Consider using Git LFS for large CSV files
4. **Environment variables** - Never commit `.env` file, only `.env.template`

## File Checklist

Before pushing, ensure you have:
- [ ] `preprocess_data_tpd.py`
- [ ] `src/streamlit_dashboard_tpd.py`
- [ ] `render_tpd.yaml`
- [ ] `README_TPD.md`
- [ ] `requirements.txt`
- [ ] `.gitignore`
- [ ] `tpd_conversion_funnel.csv` (or ensure it's available)
- [ ] `data/` directory with processed CSV files
- [ ] `.env.template` (optional)

## Next Steps

After setting up the GitHub repository:
1. Proceed to Render deployment (see `RENDER_SETUP_TPD.md`)
2. Test the dashboard locally
3. Set up automated data updates (optional)
