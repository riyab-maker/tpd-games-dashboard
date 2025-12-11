# Deployment Steps for Question Correctness Graph on Render

## ✅ Step 1: Verify All Games Are Included

The following games should be present in the `hybrid_games` table:

### correctSelections Games (Query 1):
- ✅ Relational Comparison
- ✅ Quantity Comparison  
- ✅ Relational Comparison II
- ✅ Number Comparison
- ✅ Primary Emotion I (may be "Emotion Identification" or "Primary Emotion Labelling" in DB)
- ✅ Primary Emotion II (may be "Identification of all emotions" in DB)
- ✅ Beginning Sound Pa Cha Sa

### flow Games (Query 2):
- ✅ Revision Primary Colors (may be "Revision Colors" in DB)
- ✅ Revision Primary Shapes (may be "Revision Shapes" in DB)
- ✅ Rhyming Words

### action_level Games (Query 3):
- ✅ Shape Circle
- ✅ Shape Triangle
- ✅ Shape Square
- ✅ Shape Rectangle
- ✅ Color Red
- ✅ Color Yellow
- ✅ Color Blue
- ✅ Numeracy I (may be "Numbers I" in DB)
- ✅ Numeracy II (may be "Numbers II" in DB)
- ✅ Numerals 1-10
- ✅ Beginning Sound Ma Ka La
- ✅ Beginning Sound Ba Ra Na

## 🔍 Step 2: Verify Game Names in Database

Run this query to check actual game names in your database:

```sql
SELECT DISTINCT game_name 
FROM hybrid_games 
WHERE game_name IN (
  'Relational Comparison',
  'Quantity Comparison',
  'Relational Comparison II',
  'Number Comparison',
  'Primary Emotion I',
  'Primary Emotion II',
  'Beginning Sound Pa Cha Sa',
  'Revision Primary Colors',
  'Revision Primary Shapes',
  'Rhyming Words',
  'Shape Circle',
  'Shape Triangle',
  'Shape Square',
  'Shape Rectangle',
  'Color Red',
  'Color Yellow',
  'Color Blue',
  'Numeracy I',
  'Numeracy II',
  'Numerals 1-10',
  'Beginning Sound Ma Ka La',
  'Beginning Sound Ba Ra Na'
)
ORDER BY game_name;
```

**If any game names don't match**, update them in the queries in `src/streamlit_dashboard.py`.

## 🧪 Step 3: Test Locally

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Test the dashboard locally:**
   ```bash
   streamlit run src/streamlit_dashboard.py --server.port 8501
   ```

3. **Verify the Question Correctness section:**
   - Navigate to the "✅ Question Correctness by Question Number" section
   - Check that all games appear in the dropdown
   - Verify the graph displays correctly

## 🚀 Step 4: Deploy to Render

### 4.1 Prepare for Deployment

1. **Ensure requirements.txt is up to date:**
   ```bash
   pip freeze > requirements.txt
   ```
   Make sure it includes:
   - streamlit
   - pandas
   - pymysql
   - altair

2. **Create/Update render.yaml** (if using Render Blueprint):
   ```yaml
   services:
     - type: web
       name: hybrid-dashboard
       env: python
       buildCommand: pip install -r requirements.txt
       startCommand: streamlit run src/streamlit_dashboard.py --server.port $PORT --server.address 0.0.0.0
       envVars:
         - key: DB_HOST
           sync: false
         - key: DB_PORT
           sync: false
         - key: DB_USER
           sync: false
         - key: DB_PASSWORD
           sync: false
         - key: DB_NAME
           sync: false
   ```

### 4.2 Configure Environment Variables on Render

Set these environment variables in Render dashboard:
- `DB_HOST` - Your database host
- `DB_PORT` - Your database port (usually 3306)
- `DB_USER` - Your database username
- `DB_PASSWORD` - Your database password
- `DB_NAME` - Your database name

### 4.3 Deploy

1. **Connect your GitHub repository** to Render
2. **Create a new Web Service**:
   - Select your repository
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `streamlit run src/streamlit_dashboard.py --server.port $PORT --server.address 0.0.0.0`
   - Environment: Python 3

3. **Wait for deployment** to complete

### 4.4 Verify Deployment

1. **Access your dashboard** at the Render URL
2. **Navigate to Question Correctness section**
3. **Check that:**
   - All games appear in the filter
   - Graphs render correctly
   - Data loads successfully

## 🔧 Step 5: Troubleshooting

### If games don't appear:
1. Check database connection is working
2. Verify game names match exactly in `hybrid_games` table
3. Check that `hybrid_game_links` table has correct mappings
4. Review Render logs for SQL errors

### If graphs don't render:
1. Check that query results have data
2. Verify JSON parsing functions are working
3. Check Render logs for Python errors

### If data is missing:
1. Verify date filters (server_time >= '2025-07-01' for Query 3)
2. Check that game_completed events exist for Query 1 & 2
3. Verify action_level events exist for Query 3

## 📊 Step 6: Expected Output

Once deployed, you should see:
- A filter dropdown with all games listed
- Stacked bar charts showing % Correct vs % Incorrect
- One chart per game (faceted)
- Summary statistics table below the charts

---

**Note:** Make sure the game names in the SQL queries match EXACTLY with the game names in your `hybrid_games` table. Case sensitivity matters!




