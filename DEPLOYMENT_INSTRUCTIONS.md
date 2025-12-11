# Deployment Instructions - Question Correctness Graph

## ✅ Changes Made

1. **Removed duplicate Question Correctness sections** - Only one section now uses preprocessed data
2. **Added preprocessing for question correctness** - Uses three separate queries with hybrid_games/hybrid_game_links tables
3. **Updated dashboard to use preprocessed data** - No database queries in dashboard, only loads CSV files

## 📋 Deployment Steps

### Step 1: Run Preprocessing Locally

Run the preprocessing script to generate all data files including question correctness:

```bash
python preprocess_data.py
```

This will:
- Fetch data from the three queries for question correctness
- Process and calculate question correctness for all games
- Save to `data/question_correctness_data.csv`

**Expected output:**
```
Fetching question correctness data from three queries...
  Executing Query 1 (correctSelections games)...
  Query 1 returned X records
  Executing Query 2 (flow games)...
  Query 2 returned X records
  Executing Query 3 (action_level games)...
  Query 3 returned X records
SUCCESS: Processed X question correctness records
SUCCESS: Final question correctness data: X records
SUCCESS: Saved data/question_correctness_data.csv (rows: X)
```

### Step 2: Verify Data Files

Check that these files exist in the `data/` directory:
- ✅ `question_correctness_data.csv` (NEW - must be present!)
- ✅ `score_distribution_data.csv`
- ✅ `game_conversion_numbers.csv`
- ✅ `summary_data.csv`
- ✅ `time_series_data.csv`
- ✅ `repeatability_data.csv`
- ✅ `poll_responses_data.csv`
- ✅ `metadata.json`

### Step 3: Verify Game Names

Check that all games from your list are included in the CSV:

```bash
python -c "import pandas as pd; df = pd.read_csv('data/question_correctness_data.csv'); print(df['game_name'].unique())"
```

You should see all 23 games:
- Relational Comparison, Quantity Comparison, Relational Comparison II, Number Comparison
- Primary Emotion I, Primary Emotion II, Beginning Sound Pa Cha Sa
- Revision Primary Colors, Revision Primary Shapes, Rhyming Words
- Shape Circle, Shape Triangle, Shape Square, Shape Rectangle
- Color Red, Color Yellow, Color Blue
- Numeracy I, Numeracy II, Numerals 1-10
- Beginning Sound Ma Ka La, Beginning Sound Ba Ra Na

### Step 4: Test Locally

Test the dashboard locally to verify everything works:

```bash
streamlit run src/streamlit_dashboard.py --server.port 8501
```

**Verify:**
1. Only ONE "✅ Question Correctness by Question Number" section appears
2. The section loads without errors
3. Game selector shows all games
4. Graphs render correctly

### Step 5: Commit and Push to GitHub

```bash
# Add all files (including data/question_correctness_data.csv)
git add .

# Commit changes
git commit -m "Add Question Correctness preprocessing and visualization"

# Push to GitHub
git push origin main
```

### Step 6: Deploy to Render

The dashboard on Render will automatically:
1. Pull the latest code from GitHub
2. Load the preprocessed CSV files (including `question_correctness_data.csv`)
3. Display the Question Correctness graph without database queries

**No environment variables needed** - All data is preprocessed and in CSV files!

## 🔍 Troubleshooting

### Issue: "No question correctness data available" message
**Solution:** Make sure `data/question_correctness_data.csv` exists and was pushed to GitHub

### Issue: Games missing from the list
**Solution:** 
1. Check game names match exactly in `hybrid_games` table
2. Run `verify_games.py` to see actual game names
3. Update queries in `preprocess_data.py` with correct names
4. Re-run preprocessing

### Issue: Question Correctness section not appearing
**Solution:**
1. Check that CSV file exists and has data
2. Verify the CSV has columns: `game_name`, `question_number`, `correctness`, `percent`, `user_count`, `total_users`
3. Check dashboard logs for errors

## ✅ Verification Checklist

Before deploying:
- [ ] Ran `preprocess_data.py` successfully
- [ ] `data/question_correctness_data.csv` exists and has data
- [ ] All 23 games appear in the CSV
- [ ] Tested dashboard locally - only ONE Question Correctness section
- [ ] Dashboard loads without errors
- [ ] Graphs render correctly
- [ ] Committed and pushed all files to GitHub
- [ ] Render deployment successful

## 📊 Expected Results on Render

Once deployed, you should see:
- ✅ One "Question Correctness by Question Number" section
- ✅ Game selector dropdown with all 23 games
- ✅ Stacked bar chart showing % Correct vs % Incorrect per question
- ✅ One chart per selected game
- ✅ No database connection errors

---

**Note:** The dashboard now uses **only preprocessed CSV data** - no database queries at runtime on Render!




