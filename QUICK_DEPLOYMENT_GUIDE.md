# Quick Deployment Guide - Question Correctness Graph

## 🎯 Immediate Next Steps

### Step 1: Verify Game Names (CRITICAL - Do This First!)

Before deploying, **run the verification script** to ensure all game names match:

```bash
python verify_games.py
```

This will tell you:
- ✅ Which games are found in the database
- ❌ Which games are missing
- ⚠️  Which games have different names
- 🔍 Additional games you might want to include

### Step 2: Update Game Names (If Needed)

If the verification script finds missing games or name mismatches:

1. **Note the exact game names** from the database
2. **Update the queries** in `src/streamlit_dashboard.py`:
   - Lines ~1747-1752 (Query 1 - correctSelections games)
   - Lines ~1772-1776 (Query 2 - flow games)  
   - Lines ~1801-1814 (Query 3 - action_level games)
3. **Update the `get_game_type()` function** (lines ~727-758) if needed

### Step 3: Test Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run src/streamlit_dashboard.py --server.port 8501
```

Navigate to the **"✅ Question Correctness by Question Number"** section and verify:
- All games appear in the filter dropdown
- Graphs render correctly
- No errors in the console

### Step 4: Deploy to Render

#### Option A: Manual Deployment

1. **Push code to GitHub:**
   ```bash
   git add .
   git commit -m "Add Question Correctness visualization"
   git push
   ```

2. **In Render Dashboard:**
   - Go to your Web Service
   - Click "Manual Deploy" → "Deploy latest commit"
   - Or wait for auto-deploy

#### Option B: First-Time Setup

1. **Create new Web Service in Render:**
   - Connect GitHub repository
   - Name: `hybrid-dashboard`
   - Environment: `Python 3`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `streamlit run src/streamlit_dashboard.py --server.port $PORT --server.address 0.0.0.0`

2. **Set Environment Variables:**
   - `DB_HOST` = your database host
   - `DB_PORT` = 3306 (or your port)
   - `DB_USER` = your database username
   - `DB_PASSWORD` = your database password
   - `DB_NAME` = your database name

3. **Deploy!**

### Step 5: Verify on Render

After deployment:
1. Open your Render dashboard URL
2. Scroll to "✅ Question Correctness by Question Number"
3. Check that:
   - ✅ Section loads without errors
   - ✅ Game filter dropdown appears
   - ✅ All games are listed
   - ✅ Graphs render properly
   - ✅ Summary statistics table appears

## 📋 Complete Game List

### correctSelections Games (8 games):
- Relational Comparison
- Quantity Comparison
- Relational Comparison II
- Number Comparison
- Primary Emotion I
- Primary Emotion II
- Beginning Sound Pa Cha Sa

### flow Games (3 games):
- Revision Primary Colors
- Revision Primary Shapes
- Rhyming Words

### action_level Games (13 games):
- Shape Circle
- Shape Triangle
- Shape Square
- Shape Rectangle
- Color Red
- Color Yellow
- Color Blue
- Numeracy I
- Numeracy II
- Numerals 1-10
- Beginning Sound Ma Ka La
- Beginning Sound Ba Ra Na

**Total: 24 games**

## 🔍 Troubleshooting

### Issue: No games appear in filter
**Solution:** Check that:
- Database connection is working
- Game names match exactly between queries and `hybrid_games` table
- `hybrid_game_links` table has mappings for all games

### Issue: Graphs don't render
**Solution:** Check Render logs for:
- SQL query errors
- JSON parsing errors
- Missing data errors

### Issue: Missing games
**Solution:**
1. Run `verify_games.py` to see actual game names
2. Update queries with correct names
3. Redeploy

## ✅ Checklist Before Deploying

- [ ] Run `verify_games.py` and fix any missing games
- [ ] Test dashboard locally - verify Question Correctness section works
- [ ] Ensure `requirements.txt` is up to date
- [ ] Check environment variables are set in Render
- [ ] Push latest code to GitHub
- [ ] Deploy to Render
- [ ] Verify dashboard works on Render

---

**Need help?** Check `DEPLOYMENT_STEPS.md` for detailed instructions.




