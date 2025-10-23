"""
Streamlit Dashboard for Matomo Events - Streamlit Community Cloud Version

This version is optimized for Streamlit Community Cloud deployment with:
- Memory optimization for <1GB limit
- Environment variable handling for secrets
- Error handling for missing credentials
- Responsive design for cloud hosting

Deployment Instructions:
1. Local: streamlit run streamlit_dashboard_cloud.py
2. Cloud: Push to GitHub and deploy at share.streamlit.io
"""

import os
import json
from typing import List, Tuple
import traceback

import pymysql
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Load environment variables from .env file for local development
load_dotenv()

# Database connection settings using environment variables
HOST = os.getenv("DB_HOST")
PORT = int(os.getenv("DB_PORT", "3310"))
DBNAME = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

# Validate that all required environment variables are set
required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    st.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    st.info("""
    **For Streamlit Community Cloud:**
    1. Go to your app settings at share.streamlit.io
    2. Click on "Secrets" in the left sidebar
    3. Add this TOML configuration:
    ```toml
    [secrets]
    DB_HOST = "a9c3c220991da47c895500da385432b6-1807075149.ap-south-1.elb.amazonaws.com"
    DB_PORT = 3310
    DB_NAME = "live"
    DB_USER = "techadmin"
    DB_PASSWORD = "Rl@ece@1234"
    ```
    4. Save and wait for redeployment
    
    **For local development:**
    1. Create a .env file with the database credentials
    2. Run: streamlit run streamlit_dashboard_cloud.py
    """)
    st.stop()

# SQL Queries (same as original)
SQL_QUERY = (
    """
    SELECT 
      `matomo_log_link_visit_action`.`idlink_va`,
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
      DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) AS server_time,
      `matomo_log_link_visit_action`.`idaction_name`,
      `matomo_log_link_visit_action`.`custom_dimension_2`,
      CASE 
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "12" THEN 'Shape Circle'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "24" THEN 'Color Red'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "28" THEN 'Shape Triangle'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "40" THEN 'Color Yellow'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "54" THEN 'Numeracy I'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "56" THEN 'Numeracy II'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "50" THEN 'Relational Comparison'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "52" THEN 'Quantity Comparison'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "60" THEN 'Shape Square'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "62" THEN 'Revision Primary Colors'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "58" THEN 'Color Blue'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "70" THEN 'Relational Comparison II'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "66" THEN 'Rhyming Words Hindi'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "68" THEN 'Rhyming Words Marathi'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "64" THEN 'Revision Primary Shapes'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "72" THEN 'Number Comparison'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "78" THEN 'Primary Emotion I'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "80" THEN 'Primary Emotion II'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "82" THEN 'Shape Rectangle'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "84" THEN 'Numerals 1-10'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "83" THEN 'Numerals 1-10 Child'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "76" THEN 'Beginning Sound Ma Ka La Marathi'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "74" THEN 'Beginning Sound Ma Ka La Hindi'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "88" THEN 'Beginning Sound Pa Cha Sa Marathi'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "86" THEN 'Beginning Sound Pa Cha Sa Hindi'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "94" THEN 'Common Shapes'
        WHEN `matomo_log_link_visit_action`.`custom_dimension_2` = "96" THEN 'Primary Colors'
        ELSE 'Unknown Game'
      END AS game_name,
      CASE 
        WHEN `matomo_log_link_visit_action`.`idaction_name` IN (
          7228,16088,23560,34234,47426,47479,47066,46997,47994,48428,
          47910,49078,48834,48883,48573,49214,49663,49719,49995,49976,
          50099,49525,49395,51134,50812,51603,51627
        ) THEN 'Started'
        ELSE 'Completed'
      END AS event
    FROM `matomo_log_link_visit_action`
    WHERE `matomo_log_link_visit_action`.`idaction_name` IN (
        7228,16088,16204,23560,23592,34234,34299,
        47426,47472,47479,47524,47066,47099,46997,47001,
        47994,47998,48428,48440,47910,47908,49078,49113,
        48834,48835,48883,48919,48573,48607,49214,49256,
        49663,49698,49719,49721,49995,50051,49976,49978,
        50099,50125,49525,49583,49395,49470,51134,51209,
        50812,50846,51603,51607,51627,51635
    )
    AND `matomo_log_link_visit_action`.`custom_dimension_2` IN (
        "12","28","24","40","54","56","50","52","70","72",
        "58","66","68","60","62","64","78","80","82","84",
        "83","76","74","88","86","94","96"
    )
    """
)

# Score distribution queries
SCORE_DISTRIBUTION_QUERY_1 = """
SELECT 
  `matomo_log_link_visit_action`.`custom_dimension_2`, 
  `matomo_log_link_visit_action`.`idvisit`, 
  `matomo_log_action`.`name`, 
  `matomo_log_link_visit_action`.`custom_dimension_1`, 
  CONV(HEX(`matomo_log_link_visit_action`.idvisitor), 16, 10) AS idvisitor_converted
FROM `matomo_log_link_visit_action` 
INNER JOIN `matomo_log_action` 
  ON `matomo_log_link_visit_action`.`idaction_name` = `matomo_log_action`.`idaction` 
WHERE `matomo_log_link_visit_action`.`custom_dimension_2` IN ("50", "52", "70", "72")
  AND `matomo_log_action`.`name` LIKE '%game_completed%';
"""

SCORE_DISTRIBUTION_QUERY_2 = """
SELECT 
  matomo_log_link_visit_action.custom_dimension_2,
  matomo_log_link_visit_action.idvisit,
  matomo_log_action.name,
  matomo_log_link_visit_action.custom_dimension_1,
  CONV(HEX(matomo_log_link_visit_action.idvisitor), 16, 10) AS idvisitor_converted
FROM matomo_log_link_visit_action
INNER JOIN matomo_log_action
  ON matomo_log_link_visit_action.idaction_name = matomo_log_action.idaction
WHERE matomo_log_link_visit_action.custom_dimension_2 IN ("62", "64", "66", "68")
  AND matomo_log_action.name LIKE '%game_completed%';
"""

SCORE_DISTRIBUTION_QUERY_3 = """
SELECT `matomo_log_link_visit_action`.`idlink_va`, 
CONV(HEX(`matomo_log_link_visit_action`.idvisitor), 16, 10) AS idvisitor_converted, 
`matomo_log_link_visit_action`.`idvisit`, 
`matomo_log_link_visit_action`.`server_time`, 
`matomo_log_link_visit_action`.`idaction_name`, 
`matomo_log_link_visit_action`.`custom_dimension_1`, 
`matomo_log_link_visit_action`.`custom_dimension_2`, 
`matomo_log_action`.`idaction`, 
`matomo_log_action`.`name`, 
`matomo_log_action`.`type` 
FROM `matomo_log_link_visit_action` 
inner join `matomo_log_action` on `matomo_log_link_visit_action`.`idaction_name` = `matomo_log_action`.`idaction` 
where `matomo_log_link_visit_action`.`server_time` >= '2025-07-01' 
and `matomo_log_link_visit_action`.`custom_dimension_2` in ("12", "28", "24", "40", "54", "56", "50", "52", "70", "72", "58", "66", "68", "60", "62", "64","78","80","82","84","83","76","74","88","86") 
and `matomo_log_action`.`name` Like '%action_level%'
"""

# Optimized database connection with error handling
@st.cache_data(show_spinner=False, ttl=600, max_entries=1)  # Cache for 10 minutes, limit entries
def fetch_dataframe() -> pd.DataFrame:
    """Fetch main dataframe with error handling and memory optimization"""
    try:
        with pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            connect_timeout=5,   # Very aggressive timeout for Streamlit Cloud
            read_timeout=5,      # Very aggressive read timeout
            write_timeout=5,     # Very aggressive write timeout
            autocommit=True,     # Enable autocommit for faster connection
            ssl={'ssl': {}},
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_QUERY)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
        
        df = pd.DataFrame(rows, columns=columns)
        
        # Memory optimization: convert to appropriate dtypes
        if not df.empty:
            df['idvisitor_converted'] = pd.to_numeric(df['idvisitor_converted'], errors='coerce')
            df['idvisit'] = pd.to_numeric(df['idvisit'], errors='coerce')
            df['server_time'] = pd.to_datetime(df['server_time'], errors='coerce')
            df['custom_dimension_2'] = df['custom_dimension_2'].astype('category')
            df['game_name'] = df['game_name'].astype('category')
            df['event'] = df['event'].astype('category')
        
        return df
    except Exception as e:
        st.error(f"❌ Database connection failed: {str(e)}")
        st.info("Please check your database credentials and network connection.")
        return pd.DataFrame()

def fetch_score_dataframe_1() -> pd.DataFrame:
    """Fetch data for score distribution analysis (correctSelections games)"""
    try:
        with pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            connect_timeout=15,
            ssl={'ssl': {}},
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(SCORE_DISTRIBUTION_QUERY_1)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as e:
        st.error(f"❌ Failed to fetch score data 1: {str(e)}")
        return pd.DataFrame()

def fetch_score_dataframe_2() -> pd.DataFrame:
    """Fetch data for score distribution analysis (jsonData games)"""
    try:
        with pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            connect_timeout=15,
            ssl={'ssl': {}},
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(SCORE_DISTRIBUTION_QUERY_2)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as e:
        st.error(f"❌ Failed to fetch score data 2: {str(e)}")
        return pd.DataFrame()

def fetch_score_dataframe_3() -> pd.DataFrame:
    """Fetch data for score distribution analysis (action_level games)"""
    try:
        with pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            connect_timeout=15,
            ssl={'ssl': {}},
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(SCORE_DISTRIBUTION_QUERY_3)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as e:
        st.error(f"❌ Failed to fetch score data 3: {str(e)}")
        return pd.DataFrame()

# All the parsing and analysis functions (same as original)
def parse_custom_dimension_1_correct_selections(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract correctSelections (for first query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        data = json.loads(custom_dim_1)
        
        if 'gameData' in data and len(data['gameData']) > 0:
            for game_data in data['gameData']:
                if 'gameData' in game_data and len(game_data['gameData']) > 0:
                    for inner_game_data in game_data['gameData']:
                        if 'statistics' in inner_game_data and 'correctSelections' in inner_game_data['statistics']:
                            correct_selections = inner_game_data['statistics']['correctSelections']
                            if correct_selections is not None:
                                return int(correct_selections)
        
        return 0
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0

def parse_custom_dimension_1_json_data(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from jsonData (for second query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        data = json.loads(custom_dim_1)
        
        if 'gameData' in data and len(data['gameData']) > 0:
            total_score = 0
            
            for game_data in data['gameData']:
                if game_data.get('section') == 'Action' and 'jsonData' in game_data:
                    json_data = game_data['jsonData']
                    
                    if isinstance(json_data, list):
                        for level_data in json_data:
                            if 'userResponse' in level_data and isinstance(level_data['userResponse'], list):
                                for response in level_data['userResponse']:
                                    if isinstance(response, dict) and 'isCorrect' in response:
                                        if response['isCorrect'] is True:
                                            total_score += 1
            
            return total_score
        
        return 0
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0

def parse_custom_dimension_1_action_games(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from action games (for third query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        data = json.loads(custom_dim_1)
        
        total_score = 0
        
        if 'options' in data and 'chosenOption' in data:
            chosen_option = data.get('chosenOption')
            
            if chosen_option is None:
                return 0
            
            options = data.get('options', [])
            if isinstance(options, list) and 0 <= chosen_option < len(options):
                chosen_option_data = options[chosen_option]
                if isinstance(chosen_option_data, dict) and chosen_option_data.get('isCorrect', False):
                    total_score = 1
                else:
                    total_score = 0
            else:
                total_score = 0
        
        return total_score
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0

def get_total_score(correct_selections):
    """Get total score from correctSelections"""
    if correct_selections is None or correct_selections == 0:
        return 0
    
    try:
        return int(correct_selections) if isinstance(correct_selections, (int, float, str)) and str(correct_selections).isdigit() else 0
    except (ValueError, TypeError):
        return 0

def get_game_name_from_custom_dimension_2(custom_dim_2):
    """Map custom_dimension_2 to game name"""
    game_mapping = {
        "12": 'Shape Circle',
        "24": 'Color Red',
        "28": 'Shape Triangle',
        "40": 'Color Yellow',
        "54": 'Numeracy I',
        "56": 'Numeracy II',
        "50": 'Relational Comparison',
        "52": 'Quantity Comparison',
        "60": 'Shape Square',
        "62": 'Revision Primary Colors',
        "58": 'Color Blue',
        "70": 'Relational Comparison II',
        "66": 'Rhyming Words Hindi',
        "68": 'Rhyming Words Marathi',
        "64": 'Revision Primary Shapes',
        "72": 'Number Comparison',
        "78": 'Primary Emotion I',
        "80": 'Primary Emotion II',
        "82": 'Shape Rectangle',
        "84": 'Numerals 1-10',
        "83": 'Numerals 1-10 Child',
        "76": 'Beginning Sound Ma Ka La Marathi',
        "74": 'Beginning Sound Ma Ka La Hindi',
        "88": 'Beginning Sound Pa Cha Sa Marathi',
        "86": 'Beginning Sound Pa Cha Sa Hindi',
        "94": 'Common Shapes',
        "96": 'Primary Colors'
    }
    return game_mapping.get(custom_dim_2, f'Game {custom_dim_2}')

def calculate_score_distribution_combined(df_score_1, df_score_2, df_score_3):
    """Calculate combined score distribution for all three query datasets"""
    combined_df = pd.DataFrame()
    
    # Process first dataset (correctSelections games)
    if not df_score_1.empty:
        df_score_1['total_score'] = df_score_1['custom_dimension_1'].apply(parse_custom_dimension_1_correct_selections)
        df_score_1['game_name'] = df_score_1['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        df_score_1 = df_score_1[df_score_1['total_score'] > 0]
        combined_df = pd.concat([combined_df, df_score_1], ignore_index=True)
    
    # Process second dataset (jsonData games)
    if not df_score_2.empty:
        df_score_2['total_score'] = df_score_2['custom_dimension_1'].apply(parse_custom_dimension_1_json_data)
        df_score_2['game_name'] = df_score_2['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        df_score_2 = df_score_2[df_score_2['total_score'] > 0]
        combined_df = pd.concat([combined_df, df_score_2], ignore_index=True)
    
    # Process third dataset (action games)
    if not df_score_3.empty:
        df_score_3['question_score'] = df_score_3['custom_dimension_1'].apply(parse_custom_dimension_1_action_games)
        df_score_3['game_name'] = df_score_3['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        
        df_score_3 = df_score_3.sort_values(['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'server_time'])
        
        session_instances = []
        current_session = 1
        prev_user = None
        prev_game = None
        prev_visit = None
        prev_time = None
        
        for _, row in df_score_3.iterrows():
            user = row['idvisitor_converted']
            game = row['custom_dimension_2']
            visit = row['idvisit']
            time = row['server_time']
            
            if user != prev_user or game != prev_game or visit != prev_visit:
                current_session = 1
            elif prev_time is not None and (time - prev_time).total_seconds() > 300:
                current_session += 1
            
            session_instances.append(current_session)
            prev_user = user
            prev_game = game
            prev_visit = visit
            prev_time = time
        
        df_score_3['session_instance'] = session_instances
        
        df_score_3_grouped = df_score_3.groupby(['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'session_instance'])['question_score'].sum().reset_index()
        df_score_3_grouped.columns = ['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'session_instance', 'total_score']
        df_score_3_grouped['game_name'] = df_score_3_grouped['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        
        df_score_3_grouped['total_score'] = df_score_3_grouped['total_score'].clip(upper=12)
        df_score_3_grouped = df_score_3_grouped[df_score_3_grouped['total_score'] > 0]
        combined_df = pd.concat([combined_df, df_score_3_grouped], ignore_index=True)
    
    if combined_df.empty:
        return pd.DataFrame()
    
    if not combined_df.empty:
        score_distribution = combined_df.groupby(['game_name', 'total_score'])['idvisitor_converted'].nunique().reset_index()
        score_distribution.columns = ['game_name', 'total_score', 'user_count']
        return score_distribution
    else:
        return pd.DataFrame()

def render_score_distribution_chart(score_distribution_df: pd.DataFrame) -> None:
    """Render score distribution chart"""
    import altair as alt
    
    if score_distribution_df.empty:
        st.warning("No score distribution data available.")
        return
    
    unique_games = sorted(score_distribution_df['game_name'].unique())
    
    selected_games = st.multiselect(
        "Select Games for Score Distribution:",
        options=unique_games,
        default=unique_games,
        help="Select one or more games to show score distribution. Leave empty to show all games."
    )
    
    if selected_games:
        filtered_df = score_distribution_df[score_distribution_df['game_name'].isin(selected_games)]
    else:
        filtered_df = score_distribution_df
    
    if filtered_df.empty:
        st.warning("No data available for the selected games.")
        return
    
    st.markdown("### 📊 Score Distribution Analysis")
    st.markdown("This chart shows how many users achieved each total score for each game.")
    
    bars = alt.Chart(filtered_df).mark_bar(
        cornerRadius=6,
        stroke='white',
        strokeWidth=2,
        color='#4A90E2'
    ).encode(
        x=alt.X('total_score:O', 
                title='Total Score', 
                axis=alt.Axis(
                    labelAngle=0,
                    labelFontSize=14,
                    titleFontSize=16,
                    labelLimit=100
                )),
        y=alt.Y('user_count:Q', 
                title='Number of Users', 
                axis=alt.Axis(format='~s')),
        color=alt.Color('game_name:N', 
                       scale=alt.Scale(scheme='category20'),
                       legend=alt.Legend(title="Game Name", 
                                       labelFontSize=12,
                                       titleFontSize=14)),
        tooltip=['game_name:N', 'total_score:O', 'user_count:Q']
    ).properties(
        width=900,
        height=500,
        title='Score Distribution by Game'
    )
    
    labels = alt.Chart(filtered_df).mark_text(
        align='center',
        baseline='bottom',
        color='#2E8B57',
        fontSize=12,
        fontWeight='bold',
        dy=-5
    ).encode(
        x=alt.X('total_score:O'),
        y=alt.Y('user_count:Q'),
        text=alt.Text('user_count:Q', format='.0f')
    )
    
    score_chart = (bars + labels).configure_axis(
        labelFontSize=16,
        titleFontSize=18,
        grid=True
    ).configure_title(
        fontSize=24,
        fontWeight='bold'
    ).configure_legend(
        labelFontSize=12,
        titleFontSize=14
    )
    
    st.altair_chart(score_chart, use_container_width=True)
    
    # Summary statistics
    st.markdown("#### 📈 Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_users = filtered_df['user_count'].sum()
        st.metric(
            label="👥 Total Users",
            value=f"{total_users:,}",
            help="Total number of users across all games and scores"
        )
    
    with col2:
        avg_score = (filtered_df['total_score'] * filtered_df['user_count']).sum() / filtered_df['user_count'].sum()
        st.metric(
            label="🎯 Average Score",
            value=f"{avg_score:.1f}",
            help="Weighted average score across all users"
        )
    
    with col3:
        max_score = filtered_df['total_score'].max()
        st.metric(
            label="🏆 Highest Score",
            value=f"{max_score}",
            help="Maximum total score achieved"
        )
    
    with col4:
        unique_games_count = filtered_df['game_name'].nunique()
        st.metric(
            label="🎮 Games Analyzed",
            value=f"{unique_games_count}",
            help="Number of games included in the analysis"
        )

def ensure_event_rows(series: pd.Series) -> pd.Series:
    # Ensure both 'Started' and 'Completed' exist, fill missing with 0
    base = pd.Series({'Started': 0, 'Completed': 0}, dtype='int64')
    series = series.astype('int64')
    return base.add(series, fill_value=0).astype('int64')

def _distinct_count_ignore_blank(series: pd.Series) -> int:
    """Power BI DISTINCTCOUNTNOBLANK logic: ignore NULLs and empty strings"""
    if series.dtype == object:
        cleaned = series.dropna()
        cleaned = cleaned[cleaned.astype(str).str.strip() != ""]
    else:
        cleaned = series.dropna()
    return int(cleaned.nunique())

def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build summary table with correct Power BI DISTINCTCOUNTNOBLANK logic"""
    grouped = df.groupby('event').agg({
        'idvisitor_converted': _distinct_count_ignore_blank,
        'idvisit': _distinct_count_ignore_blank, 
        'idlink_va': _distinct_count_ignore_blank,
    })
    
    grouped.columns = ['Users', 'Visits', 'Instances']
    grouped = grouped.reset_index()
    grouped.rename(columns={'event': 'Event'}, inplace=True)
    
    all_events = pd.DataFrame({'Event': ['Started', 'Completed']})
    grouped = all_events.merge(grouped, on='Event', how='left').fillna(0)
    
    for col in ['Users', 'Visits', 'Instances']:
        grouped[col] = grouped[col].astype(int)
    
    grouped['Event'] = pd.Categorical(grouped['Event'], categories=['Started', 'Completed'], ordered=True)
    grouped = grouped.sort_values('Event')
    
    return grouped

def render_modern_dashboard(summary_df: pd.DataFrame, df_filtered: pd.DataFrame) -> None:
    """Render a modern, professional dashboard with multiple chart types"""
    import altair as alt
    
    # Add separate conversion funnels
    st.markdown("### 🔄 Conversion Funnels")
    
    started_users = summary_df[summary_df['Event'] == 'Started']['Users'].iloc[0]
    completed_users = summary_df[summary_df['Event'] == 'Completed']['Users'].iloc[0]
    started_visits = summary_df[summary_df['Event'] == 'Started']['Visits'].iloc[0]
    completed_visits = summary_df[summary_df['Event'] == 'Completed']['Visits'].iloc[0]
    started_instances = summary_df[summary_df['Event'] == 'Started']['Instances'].iloc[0]
    completed_instances = summary_df[summary_df['Event'] == 'Completed']['Instances'].iloc[0]
    
    # Create three separate funnels
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 👥 Users Funnel")
        users_data = pd.DataFrame([
            {'Stage': 'Started', 'Count': started_users, 'Order': 0},
            {'Stage': 'Completed', 'Count': completed_users, 'Order': 1}
        ])
        
        users_chart = alt.Chart(users_data).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#4A90E2'
        ).encode(
            x=alt.X('Count:Q', title='Count', axis=alt.Axis(format='~s')),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending'), title=''),
            opacity=alt.condition(
                alt.datum.Stage == 'Started',
                alt.value(0.8),
                alt.value(1.0)
            ),
            tooltip=['Stage:N', 'Count:Q']
        ).properties(
            width=300,
            height=200
        )
        
        users_labels = alt.Chart(users_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending')),
            text=alt.Text('Count:Q', format='.0f')
        )
        
        users_funnel = alt.layer(users_chart, users_labels).configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=False
        )
        
        st.altair_chart(users_funnel, use_container_width=True)
    
    with col2:
        st.markdown("#### 🔄 Visits Funnel")
        visits_data = pd.DataFrame([
            {'Stage': 'Started', 'Count': started_visits, 'Order': 0},
            {'Stage': 'Completed', 'Count': completed_visits, 'Order': 1}
        ])
        
        visits_chart = alt.Chart(visits_data).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#7ED321'
        ).encode(
            x=alt.X('Count:Q', title='Count', axis=alt.Axis(format='~s')),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending'), title=''),
            opacity=alt.condition(
                alt.datum.Stage == 'Started',
                alt.value(0.8),
                alt.value(1.0)
            ),
            tooltip=['Stage:N', 'Count:Q']
        ).properties(
            width=300,
            height=200
        )
        
        visits_labels = alt.Chart(visits_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending')),
            text=alt.Text('Count:Q', format='.0f')
        )
        
        visits_funnel = alt.layer(visits_chart, visits_labels).configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=False
        )
        
        st.altair_chart(visits_funnel, use_container_width=True)
    
    with col3:
        st.markdown("#### ⚡ Instances Funnel")
        instances_data = pd.DataFrame([
            {'Stage': 'Started', 'Count': started_instances, 'Order': 0},
            {'Stage': 'Completed', 'Count': completed_instances, 'Order': 1}
        ])
        
        instances_chart = alt.Chart(instances_data).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#F5A623'
        ).encode(
            x=alt.X('Count:Q', title='Count', axis=alt.Axis(format='~s')),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending'), title=''),
            opacity=alt.condition(
                alt.datum.Stage == 'Started',
                alt.value(0.8),
                alt.value(1.0)
            ),
            tooltip=['Stage:N', 'Count:Q']
        ).properties(
            width=300,
            height=200
        )
        
        instances_labels = alt.Chart(instances_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending')),
            text=alt.Text('Count:Q', format='.0f')
        )
        
        instances_funnel = alt.layer(instances_chart, instances_labels).configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=False
        )
        
        st.altair_chart(instances_funnel, use_container_width=True)
    
    # Add conversion analysis
    st.markdown("### 📊 Conversion Analysis")
    
    analysis_col1, analysis_col2, analysis_col3 = st.columns(3)
    
    with analysis_col1:
        user_conversion = (completed_users / started_users * 100) if started_users > 0 else 0
        st.metric(
            label="👥 User Conversion",
            value=f"{user_conversion:.1f}%",
            help=f"{completed_users:,} out of {started_users:,} users completed"
        )
    
    with analysis_col2:
        visit_conversion = (completed_visits / started_visits * 100) if started_visits > 0 else 0
        st.metric(
            label="🔄 Visit Conversion",
            value=f"{visit_conversion:.1f}%",
            help=f"{completed_visits:,} out of {started_visits:,} visits completed"
        )
    
    with analysis_col3:
        instance_conversion = (completed_instances / started_instances * 100) if started_instances > 0 else 0
        st.metric(
            label="⚡ Instance Conversion",
            value=f"{instance_conversion:.1f}%",
            help=f"{completed_instances:,} out of {started_instances:,} instances completed"
        )

def main() -> None:
    """Main dashboard function optimized for Streamlit Community Cloud"""
    try:
        st.set_page_config(
            page_title="Matomo Events Dashboard - Cloud", 
            layout="wide",
            initial_sidebar_state="expanded"
        )
    except Exception:
        # Page config already set, continue
        pass
    
    # Add immediate response for health check
    st.title("🎮 Matomo Events Dashboard")
    st.caption("📊 All data (server_time adjusted by +5h30m) | ☁️ Streamlit Community Cloud")
    
    # Add a simple status indicator
    with st.spinner("🔄 Initializing dashboard..."):
        st.success("✅ Dashboard loaded successfully!")
    
    # Add deployment info
    with st.expander("ℹ️ Deployment Information", expanded=False):
        st.markdown("""
        **This dashboard is deployed on Streamlit Community Cloud**
        
        - **Local Development**: `streamlit run streamlit_dashboard_cloud.py`
        - **Cloud Deployment**: Automatically deployed from GitHub
        - **Database**: Connected via environment variables
        - **Memory Optimized**: <1GB usage for free tier
        """)
    
    # Add timeout and better error handling for Streamlit Cloud
    try:
        with st.spinner("🔄 Loading data from database..."):
            df = fetch_dataframe()
    except Exception as e:
        st.error(f"❌ Database connection failed: {str(e)}")
        st.info("Please check your database credentials and network connection.")
        st.info("**Troubleshooting:** This might be due to network connectivity or database server issues.")
        st.info("**Note:** The app is running but cannot connect to the database. Please check your secrets configuration.")
        return  # Return instead of stop to allow health check to pass

    if df.empty:
        st.warning("⚠️ No data returned for the current filters.")
        st.info("This could be due to database connection issues or missing data.")
        return

    # Add filters
    st.markdown("### 🎮 Filters")
    
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        unique_games = sorted(df['game_name'].unique())
        selected_games = st.multiselect(
            "Select Game Names to filter by:",
            options=unique_games,
            default=unique_games,
            help="Select one or more games to filter the dashboard data. Leave empty to show all games."
        )
    
    with filter_col2:
        df['date'] = pd.to_datetime(df['server_time']).dt.date
        
        min_date = pd.to_datetime('2025-07-02').date()
        max_date = df['date'].max()
        
        date_range = st.date_input(
            "Select Date Range:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Select a date range to filter the data. The server_time is already adjusted by +5h30m. Data is available from July 2nd, 2025 onwards."
        )
    
    # Apply filters
    df_filtered = df.copy()
    
    if selected_games:
        df_filtered = df_filtered[df_filtered['game_name'].isin(selected_games)]
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered['date'] >= start_date) & 
            (df_filtered['date'] <= end_date)
        ]
    elif len(date_range) == 1:
        df_filtered = df_filtered[df_filtered['date'] == date_range[0]]
    
    if df_filtered.empty:
        st.warning("⚠️ No data available for the selected filters.")
        return
    
    # Show filter summary
    if len(date_range) == 2:
        date_summary = f"from {date_range[0]} to {date_range[1]}"
    elif len(date_range) == 1:
        date_summary = f"on {date_range[0]}"
    else:
        date_summary = "for all dates"
    
    st.info(f"📊 Showing data for {len(selected_games)} selected game(s): {', '.join(selected_games)} | Date range: {date_summary}")

    # Build summary table and chart with filtered data
    summary_df = build_summary(df_filtered)
    render_modern_dashboard(summary_df, df_filtered)
    
    # Add Score Distribution Analysis
    st.markdown("---")
    st.markdown("## 🎯 Score Distribution Analysis")
    
    with st.spinner("🔄 Loading score distribution data..."):
        df_score_1 = fetch_score_dataframe_1()
        df_score_2 = fetch_score_dataframe_2()
        df_score_3 = fetch_score_dataframe_3()
        
        if not df_score_1.empty or not df_score_2.empty or not df_score_3.empty:
            score_distribution_df = calculate_score_distribution_combined(df_score_1, df_score_2, df_score_3)
            
            if not score_distribution_df.empty:
                render_score_distribution_chart(score_distribution_df)
            else:
                st.warning("⚠️ No score distribution data could be calculated. This might be due to missing or invalid custom_dimension_1 data.")
        else:
            st.warning("⚠️ No score data available for analysis.")

# Add health check for Streamlit Cloud
def health_check():
    """Simple health check for Streamlit Cloud"""
    return "OK"

# Add a simple endpoint for health checks
def simple_health_check():
    """Ultra-simple health check that doesn't require database"""
    st.write("✅ Streamlit Cloud Health Check - OK")
    return True

if __name__ == "__main__":
    # Quick health check
    health_check()
    main()
