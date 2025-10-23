import os
import json
from typing import List, Tuple

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
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")


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

# First SQL query for score distribution analysis (correctSelections games)
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

# Second SQL query for score distribution analysis (jsonData games)
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

# Third SQL query for score distribution analysis (action_level games only)
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


@st.cache_data(show_spinner=False)
def fetch_dataframe() -> pd.DataFrame:
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
            cur.execute(SQL_QUERY)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def fetch_score_dataframe_1() -> pd.DataFrame:
    """Fetch data for score distribution analysis (correctSelections games)"""
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


def fetch_score_dataframe_2() -> pd.DataFrame:
    """Fetch data for score distribution analysis (jsonData games)"""
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


def fetch_score_dataframe_3() -> pd.DataFrame:
    """Fetch data for score distribution analysis (action_level/action_completed/action_started games)"""
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
    
    # Debug: Show what data we got from third query
    print(f"Third query returned {len(df)} records")
    if not df.empty:
        print(f"Third query columns: {df.columns.tolist()}")
        print(f"Sample names: {df['name'].value_counts().head()}")
        print(f"Sample custom_dimension_2: {df['custom_dimension_2'].value_counts().head()}")
    
    return df


def parse_custom_dimension_1_correct_selections(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract correctSelections (for first query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        # Extract correctSelections from nested structure
        # Path: gameData[*].gameData[*].statistics.correctSelections
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
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        # Extract total score from jsonData structure
        # Path: gameData[*] where section="Action" -> jsonData[*] -> userResponse[*] -> isCorrect
        if 'gameData' in data and len(data['gameData']) > 0:
            total_score = 0
            
            for game_data in data['gameData']:
                # Look for section = "Action"
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
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        # Extract score from action games structure
        # Structure: {"options": [{"path": "o1.png", "isCorrect": false}, ...], "chosenOption": 1, "totalTaps": 2, "time": 1754568484640}
        total_score = 0
        
        # Check if this is a single question record
        if 'options' in data and 'chosenOption' in data:
            chosen_option = data.get('chosenOption')
            
            # If chosenOption is null, score = 0
            if chosen_option is None:
                return 0
            
            # Check if chosenOption is within bounds and if the chosen option is correct
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
    
    # correctSelections is already the total score for the game
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
    
    # Process third dataset (action games) - HANDLE MULTIPLE GAME SESSIONS PER VISIT
    if not df_score_3.empty:
        print(f"Processing third query with {len(df_score_3)} records")
        
        # Parse each record to get individual question scores (0 or 1)
        df_score_3['question_score'] = df_score_3['custom_dimension_1'].apply(parse_custom_dimension_1_action_games)
        df_score_3['game_name'] = df_score_3['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        
        print(f"Sample question scores: {df_score_3['question_score'].value_counts().head()}")
        
        # CRITICAL: Handle multiple game sessions per user+game+visit
        # Sort by user, game, visit, then by server_time to track session order
        df_score_3 = df_score_3.sort_values(['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'server_time'])
        
        # Create session_instance to handle multiple plays of same game
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
            
            # If new user, new game, or new visit, reset session
            if user != prev_user or game != prev_game or visit != prev_visit:
                current_session = 1
            # If same user+game+visit but significant time gap, new session
            elif prev_time is not None and (time - prev_time).total_seconds() > 300:  # 5 minutes gap
                current_session += 1
            
            session_instances.append(current_session)
            prev_user = user
            prev_game = game
            prev_visit = visit
            prev_time = time
        
        df_score_3['session_instance'] = session_instances
        
        print(f"Created session instances. Max session_instance: {max(session_instances)}")
        
        # Group by user, game, visit, and session_instance
        df_score_3_grouped = df_score_3.groupby(['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'session_instance'])['question_score'].sum().reset_index()
        df_score_3_grouped.columns = ['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'session_instance', 'total_score']
        df_score_3_grouped['game_name'] = df_score_3_grouped['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        
        # CRITICAL: Cap the total_score at 12 (max possible for one game session)
        df_score_3_grouped['total_score'] = df_score_3_grouped['total_score'].clip(upper=12)
        
        print(f"After grouping: {len(df_score_3_grouped)} sessions")
        print(f"Max total_score after grouping: {df_score_3_grouped['total_score'].max()}")
        print(f"Min total_score after grouping: {df_score_3_grouped['total_score'].min()}")
        print(f"Score distribution: {df_score_3_grouped['total_score'].value_counts().head()}")
        
        # Only include sessions with total_score > 0
        df_score_3_grouped = df_score_3_grouped[df_score_3_grouped['total_score'] > 0]
        combined_df = pd.concat([combined_df, df_score_3_grouped], ignore_index=True)
    
    if combined_df.empty:
        return pd.DataFrame()
    
    # Debug: Show parsing results
    print(f"Combined records: {len(combined_df)}")
    if not combined_df.empty:
        print(f"Combined columns: {combined_df.columns.tolist()}")
        print(f"Records with total_score > 0: {(combined_df['total_score'] > 0).sum()}")
        print(f"Sample total_scores: {combined_df['total_score'].value_counts().head()}")
        print(f"Max total_score: {combined_df['total_score'].max()}")
        print(f"Min total_score: {combined_df['total_score'].min()}")
    
    # Group by game and total score, then count distinct users
    # Each user-game-score combination is counted once
    if not combined_df.empty:
        score_distribution = combined_df.groupby(['game_name', 'total_score'])['idvisitor_converted'].nunique().reset_index()
        score_distribution.columns = ['game_name', 'total_score', 'user_count']
        
        print(f"Final score distribution records: {len(score_distribution)}")
        print(f"Sample score distribution: {score_distribution.head()}")
        
        return score_distribution
    else:
        return pd.DataFrame()


def render_score_distribution_chart(score_distribution_df: pd.DataFrame) -> None:
    """Render score distribution chart"""
    import altair as alt
    
    if score_distribution_df.empty:
        st.warning("No score distribution data available.")
        return
    
    # Get unique games for filter
    unique_games = sorted(score_distribution_df['game_name'].unique())
    
    # Add game filter
    selected_games = st.multiselect(
        "Select Games for Score Distribution:",
        options=unique_games,
        default=unique_games,
        help="Select one or more games to show score distribution. Leave empty to show all games."
    )
    
    # Filter data based on selected games
    if selected_games:
        filtered_df = score_distribution_df[score_distribution_df['game_name'].isin(selected_games)]
    else:
        filtered_df = score_distribution_df
    
    if filtered_df.empty:
        st.warning("No data available for the selected games.")
        return
    
    # Create the score distribution chart
    st.markdown("### 📊 Score Distribution Analysis")
    st.markdown("This chart shows how many users achieved each total score for each game.")
    
    # Create bar chart with proper formatting
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
    
    # Add data labels on top of bars
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
    
    # Combine bars and labels
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
    
    # Add summary statistics
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
    
    # Show detailed table
    st.markdown("#### 📋 Detailed Score Distribution")
    
    # Create a pivot table for better visualization
    pivot_df = filtered_df.pivot(index='total_score', columns='game_name', values='user_count').fillna(0)
    pivot_df = pivot_df.astype(int)
    
    st.dataframe(
        pivot_df,
        use_container_width=True,
        height=400
    )
    
    # Add combined score distribution (all games together)
    st.markdown("#### 🎯 Combined Score Distribution (All Games)")
    
    # Create combined data by summing user counts across all games for each score
    combined_df = filtered_df.groupby('total_score')['user_count'].sum().reset_index()
    combined_df.columns = ['total_score', 'total_users']
    
    if not combined_df.empty:
        # Create combined bar chart
        combined_bars = alt.Chart(combined_df).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#FF6B6B'
        ).encode(
            x=alt.X('total_score:O', 
                    title='Total Score', 
                    axis=alt.Axis(
                        labelAngle=0,
                        labelFontSize=14,
                        titleFontSize=16,
                        labelLimit=100
                    )),
            y=alt.Y('total_users:Q', 
                    title='Total Number of Users', 
                    axis=alt.Axis(format='~s')),
            tooltip=['total_score:O', 'total_users:Q']
        ).properties(
            width=900,
            height=400,
            title='Combined Score Distribution (All Games)'
        )
        
        # Add data labels for combined chart
        combined_labels = alt.Chart(combined_df).mark_text(
            align='center',
            baseline='bottom',
            color='#2E8B57',
            fontSize=12,
            fontWeight='bold',
            dy=-5
        ).encode(
            x=alt.X('total_score:O'),
            y=alt.Y('total_users:Q'),
            text=alt.Text('total_users:Q', format='.0f')
        )
        
        # Combine bars and labels for combined chart
        combined_chart = (combined_bars + combined_labels).configure_axis(
            labelFontSize=16,
            titleFontSize=18,
            grid=True
        ).configure_title(
            fontSize=24,
            fontWeight='bold'
        )
        
        st.altair_chart(combined_chart, use_container_width=True)


def ensure_event_rows(series: pd.Series) -> pd.Series:
    # Ensure both 'Started' and 'Completed' exist, fill missing with 0
    base = pd.Series({'Started': 0, 'Completed': 0}, dtype='int64')
    series = series.astype('int64')
    return base.add(series, fill_value=0).astype('int64')


def _distinct_count_ignore_blank(series: pd.Series) -> int:
    """Power BI DISTINCTCOUNTNOBLANK logic: ignore NULLs and empty strings"""
    if series.dtype == object:
        # For string columns: drop NULLs and empty strings
        cleaned = series.dropna()
        cleaned = cleaned[cleaned.astype(str).str.strip() != ""]
    else:
        # For numeric columns: just drop NULLs
        cleaned = series.dropna()
    return int(cleaned.nunique())


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build summary table with correct Power BI DISTINCTCOUNTNOBLANK logic"""
    # Group by event and compute distinct counts
    grouped = df.groupby('event').agg({
        'idvisitor_converted': _distinct_count_ignore_blank,
        'idvisit': _distinct_count_ignore_blank, 
        'idlink_va': _distinct_count_ignore_blank,
    })
    
    # Rename columns to match Power BI
    grouped.columns = ['Users', 'Visits', 'Instances']
    grouped = grouped.reset_index()
    grouped.rename(columns={'event': 'Event'}, inplace=True)
    
    # Ensure both Started and Completed exist (fill missing with 0)
    all_events = pd.DataFrame({'Event': ['Started', 'Completed']})
    grouped = all_events.merge(grouped, on='Event', how='left').fillna(0)
    
    # Convert to int and sort
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
    
    # Get data for each funnel
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
        
        # Add labels with complete numbers - positioned at the start of bars
        users_labels = alt.Chart(users_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),  # Fixed position at the start
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
        
        # Add labels with complete numbers - positioned at the start of bars
        visits_labels = alt.Chart(visits_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),  # Fixed position at the start
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
        
        # Add labels with complete numbers - positioned at the start of bars
        instances_labels = alt.Chart(instances_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),  # Fixed position at the start
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
        started_visits = summary_df[summary_df['Event'] == 'Started']['Visits'].iloc[0]
        completed_visits = summary_df[summary_df['Event'] == 'Completed']['Visits'].iloc[0]
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
    
    # Add repeatability graph
    st.markdown("### 🎮 Game Repeatability Analysis")
    
    # Filter for completed events only
    completed_events = df_filtered[df_filtered['event'] == 'Completed']
    
    if not completed_events.empty:
        # Count games played per user
        user_game_counts = completed_events.groupby('idvisitor_converted')['game_name'].nunique().reset_index()
        user_game_counts.columns = ['user_id', 'games_played']
        
        # Count how many users played each number of games
        repeatability_data = user_game_counts.groupby('games_played').size().reset_index()
        repeatability_data.columns = ['games_played', 'user_count']
        
        # Create complete range from 1 to 27 games
        complete_range = pd.DataFrame({'games_played': range(1, 28)})
        repeatability_data = complete_range.merge(repeatability_data, on='games_played', how='left').fillna(0)
        repeatability_data['user_count'] = repeatability_data['user_count'].astype(int)
        
        # Create the repeatability chart
        bars = alt.Chart(repeatability_data).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#50C878'
        ).encode(
            x=alt.X('games_played:O', title='Number of Games Played', axis=alt.Axis(labelAngle=0)),
            y=alt.Y('user_count:Q', title='Number of Users', axis=alt.Axis(format='~s')),
            tooltip=['games_played:O', 'user_count:Q']
        ).properties(
            width=800,
            height=400,
            title='User Distribution by Number of Games Completed'
        )
        
        # Add data labels on top of bars
        labels = alt.Chart(repeatability_data).mark_text(
            align='center',
            baseline='bottom',
            color='#2E8B57',
            fontSize=20,
            fontWeight='bold',
            dy=-10
        ).encode(
            x=alt.X('games_played:O'),
            y=alt.Y('user_count:Q'),
            text=alt.Text('user_count:Q', format='.0f')
        )
        
        # Combine bars and labels
        repeatability_chart = (bars + labels).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=True
        ).configure_title(
            fontSize=28,
            fontWeight='bold'
        )
        
        st.altair_chart(repeatability_chart, use_container_width=True)
        
        # Add summary statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_users = len(user_game_counts)
            st.metric(
                label="👥 Total Users (Completed)",
                value=f"{total_users:,}",
                help="Total number of users who completed at least one game"
            )
        
        with col2:
            avg_games_per_user = user_game_counts['games_played'].mean()
            st.metric(
                label="🎯 Avg Games per User",
                value=f"{avg_games_per_user:.1f}",
                help="Average number of games completed per user"
            )
        
        with col3:
            max_games_played = user_game_counts['games_played'].max()
            st.metric(
                label="🏆 Max Games Played",
                value=f"{max_games_played}",
                help="Maximum number of games completed by a single user"
            )
    else:
        st.warning("No completed events found for the selected filters.")
    
    # Add time-series analysis
    st.markdown("### 📈 Time-Series Analysis")
    
    # Create columns for filters
    ts_filter_col1, ts_filter_col2 = st.columns(2)
    
    with ts_filter_col1:
        # Time period filter
        time_period = st.selectbox(
            "Select Time Period:",
            options=["All time", "Month", "Week", "Day"],
            help="Choose the time aggregation for the time-series graphs"
        )
    
    with ts_filter_col2:
        # Game Name filter for time-series
        unique_games_ts = sorted(df_filtered['game_name'].unique())
        selected_games_ts = st.multiselect(
            "Select Game Names for Time-Series:",
            options=unique_games_ts,
            default=unique_games_ts,
            help="Select one or more games to filter the time-series data. Leave empty to show all games."
        )
    
    # Apply Game Name filter to time-series data
    if selected_games_ts:
        df_timeseries = df_filtered[df_filtered['game_name'].isin(selected_games_ts)]
    else:
        df_timeseries = df_filtered
    
    # Prepare time-series data
    if not df_timeseries.empty:
        # Convert server_time to datetime and extract date
        df_timeseries['datetime'] = pd.to_datetime(df_timeseries['server_time'])
        
        # Group by time period based on selection
        if time_period == "Day":
            # Limit to last 2 weeks for day view
            cutoff_date = df_timeseries['datetime'].max() - pd.Timedelta(days=14)
            df_timeseries = df_timeseries[df_timeseries['datetime'] >= cutoff_date]
            df_timeseries['time_group'] = df_timeseries['datetime'].dt.date
        elif time_period == "Week":
            # Custom week calculation starting from July 2nd, 2025 (Wednesday)
            july_2_2025 = pd.Timestamp('2025-07-02')
            df_timeseries['days_since_july_2'] = (df_timeseries['datetime'] - july_2_2025).dt.days
            df_timeseries['week_number'] = (df_timeseries['days_since_july_2'] // 7) + 1
            df_timeseries['time_group'] = 'Week ' + df_timeseries['week_number'].astype(str)
        elif time_period == "Month":
            # Get month names
            df_timeseries['time_group'] = df_timeseries['datetime'].dt.strftime('%B %Y')
        else:  # All time
            df_timeseries['time_group'] = "All Time"
        
        # Calculate metrics for each time period
        time_series_data = []
        
        for time_group in df_timeseries['time_group'].unique():
            group_data = df_timeseries[df_timeseries['time_group'] == time_group]
            
            # Users (distinct count)
            started_users = group_data[group_data['event'] == 'Started']['idvisitor_converted'].nunique()
            completed_users = group_data[group_data['event'] == 'Completed']['idvisitor_converted'].nunique()
            
            # Visits (distinct count)
            started_visits = group_data[group_data['event'] == 'Started']['idvisit'].nunique()
            completed_visits = group_data[group_data['event'] == 'Completed']['idvisit'].nunique()
            
            # Instances (total count)
            started_instances = len(group_data[group_data['event'] == 'Started'])
            completed_instances = len(group_data[group_data['event'] == 'Completed'])
            
            time_series_data.append({
                'time_period': time_group,
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances
            })
        
        time_series_df = pd.DataFrame(time_series_data)
        
        # Sort the dataframe based on time period
        if time_period == "Month":
            # Convert month names to datetime for proper sorting
            time_series_df['sort_date'] = pd.to_datetime(time_series_df['time_period'])
            time_series_df = time_series_df.sort_values('sort_date').drop('sort_date', axis=1)
            # Create ordered list for Altair
            time_order = time_series_df['time_period'].tolist()
        elif time_period == "Week":
            # Extract week number for sorting
            time_series_df['week_num'] = time_series_df['time_period'].str.extract(r'(\d+)').astype(int)
            time_series_df = time_series_df.sort_values('week_num').drop('week_num', axis=1)
            # Create ordered list for Altair
            time_order = time_series_df['time_period'].tolist()
        elif time_period == "Day":
            # Sort by date
            time_series_df['sort_date'] = pd.to_datetime(time_series_df['time_period'])
            time_series_df = time_series_df.sort_values('sort_date').drop('sort_date', axis=1)
            # Create ordered list for Altair
            time_order = time_series_df['time_period'].astype(str).tolist()
        else:
            time_order = None
        
        if not time_series_df.empty:
            # Create three separate graphs stacked vertically
            
            # Users Chart
            st.markdown("#### 👥 Users Over Time")
            
            # Prepare data for users chart
            users_data = []
            for _, row in time_series_df.iterrows():
                users_data.extend([
                    {'Time': str(row['time_period']), 'Event': 'Started', 'Count': row['started_users']},
                    {'Time': str(row['time_period']), 'Event': 'Completed', 'Count': row['completed_users']}
                ])
            users_chart_df = pd.DataFrame(users_data)
            
            # Create line chart for users with data labels
            users_lines = alt.Chart(users_chart_df).mark_line(
                strokeWidth=4,
                point=alt.OverlayMarkDef(
                    filled=True,
                    size=100,
                    stroke='white',
                    strokeWidth=2
                )
            ).encode(
                x=alt.X('Time:N', title='Time Period', 
                       sort=time_order if time_order else None,
                       axis=alt.Axis(
                           labelAngle=0,
                           labelFontSize=14,
                           labelLimit=200,
                           titleFontSize=16
                       )),
                y=alt.Y('Count:Q', title='Number of Users', axis=alt.Axis(format='~s')),
                color=alt.Color('Event:N', 
                              scale=alt.Scale(domain=['Started', 'Completed'], 
                                            range=['#FF6B6B', '#4ECDC4']),
                              legend=alt.Legend(title="Event Type")),
                tooltip=['Time:N', 'Event:N', 'Count:Q']
            ).properties(
                width=900,
                height=400,
                title='Users: Started vs Completed'
            )
            
            # Add data labels for line chart with proper positioning
            users_started_labels = alt.Chart(users_chart_df[users_chart_df['Event'] == 'Started']).mark_text(
                align='center',
                baseline='bottom',
                color='#FF6B6B',
                fontSize=14,
                fontWeight='bold',
                dy=-15
            ).encode(
                x=alt.X('Time:N', sort=time_order if time_order else None),
                y=alt.Y('Count:Q'),
                text=alt.Text('Count:Q', format='.0f')
            )
            
            users_completed_labels = alt.Chart(users_chart_df[users_chart_df['Event'] == 'Completed']).mark_text(
                align='center',
                baseline='top',
                color='#4ECDC4',
                fontSize=14,
                fontWeight='bold',
                dy=15
            ).encode(
                x=alt.X('Time:N', sort=time_order if time_order else None),
                y=alt.Y('Count:Q'),
                text=alt.Text('Count:Q', format='.0f')
            )
            
            users_chart = (users_lines + users_started_labels + users_completed_labels)
            
            users_chart = users_chart.configure_axis(
                labelFontSize=18,
                titleFontSize=20,
                grid=True
            ).configure_title(
                fontSize=24,
                fontWeight='bold'
            )
            
            st.altair_chart(users_chart, use_container_width=True)
            
            # Visits Chart
            st.markdown("#### 🔄 Visits Over Time")
            
            # Prepare data for visits chart
            visits_data = []
            for _, row in time_series_df.iterrows():
                visits_data.extend([
                    {'Time': str(row['time_period']), 'Event': 'Started', 'Count': row['started_visits']},
                    {'Time': str(row['time_period']), 'Event': 'Completed', 'Count': row['completed_visits']}
                ])
            visits_chart_df = pd.DataFrame(visits_data)
            
            # Create line chart for visits with data labels
            visits_lines = alt.Chart(visits_chart_df).mark_line(
                strokeWidth=4,
                point=alt.OverlayMarkDef(
                    filled=True,
                    size=100,
                    stroke='white',
                    strokeWidth=2
                )
            ).encode(
                x=alt.X('Time:N', title='Time Period', 
                       sort=time_order if time_order else None,
                       axis=alt.Axis(
                           labelAngle=0,
                           labelFontSize=14,
                           labelLimit=200,
                           titleFontSize=16
                       )),
                y=alt.Y('Count:Q', title='Number of Visits', axis=alt.Axis(format='~s')),
                color=alt.Color('Event:N', 
                              scale=alt.Scale(domain=['Started', 'Completed'], 
                                            range=['#FF6B6B', '#4ECDC4']),
                              legend=alt.Legend(title="Event Type")),
                tooltip=['Time:N', 'Event:N', 'Count:Q']
            ).properties(
                width=900,
                height=400,
                title='Visits: Started vs Completed'
            )
            
            # Add data labels for line chart with proper positioning
            visits_started_labels = alt.Chart(visits_chart_df[visits_chart_df['Event'] == 'Started']).mark_text(
                align='center',
                baseline='bottom',
                color='#FF6B6B',
                fontSize=14,
                fontWeight='bold',
                dy=-15
            ).encode(
                x=alt.X('Time:N', sort=time_order if time_order else None),
                y=alt.Y('Count:Q'),
                text=alt.Text('Count:Q', format='.0f')
            )
            
            visits_completed_labels = alt.Chart(visits_chart_df[visits_chart_df['Event'] == 'Completed']).mark_text(
                align='center',
                baseline='top',
                color='#4ECDC4',
                fontSize=14,
                fontWeight='bold',
                dy=15
            ).encode(
                x=alt.X('Time:N', sort=time_order if time_order else None),
                y=alt.Y('Count:Q'),
                text=alt.Text('Count:Q', format='.0f')
            )
            
            visits_chart = (visits_lines + visits_started_labels + visits_completed_labels).configure_axis(
                labelFontSize=18,
                titleFontSize=20,
                grid=True
            ).configure_title(
                fontSize=24,
                fontWeight='bold'
            )
            
            st.altair_chart(visits_chart, use_container_width=True)
            
            # Instances Chart
            st.markdown("#### ⚡ Instances Over Time")
            
            # Prepare data for instances chart
            instances_data = []
            for _, row in time_series_df.iterrows():
                instances_data.extend([
                    {'Time': str(row['time_period']), 'Event': 'Started', 'Count': row['started_instances']},
                    {'Time': str(row['time_period']), 'Event': 'Completed', 'Count': row['completed_instances']}
                ])
            instances_chart_df = pd.DataFrame(instances_data)
            
            # Create line chart for instances with data labels
            instances_lines = alt.Chart(instances_chart_df).mark_line(
                strokeWidth=4,
                point=alt.OverlayMarkDef(
                    filled=True,
                    size=100,
                    stroke='white',
                    strokeWidth=2
                )
            ).encode(
                x=alt.X('Time:N', title='Time Period', 
                       sort=time_order if time_order else None,
                       axis=alt.Axis(
                           labelAngle=0,
                           labelFontSize=14,
                           labelLimit=200,
                           titleFontSize=16
                       )),
                y=alt.Y('Count:Q', title='Number of Instances', axis=alt.Axis(format='~s')),
                color=alt.Color('Event:N', 
                              scale=alt.Scale(domain=['Started', 'Completed'], 
                                            range=['#FF6B6B', '#4ECDC4']),
                              legend=alt.Legend(title="Event Type")),
                tooltip=['Time:N', 'Event:N', 'Count:Q']
            ).properties(
                width=900,
                height=400,
                title='Instances: Started vs Completed'
            )
            
            # Add data labels for line chart with proper positioning
            instances_started_labels = alt.Chart(instances_chart_df[instances_chart_df['Event'] == 'Started']).mark_text(
                align='center',
                baseline='bottom',
                color='#FF6B6B',
                fontSize=14,
                fontWeight='bold',
                dy=-15
            ).encode(
                x=alt.X('Time:N', sort=time_order if time_order else None),
                y=alt.Y('Count:Q'),
                text=alt.Text('Count:Q', format='.0f')
            )
            
            instances_completed_labels = alt.Chart(instances_chart_df[instances_chart_df['Event'] == 'Completed']).mark_text(
                align='center',
                baseline='top',
                color='#4ECDC4',
                fontSize=14,
                fontWeight='bold',
                dy=15
            ).encode(
                x=alt.X('Time:N', sort=time_order if time_order else None),
                y=alt.Y('Count:Q'),
                text=alt.Text('Count:Q', format='.0f')
            )
            
            instances_chart = (instances_lines + instances_started_labels + instances_completed_labels).configure_axis(
                labelFontSize=18,
                titleFontSize=20,
                grid=True
            ).configure_title(
                fontSize=24,
                fontWeight='bold'
            )
            
            st.altair_chart(instances_chart, use_container_width=True)
        else:
            st.warning("No data available for the selected time period.")
    else:
        st.warning("No data available for time-series analysis.")


def main() -> None:
    st.set_page_config(page_title="Matomo Events Dashboard", layout="wide")
    st.title("Matomo Events Dashboard")
    st.caption("All data (server_time adjusted by +5h30m)")

    with st.spinner("Loading data..."):
        df = fetch_dataframe()

    if df.empty:
        st.warning("No data returned for the current filters.")
        return

    # Add filters
    st.markdown("### 🎮 Filters")
    
    # Create two columns for filters
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        # Game Name filter
        unique_games = sorted(df['game_name'].unique())
        selected_games = st.multiselect(
            "Select Game Names to filter by:",
            options=unique_games,
            default=unique_games,  # Show all games by default
            help="Select one or more games to filter the dashboard data. Leave empty to show all games."
        )
    
    with filter_col2:
        # Date filter
        # Convert server_time to datetime and extract date
        df['date'] = pd.to_datetime(df['server_time']).dt.date
        
        # Set minimum date to July 2nd, 2025
        min_date = pd.to_datetime('2025-07-02').date()
        max_date = df['date'].max()
        
        # Create date range picker
        date_range = st.date_input(
            "Select Date Range:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Select a date range to filter the data. The server_time is already adjusted by +5h30m. Data is available from July 2nd, 2025 onwards."
        )
    
    # Apply filters
    df_filtered = df.copy()
    
    # Apply game filter
    if selected_games:
        df_filtered = df_filtered[df_filtered['game_name'].isin(selected_games)]
    
    # Apply date filter
    if len(date_range) == 2:  # Both start and end date selected
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered['date'] >= start_date) & 
            (df_filtered['date'] <= end_date)
        ]
    elif len(date_range) == 1:  # Only one date selected
        df_filtered = df_filtered[df_filtered['date'] == date_range[0]]
    
    if df_filtered.empty:
        st.warning("No data available for the selected filters.")
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
    
    with st.spinner("Loading score distribution data..."):
        # Fetch score data from all three queries
        df_score_1 = fetch_score_dataframe_1()  # correctSelections games
        df_score_2 = fetch_score_dataframe_2()  # jsonData games
        df_score_3 = fetch_score_dataframe_3()  # action games
        
        if not df_score_1.empty or not df_score_2.empty or not df_score_3.empty:
            # Calculate combined score distribution
            score_distribution_df = calculate_score_distribution_combined(df_score_1, df_score_2, df_score_3)
            
            if not score_distribution_df.empty:
                render_score_distribution_chart(score_distribution_df)
            else:
                st.warning("No score distribution data could be calculated. This might be due to missing or invalid custom_dimension_1 data.")
        else:
            st.warning("No score data available for analysis.")


if __name__ == "__main__":
    main()



