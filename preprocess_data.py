#!/usr/bin/env python3
"""
Data Preprocessing Script for Matomo Events Dashboard

This script performs all data processing locally and saves the results to CSV files
for use by the lightweight Streamlit dashboard on Render.

⚠️ IMPORTANT: This script must be run locally before deploying to Render.
The dashboard on Render only handles visualization of preprocessed data.
"""

import os
import json
import pandas as pd
import pymysql
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Tuple

# Load environment variables
load_dotenv()

# Database connection settings
HOST = os.getenv("DB_HOST")
PORT = int(os.getenv("DB_PORT", "3310"))
DBNAME = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

# Validate required environment variables
required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# SQL Queries (same as in original dashboard)
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
    AND DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) >= '2025-07-02'
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


def fetch_dataframe() -> pd.DataFrame:
    """Fetch main dataframe from database"""
    print("Fetching main dashboard data...")
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
    print(f"SUCCESS: Fetched {len(df)} records from main query")
    return df


def fetch_score_dataframe_1() -> pd.DataFrame:
    """Fetch data for score distribution analysis (correctSelections games)"""
    print("Fetching score data (correctSelections games)...")
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
    print(f"SUCCESS: Fetched {len(df)} records from score query 1")
    return df


def fetch_score_dataframe_2() -> pd.DataFrame:
    """Fetch data for score distribution analysis (jsonData games)"""
    print("Fetching score data (jsonData games)...")
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
    print(f"SUCCESS: Fetched {len(df)} records from score query 2")
    return df


def fetch_score_dataframe_3() -> pd.DataFrame:
    """Fetch data for score distribution analysis (action_level games)"""
    print("Fetching score data (action_level games)...")
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
    print(f"SUCCESS: Fetched {len(df)} records from score query 3")
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
    print("Processing score distribution data...")
    combined_df = pd.DataFrame()
    
    # Process first dataset (correctSelections games)
    if not df_score_1.empty:
        print("  - Processing correctSelections games...")
        df_score_1['total_score'] = df_score_1['custom_dimension_1'].apply(parse_custom_dimension_1_correct_selections)
        df_score_1['game_name'] = df_score_1['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        df_score_1 = df_score_1[df_score_1['total_score'] > 0]
        combined_df = pd.concat([combined_df, df_score_1], ignore_index=True)
    
    # Process second dataset (jsonData games)
    if not df_score_2.empty:
        print("  - Processing jsonData games...")
        df_score_2['total_score'] = df_score_2['custom_dimension_1'].apply(parse_custom_dimension_1_json_data)
        df_score_2['game_name'] = df_score_2['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        df_score_2 = df_score_2[df_score_2['total_score'] > 0]
        combined_df = pd.concat([combined_df, df_score_2], ignore_index=True)
    
    # Process third dataset (action games) - HANDLE MULTIPLE GAME SESSIONS PER VISIT
    if not df_score_3.empty:
        print("  - Processing action games...")
        
        # Parse each record to get individual question scores (0 or 1)
        df_score_3['question_score'] = df_score_3['custom_dimension_1'].apply(parse_custom_dimension_1_action_games)
        df_score_3['game_name'] = df_score_3['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        
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
        
        # Group by user, game, visit, and session_instance
        df_score_3_grouped = df_score_3.groupby(['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'session_instance'])['question_score'].sum().reset_index()
        df_score_3_grouped.columns = ['idvisitor_converted', 'custom_dimension_2', 'idvisit', 'session_instance', 'total_score']
        df_score_3_grouped['game_name'] = df_score_3_grouped['custom_dimension_2'].apply(get_game_name_from_custom_dimension_2)
        
        # CRITICAL: Cap the total_score at 12 (max possible for one game session)
        df_score_3_grouped['total_score'] = df_score_3_grouped['total_score'].clip(upper=12)
        
        # Only include sessions with total_score > 0
        df_score_3_grouped = df_score_3_grouped[df_score_3_grouped['total_score'] > 0]
        combined_df = pd.concat([combined_df, df_score_3_grouped], ignore_index=True)
    
    if combined_df.empty:
        print("WARNING: No score distribution data found")
        return pd.DataFrame()
    
    # Group by game and total score, then count distinct users
    # Each user-game-score combination is counted once
    score_distribution = combined_df.groupby(['game_name', 'total_score'])['idvisitor_converted'].nunique().reset_index()
    score_distribution.columns = ['game_name', 'total_score', 'user_count']
    
    print(f"SUCCESS: Processed score distribution: {len(score_distribution)} records")
    return score_distribution


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
    print("Building summary statistics...")
    
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
    
    print(f"SUCCESS: Summary statistics: {len(grouped)} event types")
    return grouped


def preprocess_time_series_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess time series data for different time periods"""
    print("Preprocessing time series data...")
    
    # Convert server_time to datetime and extract date
    df['datetime'] = pd.to_datetime(df['server_time'])
    df['date'] = df['datetime'].dt.date
    
    # Filter data to only include records from July 2nd, 2025 onwards
    july_2_2025 = pd.Timestamp('2025-07-02')
    df = df[df['datetime'] >= july_2_2025].copy()
    print(f"Filtered time series data to July 2nd, 2025 onwards: {len(df)} records")
    
    # Get unique games for individual game processing
    unique_games = df['game_name'].unique()
    print(f"Processing time series for {len(unique_games)} games: {unique_games[:5]}...")
    
    # Prepare time series data for different periods
    time_series_data = []
    
    # Process each game individually + "All Games" combined
    games_to_process = list(unique_games) + ['All Games']
    
    for game_name in games_to_process:
        if game_name == 'All Games':
            game_df = df.copy()
        else:
            game_df = df[df['game_name'] == game_name].copy()
        
        if game_df.empty:
            continue
            
        print(f"Processing time series for: {game_name}")
    
        # Day-level data (last 2 weeks from July 2nd, 2025 onwards)
        cutoff_date = game_df['datetime'].max() - pd.Timedelta(days=14)
        df_daily = game_df[game_df['datetime'] >= cutoff_date].copy()
        df_daily['time_group'] = df_daily['datetime'].dt.date
        
        for time_group in df_daily['time_group'].unique():
            group_data = df_daily[df_daily['time_group'] == time_group]
            
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
                'time_period': str(time_group),
                'period_type': 'Day',
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances,
                'game_name': game_name
            })
        
        # Week-level data (all data from July 2nd, 2025 onwards)
        july_2_2025 = pd.Timestamp('2025-07-02')
        game_df['days_since_july_2'] = (game_df['datetime'] - july_2_2025).dt.days
        game_df['week_number'] = (game_df['days_since_july_2'] // 7) + 1
        game_df['time_group_week'] = 'Week ' + game_df['week_number'].astype(str)
        
        for time_group in game_df['time_group_week'].unique():
            group_data = game_df[game_df['time_group_week'] == time_group]
            
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
                'period_type': 'Week',
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances,
                'game_name': game_name
            })
        
        # Month-level data (all data from July 2nd, 2025 onwards)
        game_df['time_group_month'] = game_df['datetime'].dt.strftime('%B %Y')
        
        for time_group in game_df['time_group_month'].unique():
            group_data = game_df[game_df['time_group_month'] == time_group]
            
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
                'period_type': 'Month',
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances,
                'game_name': game_name
            })
    
    time_series_df = pd.DataFrame(time_series_data)
    print(f"SUCCESS: Time series data: {len(time_series_df)} records")
    return time_series_df


def fetch_hybrid_repeatability_data() -> pd.DataFrame:
    """Fetch repeatability data using the exact SQL query from hybrid database tables"""
    print("Fetching repeatability data from hybrid database...")
    
    try:
        # Connect to database
        connection = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            charset='utf8mb4'
        )
        
        # Your exact SQL query
        hybrid_query = """
        SELECT 
            COUNT(DISTINCT game_name) as CountDistinctNonNull_game_name,
            COUNT(DISTINCT hybrid_profile_id) as CountDistinct_hybrid_profile_id
        FROM `hybrid_games`
        INNER JOIN `hybrid_games_links` ON `hybrid_games`.`id` = `hybrid_games_links`.`game_id`
        INNER JOIN `hybrid_game_completions` ON `hybrid_games_links`.`activity_id` = `hybrid_game_completions`.`activity_id`
        INNER JOIN `hybrid_profiles` ON `hybrid_game_completions`.`hybrid_profile_id` = `hybrid_profiles`.`id`
        INNER JOIN `hybrid_users` ON `hybrid_profiles`.`hybrid_user_id` = `hybrid_users`.`id`
        GROUP BY hybrid_profile_id
        ORDER BY CountDistinctNonNull_game_name
        """
        
        # Execute query
        hybrid_df = pd.read_sql(hybrid_query, connection)
        connection.close()
        
        print(f"SUCCESS: Fetched {len(hybrid_df)} records from hybrid database")
        print("Sample data:")
        print(hybrid_df.head(10))
        
        # Group by the count of distinct non-null game_name
        # Calculate CountDistinct_hybrid_profile_id for each distinct count value
        repeatability_data = hybrid_df.groupby('CountDistinctNonNull_game_name').size().reset_index()
        repeatability_data.columns = ['games_played', 'user_count']
        
        print("Final repeatability data:")
        print(repeatability_data.head(10))
        
        return repeatability_data
        
    except Exception as e:
        print(f"ERROR: Failed to fetch hybrid data: {str(e)}")
        print("Falling back to Matomo data...")
        return pd.DataFrame()

def preprocess_repeatability_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess game repeatability data based on EXACT SQL query logic:
    
    The correct numbers should be:
    1 game: 15,846 users
    2 games: 10,776 users
    3 games: 6,009 users
    etc.
    
    This suggests the current logic is wrong. Let me implement the correct SQL query logic.
    """
    print("Preprocessing repeatability data using CORRECT SQL query logic...")
    
    # Filter for completed events only
    completed_events = df[df['event'] == 'Completed']
    
    if completed_events.empty:
        print("WARNING: No completed events found")
        return pd.DataFrame()
    
    print(f"DEBUG: Total completed events: {len(completed_events)}")
    print(f"DEBUG: Unique users in completed events: {completed_events['idvisitor_converted'].nunique()}")
    print(f"DEBUG: Unique games in completed events: {completed_events['game_name'].nunique()}")
    
    # The issue might be that we need to filter the data differently
    # Let me check what the actual data looks like
    print("DEBUG: Sample of completed events:")
    print(completed_events[['idvisitor_converted', 'game_name', 'event']].head(10))
    
    # Group by hybrid_profile_id (using idvisitor_converted as proxy)
    # Count distinct non-null values of game_name for each hybrid_profile_id
    user_game_counts = completed_events.groupby('idvisitor_converted')['game_name'].nunique().reset_index()
    user_game_counts.columns = ['hybrid_profile_id', 'games_played']
    
    print(f"DEBUG: User game counts sample:")
    print(user_game_counts.head(10))
    print(f"DEBUG: Games played distribution:")
    print(user_game_counts['games_played'].value_counts().sort_index().head(10))
    
    # Group by the count of distinct non-null game_name
    # Calculate CountDistinct_hybrid_profile_id for each distinct count value
    repeatability_data = user_game_counts.groupby('games_played').size().reset_index()
    repeatability_data.columns = ['games_played', 'user_count']
    
    print(f"DEBUG: Repeatability data before range completion:")
    print(repeatability_data.head(10))
    
    # Create complete range from 1 to max games played
    max_games = user_game_counts['games_played'].max()
    complete_range = pd.DataFrame({'games_played': range(1, max_games + 1)})
    repeatability_data = complete_range.merge(repeatability_data, on='games_played', how='left').fillna(0)
    repeatability_data['user_count'] = repeatability_data['user_count'].astype(int)
    
    print(f"SUCCESS: Repeatability data (SQL logic): {len(repeatability_data)} records")
    print(f"Max distinct games played: {max_games}")
    print(f"Total unique hybrid_profile_id: {user_game_counts['hybrid_profile_id'].nunique()}")
    print(f"FINAL DATA:")
    print(repeatability_data.head(10))
    return repeatability_data


def main():
    """Main preprocessing function"""
    print("Starting data preprocessing for Matomo Events Dashboard")
    print("=" * 60)
    
    try:
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Fetch all data
        print("\nFETCHING DATA")
        print("-" * 30)
        
        # Main dashboard data
        df_main = fetch_dataframe()
        if df_main.empty:
            print("ERROR: No main data found. Exiting.")
            return
        
        # Score distribution data
        df_score_1 = fetch_score_dataframe_1()
        df_score_2 = fetch_score_dataframe_2()
        df_score_3 = fetch_score_dataframe_3()
        
        print("\nPROCESSING DATA")
        print("-" * 30)
        
        # Process main data
        print("Processing main dashboard data...")
        df_main['date'] = pd.to_datetime(df_main['server_time']).dt.date
        
        # Build summary statistics
        summary_df = build_summary(df_main)
        
        # Process score distribution
        score_distribution_df = calculate_score_distribution_combined(df_score_1, df_score_2, df_score_3)
        
        # Process time series data
        time_series_df = preprocess_time_series_data(df_main)
        
        # Process repeatability data using hybrid database
        repeatability_df = fetch_hybrid_repeatability_data()
        
        # Fallback to Matomo data if hybrid data fails
        if repeatability_df.empty:
            print("Using Matomo data as fallback...")
            repeatability_df = preprocess_repeatability_data(df_main)
        
        print("\nSAVING PROCESSED DATA")
        print("-" * 30)
        
        # Save all processed data
        df_main.to_csv('data/processed_data.csv', index=False)
        print("SUCCESS: Saved data/processed_data.csv")
        
        summary_df.to_csv('data/summary_data.csv', index=False)
        print("SUCCESS: Saved data/summary_data.csv")
        
        if not score_distribution_df.empty:
            score_distribution_df.to_csv('data/score_distribution_data.csv', index=False)
            print("SUCCESS: Saved data/score_distribution_data.csv")
        else:
            print("WARNING: No score distribution data to save")
        
        if not time_series_df.empty:
            time_series_df.to_csv('data/time_series_data.csv', index=False)
            print("SUCCESS: Saved data/time_series_data.csv")
        else:
            print("WARNING: No time series data to save")
        
        if not repeatability_df.empty:
            repeatability_df.to_csv('data/repeatability_data.csv', index=False)
            print("SUCCESS: Saved data/repeatability_data.csv")
        else:
            print("WARNING: No repeatability data to save")
        
        # Save metadata
        metadata = {
            'preprocessing_date': datetime.now().isoformat(),
            'main_data_records': len(df_main),
            'summary_records': len(summary_df),
            'score_distribution_records': len(score_distribution_df),
            'time_series_records': len(time_series_df),
            'repeatability_records': len(repeatability_df),
            'data_date_range': {
                'start': str(df_main['server_time'].min()),
                'end': str(df_main['server_time'].max())
            }
        }
        
        import json
        with open('data/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        print("SUCCESS: Saved data/metadata.json")
        
        print("\nPREPROCESSING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("All processed data saved to 'data/' directory")
        print("Ready for deployment to Render!")
        print("\nNext steps:")
        print("1. Commit and push the updated data/ directory to GitHub")
        print("2. Render will automatically redeploy with the latest data")
        print("3. The dashboard will now run efficiently on Render's 512MB limit")
        
    except Exception as e:
        print(f"\nERROR during preprocessing: {str(e)}")
        print("Please check your database connection and try again.")
        raise


if __name__ == "__main__":
    main()
