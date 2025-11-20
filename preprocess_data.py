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
import sys
import argparse
import pandas as pd
import pymysql
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Tuple, Optional

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

# SQL Queries - Updated with new event categorization
# Event stages: started, introduction, mid_introduction, parent_poll, validation, rewards, questions, completed
SQL_QUERY = (
    """
    SELECT DISTINCT
      `matomo_log_link_visit_action`.`idlink_va`,
      DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) AS server_time,
      `hybrid_games`.`game_name`,
      `hybrid_games_links`.`game_id`, 
      `matomo_log_action`.`name`,
      `matomo_log_link_visit_action`.`idpageview`,
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
      CASE 
        WHEN `matomo_log_action`.`name` LIKE '%_started%' THEN 'started'
        WHEN `matomo_log_action`.`name` LIKE '%introduction_completed%' AND `matomo_log_action`.`name` NOT LIKE '%mid%' THEN 'introduction'
        WHEN `matomo_log_action`.`name` LIKE '%_mid_introduction%' THEN 'mid_introduction'
        WHEN `matomo_log_action`.`name` LIKE '%_poll_completed%' THEN 'parent_poll'
        WHEN `matomo_log_action`.`name` LIKE '%action_completed%' THEN 'validation'
        WHEN `matomo_log_action`.`name` LIKE '%reward_completed%' THEN 'rewards'
        WHEN `matomo_log_action`.`name` LIKE '%question_completed%' THEN 'questions'
        WHEN `matomo_log_action`.`name` LIKE '%completed%' 
             AND `matomo_log_action`.`name` NOT LIKE '%introduction%'
             AND `matomo_log_action`.`name` NOT LIKE '%reward%'
             AND `matomo_log_action`.`name` NOT LIKE '%question%'
             AND `matomo_log_action`.`name` NOT LIKE '%mid_introduction%'
             AND `matomo_log_action`.`name` NOT LIKE '%poll%'
             AND `matomo_log_action`.`name` NOT LIKE '%action%' THEN 'completed'
        ELSE NULL
      END AS event
    FROM `matomo_log_link_visit_action`
    INNER JOIN `matomo_log_action` 
      ON `matomo_log_link_visit_action`.`idaction_name` = `matomo_log_action`.`idaction`
    INNER JOIN `hybrid_games_links` 
      ON `matomo_log_link_visit_action`.`custom_dimension_2` = `hybrid_games_links`.`activity_id`
    INNER JOIN `hybrid_games` 
      ON `hybrid_games_links`.`game_id` = `hybrid_games`.`id`
    WHERE `matomo_log_link_visit_action`.`server_time` > '2025-07-01' 
      AND (
        `matomo_log_action`.`name` LIKE '%introduction_completed%' OR
        `matomo_log_action`.`name` LIKE '%reward_completed%' OR
        `matomo_log_action`.`name` LIKE '%mcq_completed%' OR
        `matomo_log_action`.`name` LIKE '%game_completed%' OR
        `matomo_log_action`.`name` LIKE '%mcq_started%' OR
        `matomo_log_action`.`name` LIKE '%game_started%' OR
        `matomo_log_action`.`name` LIKE '%action_completed%' OR 
        `matomo_log_action`.`name` LIKE '%question_completed%' OR
        `matomo_log_action`.`name` LIKE '%poll_completed%'
      )
    """
)

# Score distribution query - Updated to use hybrid_games and hybrid_games_links tables
SCORE_DISTRIBUTION_QUERY = """
SELECT 
  hg.game_name AS game_name,
  mllva.idvisit,
  mla.name AS action_name,
  mllva.custom_dimension_1,
  CONV(HEX(mllva.idvisitor), 16, 10) AS idvisitor_converted,
  mllva.server_time,
  mllva.idaction_name,
  mllva.custom_dimension_2,
  mla.idaction,
  mla.type
FROM hybrid_games hg
INNER JOIN hybrid_games_links hgl ON hg.id = hgl.game_id
INNER JOIN matomo_log_link_visit_action mllva ON hgl.activity_id = mllva.custom_dimension_2
INNER JOIN matomo_log_action mla ON mllva.idaction_name = mla.idaction
WHERE mllva.server_time >= '2025-07-01'
  AND hgl.activity_id IS NOT NULL
  AND (
    mla.name LIKE '%game_completed%' 
    OR mla.name LIKE '%action_level%'
  )
"""

# Question Correctness Queries - Three separate queries for different game types
QUESTION_CORRECTNESS_QUERY_1 = """
SELECT 
  `matomo_log_link_visit_action`.`custom_dimension_2`, 
  `matomo_log_link_visit_action`.`idvisit`, 
  `matomo_log_action`.`name`, 
  `matomo_log_link_visit_action`.`custom_dimension_1`, 
  CONV(HEX(`matomo_log_link_visit_action`.idvisitor), 16, 10) AS idvisitor_converted,
  `hybrid_games`.`game_name`
FROM `matomo_log_link_visit_action` 
INNER JOIN `matomo_log_action` 
  ON `matomo_log_link_visit_action`.`idaction_name` = `matomo_log_action`.`idaction`
INNER JOIN `hybrid_games_links`
  ON `matomo_log_link_visit_action`.`custom_dimension_2` = `hybrid_games_links`.`activity_id`
INNER JOIN `hybrid_games`
  ON `hybrid_games_links`.`game_id` = `hybrid_games`.`id`
WHERE `matomo_log_action`.`name` LIKE '%game_completed%'
  AND `hybrid_games`.`game_name` IN (
    'Relational Comparison',
    'Quantitative Comparison',
    'Relational Comparison II',
    'Number Comparison',
    'Primary Emotion Labelling',
    'Emotion Identification',
    'Identification of all emotions',
    'Beginning Sound Pa Cha Sa'
  );
"""

QUESTION_CORRECTNESS_QUERY_2 = """
SELECT 
  matomo_log_link_visit_action.custom_dimension_2,
  matomo_log_link_visit_action.idvisit,
  matomo_log_action.name,
  matomo_log_link_visit_action.custom_dimension_1,
  CONV(HEX(matomo_log_link_visit_action.idvisitor), 16, 10) AS idvisitor_converted,
  hybrid_games.game_name
FROM matomo_log_link_visit_action
INNER JOIN matomo_log_action
  ON matomo_log_link_visit_action.idaction_name = matomo_log_action.idaction
INNER JOIN hybrid_games_links
  ON matomo_log_link_visit_action.custom_dimension_2 = hybrid_games_links.activity_id
INNER JOIN hybrid_games
  ON hybrid_games_links.game_id = hybrid_games.id
WHERE matomo_log_action.name LIKE '%game_completed%'
  AND hybrid_games.game_name IN (
    'Revision Primary Colors',
    'Revision Primary Shapes',
    'Rhyming Words'
  );
"""

QUESTION_CORRECTNESS_QUERY_3 = """
SELECT `matomo_log_link_visit_action`.`idlink_va`, 
CONV(HEX(`matomo_log_link_visit_action`.idvisitor), 16, 10) AS idvisitor_converted, 
`matomo_log_link_visit_action`.`idvisit`, 
`matomo_log_link_visit_action`.`server_time`, 
`matomo_log_link_visit_action`.`idaction_name`, 
`matomo_log_link_visit_action`.`custom_dimension_1`, 
`matomo_log_link_visit_action`.`custom_dimension_2`, 
`matomo_log_action`.`idaction`, 
`matomo_log_action`.`name`, 
`matomo_log_action`.`type`,
`hybrid_games`.`game_name`
FROM `matomo_log_link_visit_action` 
INNER JOIN `matomo_log_action` 
  ON `matomo_log_link_visit_action`.`idaction_name` = `matomo_log_action`.`idaction`
INNER JOIN `hybrid_games_links`
  ON `matomo_log_link_visit_action`.`custom_dimension_2` = `hybrid_games_links`.`activity_id`
INNER JOIN `hybrid_games`
  ON `hybrid_games_links`.`game_id` = `hybrid_games`.`id`
WHERE `matomo_log_link_visit_action`.`server_time` >= '2025-07-01' 
  AND `matomo_log_action`.`name` LIKE '%action_level%'
  AND `hybrid_games`.`game_name` IN (
    'Shape Circle',
    'Shape Triangle',
    'Shape Square',
    'Shape Rectangle',
    'Color Red',
    'Color Yellow',
    'Color Blue',
    'Numbers I',
    'Numbers II',
    'Numerals 1-10',
    'Beginning Sound Ma Ka La',
    'Beginning Sound Ba Ra Na'
  );
"""

# Parent Poll Query
PARENT_POLL_QUERY = """
SELECT 
  `matomo_log_link_visit_action`.*,
  CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
  `hybrid_games`.`game_name`
FROM `matomo_log_link_visit_action` 
INNER JOIN `matomo_log_action` ON `matomo_log_link_visit_action`.`idaction_name` = `matomo_log_action`.`idaction` 
INNER JOIN `hybrid_games_links` ON `hybrid_games_links`.`activity_id` = `matomo_log_link_visit_action`.`custom_dimension_2` 
INNER JOIN `hybrid_games` ON `hybrid_games`.`id` = `hybrid_games_links`.`game_id` 
WHERE `matomo_log_action`.`name` LIKE "%_completed%" 
  AND `matomo_log_link_visit_action`.`custom_dimension_1` IS NOT NULL 
  AND `matomo_log_link_visit_action`.`custom_dimension_1` LIKE "%poll%" 
  AND `matomo_log_link_visit_action`.`server_time` > '2025-07-01' 
  AND `hybrid_games_links`.`activity_id` IS NOT NULL;
"""

# Time Series Analysis Query - Uses same logic as conversion funnel query
# Includes action_name and event classification (Started/Completed)
TIME_SERIES_QUERY = """
SELECT 
  mllva.idlink_va,
  CONV(HEX(mllva.idvisitor), 16, 10) AS idvisitor_converted,
  mllva.idvisit,
  DATE_ADD(mllva.server_time, INTERVAL 330 MINUTE) AS server_time,
  mllva.idaction_name,
  mllva.custom_dimension_2,
  hg.game_name,
  mla.name AS action_name,
  CASE 
    WHEN mla.name LIKE '%hybrid_game_started%' OR mla.name LIKE '%hybrid_mcq_started%' THEN 'Started'
    WHEN mla.name LIKE '%hybrid_game_completed%' OR mla.name LIKE '%hybrid_mcq_completed%' THEN 'Completed'
    ELSE NULL
  END AS event
FROM matomo_log_link_visit_action mllva
INNER JOIN matomo_log_action mla ON mllva.idaction_name = mla.idaction
INNER JOIN hybrid_games_links hgl ON mllva.custom_dimension_2 = hgl.activity_id
INNER JOIN hybrid_games hg ON hgl.game_id = hg.id
WHERE (mla.name LIKE '%hybrid_game_started%' 
       OR mla.name LIKE '%hybrid_mcq_started%' 
       OR mla.name LIKE '%hybrid_game_completed%' 
       OR mla.name LIKE '%hybrid_mcq_completed%')
  AND hgl.activity_id IS NOT NULL
  AND DATE_ADD(mllva.server_time, INTERVAL 330 MINUTE) >= '2025-07-02'
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
    # Remove duplicates on idlink_va as requested (DISTINCT in SQL should handle this, but doing it here as well for safety)
    initial_count = len(df)
    df = df.drop_duplicates(subset=['idlink_va'], keep='first')
    if initial_count != len(df):
        print(f"Removed {initial_count - len(df)} duplicate idlink_va records")
    return df


def fetch_score_dataframe() -> pd.DataFrame:
    """Fetch data for score distribution analysis using hybrid_games and hybrid_games_links tables"""
    print("Fetching score data using hybrid_games and hybrid_games_links tables...")
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
            cur.execute(SCORE_DISTRIBUTION_QUERY)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    print(f"SUCCESS: Fetched {len(df)} records from score distribution query")
    return df


def parse_correct_selections_questions(custom_dim_1, game_name):
    """Parse correctSelections structure to extract question correctness (for "This or That" games)"""
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        data = json.loads(custom_dim_1)
        
        # Check for roundDetails structure
        if 'roundDetails' in data and isinstance(data['roundDetails'], list):
            for round_detail in data['roundDetails']:
                if 'roundNumber' in round_detail:
                    question_num = round_detail['roundNumber']
                    cards = round_detail.get('cards', [])
                    selections = round_detail.get('selections', [])
                    
                    if cards and selections:
                        # Find the correct card (status = true)
                        correct_card_index = None
                        for idx, card in enumerate(cards):
                            if card.get('status') is True:
                                correct_card_index = idx
                                break
                        
                        # Get selected card
                        selected_card_index = None
                        if selections and len(selections) > 0:
                            selected_card_index = selections[0].get('card')
                        
                        # Determine correctness
                        is_correct = (selected_card_index is not None and 
                                    correct_card_index is not None and 
                                    selected_card_index == correct_card_index)
                        
                        results.append({
                            'question_number': question_num,
                            'is_correct': 1 if is_correct else 0,
                            'game_name': game_name
                        })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError) as e:
        return results


def parse_flow_stop_go_questions(custom_dim_1, game_name):
    """Parse flow stop&go structure to extract question correctness (for "Flow Stop & Go" games)"""
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        data = json.loads(custom_dim_1)
        
        # Check for gameData structure with Action section
        if 'gameData' in data and isinstance(data['gameData'], list):
            for game_data in data['gameData']:
                if game_data.get('section') == 'Action' and 'jsonData' in game_data:
                    json_data = game_data['jsonData']
                    
                    if isinstance(json_data, list):
                        for level_data in json_data:
                            if 'level' in level_data and 'flow' in level_data and level_data.get('flow') == 'stop&Go':
                                question_num = level_data.get('level', 1)
                                user_responses = level_data.get('userResponse', [])
                                
                                if user_responses and isinstance(user_responses, list):
                                    # Get the first response
                                    response = user_responses[0] if len(user_responses) > 0 else {}
                                    is_correct = response.get('isCorrect', False)
                                    
                                    results.append({
                                        'question_number': question_num,
                                        'is_correct': 1 if is_correct else 0,
                                        'game_name': game_name
                                    })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError) as e:
        return results


def parse_action_level_questions(custom_dim_1, game_name, level_number):
    """Parse action_level structure to extract question correctness (for "Action Level" games)"""
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        data = json.loads(custom_dim_1)
        
        # Check for options and chosenOption structure
        if 'options' in data and 'chosenOption' in data:
            chosen_option = data.get('chosenOption')
            
            if chosen_option is None:
                is_correct = 0
            else:
                options = data.get('options', [])
                if isinstance(options, list) and 0 <= chosen_option < len(options):
                    chosen_option_data = options[chosen_option]
                    if isinstance(chosen_option_data, dict):
                        is_correct = 1 if chosen_option_data.get('isCorrect', False) else 0
                    else:
                        is_correct = 0
                else:
                    is_correct = 0
            
            results.append({
                'question_number': level_number,
                'is_correct': is_correct,
                'game_name': game_name
            })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError) as e:
        return results


def get_game_type(game_name):
    """Map game name to its processing type"""
    game_type_mapping = {
        # correctSelections games
        'Relational Comparison': 'correctSelections',
        'Quantitative Comparison': 'correctSelections',
        'Relational Comparison II': 'correctSelections',
        'Number Comparison': 'correctSelections',
        'Primary Emotion Labelling': 'correctSelections',
        'Emotion Identification': 'correctSelections',
        'Identification of all emotions': 'correctSelections',
        'Beginning Sound Pa Cha Sa': 'correctSelections',
        
        # flow games
        'Revision Primary Colors': 'flow',
        'Revision Primary Shapes': 'flow',
        'Rhyming Words': 'flow',
        
        # action level games
        'Shape Circle': 'action_level',
        'Shape Triangle': 'action_level',
        'Shape Square': 'action_level',
        'Shape Rectangle': 'action_level',
        'Color Red': 'action_level',
        'Color Yellow': 'action_level',
        'Color Blue': 'action_level',
        'Numbers I': 'action_level',
        'Numbers II': 'action_level',
        'Numerals 1-10': 'action_level',
        'Beginning Sound Ma Ka La': 'action_level',
        'Beginning Sound Ba Ra Na': 'action_level',
    }
    return game_type_mapping.get(game_name, None)


def fetch_question_correctness_data() -> pd.DataFrame:
    """Fetch question correctness data using three separate queries"""
    print("Fetching question correctness data from three queries...")
    all_results = []
    
    # Helper function to execute a single query
    def execute_query(query, query_name):
        """Execute a single query and return DataFrame, handling errors gracefully"""
        try:
            with pymysql.connect(
                host=HOST,
                port=PORT,
                user=USER,
                password=PASSWORD,
                database=DBNAME,
                connect_timeout=30,  # Increased timeout
                read_timeout=300,     # 5 minutes for large queries
                write_timeout=300,
                ssl={'ssl': {}},
            ) as conn:
                with conn.cursor() as cur:
                    print(f"  Executing {query_name}...")
                    cur.execute(query)
                    rows = cur.fetchall()
                    cols = [d[0] for d in cur.description]
                    df = pd.DataFrame(rows, columns=cols)
                    print(f"  {query_name} returned {len(df)} records")
                    return df
        except Exception as e:
            print(f"  ERROR: {query_name} failed: {str(e)}")
            print(f"  Continuing with other queries...")
            return pd.DataFrame()
    
    # Execute each query separately with error handling
    df_score_1 = execute_query(QUESTION_CORRECTNESS_QUERY_1, "Query 1 (correctSelections games)")
    df_score_2 = execute_query(QUESTION_CORRECTNESS_QUERY_2, "Query 2 (flow games)")
    df_score_3 = execute_query(QUESTION_CORRECTNESS_QUERY_3, "Query 3 (action_level games)")
    
    # Process Query 1: correctSelections games
    if not df_score_1.empty:
        print("  Processing Query 1 results...")
        if 'game_name' not in df_score_1.columns:
            print("  WARNING: game_name column missing from Query 1 results")
        else:
            processed_count = 0
            for _, row in df_score_1.iterrows():
                try:
                    game_name = row['game_name']
                    custom_dim_1 = row['custom_dimension_1']
                    game_type = get_game_type(game_name)
                    
                    if game_type == 'correctSelections':
                        questions = parse_correct_selections_questions(custom_dim_1, game_name)
                        for q in questions:
                            all_results.append({
                                'game_name': q['game_name'],
                                'question_number': q['question_number'],
                                'is_correct': q['is_correct'],
                                'idvisitor_converted': row['idvisitor_converted'],
                                'idvisit': row['idvisit']
                            })
                            processed_count += 1
                except Exception as e:
                    print(f"  WARNING: Error processing row in Query 1: {str(e)}")
                    continue
            print(f"  Processed {processed_count} question records from Query 1")
    
    # Process Query 2: flow games
    if not df_score_2.empty:
        print("  Processing Query 2 results...")
        if 'game_name' not in df_score_2.columns:
            print("  WARNING: game_name column missing from Query 2 results")
        else:
            processed_count = 0
            for _, row in df_score_2.iterrows():
                try:
                    game_name = row['game_name']
                    custom_dim_1 = row['custom_dimension_1']
                    game_type = get_game_type(game_name)
                    
                    if game_type == 'flow':
                        questions = parse_flow_stop_go_questions(custom_dim_1, game_name)
                        for q in questions:
                            all_results.append({
                                'game_name': q['game_name'],
                                'question_number': q['question_number'],
                                'is_correct': q['is_correct'],
                                'idvisitor_converted': row['idvisitor_converted'],
                                'idvisit': row['idvisit']
                            })
                            processed_count += 1
                except Exception as e:
                    print(f"  WARNING: Error processing row in Query 2: {str(e)}")
                    continue
            print(f"  Processed {processed_count} question records from Query 2")
    
    # Process Query 3: action_level games
    if not df_score_3.empty:
        print("  Processing Query 3 results...")
        if 'game_name' not in df_score_3.columns:
            print("  WARNING: game_name column missing from Query 3 results")
        else:
            # Filter only action_level_* records
            df_score_3_filtered = df_score_3[df_score_3['name'].str.contains('action_level_', na=False)].copy()
            
            # Extract level number from name
            def extract_level_number(name):
                try:
                    if 'action_level_' in str(name):
                        return int(str(name).split('action_level_')[1])
                    return None
                except:
                    return None
            
            df_score_3_filtered['level_number'] = df_score_3_filtered['name'].apply(extract_level_number)
            
            processed_count = 0
            for _, row in df_score_3_filtered.iterrows():
                try:
                    game_name = row['game_name']
                    custom_dim_1 = row['custom_dimension_1']
                    level_number = row['level_number']
                    
                    if level_number is not None:
                        questions = parse_action_level_questions(custom_dim_1, game_name, level_number)
                        for q in questions:
                            all_results.append({
                                'game_name': q['game_name'],
                                'question_number': q['question_number'],
                                'is_correct': q['is_correct'],
                                'idvisitor_converted': row['idvisitor_converted'],
                                'idvisit': row['idvisit']
                            })
                            processed_count += 1
                except Exception as e:
                    print(f"  WARNING: Error processing row in Query 3: {str(e)}")
                    continue
            print(f"  Processed {processed_count} question records from Query 3")
    
    if not all_results:
        print("WARNING: No question correctness data found after processing all queries")
        return pd.DataFrame(columns=['game_name','question_number','correctness','percent','user_count','total_users'])
    
    # Convert to DataFrame
    print(f"\nAggregating {len(all_results)} question records...")
    results_df = pd.DataFrame(all_results)
    print(f"SUCCESS: Processed {len(results_df)} question correctness records")
    
    # Transform to expected format (with correctness, percent, user_count, total_users)
    # Aggregate by game and question: distinct users per correctness
    total_by_q = (
        results_df
        .groupby(['game_name', 'question_number'])['idvisitor_converted']
        .nunique()
        .reset_index(name='total_users')
    )
    
    # Correct and incorrect distinct users
    agg = (
        results_df
        .groupby(['game_name', 'question_number', 'is_correct'])['idvisitor_converted']
        .nunique()
        .reset_index(name='user_count')
    )
    agg = agg.merge(total_by_q, on=['game_name', 'question_number'], how='left')
    agg['percent'] = (agg['user_count'] / agg['total_users'].where(agg['total_users'] > 0, 1) * 100).round(2)
    agg['correctness'] = agg['is_correct'].map({1: 'Correct', 0: 'Incorrect'})
    question_correctness_df = agg[['game_name', 'question_number', 'correctness', 'percent', 'user_count', 'total_users']]
    
    print(f"SUCCESS: Final question correctness data: {len(question_correctness_df)} records")
    print(f"  Games: {question_correctness_df['game_name'].nunique()}")
    print(f"  Questions: {question_correctness_df['question_number'].nunique()}")
    return question_correctness_df


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


def extract_per_question_correctness(df_score: pd.DataFrame) -> pd.DataFrame:
    """Extract per-question correctness across games using the unified score query dataset.

    Output columns:
    - game_name: str
    - idvisitor_converted: str/int
    - idvisit: int
    - session_instance: int (for action-level; 1 for others)
    - question_number: int (1-based)
    - is_correct: int (1 correct, 0 incorrect)
    """
    if df_score.empty:
        return pd.DataFrame(columns=[
            'game_name', 'idvisitor_converted', 'idvisit', 'session_instance', 'question_number', 'is_correct'
        ])

    # Ensure expected columns exist
    cols_needed = ['game_name', 'idvisit', 'action_name', 'custom_dimension_1', 'idvisitor_converted', 'server_time']
    for c in cols_needed:
        if c not in df_score.columns:
            return pd.DataFrame(columns=[
                'game_name', 'idvisitor_converted', 'idvisit', 'session_instance', 'question_number', 'is_correct'
            ])

    # Parse timestamps
    df_score = df_score.copy()
    try:
        df_score['server_time'] = pd.to_datetime(df_score['server_time'])
    except Exception:
        pass

    # Target games and mechanics mapping
    MECHANIC_BY_GAME = {
        'Beginning Sound Ba/Ra/Na': 'action_level',
        'Beginning Sounds Ma/Ka/La': 'action_level',
        'Beginning Sounds Pa/Cha/Sa': 'correct_selections',
        'Color Blue': 'action_level',
        'Color Red': 'action_level',
        'Color Yellow': 'action_level',
        'Emotion Identification': 'correct_selections',
        'Identification of all emotions': 'correct_selections',
        'Numbers Comparison': 'correct_selections',
        'Numbers I': 'action_level',
        'Numbers II': 'action_level',
        'Numerals 1-10': 'action_level',
        'Primary Emotion Labelling': 'correct_selections',
        'Quantitative Comparison': 'correct_selections',
        'Relational Comparison': 'correct_selections',
        'Relational Comparison II': 'correct_selections',
        'Revision Colors': 'flow',
        'Revision Shapes': 'flow',
        'Rhyming Words': 'flow',
        'Shape Circle': 'action_level',
        'Shape Rectangle': 'action_level',
        'Shape Square': 'action_level',
        'Shape Triangle': 'action_level',
    }

    # Debug: Show all unique game names in the data
    print("\nDEBUG: All unique game names in df_score:")
    unique_games = sorted(df_score['game_name'].unique().tolist())
    for g in unique_games:
        print(f"  - '{g}'")
    print(f"Total unique games: {len(unique_games)}")

    # Exclude any sorting games, robustly by name match
    df_score = df_score[~df_score['game_name'].astype(str).str.contains('sorting', case=False, na=False)].copy()

    # Create a more flexible matching: case-insensitive and handle variations
    def _find_matching_game(actual_name: str, target_map: dict) -> tuple:
        """Find matching game name from target map, case-insensitive and flexible"""
        actual_lower = str(actual_name).lower().strip()
        for target_name, mech in target_map.items():
            target_lower = str(target_name).lower().strip()
            # Exact match
            if actual_lower == target_lower:
                return (target_name, mech)
            # Partial match (contains)
            if target_lower in actual_lower or actual_lower in target_lower:
                return (target_name, mech)
        return (None, None)
    
    # Create a mapping from actual game names to canonical names
    game_mapping = {}
    for actual_name in df_score['game_name'].unique():
        canonical, mech = _find_matching_game(actual_name, MECHANIC_BY_GAME)
        if canonical:
            game_mapping[actual_name] = (canonical, mech)
            print(f"DEBUG: Mapped '{actual_name}' -> '{canonical}' ({mech})")
    
    print(f"\nDEBUG: Successfully matched {len(game_mapping)} games out of {len(MECHANIC_BY_GAME)} target games")
    unmatched_targets = set(MECHANIC_BY_GAME.keys()) - {v[0] for v in game_mapping.values()}
    if unmatched_targets:
        print(f"DEBUG: Unmatched target games: {sorted(unmatched_targets)}")
    
    # Filter and normalize game names
    df_score = df_score[df_score['game_name'].isin(game_mapping.keys())].copy()
    df_score['game_name_canonical'] = df_score['game_name'].map(lambda x: game_mapping.get(x, (None, None))[0])
    df_score = df_score[df_score['game_name_canonical'].notna()].copy()
    df_score['game_name'] = df_score['game_name_canonical']
    df_score = df_score.drop(columns=['game_name_canonical'])

    # Split by action types
    game_completed_data = df_score[df_score['action_name'].str.contains('game_completed', na=False)].copy()
    action_level_data = df_score[df_score['action_name'].str.contains('action_level', na=False)].copy()

    per_question_rows: list[dict] = []

    # 1) Handle game_completed with correctSelections / roundDetails schema OR flow
    if not game_completed_data.empty:
        for _, row in game_completed_data.iterrows():
            # Only process this record using the intended mechanic for the game
            # Game name is already canonicalized above
            mech = MECHANIC_BY_GAME.get(str(row['game_name']))
            if mech not in ('correct_selections', 'flow'):
                continue
            raw = row.get('custom_dimension_1')
            if pd.isna(raw) or raw in (None, '', 'null'):
                continue
            try:
                obj = json.loads(raw)
            except Exception:
                continue

            # Prefer roundDetails for per-question ("This or That" mechanic) when mapped
            round_details = obj.get('roundDetails') if isinstance(obj, dict) else None
            if mech == 'correct_selections' and isinstance(round_details, list) and len(round_details) > 0:
                for rd in round_details:
                    try:
                        # Determine correct card index
                        correct_index = None
                        cards = rd.get('cards', [])
                        if isinstance(cards, list):
                            for idx, card in enumerate(cards):
                                if isinstance(card, dict) and card.get('status') is True:
                                    correct_index = idx
                                    break

                        # Determine selected card index (use last selection if multiple)
                        selections = rd.get('selections', [])
                        selected_index = None
                        if isinstance(selections, list) and len(selections) > 0:
                            sel = selections[-1]
                            if isinstance(sel, dict) and 'card' in sel:
                                selected_index = sel.get('card')

                        # Compute correctness
                        is_correct = 1 if (correct_index is not None and selected_index is not None and int(selected_index) == int(correct_index)) else 0

                        # Question number from roundNumber, fallback to sequence
                        qn = rd.get('roundNumber')
                        if isinstance(qn, int):
                            question_number = qn
                        else:
                            # Fallback to 1-based index
                            question_number = 1

                        per_question_rows.append({
                            'game_name': row['game_name'],
                            'idvisitor_converted': row['idvisitor_converted'],
                            'idvisit': row['idvisit'],
                            'session_instance': 1,
                            'question_number': int(question_number),
                            'is_correct': int(is_correct)
                        })
                    except Exception:
                        continue
            elif mech == 'flow':
                # Try jsonData structure with userResponse[].isCorrect per level ("Flow Stop & Go" mechanic)
                try:
                    game_data = obj.get('gameData') if isinstance(obj, dict) else None
                    if isinstance(game_data, list):
                        question_idx = 0
                        for gd in game_data:
                            if isinstance(gd, dict) and gd.get('section') == 'Action' and isinstance(gd.get('jsonData'), list):
                                for level in gd['jsonData']:
                                    if not isinstance(level, dict):
                                        continue
                                    # Only consider stop&Go flow where applicable
                                    flow_val = level.get('flow')
                                    # If flow is present, require stop&Go; if flow absent, still process as single question
                                    if flow_val is not None and str(flow_val).lower() != 'stop&go':
                                        continue
                                    # Use first userResponse if list exists
                                    user_resp = level.get('userResponse')
                                    if isinstance(user_resp, list) and len(user_resp) > 0 and isinstance(user_resp[0], dict):
                                        is_corr = 1 if bool(user_resp[0].get('isCorrect')) else 0
                                        question_idx += 1
                                        per_question_rows.append({
                                            'game_name': row['game_name'],
                                            'idvisitor_converted': row['idvisitor_converted'],
                                            'idvisit': row['idvisit'],
                                            'session_instance': 1,
                                            'question_number': int(question_idx),
                                            'is_correct': is_corr
                                        })
                except Exception:
                    pass
            else:
                # If mapping says correct_selections but roundDetails absent, skip
                # If mapping says flow but flow data absent, skip
                continue

    # 2) Handle action_level records (single-question per record) — "Action Level" mechanic
    if not action_level_data.empty:
        # Filter to only games that are action_level
        action_level_games = {g for g, m in MECHANIC_BY_GAME.items() if m == 'action_level'}
        action_level_data = action_level_data[action_level_data['game_name'].isin(action_level_games)].copy()
        # Sort and assign session_instance per (user, game, visit) based on time gaps
        action_level_data = action_level_data.sort_values(['idvisitor_converted', 'game_name', 'idvisit', 'server_time'])

        session_instances = []
        current_session = 1
        prev_user = prev_game = prev_visit = None
        prev_time = None
        for _, r in action_level_data.iterrows():
            user = r['idvisitor_converted']
            game = r['game_name']
            visit = r['idvisit']
            t = r['server_time']
            if user != prev_user or game != prev_game or visit != prev_visit:
                current_session = 1
            elif prev_time is not None:
                try:
                    gap = (t - prev_time).total_seconds()
                    if gap > 300:
                        current_session += 1
                except Exception:
                    pass
            session_instances.append(current_session)
            prev_user, prev_game, prev_visit, prev_time = user, game, visit, t
        action_level_data['session_instance'] = session_instances

        # Prefer explicit level from action_name like "action_level_3"; fallback to sequence per session
        import re
        def _extract_level(action_name: str):
            try:
                m = re.search(r'action_level[_\- ]?(\d+)', str(action_name))
                if m:
                    return int(m.group(1))
            except Exception:
                pass
            return None

        levels = action_level_data['action_name'].apply(_extract_level)
        action_level_data['question_number'] = levels
        # Fallback numbering where level not found
        mask_missing = action_level_data['question_number'].isna()
        if mask_missing.any():
            action_level_data.loc[mask_missing, 'question_number'] = (
                action_level_data[mask_missing]
                .groupby(['idvisitor_converted', 'game_name', 'idvisit', 'session_instance'])
                .cumcount() + 1
            )

        # Compute correctness per record
        def _safe_action_score(x: str) -> int:
            try:
                return int(parse_custom_dimension_1_action_games(x))
            except Exception:
                return 0

        action_level_data['is_correct'] = action_level_data['custom_dimension_1'].apply(_safe_action_score)

        for _, r in action_level_data.iterrows():
            per_question_rows.append({
                'game_name': r['game_name'],
                'idvisitor_converted': r['idvisitor_converted'],
                'idvisit': r['idvisit'],
                'session_instance': int(r['session_instance']),
                'question_number': int(r['question_number']),
                'is_correct': int(r['is_correct'])
            })

    if not per_question_rows:
        return pd.DataFrame(columns=[
            'game_name', 'idvisitor_converted', 'idvisit', 'session_instance', 'question_number', 'is_correct'
        ])

    return pd.DataFrame.from_records(per_question_rows)


def calculate_score_distribution_combined(df_score):
    """Calculate score distribution using the new unified query with hybrid_games table"""
    print("Processing score distribution data...")
    
    if df_score.empty:
        print("WARNING: No score distribution data found")
        return pd.DataFrame()
    
    # The game_name is now directly available from the hybrid_games table
    # We need to determine the score calculation method based on the action_name
    combined_df = pd.DataFrame()
    
    # Separate data based on action type for different score calculation methods
    game_completed_data = df_score[df_score['action_name'].str.contains('game_completed', na=False)]
    action_level_data = df_score[df_score['action_name'].str.contains('action_level', na=False)]
    
    # Process game_completed data (correctSelections and jsonData games)
    if not game_completed_data.empty:
        print("  - Processing game_completed data...")
        
        # Process each game individually to determine the correct score calculation method
        for game_name in game_completed_data['game_name'].unique():
            game_data = game_completed_data[game_completed_data['game_name'] == game_name].copy()
            
            # Try different score calculation methods and use the one that produces valid results
            # Method 1: correctSelections (for Relational Comparison, Quantity Comparison, etc.)
            game_data['total_score_correct'] = game_data['custom_dimension_1'].apply(parse_custom_dimension_1_correct_selections)
            correct_count = (game_data['total_score_correct'] > 0).sum()
            
            # Method 2: jsonData (for Revision games, Rhyming Words, etc.)
            game_data['total_score_json'] = game_data['custom_dimension_1'].apply(parse_custom_dimension_1_json_data)
            json_count = (game_data['total_score_json'] > 0).sum()
            
            # Choose the method that produces more valid scores
            if correct_count >= json_count and correct_count > 0:
                print(f"    - {game_name}: Using correctSelections method ({correct_count} valid scores)")
                game_data['total_score'] = game_data['total_score_correct']
            elif json_count > 0:
                print(f"    - {game_name}: Using jsonData method ({json_count} valid scores)")
                game_data['total_score'] = game_data['total_score_json']
            else:
                print(f"    - {game_name}: No valid scores found, skipping")
                continue
            
            # Filter out zero scores and add to combined data
            game_data = game_data[game_data['total_score'] > 0]
            if not game_data.empty:
                combined_df = pd.concat([combined_df, game_data], ignore_index=True)
    
    # Process action_level data (action games) - HANDLE MULTIPLE GAME SESSIONS PER VISIT
    if not action_level_data.empty:
        print("  - Processing action_level games...")
        
        # Parse each record to get individual question scores (0 or 1)
        action_level_data = action_level_data.copy()
        action_level_data['question_score'] = action_level_data['custom_dimension_1'].apply(parse_custom_dimension_1_action_games)
        
        # CRITICAL: Handle multiple game sessions per user+game+visit
        # Sort by user, game, visit, then by server_time to track session order
        action_level_data = action_level_data.sort_values(['idvisitor_converted', 'game_name', 'idvisit', 'server_time'])
        
        # Create session_instance to handle multiple plays of same game
        session_instances = []
        current_session = 1
        prev_user = None
        prev_game = None
        prev_visit = None
        prev_time = None
        
        for _, row in action_level_data.iterrows():
            user = row['idvisitor_converted']
            game = row['game_name']  # Use game_name instead of custom_dimension_2
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
        
        action_level_data['session_instance'] = session_instances
        
        # Group by user, game, visit, and session_instance
        action_level_grouped = action_level_data.groupby(['idvisitor_converted', 'game_name', 'idvisit', 'session_instance'])['question_score'].sum().reset_index()
        action_level_grouped.columns = ['idvisitor_converted', 'game_name', 'idvisit', 'session_instance', 'total_score']
        
        # CRITICAL: Cap the total_score at 12 (max possible for one game session)
        action_level_grouped['total_score'] = action_level_grouped['total_score'].clip(upper=12)
        
        # Only include sessions with total_score > 0
        action_level_grouped = action_level_grouped[action_level_grouped['total_score'] > 0]
        combined_df = pd.concat([combined_df, action_level_grouped], ignore_index=True)
    
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
    
    # Ensure all funnel stages exist (fill missing with 0)
    # Order: started, introduction, mid_introduction, parent_poll, validation, rewards, questions, completed
    all_events = pd.DataFrame({'Event': ['started', 'introduction', 'mid_introduction', 'parent_poll', 'validation', 'rewards', 'questions', 'completed']})
    grouped = all_events.merge(grouped, on='Event', how='left').fillna(0)
    
    # Convert to int and sort
    for col in ['Users', 'Visits', 'Instances']:
        grouped[col] = grouped[col].astype(int)
    
    grouped['Event'] = pd.Categorical(grouped['Event'], 
                                     categories=['started', 'introduction', 'mid_introduction', 'parent_poll', 'validation', 'rewards', 'questions', 'completed'], 
                                     ordered=True)
    grouped = grouped.sort_values('Event')
    
    print(f"SUCCESS: Summary statistics: {len(grouped)} event types")
    return grouped


def preprocess_time_series_data_instances(df_instances: pd.DataFrame) -> pd.DataFrame:
    """DEPRECATED: Preprocess time series data for instances only - using created_at and distinct id counts
    This function is no longer used. Instances are now calculated from idlink_va in the combined query."""
    print("Preprocessing time series data for instances...")
    
    if df_instances.empty:
        print("WARNING: No instances data to process")
        return pd.DataFrame(columns=['period_label', 'game_name', 'instances', 'period_type'])
    
    # Convert created_at to datetime
    df_instances['created_at'] = pd.to_datetime(df_instances['created_at'])
    
    # Filter data to only include records from July 2nd, 2025 onwards
    july_2_2025 = pd.Timestamp('2025-07-02')
    df_instances = df_instances[df_instances['created_at'] >= july_2_2025].copy()
    print(f"Filtered instances data to July 2nd, 2025 onwards: {len(df_instances)} records")
    
    if df_instances.empty:
        print("WARNING: No instances data after filtering")
        return pd.DataFrame(columns=['period_label', 'game_name', 'instances', 'period_type'])
    
    # Get unique games
    unique_games = df_instances['game_name'].unique()
    print(f"Processing time series for {len(unique_games)} games")
    
    # Prepare time series data for different periods
    time_series_data = []
    
    # Process each game individually + "All Games" combined
    games_to_process = list(unique_games) + ['All Games']
    
    for game_name in games_to_process:
        if game_name == 'All Games':
            game_df = df_instances.copy()
        else:
            game_df = df_instances[df_instances['game_name'] == game_name].copy()
        
        if game_df.empty:
            continue
            
        print(f"  Processing time series for: {game_name}")
    
        # Daily aggregation - format: YYYY-MM-DD
        game_df['date'] = game_df['created_at'].dt.date
        if game_name == 'All Games':
            # For "All Games", aggregate across all games (don't group by game_name)
            daily_agg = game_df.groupby('date')['id'].nunique().reset_index()
            daily_agg.columns = ['period_label', 'instances']
            daily_agg['period_label'] = daily_agg['period_label'].astype(str)
            daily_agg['game_name'] = 'All Games'
        else:
            # For individual games, group by date and game_name
            daily_agg = game_df.groupby(['date', 'game_name'])['id'].nunique().reset_index()
            daily_agg.columns = ['period_label', 'game_name', 'instances']
            daily_agg['period_label'] = daily_agg['period_label'].astype(str)
        daily_agg['period_type'] = 'Day'
        time_series_data.extend(daily_agg.to_dict('records'))
        
        # Monthly aggregation - format: YYYY_MM (underscore, not hyphen)
        game_df['year'] = game_df['created_at'].dt.year
        game_df['month'] = game_df['created_at'].dt.month
        game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['month'].astype(str).str.zfill(2)
        if game_name == 'All Games':
            # For "All Games", aggregate across all games
            monthly_agg = game_df.groupby('period_label')['id'].nunique().reset_index()
            monthly_agg.columns = ['period_label', 'instances']
            monthly_agg['game_name'] = 'All Games'
        else:
            # For individual games, group by period_label and game_name
            monthly_agg = game_df.groupby(['period_label', 'game_name'])['id'].nunique().reset_index()
            monthly_agg.columns = ['period_label', 'game_name', 'instances']
        monthly_agg['period_type'] = 'Month'
        time_series_data.extend(monthly_agg.to_dict('records'))
            
        # Weekly aggregation - starts from Wednesday, format: YYYY_WW
        # Reset game_df to original for weekly processing
        if game_name == 'All Games':
            game_df = df_instances.copy()
        else:
            game_df = df_instances[df_instances['game_name'] == game_name].copy()
        # Shift date by -2 days before calculating week number (so Wednesday becomes Monday)
        game_df['shifted_date'] = game_df['created_at'] - pd.Timedelta(days=2)
        game_df['year'] = game_df['shifted_date'].dt.year
        # Use strftime('%W') which calculates week number with Monday as first day of week
        # This matches MySQL's WEEK() function behavior
        game_df['week'] = game_df['shifted_date'].dt.strftime('%W').astype(int)
        game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['week'].astype(str).str.zfill(2)
        
        if game_name == 'All Games':
            # For "All Games", aggregate across all games
            weekly_agg = game_df.groupby('period_label')['id'].nunique().reset_index()
            weekly_agg.columns = ['period_label', 'instances']
            weekly_agg['game_name'] = 'All Games'
        else:
            # For individual games, group by period_label and game_name
            weekly_agg = game_df.groupby(['period_label', 'game_name'])['id'].nunique().reset_index()
            weekly_agg.columns = ['period_label', 'game_name', 'instances']
        weekly_agg['period_type'] = 'Week'
        time_series_data.extend(weekly_agg.to_dict('records'))
            
    time_series_df = pd.DataFrame(time_series_data)
    print(f"SUCCESS: Time series instances data: {len(time_series_df)} records")
    print(f"  Daily records: {len(time_series_df[time_series_df['period_type'] == 'Day'])}")
    print(f"  Weekly records: {len(time_series_df[time_series_df['period_type'] == 'Week'])}")
    print(f"  Monthly records: {len(time_series_df[time_series_df['period_type'] == 'Month'])}")
    
    return time_series_df


def preprocess_time_series_data_visits_users(df_visits_users: pd.DataFrame) -> pd.DataFrame:
    """Preprocess time series data for instances, visits and users - using server_time
    Calculates Started and Completed separately for each metric"""
    print("Preprocessing time series data for instances, visits and users (with Started/Completed)...")
    
    if df_visits_users.empty:
        print("WARNING: No time series data to process")
        return pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type'])
    
    # Convert server_time to datetime
    df_visits_users['server_time'] = pd.to_datetime(df_visits_users['server_time'])
    
    # Filter out NULL events
    df_visits_users = df_visits_users[df_visits_users['event'].notna()].copy()
    
    # Filter data to only include records from July 2nd, 2025 onwards
    july_2_2025 = pd.Timestamp('2025-07-02')
    df_visits_users = df_visits_users[df_visits_users['server_time'] >= july_2_2025].copy()
    print(f"Filtered time series data to July 2nd, 2025 onwards: {len(df_visits_users)} records")
    
    if df_visits_users.empty:
        print("WARNING: No time series data after filtering")
        return pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type'])
    
    # Get unique games
    unique_games = df_visits_users['game_name'].unique()
    print(f"Processing time series for {len(unique_games)} games")
    
    # Prepare time series data for different periods
    time_series_data = []
    
    # Process each game individually + "All Games" combined
    games_to_process = list(unique_games) + ['All Games']
    
    for game_name in games_to_process:
        if game_name == 'All Games':
            game_df = df_visits_users.copy()
        else:
            game_df = df_visits_users[df_visits_users['game_name'] == game_name].copy()
        
        if game_df.empty:
            continue
            
        print(f"  Processing time series for: {game_name}")
    
        # Daily aggregation - format: YYYY-MM-DD
        game_df['date'] = game_df['server_time'].dt.date
        for period_type, group_col in [('Day', 'date'), ('Month', None), ('Week', None)]:
            if period_type == 'Day':
                group_by_cols = ['date', 'event'] if game_name == 'All Games' else ['date', 'game_name', 'event']
            elif period_type == 'Month':
                game_df['year'] = game_df['server_time'].dt.year
                game_df['month'] = game_df['server_time'].dt.month
                game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['month'].astype(str).str.zfill(2)
                group_by_cols = ['period_label', 'event'] if game_name == 'All Games' else ['period_label', 'game_name', 'event']
            else:  # Week
                game_df['shifted_date'] = game_df['server_time'] - pd.Timedelta(days=2)
                game_df['year'] = game_df['shifted_date'].dt.year
                game_df['week'] = game_df['shifted_date'].dt.strftime('%W').astype(int)
                game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['week'].astype(str).str.zfill(2)
                group_by_cols = ['period_label', 'event'] if game_name == 'All Games' else ['period_label', 'game_name', 'event']
            
            # Aggregate by event type (Started/Completed) for each metric
            if period_type == 'Day':
                agg_df = game_df.groupby(group_by_cols).agg({
                    'idlink_va': 'nunique',      # Instances
                    'idvisit': 'nunique',        # Visits
                    'idvisitor_converted': 'nunique'  # Users
                }).reset_index()
                if game_name == 'All Games':
                    agg_df.columns = ['period_label', 'event', 'instances', 'visits', 'users']
                    agg_df['game_name'] = 'All Games'
                else:
                    agg_df.columns = ['period_label', 'game_name', 'event', 'instances', 'visits', 'users']
                agg_df['period_label'] = agg_df['period_label'].astype(str)
            else:
                agg_df = game_df.groupby(group_by_cols).agg({
                    'idlink_va': 'nunique',      # Instances
                    'idvisit': 'nunique',        # Visits
                    'idvisitor_converted': 'nunique'  # Users
                }).reset_index()
                if game_name == 'All Games':
                    agg_df.columns = ['period_label', 'event', 'instances', 'visits', 'users']
                    agg_df['game_name'] = 'All Games'
                else:
                    agg_df.columns = ['period_label', 'game_name', 'event', 'instances', 'visits', 'users']
            
            # Reshape to long format: one row per metric-event combination
            for metric in ['instances', 'visits', 'users']:
                metric_df = agg_df[['period_label', 'game_name', 'event', metric]].copy()
                metric_df.columns = ['period_label', 'game_name', 'event', 'count']
                metric_df['metric'] = metric
                metric_df['period_type'] = period_type
                time_series_data.extend(metric_df.to_dict('records'))
    
    time_series_df = pd.DataFrame(time_series_data)
    print(f"SUCCESS: Time series data (with Started/Completed): {len(time_series_df)} records")
    print(f"  Daily records: {len(time_series_df[time_series_df['period_type'] == 'Day'])}")
    print(f"  Weekly records: {len(time_series_df[time_series_df['period_type'] == 'Week'])}")
    print(f"  Monthly records: {len(time_series_df[time_series_df['period_type'] == 'Month'])}")
    
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


def process_main_data() -> pd.DataFrame:
    """Process main dashboard data and game conversion numbers"""
    print("\n" + "=" * 60)
    print("PROCESSING: Main Dashboard Data")
    print("=" * 60)
    
    df_main = fetch_dataframe()
    if df_main.empty:
        print("ERROR: No main data found.")
        return pd.DataFrame()
    
    print(f"SUCCESS: Fetched {len(df_main)} records from main query")
    # Remove duplicates on idlink_va as requested
    initial_count = len(df_main)
    df_main = df_main.drop_duplicates(subset=['idlink_va'], keep='first')
    print(f"After removing duplicates on idlink_va: {len(df_main)} records (removed {initial_count - len(df_main)} duplicates)")
    df_main['date'] = pd.to_datetime(df_main['server_time']).dt.date
    
    # Save main data
    df_main.to_csv('data/processed_data.csv', index=False)
    print("SUCCESS: Saved data/processed_data.csv")
    
    # Create and save game-specific conversion numbers
    # Track all funnel stages for each game
    game_conversion_data = []
    for game in df_main['game_name'].unique():
        if game != 'Unknown Game':
            game_data = df_main[df_main['game_name'] == game]
            
            # Calculate metrics for each funnel stage
            funnel_stages = ['started', 'introduction', 'mid_introduction', 'parent_poll', 'validation', 'rewards', 'questions', 'completed']
            game_stats = {'game_name': game}
            
            for stage in funnel_stages:
                stage_data = game_data[game_data['event'] == stage]
                game_stats[f'{stage}_users'] = stage_data['idvisitor_converted'].nunique()
                game_stats[f'{stage}_visits'] = stage_data['idvisit'].nunique()
                game_stats[f'{stage}_instances'] = len(stage_data)
            
            game_conversion_data.append(game_stats)
    
    game_conversion_df = pd.DataFrame(game_conversion_data)
    game_conversion_df.to_csv('data/game_conversion_numbers.csv', index=False)
    print("SUCCESS: Saved data/game_conversion_numbers.csv")
    
    return df_main


def process_summary_data(df_main: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Process summary statistics"""
    print("\n" + "=" * 60)
    print("PROCESSING: Summary Statistics")
    print("=" * 60)
    
    if df_main is None:
        print("Loading main data from CSV...")
        df_main = pd.read_csv('data/processed_data.csv')
        df_main['server_time'] = pd.to_datetime(df_main['server_time'])
        df_main['date'] = pd.to_datetime(df_main['server_time']).dt.date
    
    summary_df = build_summary(df_main)
    summary_df.to_csv('data/summary_data.csv', index=False)
    print(f"SUCCESS: Saved data/summary_data.csv ({len(summary_df)} records)")
    
    return summary_df


def process_score_distribution() -> pd.DataFrame:
    """Process score distribution data"""
    print("\n" + "=" * 60)
    print("PROCESSING: Score Distribution")
    print("=" * 60)
    
    df_score = fetch_score_dataframe()
    score_distribution_df = calculate_score_distribution_combined(df_score)
    
    if not score_distribution_df.empty:
        score_distribution_df.to_csv('data/score_distribution_data.csv', index=False)
        print(f"SUCCESS: Saved data/score_distribution_data.csv ({len(score_distribution_df)} records)")
    else:
        print("WARNING: No score distribution data to save")
    
    return score_distribution_df


def process_time_series(df_main: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Process time series data for instances, visits, and users
    All metrics are calculated from a single query using idlink_va for instances"""
    print("\n" + "=" * 60)
    print("PROCESSING: Time Series Data (Instances, Visits, Users)")
    print("=" * 60)
    
    # Fetch time series data from database (single query for all metrics)
    print("Fetching time series data from database...")
    df_time_series = pd.DataFrame()
    try:
        with pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            connect_timeout=30,
            read_timeout=300,
            write_timeout=300,
            ssl={'ssl': {}},
        ) as conn:
            with conn.cursor() as cur:
                print("  Executing time series query...")
                cur.execute(TIME_SERIES_QUERY)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                df_time_series = pd.DataFrame(rows, columns=cols)
                print(f"  Time series query returned {len(df_time_series)} records")
    except Exception as e:
        print(f"  ERROR: Failed to fetch time series data: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Process time series data (instances, visits, users all from same query)
    time_series_df = pd.DataFrame()
    if not df_time_series.empty:
        try:
            time_series_df = preprocess_time_series_data_visits_users(df_time_series)
        except Exception as e:
            print(f"WARNING: Failed to process time series data: {str(e)}")
            import traceback
            traceback.print_exc()
            time_series_df = pd.DataFrame()
    else:
        print("WARNING: No time series data to process")
        time_series_df = pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type'])
    
    # Sort by period_type, game_name, and period_label
    time_series_df = time_series_df.sort_values(['period_type', 'game_name', 'period_label'])
    
    # Save to CSV
    if not time_series_df.empty:
        time_series_df.to_csv('data/time_series_data.csv', index=False)
        print(f"SUCCESS: Saved data/time_series_data.csv ({len(time_series_df)} records)")
        print(f"  Columns: {list(time_series_df.columns)}")
        print(f"  Sample row: {time_series_df.iloc[0].to_dict() if len(time_series_df) > 0 else 'N/A'}")
    else:
        print("WARNING: No time series data to save")
        empty_df = pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type'])
        empty_df.to_csv('data/time_series_data.csv', index=False)
    
    return time_series_df


def process_repeatability(df_main: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Process repeatability data"""
    print("\n" + "=" * 60)
    print("PROCESSING: Repeatability Data")
    print("=" * 60)
    
    repeatability_df = fetch_hybrid_repeatability_data()
    
    # Fallback to Matomo data if hybrid data fails
    if repeatability_df.empty:
        print("Using Matomo data as fallback...")
        if df_main is None:
            print("Loading main data from CSV...")
            df_main = pd.read_csv('data/processed_data.csv')
            df_main['server_time'] = pd.to_datetime(df_main['server_time'])
        repeatability_df = preprocess_repeatability_data(df_main)
        
        if not repeatability_df.empty:
            repeatability_df.to_csv('data/repeatability_data.csv', index=False)
            print(f"SUCCESS: Saved data/repeatability_data.csv ({len(repeatability_df)} records)")
        else:
            print("WARNING: No repeatability data to save")

    return repeatability_df


def process_parent_poll() -> pd.DataFrame:
    """Process parent poll responses data from Excel file (NOT from database)"""
    import sys
    
    print("\n" + "=" * 60, flush=True)
    print("PROCESSING: Parent Poll Responses", flush=True)
    print("=" * 60, flush=True)
    print("NOTE: Reading from Excel file, NOT from database", flush=True)
    
    # Read poll data from Excel file
    excel_file = 'poll_responses_raw_data.xlsx'
    print(f"\n[STEP 1] Reading parent poll data from Excel file: {excel_file}", flush=True)
    print("  This step reads the Excel file into memory...", flush=True)
    
    try:
        # Read entire Excel file
        print("  [ACTION] Starting to read Excel file (this may take a moment for large files)...", flush=True)
        sys.stdout.flush()  # Force flush
        
        df_poll = pd.read_excel(excel_file)
        
        print(f"  [SUCCESS] Excel file loaded successfully!", flush=True)
        print(f"  Total records loaded: {len(df_poll):,}", flush=True)
        sys.stdout.flush()
            
    except FileNotFoundError:
        print(f"  ERROR: File '{excel_file}' not found")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    except Exception as e:
        print(f"  ERROR: Failed to read Excel file: {str(e)}")
        import traceback
        traceback.print_exc()
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    if df_poll.empty:
        print("WARNING: No parent poll data found in Excel file")
        # Create empty dataframe with expected headers
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    # Ensure required columns exist
    if 'custom_dimension_1' not in df_poll.columns:
        print("ERROR: 'custom_dimension_1' column not found in Excel file")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    if 'game_name' not in df_poll.columns:
        print("ERROR: 'game_name' column not found in Excel file")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    print(f"\n[STEP 2] Validating data structure...", flush=True)
    print(f"  Available columns: {list(df_poll.columns)}", flush=True)
    
    # Process each record
    processed_records = []
    debug_count = 0
    skipped_no_json = 0
    skipped_no_structure = 0
    records_with_poll_items = 0
    total_poll_items_found = 0
    encoding_errors = 0
    
    print(f"\n[STEP 3] Processing {len(df_poll):,} poll records...", flush=True)
    print(f"  This step extracts poll responses from JSON in custom_dimension_1 column", flush=True)
    print(f"  Progress will be shown every 10,000 records...", flush=True)
    sys.stdout.flush()
    
    for idx, row in df_poll.iterrows():
        try:
            custom_dim_1 = row.get('custom_dimension_1')
            game_name = row.get('game_name')
            idvisit = row.get('idvisit')
            
            # Progress indicator
            if (idx + 1) % 10000 == 0:
                print(f"\n    [PROGRESS] {idx + 1:,}/{len(df_poll):,} records processed", flush=True)
                print(f"      - Records with poll items: {records_with_poll_items:,}", flush=True)
                print(f"      - Total poll responses extracted: {total_poll_items_found:,}", flush=True)
                print(f"      - Skipped (no JSON): {skipped_no_json:,}", flush=True)
                print(f"      - Skipped (no structure): {skipped_no_structure:,}", flush=True)
                sys.stdout.flush()
            
            # Parse JSON from custom_dimension_1
            if not custom_dim_1 or pd.isna(custom_dim_1):
                skipped_no_json += 1
                if debug_count < 3:
                    print(f"    [SKIP] Record {idx+1}: custom_dimension_1 is empty or NaN")
                    debug_count += 1
                continue
            
            try:
                poll_data = json.loads(custom_dim_1)
            except json.JSONDecodeError as e:
                skipped_no_json += 1
                if debug_count < 3:
                    print(f"    [SKIP] Record {idx+1}: JSON decode error - {str(e)[:50]}")
                    debug_count += 1
                continue
            
            if not isinstance(poll_data, dict):
                skipped_no_structure += 1
                if debug_count < 3:
                    print(f"    [SKIP] Record {idx+1}: poll_data is not a dict (type: {type(poll_data)})")
                    debug_count += 1
                continue
            
            # Look for poll data in the JSON structure
            # Poll data can be:
            # 1. At root level in a 'poll' key
            # 2. In gameData array (nested structure)
            # 3. At root level with options/chosenOption
            
            poll_items = []
            
            # Check for 'poll' key at root
            if 'poll' in poll_data:
                poll_section = poll_data.get('poll')
                if isinstance(poll_section, list):
                    poll_items.extend(poll_section)
                    if debug_count < 2:
                        print(f"    [FOUND] Record {idx+1}: 'poll' key at root (list with {len(poll_section)} items)")
                elif isinstance(poll_section, dict):
                    poll_items.append(poll_section)
                    if debug_count < 2:
                        print(f"    [FOUND] Record {idx+1}: 'poll' key at root (dict)")
            
            # Check if root has poll-like structure
            if 'options' in poll_data and 'chosenOption' in poll_data:
                poll_items.append(poll_data)
                if debug_count < 2:
                    print(f"    [FOUND] Record {idx+1}: Poll structure at root level")
            
            # Search through gameData array for poll responses
            # IMPORTANT: Only extract from "Poll" section, not from "Action" section
            game_data = poll_data.get('gameData', [])
            if isinstance(game_data, list):
                game_data_poll_count = 0
                for game_item in game_data:
                    if not isinstance(game_item, dict):
                        continue
                    
                    # Check if this is the "Poll" section
                    section = game_item.get('section', '')
                    if section.lower() == 'poll':
                        # This is the Poll section - extract poll questions from here
                        nested_game_data = game_item.get('gameData', [])
                        if isinstance(nested_game_data, list):
                            for question_idx, nested_item in enumerate(nested_game_data):
                                if isinstance(nested_item, dict) and 'options' in nested_item and 'chosenOption' in nested_item:
                                    # Add question number (1, 2, or 3) to the poll item
                                    nested_item['_poll_question_number'] = question_idx + 1
                                    poll_items.append(nested_item)
                                    game_data_poll_count += 1
                
                if game_data_poll_count > 0 and debug_count < 2:
                    print(f"    [FOUND] Record {idx+1}: Found {game_data_poll_count} poll items in Poll section")
            
            # If no poll items found, skip this record
            if not poll_items:
                skipped_no_structure += 1
                if debug_count < 5:
                    root_keys = list(poll_data.keys())[:10]
                    print(f"    [SKIP] Record {idx+1} ({game_name}): No poll structure found. Root keys: {root_keys}")
                    debug_count += 1
                continue
            
            # Track records with poll items
            records_with_poll_items += 1
            total_poll_items_found += len(poll_items)
            
            if debug_count < 2:
                print(f"    [PROCESSING] Record {idx+1} ({game_name}): Found {len(poll_items)} poll items")
            
            # Process each poll item found
            for poll_item_idx, poll_item in enumerate(poll_items):
                if not isinstance(poll_item, dict):
                    continue
                
                options = poll_item.get('options', [])
                chosen_option = poll_item.get('chosenOption')
                
                if isinstance(options, list) and len(options) > 0 and chosen_option is not None:
                    try:
                        chosen_option_idx = int(chosen_option)
                        if 0 <= chosen_option_idx < len(options):
                            selected_option = options[chosen_option_idx]
                            # Try different possible fields for option text
                            # Handle encoding issues by using safe string conversion
                            try:
                                # First try to get message field
                                message_field = selected_option.get('message', '')
                                
                                # If message is a dict (with language codes), extract a readable value
                                if isinstance(message_field, dict):
                                    # Try to get English first, then any available language
                                    option_message = (
                                        message_field.get('en', '') or
                                        message_field.get('en_US', '') or
                                        message_field.get('en_IN', '') or
                                        # If no English, get the first available value
                                        (list(message_field.values())[0] if message_field else '')
                                    )
                                elif message_field:
                                    option_message = message_field
                                else:
                                    # Fallback to other fields
                                    option_message = (
                                        selected_option.get('text', '') or 
                                        selected_option.get('label', '') or
                                        str(selected_option.get('path', '')) or
                                        str(selected_option.get('id', '')) or
                                        f"Option {chosen_option_idx + 1}"
                                    )
                                
                                # Ensure it's a string and handle encoding
                                if isinstance(option_message, bytes):
                                    option_message = option_message.decode('utf-8', errors='replace')
                                else:
                                    option_message = str(option_message)
                                
                                # Clean up the message - remove extra whitespace
                                option_message = option_message.strip()
                                
                                # If still empty or looks like a dict string, use option number
                                if not option_message or option_message.startswith('{') or option_message.startswith('['):
                                    option_message = f"Option {chosen_option_idx + 1}"
                                
                                # Replace any problematic characters for display
                                try:
                                    option_message = option_message.encode('utf-8', errors='replace').decode('utf-8')
                                except:
                                    option_message = f"Option {chosen_option_idx + 1}"
                            except Exception as e:
                                encoding_errors += 1
                                # If all else fails, use a safe representation
                                option_message = f"Option_{chosen_option_idx}"
                                if debug_count < 3:
                                    print(f"      [ENCODING ERROR] Record {idx+1}, Poll Item {poll_item_idx+1}: {str(e)[:50]}")
                                    debug_count += 1
                            
                            if option_message:
                                # Get question number from poll item (1, 2, or 3)
                                question_number = poll_item.get('_poll_question_number')
                                
                                # If we have a question number, use it
                                if question_number:
                                    question_text = f"Question {question_number}"
                                else:
                                    # Fallback: try to get from poll_item fields
                                    question_text = (
                                        poll_item.get('question', '') or 
                                        poll_item.get('questionText', '') or
                                        poll_item.get('questionId', '') or
                                        poll_item.get('question_id', '')
                                    )
                                    
                                    # If still no question text, use a generic identifier
                                    if not question_text:
                                        question_text = "Question (unknown)"
                                
                                processed_records.append({
                                    'game_name': game_name,
                                    'question': question_text,
                                    'option': option_message
                                })
                    except (ValueError, IndexError, TypeError):
                        continue
                
        except Exception as e:
            print(f"  WARNING: Error processing poll record {idx+1}: {str(e)}")
            if debug_count < 3:
                import traceback
                traceback.print_exc()
                debug_count += 1
            continue
    
    print(f"\n[STEP 4] Processing Summary:", flush=True)
    print(f"    - Total records processed: {len(df_poll):,}", flush=True)
    print(f"    - Records with poll items: {records_with_poll_items:,}", flush=True)
    print(f"    - Total poll items found: {total_poll_items_found:,}", flush=True)
    print(f"    - Skipped (no JSON): {skipped_no_json:,}", flush=True)
    print(f"    - Skipped (no poll structure): {skipped_no_structure:,}", flush=True)
    print(f"    - Encoding errors handled: {encoding_errors:,}", flush=True)
    print(f"    - Valid poll responses extracted: {len(processed_records):,}", flush=True)
    sys.stdout.flush()
    
    if not processed_records:
        print("\n  WARNING: No valid poll responses found after processing")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    # Convert to DataFrame
    print(f"\n[STEP 5] Converting to DataFrame...", flush=True)
    results_df = pd.DataFrame(processed_records)
    print(f"    Created DataFrame with {len(results_df)} rows", flush=True)
    
    # Aggregate: count all responses per game, question, and option
    # This counts all responses (including multiple responses from the same user across different game plays)
    print(f"[STEP 6] Aggregating responses by game, question, and option...", flush=True)
    sys.stdout.flush()
    agg_df = (
        results_df
        .groupby(['game_name', 'question', 'option'])
        .size()
        .reset_index(name='count')
    )
    
    # Sort for consistent output
    agg_df = agg_df.sort_values(['game_name', 'question', 'option'])
    
    print(f"\n[STEP 7] Final Aggregation Summary:", flush=True)
    print(f"    - Unique records: {len(agg_df):,}", flush=True)
    print(f"    - Games: {agg_df['game_name'].nunique()}", flush=True)
    print(f"    - Questions: {agg_df['question'].nunique()}", flush=True)
    print(f"    - Total response count: {agg_df['count'].sum():,}", flush=True)
    
    # Show sample of games
    if len(agg_df) > 0:
        print(f"\n  Games with poll data:", flush=True)
        game_counts = agg_df.groupby('game_name')['count'].sum().sort_values(ascending=False)
        for game, count in game_counts.head(10).items():
            print(f"    - {game}: {count:,} responses", flush=True)
        if len(game_counts) > 10:
            print(f"    ... and {len(game_counts) - 10} more games", flush=True)
    
    # Save to CSV
    print(f"\n[STEP 8] Saving to data/poll_responses_data.csv...", flush=True)
    agg_df.to_csv('data/poll_responses_data.csv', index=False)
    print(f"  [SUCCESS] Saved data/poll_responses_data.csv ({len(agg_df)} records)", flush=True)
    sys.stdout.flush()
    
    return agg_df


def process_question_correctness() -> pd.DataFrame:
    """Process question correctness data"""
    print("\n" + "=" * 60)
    print("PROCESSING: Question Correctness Data")
    print("=" * 60)
    
    question_correctness_df = fetch_question_correctness_data()
    
    if question_correctness_df.empty:
        # Create empty dataframe with expected headers
        question_correctness_df = pd.DataFrame(columns=['game_name','question_number','correctness','percent','user_count','total_users'])
        print("WARNING: No question correctness data found")

        # Always write the CSV (even if empty) so the dashboard can load gracefully
        question_correctness_df.to_csv('data/question_correctness_data.csv', index=False)
    print(f"SUCCESS: Saved data/question_correctness_data.csv ({len(question_correctness_df)} records)")
    
    return question_correctness_df


def update_metadata(df_main: Optional[pd.DataFrame] = None):
    """Update metadata JSON file"""
    print("\n" + "=" * 60)
    print("UPDATING: Metadata")
    print("=" * 60)
    
    # Try to load existing metadata or create new one
    metadata_file = 'data/metadata.json'
    metadata = {}
    
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
    
    # Load data files to get current record counts
    record_counts = {}
    
    if df_main is not None:
        record_counts['main_data_records'] = len(df_main)
        record_counts['data_date_range'] = {
                'start': str(df_main['server_time'].min()),
                'end': str(df_main['server_time'].max())
            }
    else:
        if os.path.exists('data/processed_data.csv'):
            df_check = pd.read_csv('data/processed_data.csv')
            record_counts['main_data_records'] = len(df_check)
            if 'server_time' in df_check.columns:
                df_check['server_time'] = pd.to_datetime(df_check['server_time'])
                record_counts['data_date_range'] = {
                    'start': str(df_check['server_time'].min()),
                    'end': str(df_check['server_time'].max())
                }
    
    # Update record counts for each CSV
    for csv_file, key in [
        ('data/summary_data.csv', 'summary_records'),
        ('data/score_distribution_data.csv', 'score_distribution_records'),
        ('data/time_series_data.csv', 'time_series_records'),
        ('data/repeatability_data.csv', 'repeatability_records'),
        ('data/question_correctness_data.csv', 'question_correctness_records'),
        ('data/poll_responses_data.csv', 'poll_responses_records'),
    ]:
        if os.path.exists(csv_file):
            df_check = pd.read_csv(csv_file)
            record_counts[key] = len(df_check)
        else:
            record_counts[key] = 0
    
    # Update metadata
    metadata.update({
        'preprocessing_date': datetime.now().isoformat(),
        **record_counts
    })
    
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print("SUCCESS: Saved data/metadata.json")
        

def main():
    """Main preprocessing function with modular processing options"""
    parser = argparse.ArgumentParser(
        description='Preprocess data for Matomo Events Dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all visuals (default behavior)
  python preprocess_data.py --all

  # Process only score distribution
  python preprocess_data.py --score-distribution

  # Process only question correctness
  python preprocess_data.py --question-correctness

  # Process only parent poll responses
  python preprocess_data.py --parent-poll

  # Process multiple visuals
  python preprocess_data.py --time-series --repeatability

Available visuals:
  --main               Main dashboard data and game conversion numbers
  --summary            Summary statistics
  --score-distribution Score distribution
  --time-series        Time series data
  --repeatability      Repeatability data
  --question-correctness Question correctness data
  --parent-poll        Parent poll responses data
  --all                Process all visuals (default if no flags provided)
  --metadata           Update metadata file
        """
    )
    
    parser.add_argument('--main', action='store_true', help='Process main dashboard data')
    parser.add_argument('--summary', action='store_true', help='Process summary statistics')
    parser.add_argument('--score-distribution', action='store_true', help='Process score distribution')
    parser.add_argument('--time-series', action='store_true', help='Process time series data')
    parser.add_argument('--repeatability', action='store_true', help='Process repeatability data')
    parser.add_argument('--question-correctness', action='store_true', help='Process question correctness data')
    parser.add_argument('--parent-poll', action='store_true', help='Process parent poll responses data')
    parser.add_argument('--all', action='store_true', help='Process all visuals (default)')
    parser.add_argument('--metadata', action='store_true', help='Update metadata file')
    
    args = parser.parse_args()
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Determine what to process
    process_all = args.all or not any([
        args.main, args.summary, args.score_distribution,
        args.time_series, args.repeatability, args.question_correctness, args.parent_poll
    ])
    
    if process_all:
        print("=" * 60)
        print("PROCESSING ALL VISUALS")
        print("=" * 60)
        print("\nNote: Use specific flags to process individual visuals:")
        print("  python preprocess_data.py --question-correctness")
        print("  python preprocess_data.py --score-distribution --time-series")
        print("  See --help for more options.\n")
    
    try:
        df_main = None
        
        # Process main data if requested or if processing all
        if args.main or process_all:
            df_main = process_main_data()
        
        # Process summary if requested or if processing all
        if args.summary or process_all:
            process_summary_data(df_main)
        
        # Process score distribution if requested or if processing all
        if args.score_distribution or process_all:
            process_score_distribution()
        
        # Process time series if requested or if processing all
        if args.time_series or process_all:
            process_time_series(df_main)
        
        # Process repeatability if requested or if processing all
        if args.repeatability or process_all:
            process_repeatability(df_main)
        
        # Process question correctness if requested or if processing all
        if args.question_correctness or process_all:
            process_question_correctness()
        
        # Process parent poll if requested or if processing all
        if args.parent_poll or process_all:
            process_parent_poll()
        
        # Update metadata if requested or if processing all
        if args.metadata or process_all:
            update_metadata(df_main)
        
        print("\n" + "=" * 60)
        print("PREPROCESSING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("All processed data saved to 'data/' directory")
        print("Ready for deployment to Render!")
        print("\nNext steps:")
        print("1. Commit and push the updated data/ directory to GitHub")
        print("2. Render will automatically redeploy with the latest data")
        
    except Exception as e:
        print(f"\nERROR during preprocessing: {str(e)}")
        import traceback
        traceback.print_exc()
        print("Please check your database connection and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
