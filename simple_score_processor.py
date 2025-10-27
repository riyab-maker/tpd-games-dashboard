#!/usr/bin/env python3
"""
Simple Score Distribution Data Processor (No Emojis for Windows Compatibility)
"""

import os
import json
import pandas as pd
import pymysql
from datetime import datetime
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Database configuration
HOST = os.getenv('DB_HOST')
PORT = int(os.getenv('DB_PORT', 3306))
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
DATABASE = os.getenv('DB_NAME')

def parse_custom_dimension_1_correct_selections(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract correctSelections (for first query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        data = json.loads(custom_dim_1)
        if isinstance(data, dict) and 'correctSelections' in data:
            return int(data['correctSelections'])
        return 0
    except (json.JSONDecodeError, ValueError, TypeError, KeyError):
        return 0

def parse_custom_dimension_1_json_data(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from jsonData (for second query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        data = json.loads(custom_dim_1)
        if isinstance(data, dict) and 'jsonData' in data:
            json_data = data['jsonData']
            if isinstance(json_data, dict) and 'totalScore' in json_data:
                return int(json_data['totalScore'])
        return 0
    except (json.JSONDecodeError, ValueError, TypeError, KeyError):
        return 0

def parse_custom_dimension_1_action_games(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from action games (for third query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        data = json.loads(custom_dim_1)
        if isinstance(data, dict) and 'action_level' in data:
            action_level = data['action_level']
            if isinstance(action_level, dict) and 'totalScore' in action_level:
                return int(action_level['totalScore'])
        return 0
    except (json.JSONDecodeError, ValueError, TypeError, KeyError):
        return 0

def fetch_score_data() -> pd.DataFrame:
    """Fetch score distribution data from database"""
    print("Fetching score distribution data...")
    
    sql_query = """
    SELECT 
      `matomo_log_link_visit_action`.`custom_dimension_1`,
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
      END AS game_name
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
    
    try:
        with pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            ssl={'ssl': {}},
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
        
        df = pd.DataFrame(rows, columns=columns)
        print(f"SUCCESS: Fetched {len(df)} records from database")
        return df
        
    except Exception as e:
        print(f"ERROR: Error fetching data: {e}")
        sys.exit(1)

def process_score_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Process score distribution data"""
    print("Processing score distribution data...")
    
    # Parse scores from custom_dimension_1
    df['correct_selections'] = df['custom_dimension_1'].apply(parse_custom_dimension_1_correct_selections)
    df['json_data_score'] = df['custom_dimension_1'].apply(parse_custom_dimension_1_json_data)
    df['action_level_score'] = df['custom_dimension_1'].apply(parse_custom_dimension_1_action_games)
    
    # Combine all scores
    df['score'] = df[['correct_selections', 'json_data_score', 'action_level_score']].max(axis=1)
    
    # Filter out zero scores and unknown games
    df = df[(df['score'] > 0) & (df['game_name'] != 'Unknown Game')]
    
    # Create score ranges
    df['score_range'] = pd.cut(
        df['score'], 
        bins=[0, 20, 40, 60, 80, 100], 
        labels=['0-20', '21-40', '41-60', '61-80', '81-100'],
        include_lowest=True
    )
    
    # Group by game and score range
    score_distribution = df.groupby(['game_name', 'score_range']).size().reset_index(name='count')
    
    # Pivot to get score ranges as columns
    pivot_df = score_distribution.pivot_table(
        index='game_name', 
        columns='score_range', 
        values='count', 
        fill_value=0
    ).reset_index()
    
    # Flatten column names
    pivot_df.columns.name = None
    
    print(f"SUCCESS: Score distribution: {len(pivot_df)} records")
    return pivot_df

def save_processed_data(score_distribution: pd.DataFrame):
    """Save processed data to CSV file"""
    print("Saving processed data...")
    
    # Ensure processed_data directory exists
    os.makedirs('processed_data', exist_ok=True)
    
    # Save score distribution data
    file_path = 'processed_data/score_distribution.csv'
    score_distribution.to_csv(file_path, index=False)
    print(f"SUCCESS: Saved score distribution to {file_path}")
    
    # Print sample data for verification
    print("\nScore Distribution Data (first 10 rows):")
    print(score_distribution.head(10).to_string(index=False))
    
    # Print summary statistics
    total_games = score_distribution['game_name'].nunique()
    print(f"\nSummary:")
    print(f"  Total Games: {total_games}")
    print(f"  Records: {len(score_distribution)}")

def main():
    """Main processing function"""
    print("Starting Score Distribution Data Processing...")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Fetch score data
        df = fetch_score_data()
        
        # Process score distribution
        score_distribution = process_score_distribution(df)
        
        # Save processed data
        save_processed_data(score_distribution)
        
        print(f"\nScore distribution processing completed successfully!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"ERROR: Error in score distribution processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
