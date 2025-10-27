#!/usr/bin/env python3
"""
Score Distribution Data Processor

This script processes raw data to generate score distribution metrics:
- Score distribution for different game types (correctSelections, jsonData, action_level)
- Game-specific score distributions
- Saves lightweight CSV files for dashboard consumption

Performance optimized for deployment - no heavy processing on Render.
"""

import os
import json
import pandas as pd
import pymysql
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Tuple
import sys

# Load environment variables
load_dotenv()

# Database configuration
HOST = os.getenv('DB_HOST')
PORT = int(os.getenv('DB_PORT', 3306))
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
DATABASE = os.getenv('DB_NAME')

# Validate required environment variables
required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

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

def fetch_score_data_1() -> pd.DataFrame:
    """Fetch data for score distribution analysis (correctSelections games)"""
    print("🔄 Fetching score data (correctSelections games)...")
    
    sql_query = """
    SELECT 
      `matomo_log_link_visit_action`.`idlink_va`,
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
      DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) AS server_time,
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
        print(f"✅ Fetched {len(df)} records from score query 1")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching score data 1: {e}")
        return pd.DataFrame()

def fetch_score_data_2() -> pd.DataFrame:
    """Fetch data for score distribution analysis (jsonData games)"""
    print("🔄 Fetching score data (jsonData games)...")
    
    sql_query = """
    SELECT 
      `matomo_log_link_visit_action`.`idlink_va`,
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
      DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) AS server_time,
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
        print(f"✅ Fetched {len(df)} records from score query 2")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching score data 2: {e}")
        return pd.DataFrame()

def fetch_score_data_3() -> pd.DataFrame:
    """Fetch data for score distribution analysis (action_level games)"""
    print("🔄 Fetching score data (action_level games)...")
    
    sql_query = """
    SELECT 
      `matomo_log_link_visit_action`.`idlink_va`,
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
      DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) AS server_time,
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
        print(f"✅ Fetched {len(df)} records from score query 3")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching score data 3: {e}")
        return pd.DataFrame()

def process_score_distribution_combined(df_score_1, df_score_2, df_score_3) -> pd.DataFrame:
    """Calculate combined score distribution for all three query datasets"""
    print("📊 Processing score distribution data...")
    
    combined_df = pd.DataFrame()
    
    # Process correctSelections data
    if not df_score_1.empty:
        print("  📈 Processing correctSelections scores...")
        df_score_1['score'] = df_score_1['custom_dimension_1'].apply(parse_custom_dimension_1_correct_selections)
        df_score_1['score_type'] = 'correctSelections'
        combined_df = pd.concat([combined_df, df_score_1[['game_name', 'score', 'score_type']]], ignore_index=True)
    
    # Process jsonData scores
    if not df_score_2.empty:
        print("  📈 Processing jsonData scores...")
        df_score_2['score'] = df_score_2['custom_dimension_1'].apply(parse_custom_dimension_1_json_data)
        df_score_2['score_type'] = 'jsonData'
        combined_df = pd.concat([combined_df, df_score_2[['game_name', 'score', 'score_type']]], ignore_index=True)
    
    # Process action_level scores
    if not df_score_3.empty:
        print("  📈 Processing action_level scores...")
        df_score_3['score'] = df_score_3['custom_dimension_1'].apply(parse_custom_dimension_1_action_games)
        df_score_3['score_type'] = 'action_level'
        combined_df = pd.concat([combined_df, df_score_3[['game_name', 'score', 'score_type']]], ignore_index=True)
    
    # Filter out zero scores and unknown games
    combined_df = combined_df[(combined_df['score'] > 0) & (combined_df['game_name'] != 'Unknown Game')]
    
    # Create score ranges
    combined_df['score_range'] = pd.cut(
        combined_df['score'], 
        bins=[0, 20, 40, 60, 80, 100], 
        labels=['0-20', '21-40', '41-60', '61-80', '81-100'],
        include_lowest=True
    )
    
    # Group by game, score type, and score range
    score_distribution = combined_df.groupby(['game_name', 'score_type', 'score_range']).size().reset_index(name='count')
    
    # Pivot to get score ranges as columns
    pivot_df = score_distribution.pivot_table(
        index=['game_name', 'score_type'], 
        columns='score_range', 
        values='count', 
        fill_value=0
    ).reset_index()
    
    # Flatten column names
    pivot_df.columns.name = None
    
    print(f"✅ Score distribution: {len(pivot_df)} records")
    return pivot_df

def save_processed_data(score_distribution: pd.DataFrame):
    """Save processed data to CSV file"""
    print("💾 Saving processed data...")
    
    # Ensure processed_data directory exists
    os.makedirs('processed_data', exist_ok=True)
    
    # Save score distribution data
    file_path = 'processed_data/score_distribution.csv'
    score_distribution.to_csv(file_path, index=False)
    print(f"✅ Saved score distribution to {file_path}")
    
    # Print sample data for verification
    print("\n📊 Score Distribution Data:")
    print(score_distribution.head(10).to_string(index=False))
    
    # Print summary statistics
    total_games = score_distribution['game_name'].nunique()
    total_score_types = score_distribution['score_type'].nunique()
    print(f"\n📈 Summary:")
    print(f"  Total Games: {total_games}")
    print(f"  Score Types: {total_score_types}")
    print(f"  Records: {len(score_distribution)}")

def main():
    """Main processing function"""
    print("🚀 Starting Score Distribution Data Processing...")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Fetch all score data
        df_score_1 = fetch_score_data_1()
        df_score_2 = fetch_score_data_2()
        df_score_3 = fetch_score_data_3()
        
        # Process combined score distribution
        score_distribution = process_score_distribution_combined(df_score_1, df_score_2, df_score_3)
        
        # Save processed data
        save_processed_data(score_distribution)
        
        print(f"\n✅ Score distribution processing completed successfully!")
        print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ Error in score distribution processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
