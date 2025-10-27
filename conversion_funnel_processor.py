#!/usr/bin/env python3
"""
Conversion Funnel Data Processor

This script processes raw data to generate conversion funnel metrics:
- Total numbers (all games combined)
- Game-specific numbers
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

def _distinct_count_ignore_blank(series):
    """Equivalent to DISTINCTCOUNTNOBLANK in Power BI"""
    return series.dropna().nunique()

def fetch_conversion_data() -> pd.DataFrame:
    """Fetch conversion funnel data from database"""
    print("Fetching conversion funnel data...")
    
    # SQL Query for conversion funnel data
    sql_query = """
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
        print(f"✅ Fetched {len(df)} records from database")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        sys.exit(1)

def process_total_conversion_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Process total conversion metrics (all games combined)"""
    print("📊 Processing total conversion metrics...")
    
    # Group by event and compute distinct counts using DISTINCTCOUNTNOBLANK logic
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
    
    print(f"✅ Total metrics: {len(grouped)} event types")
    return grouped

def process_game_specific_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Process game-specific conversion metrics"""
    print("🎮 Processing game-specific conversion metrics...")
    
    # Get unique games
    unique_games = df['game_name'].unique()
    print(f"📋 Found {len(unique_games)} unique games")
    
    all_game_metrics = []
    
    for game in unique_games:
        if game == 'Unknown Game':
            continue
            
        # Filter data for this game
        game_data = df[df['game_name'] == game]
        
        # Calculate metrics for this game
        game_grouped = game_data.groupby('event').agg({
            'idvisitor_converted': _distinct_count_ignore_blank,
            'idvisit': _distinct_count_ignore_blank, 
            'idlink_va': _distinct_count_ignore_blank,
        })
        
        # Rename columns
        game_grouped.columns = ['Users', 'Visits', 'Instances']
        game_grouped = game_grouped.reset_index()
        game_grouped.rename(columns={'event': 'Event'}, inplace=True)
        
        # Add game name
        game_grouped['game_name'] = game
        
        # Ensure both Started and Completed exist
        all_events = pd.DataFrame({'Event': ['Started', 'Completed']})
        game_grouped = all_events.merge(game_grouped, on='Event', how='left').fillna(0)
        
        # Convert to int
        for col in ['Users', 'Visits', 'Instances']:
            game_grouped[col] = game_grouped[col].astype(int)
        
        all_game_metrics.append(game_grouped)
    
    # Combine all game metrics
    combined_df = pd.concat(all_game_metrics, ignore_index=True)
    
    # Sort by game name and event
    combined_df = combined_df.sort_values(['game_name', 'Event'])
    
    print(f"✅ Game-specific metrics: {len(combined_df)} records for {len(unique_games)} games")
    return combined_df

def save_processed_data(total_metrics: pd.DataFrame, game_metrics: pd.DataFrame):
    """Save processed data to CSV files"""
    print("💾 Saving processed data...")
    
    # Ensure processed_data directory exists
    os.makedirs('processed_data', exist_ok=True)
    
    # Save total metrics
    total_file = 'processed_data/conversion_funnel_total.csv'
    total_metrics.to_csv(total_file, index=False)
    print(f"✅ Saved total metrics to {total_file}")
    
    # Save game-specific metrics
    game_file = 'processed_data/conversion_funnel_games.csv'
    game_metrics.to_csv(game_file, index=False)
    print(f"✅ Saved game metrics to {game_file}")
    
    # Print sample data for verification
    print("\n📊 Total Conversion Metrics:")
    print(total_metrics.to_string(index=False))
    
    print(f"\n🎮 Sample Game Metrics (first 10 rows):")
    print(game_metrics.head(10).to_string(index=False))

def main():
    """Main processing function"""
    print("Starting Conversion Funnel Data Processing...")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Fetch raw data
        df = fetch_conversion_data()
        
        # Process total metrics
        total_metrics = process_total_conversion_metrics(df)
        
        # Process game-specific metrics
        game_metrics = process_game_specific_metrics(df)
        
        # Save processed data
        save_processed_data(total_metrics, game_metrics)
        
        print(f"\n✅ Conversion funnel processing completed successfully!")
        print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ Error in conversion funnel processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
