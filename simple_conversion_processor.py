#!/usr/bin/env python3
"""
Simple Conversion Funnel Data Processor (No Emojis for Windows Compatibility)
"""

import os
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

def _distinct_count_ignore_blank(series):
    """Equivalent to DISTINCTCOUNTNOBLANK in Power BI"""
    return series.dropna().nunique()

def fetch_conversion_data() -> pd.DataFrame:
    """Fetch conversion funnel data from database"""
    print("Fetching conversion funnel data...")
    
    # Simple SQL query for testing
    sql_query = """
    SELECT 
      `matomo_log_link_visit_action`.`idlink_va`,
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
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
        print(f"SUCCESS: Fetched {len(df)} records from database")
        return df
        
    except Exception as e:
        print(f"ERROR: Error fetching data: {e}")
        sys.exit(1)

def process_total_conversion_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Process total conversion metrics (all games combined)"""
    print("Processing total conversion metrics...")
    
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
    
    print(f"SUCCESS: Total metrics: {len(grouped)} event types")
    print(f"DEBUG: Total metrics - Started: {grouped[grouped['Event'] == 'Started']['Users'].iloc[0]} users, Completed: {grouped[grouped['Event'] == 'Completed']['Users'].iloc[0]} users")
    return grouped

def process_game_specific_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Process game-specific conversion metrics"""
    print("Processing game-specific conversion metrics...")
    
    # Filter out invalid games first
    df_clean = df[~df['game_name'].isin(['Unknown Game', '0', 0]) & df['game_name'].notna()]
    
    # Get unique games
    unique_games = df_clean['game_name'].unique()
    print(f"Found {len(unique_games)} valid games: {list(unique_games)}")
    
    # Debug: check what games are in the original data
    print(f"Original games: {df['game_name'].unique()}")
    
    all_game_metrics = []
    
    for game in unique_games:
            
        # Filter data for this game
        game_data = df_clean[df_clean['game_name'] == game]
        
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
        
        # Only add if we have valid data (not all zeros)
        if game_grouped['Users'].sum() > 0 or game_grouped['Visits'].sum() > 0 or game_grouped['Instances'].sum() > 0:
            all_game_metrics.append(game_grouped)
    
    # Combine all game metrics
    combined_df = pd.concat(all_game_metrics, ignore_index=True)
    
    # Filter out any rows with game_name = '0' or 0
    combined_df = combined_df[~combined_df['game_name'].isin(['0', 0])]
    
    # Sort by game name and event
    combined_df = combined_df.sort_values(['game_name', 'Event'])
    
    print(f"SUCCESS: Game-specific metrics: {len(combined_df)} records for {len(unique_games)} games")
    
    # Debug: show sum of game-specific metrics
    game_sum = combined_df.groupby('Event').agg({'Users': 'sum', 'Visits': 'sum', 'Instances': 'sum'})
    print(f"DEBUG: Game-specific sum - Started: {game_sum.loc['Started', 'Users']} users, Completed: {game_sum.loc['Completed', 'Users']} users")
    
    return combined_df

def save_processed_data(total_metrics: pd.DataFrame, game_metrics: pd.DataFrame):
    """Save processed data to CSV files"""
    print("Saving processed data...")
    
    # Ensure processed_data directory exists
    os.makedirs('processed_data', exist_ok=True)
    
    # Save total metrics
    total_file = 'processed_data/conversion_funnel_total.csv'
    total_metrics.to_csv(total_file, index=False)
    print(f"SUCCESS: Saved total metrics to {total_file}")
    
    # Save game-specific metrics
    game_file = 'processed_data/conversion_funnel_games.csv'
    game_metrics.to_csv(game_file, index=False)
    print(f"SUCCESS: Saved game metrics to {game_file}")
    
    # Print sample data for verification
    print("\nTotal Conversion Metrics:")
    print(total_metrics.to_string(index=False))
    
    print(f"\nGame-Specific Metrics (first 10 rows):")
    print(game_metrics.head(10).to_string(index=False))
    
    # Print summary by game
    print(f"\nGame Summary (Started vs Completed Users):")
    # Filter out games with name '0' first
    clean_game_metrics = game_metrics[game_metrics['game_name'] != '0']
    
    if not clean_game_metrics.empty:
        # Create a proper summary
        game_summary = []
        for game in clean_game_metrics['game_name'].unique():
            game_data = clean_game_metrics[clean_game_metrics['game_name'] == game]
            started_users = game_data[game_data['Event'] == 'Started']['Users'].iloc[0] if len(game_data[game_data['Event'] == 'Started']) > 0 else 0
            completed_users = game_data[game_data['Event'] == 'Completed']['Users'].iloc[0] if len(game_data[game_data['Event'] == 'Completed']) > 0 else 0
            game_summary.append({'game_name': game, 'Users': f"{started_users} vs {completed_users}"})
        
        summary_df = pd.DataFrame(game_summary)
        print(summary_df.to_string(index=False))
    else:
        print("No valid games found")

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
        
        print(f"\nConversion funnel processing completed successfully!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"ERROR: Error in conversion funnel processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
