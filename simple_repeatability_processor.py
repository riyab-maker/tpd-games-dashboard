#!/usr/bin/env python3
"""
Simple Repeatability Data Processor (No Emojis for Windows Compatibility)
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

def fetch_repeatability_data() -> pd.DataFrame:
    """Fetch repeatability data using the exact SQL query from hybrid database tables"""
    print("Fetching repeatability data from hybrid database...")
    
    try:
        # Connect to database
        connection = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            charset='utf8mb4'
        )
        
        # SQL query for repeatability analysis
        hybrid_query = """
        SELECT 
            COUNT(DISTINCT game_name) as CountDistinctNonNull_game_name,
            COUNT(DISTINCT hybrid_profile_id) as CountDistinct_hybrid_profile_id
        FROM (
            SELECT 
                hg.game_name,
                hgl.hybrid_profile_id
            FROM hybrid_games hg
            JOIN hybrid_games_links hgl ON hg.id = hgl.hybrid_game_id
            JOIN hybrid_game_completions hgc ON hgl.id = hgc.hybrid_game_link_id
            JOIN hybrid_profiles hp ON hgl.hybrid_profile_id = hp.id
            JOIN hybrid_users hu ON hp.hybrid_user_id = hu.id
            WHERE hg.game_name IS NOT NULL
        ) subquery
        GROUP BY hybrid_profile_id
        ORDER BY CountDistinctNonNull_game_name
        """
        
        # Execute query
        hybrid_df = pd.read_sql(hybrid_query, connection)
        connection.close()
        
        print(f"SUCCESS: Fetched {len(hybrid_df)} records from hybrid database")
        return hybrid_df
        
    except Exception as e:
        print(f"ERROR: Error fetching hybrid data: {e}")
        print("Falling back to Matomo data...")
        return fetch_matomo_repeatability_data()

def fetch_matomo_repeatability_data() -> pd.DataFrame:
    """Fallback: Fetch repeatability data from Matomo tables"""
    print("Fetching repeatability data from Matomo...")
    
    # SQL Query for Matomo repeatability data
    sql_query = """
    SELECT 
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
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
        print(f"SUCCESS: Fetched {len(df)} records from Matomo")
        return df
        
    except Exception as e:
        print(f"ERROR: Error fetching Matomo data: {e}")
        sys.exit(1)

def process_repeatability_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process repeatability data"""
    print("Processing repeatability data...")
    
    # Check if we got hybrid data (has specific columns)
    if 'CountDistinctNonNull_game_name' in df.columns:
        # Process hybrid data
        repeatability_data = df.groupby('CountDistinctNonNull_game_name').agg({
            'CountDistinct_hybrid_profile_id': 'sum'
        }).reset_index()
        
        # Rename columns for consistency
        repeatability_data.columns = ['games_played', 'user_count']
        
        # Sort by games played
        repeatability_data = repeatability_data.sort_values('games_played')
        
        print(f"SUCCESS: Hybrid repeatability: {len(repeatability_data)} records")
        return repeatability_data
    else:
        # Process Matomo data
        completed_events = df[df['event'] == 'Completed']
        
        # Group by user and count distinct games played
        user_game_counts = completed_events.groupby('idvisitor_converted').agg({
            'game_name': 'nunique'
        }).reset_index()
        
        user_game_counts.columns = ['hybrid_profile_id', 'games_played']
        
        # Group by number of games played and count users
        repeatability_data = user_game_counts.groupby('games_played').size().reset_index()
        repeatability_data.columns = ['games_played', 'user_count']
        
        # Sort by games played
        repeatability_data = repeatability_data.sort_values('games_played')
        
        print(f"SUCCESS: Matomo repeatability: {len(repeatability_data)} records")
        return repeatability_data

def save_processed_data(repeatability_data: pd.DataFrame):
    """Save processed data to CSV file"""
    print("Saving processed data...")
    
    # Ensure processed_data directory exists
    os.makedirs('processed_data', exist_ok=True)
    
    # Save repeatability data
    file_path = 'processed_data/repeatability_data.csv'
    repeatability_data.to_csv(file_path, index=False)
    print(f"SUCCESS: Saved repeatability data to {file_path}")
    
    # Print sample data for verification
    print("\nRepeatability Data:")
    print(repeatability_data.to_string(index=False))
    
    # Print summary statistics
    total_users = repeatability_data['user_count'].sum()
    max_games = repeatability_data['games_played'].max()
    print(f"\nSummary:")
    print(f"  Total Users: {total_users:,}")
    print(f"  Max Games Played: {max_games}")
    print(f"  Records: {len(repeatability_data)}")

def main():
    """Main processing function"""
    print("Starting Repeatability Data Processing...")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Try to fetch hybrid data first
        df = fetch_repeatability_data()
        
        # Process repeatability data
        repeatability_data = process_repeatability_data(df)
        
        # Save processed data
        save_processed_data(repeatability_data)
        
        print(f"\nRepeatability processing completed successfully!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"ERROR: Error in repeatability processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
