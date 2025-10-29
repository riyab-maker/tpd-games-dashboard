#!/usr/bin/env python3
"""
Implement new Time Series Analysis with exact requirements
"""
import os
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection settings
HOST = os.getenv("DB_HOST")
PORT = int(os.getenv("DB_PORT", "3310"))
DBNAME = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

# Exact queries as specified
VISITS_USERS_QUERY = """
SELECT 
  hybrid_games.game_name,
  matomo_log_link_visit_action.server_time,
  matomo_log_link_visit_action.idvisit,
  matomo_log_link_visit_action.idvisitor
FROM hybrid_games
INNER JOIN hybrid_games_links 
  ON hybrid_games.id = hybrid_games_links.game_id
INNER JOIN matomo_log_link_visit_action 
  ON hybrid_games_links.activity_id = matomo_log_link_visit_action.custom_dimension_2
INNER JOIN matomo_log_visit 
  ON matomo_log_link_visit_action.idvisit = matomo_log_visit.idvisit
WHERE matomo_log_link_visit_action.server_time > '2025-07-02'
"""

INSTANCES_QUERY = """
SELECT 
  hybrid_game_completions.created_at,
  hybrid_game_completions.id
FROM hybrid_game_completions
WHERE created_at > '2025-07-02'
"""

def fetch_visits_users_data():
    """Fetch visits and users data using the exact query"""
    print("Fetching visits and users data...")
    
    connection = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DBNAME,
        charset='utf8mb4'
    )
    
    try:
        df = pd.read_sql(VISITS_USERS_QUERY, connection)
        print(f"✅ Fetched {len(df)} visits/users records")
        return df
    finally:
        connection.close()

def fetch_instances_data():
    """Fetch instances data using the exact query"""
    print("Fetching instances data...")
    
    connection = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DBNAME,
        charset='utf8mb4'
    )
    
    try:
        df = pd.read_sql(INSTANCES_QUERY, connection)
        print(f"✅ Fetched {len(df)} instances records")
        return df
    finally:
        connection.close()

def process_visits_users_data(df):
    """Process visits and users data according to requirements"""
    print("Processing visits and users data...")
    
    # Convert server_time to datetime
    df['server_time'] = pd.to_datetime(df['server_time'])
    
    # Add 5 hours and 30 minutes (5.5 hours = 330 minutes)
    df['adjusted_time'] = df['server_time'] + timedelta(hours=5, minutes=30)
    
    # Convert idvisitor to proper readable format (hex to decimal)
    df['idvisitor_converted'] = df['idvisitor'].apply(lambda x: int(str(x), 16) if pd.notna(x) else 0)
    
    # Extract time components
    df['date'] = df['adjusted_time'].dt.date
    df['week'] = df['adjusted_time'].dt.to_period('W-MON').dt.start_time.dt.date
    df['month'] = df['adjusted_time'].dt.to_period('M').dt.start_time.dt.date
    
    print(f"✅ Processed {len(df)} visits/users records")
    return df

def process_instances_data(df):
    """Process instances data according to requirements"""
    print("Processing instances data...")
    
    # Convert created_at to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    # Extract time components
    df['date'] = df['created_at'].dt.date
    df['week'] = df['created_at'].dt.to_period('W-MON').dt.start_time.dt.date
    df['month'] = df['created_at'].dt.to_period('M').dt.start_time.dt.date
    
    print(f"✅ Processed {len(df)} instances records")
    return df

def aggregate_data(visits_users_df, instances_df):
    """Aggregate data by time periods and create final dataset"""
    print("Aggregating data by time periods...")
    
    all_periods = []
    
    # Process each time period
    for period_name, period_col in [('Daily', 'date'), ('Weekly', 'week'), ('Monthly', 'month')]:
        print(f"Processing {period_name} data...")
        
        # Aggregate visits and users data
        visits_users_agg = visits_users_df.groupby([period_col, 'game_name']).agg({
            'idvisit': 'nunique',  # Visits: SUM(UniqueCountDistinct(idvisit))
            'idvisitor_converted': 'nunique'  # Users: CountDistinct(idvisitor)
        }).reset_index()
        
        visits_users_agg['period_type'] = period_name
        visits_users_agg['time_period'] = visits_users_agg[period_col].astype(str)
        visits_users_agg['visits'] = visits_users_agg['idvisit']
        visits_users_agg['users'] = visits_users_agg['idvisitor_converted']
        
        # Aggregate instances data
        instances_agg = instances_df.groupby([period_col]).agg({
            'id': 'nunique'  # Count distinct id values for completed instances
        }).reset_index()
        
        instances_agg['period_type'] = period_name
        instances_agg['time_period'] = instances_agg[period_col].astype(str)
        instances_agg['instances'] = instances_agg['id']
        
        # Merge visits/users with instances
        merged_df = visits_users_agg.merge(
            instances_agg[['time_period', 'instances']], 
            on='time_period', 
            how='left'
        ).fillna(0)
        
        # Add compatibility columns for existing dashboard
        merged_df['completed_visits'] = merged_df['visits']
        merged_df['completed_users'] = merged_df['users']
        merged_df['completed_instances'] = merged_df['instances']
        merged_df['started_visits'] = 0
        merged_df['started_users'] = 0
        merged_df['started_instances'] = 0
        
        all_periods.append(merged_df)
    
    # Combine all periods
    final_df = pd.concat(all_periods, ignore_index=True)
    
    # Reorder columns
    column_order = [
        'time_period', 'period_type', 'game_name',
        'visits', 'users', 'instances',
        'started_visits', 'started_users', 'started_instances',
        'completed_visits', 'completed_users', 'completed_instances'
    ]
    
    final_df = final_df[column_order]
    
    print(f"✅ Created final dataset with {len(final_df)} records")
    return final_df

def main():
    """Main function to implement new time series analysis"""
    print("🚀 Implementing new Time Series Analysis...")
    print("=" * 60)
    
    try:
        # Fetch data
        print("STEP 1: Fetching data from database")
        print("-" * 40)
        visits_users_df = fetch_visits_users_data()
        instances_df = fetch_instances_data()
        
        if visits_users_df.empty and instances_df.empty:
            print("❌ No data found. Exiting.")
            return
        
        # Process data
        print("\nSTEP 2: Processing data")
        print("-" * 40)
        visits_users_processed = process_visits_users_data(visits_users_df)
        instances_processed = process_instances_data(instances_df)
        
        # Aggregate data
        print("\nSTEP 3: Aggregating data by time periods")
        print("-" * 40)
        final_df = aggregate_data(visits_users_processed, instances_processed)
        
        # Save data
        print("\nSTEP 4: Saving processed data")
        print("-" * 40)
        final_df.to_csv('data/time_series_data.csv', index=False)
        print("✅ Saved data/time_series_data.csv")
        
        # Show results
        print("\nSTEP 5: Results Summary")
        print("-" * 40)
        print(f"Total records: {len(final_df)}")
        print(f"Columns: {final_df.columns.tolist()}")
        print(f"Unique games: {final_df['game_name'].nunique()}")
        print(f"Period types: {final_df['period_type'].unique()}")
        
        print("\nSample data:")
        print(final_df.head(10))
        
        print("\nStatistics:")
        print(f"Total visits: {final_df['visits'].sum()}")
        print(f"Total users: {final_df['users'].sum()}")
        print(f"Total instances: {final_df['instances'].sum()}")
        
        print("\n✅ New Time Series Analysis implementation completed!")
        print("The dashboard is ready to use the updated data.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

