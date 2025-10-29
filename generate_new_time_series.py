#!/usr/bin/env python3
"""
Generate new time series data with the updated logic
"""
import os
import pandas as pd
import pymysql
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection settings
HOST = os.getenv("DB_HOST")
PORT = int(os.getenv("DB_PORT", "3310"))
DBNAME = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

# New Time Series Analysis Queries with LIMIT for testing
VISITS_USERS_QUERY = """
SELECT 
  hg.game_name,
  DATE_ADD(mllva.server_time, INTERVAL 330 MINUTE) AS server_time,
  mllva.idvisit,
  CONV(HEX(mllva.idvisitor), 16, 10) AS idvisitor_converted
FROM hybrid_games hg
INNER JOIN hybrid_games_links hgl ON hg.id = hgl.game_id
INNER JOIN matomo_log_link_visit_action mllva ON hgl.activity_id = mllva.custom_dimension_2
INNER JOIN matomo_log_visit mlv ON mllva.idvisit = mlv.idvisit
WHERE mllva.server_time > '2025-07-02'
LIMIT 5000
"""

INSTANCES_QUERY = """
SELECT 
  hgc.created_at,
  hgc.id
FROM hybrid_game_completions hgc
WHERE hgc.created_at > '2025-07-02'
LIMIT 5000
"""

def fetch_data():
    """Fetch data from database"""
    print("Connecting to database...")
    connection = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DBNAME,
        charset='utf8mb4'
    )
    
    try:
        print("Fetching visits and users data...")
        df_visits_users = pd.read_sql(VISITS_USERS_QUERY, connection)
        print(f"✅ Fetched {len(df_visits_users)} visits/users records")
        
        print("Fetching instances data...")
        df_instances = pd.read_sql(INSTANCES_QUERY, connection)
        print(f"✅ Fetched {len(df_instances)} instances records")
        
        return df_visits_users, df_instances
        
    finally:
        connection.close()

def process_time_series_data(df_visits_users, df_instances):
    """Process time series data with new logic"""
    print("Processing time series data...")
    
    # Process visits and users data
    df_visits_users['datetime'] = pd.to_datetime(df_visits_users['server_time'])
    df_visits_users['date'] = df_visits_users['datetime'].dt.date
    
    # Process instances data
    df_instances['datetime'] = pd.to_datetime(df_instances['created_at'])
    df_instances['date'] = df_instances['datetime'].dt.date
    
    print(f"Visits/Users data: {len(df_visits_users)} records")
    print(f"Instances data: {len(df_instances)} records")
    
    # Group by different time periods
    time_periods = []
    
    # Daily aggregation
    print("Processing daily data...")
    daily_visits_users = df_visits_users.groupby(['date', 'game_name']).agg({
        'idvisit': 'nunique',
        'idvisitor_converted': 'nunique'
    }).reset_index()
    daily_visits_users['time_period'] = daily_visits_users['date'].astype(str)
    daily_visits_users['period_type'] = 'Daily'
    daily_visits_users['visits'] = daily_visits_users['idvisit']
    daily_visits_users['users'] = daily_visits_users['idvisitor_converted']
    
    # For instances, we need to count distinct IDs per day
    daily_instances = df_instances.groupby(['date']).agg({
        'id': 'nunique'
    }).reset_index()
    daily_instances['time_period'] = daily_instances['date'].astype(str)
    daily_instances['period_type'] = 'Daily'
    daily_instances['instances'] = daily_instances['id']
    
    # Merge visits/users with instances for daily data
    daily_combined = daily_visits_users.merge(
        daily_instances[['time_period', 'instances']], 
        on='time_period', 
        how='left'
    ).fillna(0)
    time_periods.append(daily_combined)
    
    # Weekly aggregation (Monday to Sunday)
    print("Processing weekly data...")
    df_visits_users['week_start'] = df_visits_users['datetime'].dt.to_period('W-MON').dt.start_time.dt.date
    weekly_visits_users = df_visits_users.groupby(['week_start', 'game_name']).agg({
        'idvisit': 'nunique',
        'idvisitor_converted': 'nunique'
    }).reset_index()
    weekly_visits_users['time_period'] = weekly_visits_users['week_start'].astype(str)
    weekly_visits_users['period_type'] = 'Weekly'
    weekly_visits_users['visits'] = weekly_visits_users['idvisit']
    weekly_visits_users['users'] = weekly_visits_users['idvisitor_converted']
    
    df_instances['week_start'] = df_instances['datetime'].dt.to_period('W-MON').dt.start_time.dt.date
    weekly_instances = df_instances.groupby(['week_start']).agg({
        'id': 'nunique'
    }).reset_index()
    weekly_instances['time_period'] = weekly_instances['week_start'].astype(str)
    weekly_instances['period_type'] = 'Weekly'
    weekly_instances['instances'] = weekly_instances['id']
    
    # Merge for weekly data
    weekly_combined = weekly_visits_users.merge(
        weekly_instances[['time_period', 'instances']], 
        on='time_period', 
        how='left'
    ).fillna(0)
    time_periods.append(weekly_combined)
    
    # Monthly aggregation
    print("Processing monthly data...")
    df_visits_users['month_start'] = df_visits_users['datetime'].dt.to_period('M').dt.start_time.dt.date
    monthly_visits_users = df_visits_users.groupby(['month_start', 'game_name']).agg({
        'idvisit': 'nunique',
        'idvisitor_converted': 'nunique'
    }).reset_index()
    monthly_visits_users['time_period'] = monthly_visits_users['month_start'].astype(str)
    monthly_visits_users['period_type'] = 'Monthly'
    monthly_visits_users['visits'] = monthly_visits_users['idvisit']
    monthly_visits_users['users'] = monthly_visits_users['idvisitor_converted']
    
    df_instances['month_start'] = df_instances['datetime'].dt.to_period('M').dt.start_time.dt.date
    monthly_instances = df_instances.groupby(['month_start']).agg({
        'id': 'nunique'
    }).reset_index()
    monthly_instances['time_period'] = monthly_instances['month_start'].astype(str)
    monthly_instances['period_type'] = 'Monthly'
    monthly_instances['instances'] = monthly_instances['id']
    
    # Merge for monthly data
    monthly_combined = monthly_visits_users.merge(
        monthly_instances[['time_period', 'instances']], 
        on='time_period', 
        how='left'
    ).fillna(0)
    time_periods.append(monthly_combined)
    
    # Combine all time periods
    combined_df = pd.concat(time_periods, ignore_index=True)
    
    # Add completed events columns for compatibility with existing dashboard
    combined_df['completed_visits'] = combined_df['visits']
    combined_df['completed_users'] = combined_df['users']
    combined_df['completed_instances'] = combined_df['instances']
    
    # Add started columns (set to 0 since we're only tracking completed events)
    combined_df['started_visits'] = 0
    combined_df['started_users'] = 0
    combined_df['started_instances'] = 0
    
    print(f"✅ Processed time series data: {len(combined_df)} records")
    return combined_df

def main():
    """Main function"""
    print("🚀 Generating new time series data...")
    
    try:
        print("Step 1: Fetching data...")
        # Fetch data
        df_visits_users, df_instances = fetch_data()
        
        print("Step 2: Processing data...")
        # Process data
        time_series_df = process_time_series_data(df_visits_users, df_instances)
        
        print("Step 3: Saving data...")
        # Save data
        time_series_df.to_csv('data/time_series_data.csv', index=False)
        print("✅ Saved data/time_series_data.csv")
        
        # Show sample data
        print("\nSample data:")
        print(time_series_df.head(10))
        print(f"\nColumns: {time_series_df.columns.tolist()}")
        print(f"Total records: {len(time_series_df)}")
        
        print("✅ Script completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
