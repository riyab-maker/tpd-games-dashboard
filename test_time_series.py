#!/usr/bin/env python3
import os
import pandas as pd
import pymysql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection settings
HOST = os.getenv("DB_HOST")
PORT = int(os.getenv("DB_PORT", "3310"))
DBNAME = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

# Test queries with LIMIT
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
LIMIT 100
"""

INSTANCES_QUERY = """
SELECT 
  hgc.created_at,
  hgc.id
FROM hybrid_game_completions hgc
WHERE hgc.created_at > '2025-07-02'
LIMIT 100
"""

def test_time_series_processing():
    print("Testing time series processing...")
    
    try:
        connection = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            charset='utf8mb4'
        )
        print("✅ Database connection successful")
        
        # Fetch visits/users data
        print("Fetching visits/users data...")
        df_visits_users = pd.read_sql(VISITS_USERS_QUERY, connection)
        print(f"✅ Fetched {len(df_visits_users)} visits/users records")
        print(f"Columns: {df_visits_users.columns.tolist()}")
        print(f"Sample data:\n{df_visits_users.head()}")
        
        # Fetch instances data
        print("\nFetching instances data...")
        df_instances = pd.read_sql(INSTANCES_QUERY, connection)
        print(f"✅ Fetched {len(df_instances)} instances records")
        print(f"Columns: {df_instances.columns.tolist()}")
        print(f"Sample data:\n{df_instances.head()}")
        
        connection.close()
        
        # Test the time series processing function
        print("\nTesting time series processing function...")
        import preprocess_data
        time_series_df = preprocess_data.preprocess_time_series_data_new(df_visits_users, df_instances)
        print(f"✅ Processed time series data: {len(time_series_df)} records")
        print(f"Columns: {time_series_df.columns.tolist()}")
        print(f"Sample data:\n{time_series_df.head()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_time_series_processing()

