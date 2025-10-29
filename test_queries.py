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

# Test queries
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
"""

INSTANCES_QUERY = """
SELECT 
  hgc.created_at,
  hgc.id
FROM hybrid_game_completions hgc
WHERE hgc.created_at > '2025-07-02'
"""

def test_queries():
    print("Testing database queries...")
    
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
        
        # Test visits/users query
        print("\nTesting visits/users query...")
        with connection.cursor() as cursor:
            cursor.execute(VISITS_USERS_QUERY)
            result = cursor.fetchall()
            print(f"✅ Visits/users query returned {len(result)} records")
            if result:
                print(f"Sample record: {result[0]}")
        
        # Test instances query
        print("\nTesting instances query...")
        with connection.cursor() as cursor:
            cursor.execute(INSTANCES_QUERY)
            result = cursor.fetchall()
            print(f"✅ Instances query returned {len(result)} records")
            if result:
                print(f"Sample record: {result[0]}")
        
        connection.close()
        print("\n✅ All queries successful!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_queries()

