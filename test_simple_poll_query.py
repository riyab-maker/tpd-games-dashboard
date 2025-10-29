#!/usr/bin/env python3
"""
Simple test query to check poll data structure
"""

import os
import json
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

# Optimized test query with game name filtering
SIMPLE_POLL_QUERY = """
SELECT 
  hg.game_name,
  mllva.idvisit,
  mla.name AS action_name,
  mllva.custom_dimension_1
FROM hybrid_games hg
INNER JOIN hybrid_games_links hgl ON hg.id = hgl.game_id
INNER JOIN matomo_log_link_visit_action mllva ON hgl.activity_id = mllva.custom_dimension_2
INNER JOIN matomo_log_action mla ON mllva.idaction_name = mla.idaction
WHERE mla.name LIKE '%game_completed%'
  AND mllva.custom_dimension_1 LIKE '%Poll%'
  AND hg.game_name IN ('Color Red', 'Color Yellow', 'Color Blue', 'Shape Circle', 'Shape Triangle', 'Quantity Comparison', 'Relational Comparison', 'Numeracy I', 'Numeracy II')
LIMIT 10
"""

def test_simple_poll_query():
    """Test simple poll query to understand data structure"""
    print("🔍 Testing simple poll query...")
    
    try:
        # Connect to database
        conn = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DBNAME,
            charset='utf8mb4'
        )
        
        with conn.cursor() as cur:
            print("📊 Executing simple poll query...")
            cur.execute(SIMPLE_POLL_QUERY)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]
            
        conn.close()
        
        if not rows:
            print("❌ No data found with the simple query")
            return
            
        df = pd.DataFrame(rows, columns=columns)
        print(f"✅ Found {len(df)} records")
        print("\n📋 Sample data:")
        print(df.head())
        
        print("\n🔍 Examining custom_dimension_1 content:")
        for i, row in df.iterrows():
            print(f"\n--- Record {i+1} ---")
            print(f"Game Name: {row['game_name']}")
            print(f"ID Visit: {row['idvisit']}")
            print(f"Action Name: {row['action_name']}")
            print(f"Custom Dimension 1: {row['custom_dimension_1'][:200]}...")  # First 200 chars
            
            # Try to parse JSON
            try:
                data = json.loads(row['custom_dimension_1'])
                print("✅ JSON is valid")
                
                # Look for Poll section
                if isinstance(data, dict) and 'Poll' in data:
                    print("✅ Found 'Poll' section!")
                    poll_data = data['Poll']
                    print(f"Poll data: {poll_data}")
                else:
                    print("❌ No 'Poll' section found")
                    print(f"Available keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing error: {e}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_simple_poll_query()
