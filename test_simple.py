#!/usr/bin/env python3
print("Starting test...")

try:
    print("Testing import...")
    import os
    print("✅ os imported")
    
    import pandas as pd
    print("✅ pandas imported")
    
    import pymysql
    print("✅ pymysql imported")
    
    from dotenv import load_dotenv
    print("✅ dotenv imported")
    
    print("Testing database connection...")
    load_dotenv()
    
    HOST = os.getenv("DB_HOST")
    print(f"Host: {HOST}")
    
    connection = pymysql.connect(
        host=HOST,
        port=3310,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        charset='utf8mb4'
    )
    
    print("✅ Connected to database")
    
    # Test simple query
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM hybrid_games")
        result = cursor.fetchone()
        print(f"✅ Found {result[0]} games")
    
    connection.close()
    print("✅ Connection closed")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("Test completed")
