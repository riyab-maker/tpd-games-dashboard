#!/usr/bin/env python3
print("Starting simple test...")

try:
    print("Testing database connection...")
    import os
    import pymysql
    from dotenv import load_dotenv
    
    load_dotenv()
    
    HOST = os.getenv("DB_HOST")
    PORT = int(os.getenv("DB_PORT", "3310"))
    DBNAME = os.getenv("DB_NAME")
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    
    print(f"Connecting to {HOST}:{PORT}")
    
    connection = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DBNAME,
        charset='utf8mb4'
    )
    
    print("✅ Connected successfully")
    
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

