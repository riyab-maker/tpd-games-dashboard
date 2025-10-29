#!/usr/bin/env python3
import os
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

print(f"Testing database connection...")
print(f"Host: {HOST}")
print(f"Port: {PORT}")
print(f"Database: {DBNAME}")
print(f"User: {USER}")

try:
    connection = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DBNAME,
        charset='utf8mb4'
    )
    print("✅ Database connection successful!")
    
    # Test a simple query
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM hybrid_games")
        result = cursor.fetchone()
        print(f"✅ Found {result[0]} games in hybrid_games table")
        
    connection.close()
    print("✅ Database connection closed successfully")
    
except Exception as e:
    print(f"❌ Database connection failed: {e}")

