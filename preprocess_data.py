#!/usr/bin/env python3
"""
Data Preprocessing Script for Matomo Events Dashboard

This script performs all data processing locally and saves the results to CSV files
for use by the lightweight Streamlit dashboard on Render.

⚠️ IMPORTANT: This script must be run locally before deploying to Render.
The dashboard on Render only handles visualization of preprocessed data.
"""

# Trigger Render redeploy

import os
import json
import sys
import argparse
import pandas as pd
try:
    import psycopg2  # For Redshift connection
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("WARNING: psycopg2 not installed. Install it with: pip install psycopg2-binary")
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Tuple, Optional

# Load environment variables
load_dotenv()

# Redshift connection settings (from environment variables for security)
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "redshift-cluster.c9fcj1g6yq2x.ap-south-1.redshift.amazonaws.com")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "rl_dwh_prod")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "rl_product")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "Rlproduct@1234")

# Schema prefix for all queries
SCHEMA_PREFIX = "rl_dwh_prod.live"

# Print database configuration at startup (only if psycopg2 is available)
# Note: Question correctness now uses Redshift (same query as score distribution)
if PSYCOPG2_AVAILABLE:
    print("\n" + "=" * 60)
    print("DATABASE CONFIGURATION (for functions that need Redshift)")
    print("=" * 60)
    print(f"  Database Type: REDSHIFT (NOT MySQL/SQL)")
    print(f"  Connection Library: psycopg2 (PostgreSQL/Redshift driver)")
    print(f"  Host: {REDSHIFT_HOST}")
    print(f"  Database: {REDSHIFT_DATABASE}")
    print(f"  Port: {REDSHIFT_PORT}")
    print(f"  User: {REDSHIFT_USER}")
    print(f"  Schema Prefix: {SCHEMA_PREFIX}")
    print("=" * 60 + "\n")
else:
    print("\n" + "=" * 60)
    print("NOTE: psycopg2 not available - Redshift functions will be disabled")
    print("=" * 60)
    print("  All functions including question correctness require Redshift")
    print("  Functions that need Redshift will show errors")
    print("=" * 60 + "\n")

# Validate psycopg2 only when needed (all functions now use Redshift)
# The validation will happen in functions that actually need Redshift connection

# SQL Queries - Updated with new event categorization
# Event stages: started, introduction, questions, mid_introduction, validation, parent_poll, rewards, completed
# Optimized query: Filter by action names first to reduce JOIN overhead
SQL_QUERY = (
    """
    SELECT DISTINCT
      mllva.idlink_va,
      DATEADD(minute, 330, mllva.server_time) AS server_time,
      hg.game_name,
      hgl.game_id, 
      mla.name,
      mllva.idpageview,
      TO_HEX(mllva.idvisitor) AS idvisitor_hex,
      mllva.idvisit,
      CASE 
        WHEN mla.name LIKE '%_started%' THEN 'started'
        WHEN mla.name LIKE '%introduction_completed%' AND mla.name NOT LIKE '%mid%' THEN 'introduction'
        WHEN mla.name LIKE '%_mid_introduction%' THEN 'mid_introduction'
        WHEN mla.name LIKE '%_poll_completed%' THEN 'parent_poll'
        WHEN mla.name LIKE '%action_completed%' THEN 'questions'
        WHEN mla.name LIKE '%reward_completed%' THEN 'rewards'
        WHEN mla.name LIKE '%question_completed%' THEN 'validation'
        WHEN mla.name LIKE '%completed%' 
             AND mla.name NOT LIKE '%introduction%'
             AND mla.name NOT LIKE '%reward%'
             AND mla.name NOT LIKE '%question%'
             AND mla.name NOT LIKE '%mid_introduction%'
             AND mla.name NOT LIKE '%poll%'
             AND mla.name NOT LIKE '%action%' THEN 'completed'
        ELSE NULL
      END AS event
    FROM rl_dwh_prod.live.matomo_log_link_visit_action mllva
    INNER JOIN rl_dwh_prod.live.matomo_log_action mla 
      ON mllva.idaction_name = mla.idaction
      AND (
        mla.name LIKE '%introduction_completed%' OR
        mla.name LIKE '%reward_completed%' OR
        mla.name LIKE '%mcq_completed%' OR
        mla.name LIKE '%game_completed%' OR
        mla.name LIKE '%mcq_started%' OR
        mla.name LIKE '%game_started%' OR
        mla.name LIKE '%action_completed%' OR 
        mla.name LIKE '%question_completed%' OR
        mla.name LIKE '%poll_completed%'
      )
    INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl 
      ON mllva.custom_dimension_2 = hgl.activity_id
    INNER JOIN rl_dwh_prod.live.hybrid_games hg 
      ON hgl.game_id = hg.id
    WHERE mllva.server_time > '2025-07-01'
    """
)

# Score distribution query - Updated to use hybrid_games and hybrid_games_links tables
# Includes only game_completed and mcq_completed (action_level is no longer used)
SCORE_DISTRIBUTION_QUERY = """
select "mllva"."idlink_va",
	"hg"."game_name" as "game_name",
	"mllva"."idvisit",
	"mla"."name" as "action_name",
	"mllva"."custom_dimension_1",
	TO_HEX("mllva"."idvisitor") as "idvisitor_hex",
	"mllva"."server_time",
	"mllva"."idaction_name",
	"mllva"."custom_dimension_2",
	"mla"."idaction",
	"mla"."type",
	"hg"."game_code",
	"rl_dwh_prod"."live"."matomo_log_action"."name" as "language"
from "rl_dwh_prod"."live"."hybrid_games" "hg" 
	inner join "rl_dwh_prod"."live"."hybrid_games_links" "hgl" on "hg"."id" = "hgl"."game_id" 
	inner join "rl_dwh_prod"."live"."matomo_log_link_visit_action" "mllva" on "hgl"."activity_id" = "mllva"."custom_dimension_2" 
	inner join "rl_dwh_prod"."live"."matomo_log_action" "mla" on "mllva"."idaction_name" = "mla"."idaction" 
	inner join "rl_dwh_prod"."live"."matomo_log_action" on "mllva"."idaction_url_ref" = "rl_dwh_prod"."live"."matomo_log_action"."idaction" 
where ("mla"."name" Like '%game_completed%' or "mla"."name" Like '%mcq_completed%') 
	and "mllva"."server_time" >= '2025-07-01' 
	and "hgl"."activity_id" is not NULL
"""

# Note: Question Correctness now uses SCORE_DISTRIBUTION_QUERY (same as score distribution)
# The old QUESTION_CORRECTNESS_QUERY_1, QUERY_2, and QUERY_3 are no longer used
# They are kept below for reference but should not be used
QUESTION_CORRECTNESS_QUERY_1_DEPRECATED = """
SELECT 
  matomo_log_link_visit_action.custom_dimension_2, 
  matomo_log_link_visit_action.idvisit, 
  matomo_log_action.name, 
  matomo_log_link_visit_action.custom_dimension_1, 
  TO_HEX(matomo_log_link_visit_action.idvisitor) AS idvisitor_hex,
  hybrid_games.game_name
FROM rl_dwh_prod.live.matomo_log_link_visit_action 
INNER JOIN rl_dwh_prod.live.matomo_log_action 
  ON matomo_log_link_visit_action.idaction_name = matomo_log_action.idaction
INNER JOIN rl_dwh_prod.live.hybrid_games_links
  ON matomo_log_link_visit_action.custom_dimension_2 = hybrid_games_links.activity_id
INNER JOIN rl_dwh_prod.live.hybrid_games
  ON hybrid_games_links.game_id = hybrid_games.id
WHERE matomo_log_action.name LIKE '%game_completed%'
  AND hybrid_games.game_name IN (
    'Relational Comparison',
    'Quantitative Comparison',
    'Relational Comparison II',
    'Number Comparison',
    'Primary Emotion Labelling',
    'Emotion Identification',
    'Identification of all emotions',
    'Beginning Sound Pa Cha Sa'
  )
"""

QUESTION_CORRECTNESS_QUERY_2_DEPRECATED = """
SELECT 
  matomo_log_link_visit_action.custom_dimension_2,
  matomo_log_link_visit_action.idvisit,
  matomo_log_action.name,
  matomo_log_link_visit_action.custom_dimension_1,
  TO_HEX(matomo_log_link_visit_action.idvisitor) AS idvisitor_hex,
  hybrid_games.game_name
FROM rl_dwh_prod.live.matomo_log_link_visit_action
INNER JOIN rl_dwh_prod.live.matomo_log_action
  ON matomo_log_link_visit_action.idaction_name = matomo_log_action.idaction
INNER JOIN rl_dwh_prod.live.hybrid_games_links
  ON matomo_log_link_visit_action.custom_dimension_2 = hybrid_games_links.activity_id
INNER JOIN rl_dwh_prod.live.hybrid_games
  ON hybrid_games_links.game_id = hybrid_games.id
WHERE matomo_log_action.name LIKE '%game_completed%'
  AND hybrid_games.game_name IN (
    'Revision Primary Colors',
    'Revision Primary Shapes',
    'Rhyming Words'
  )
"""

QUESTION_CORRECTNESS_QUERY_3_DEPRECATED = """
SELECT matomo_log_link_visit_action.idlink_va, 
TO_HEX(matomo_log_link_visit_action.idvisitor) AS idvisitor_hex, 
matomo_log_link_visit_action.idvisit, 
matomo_log_link_visit_action.server_time, 
matomo_log_link_visit_action.idaction_name, 
matomo_log_link_visit_action.custom_dimension_1, 
matomo_log_link_visit_action.custom_dimension_2, 
matomo_log_action.idaction, 
matomo_log_action.name, 
matomo_log_action.type,
hybrid_games.game_name
FROM rl_dwh_prod.live.matomo_log_link_visit_action 
INNER JOIN rl_dwh_prod.live.matomo_log_action 
  ON matomo_log_link_visit_action.idaction_name = matomo_log_action.idaction
INNER JOIN rl_dwh_prod.live.hybrid_games_links
  ON matomo_log_link_visit_action.custom_dimension_2 = hybrid_games_links.activity_id
INNER JOIN rl_dwh_prod.live.hybrid_games
  ON hybrid_games_links.game_id = hybrid_games.id
WHERE matomo_log_link_visit_action.server_time >= '2025-07-01' 
  AND matomo_log_action.name LIKE '%action_level%'
  AND hybrid_games.game_name IN (
    'Shape Circle',
    'Shape Triangle',
    'Shape Square',
    'Shape Rectangle',
    'Color Red',
    'Color Yellow',
    'Color Blue',
    'Numbers I',
    'Numbers II',
    'Numerals 1-10',
    'Beginning Sound Ma Ka La',
    'Beginning Sound Ba Ra Na'
  )
"""

# Parent Poll Query
PARENT_POLL_QUERY = """
SELECT 
  matomo_log_link_visit_action.*,
  TO_HEX(matomo_log_link_visit_action.idvisitor) AS idvisitor_hex,
  hybrid_games.game_name
FROM rl_dwh_prod.live.matomo_log_link_visit_action 
INNER JOIN rl_dwh_prod.live.matomo_log_action ON matomo_log_link_visit_action.idaction_name = matomo_log_action.idaction 
INNER JOIN rl_dwh_prod.live.hybrid_games_links ON hybrid_games_links.activity_id = matomo_log_link_visit_action.custom_dimension_2 
INNER JOIN rl_dwh_prod.live.hybrid_games ON hybrid_games.id = hybrid_games_links.game_id 
WHERE matomo_log_action.name LIKE '%_completed%' 
  AND matomo_log_link_visit_action.custom_dimension_1 IS NOT NULL 
  AND matomo_log_link_visit_action.custom_dimension_1 LIKE '%poll%' 
  AND matomo_log_link_visit_action.server_time > '2025-07-01' 
  AND hybrid_games_links.activity_id IS NOT NULL
"""

# Time Series Analysis Query - Uses same logic as conversion funnel query
# Includes action_name and event classification (Started/Completed)
# Includes game_code and language for aggregation
# Appends missing game-activity mappings before joining
TIME_SERIES_QUERY = """
WITH game_activity_mappings AS (
  -- Existing mappings from hybrid_games and hybrid_games_links
  SELECT 
    hg.game_name,
    hg.game_code,
    hgl.activity_id
  FROM rl_dwh_prod.live.hybrid_games hg
  INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl ON hg.id = hgl.game_id
  
  UNION ALL
  
  -- Missing game-activity mappings
  SELECT 
    'Primary Emotion Labelling : Happy' AS game_name,
    NULL AS game_code,
    140 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Happy' AS game_name,
    NULL AS game_code,
    140 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Sad' AS game_name,
    NULL AS game_code,
    130 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Sad' AS game_name,
    NULL AS game_code,
    130 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Anger' AS game_name,
    NULL AS game_code,
    131 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Anger' AS game_name,
    NULL AS game_code,
    132 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Fear' AS game_name,
    NULL AS game_code,
    132 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Fear' AS game_name,
    NULL AS game_code,
    132 AS activity_id
)
SELECT 
  mllva.idlink_va,
  TO_HEX(mllva.idvisitor) AS idvisitor_hex,
  mllva.idvisit,
  DATEADD(minute, 330, mllva.server_time) AS server_time,
  mllva.idaction_name,
  mllva.custom_dimension_2,
  gam.game_name,
  gam.game_code,
  mla.name AS action_name,
  matomo_log_action1.name AS language,
  CASE 
    WHEN mla.name LIKE '%hybrid_game_started%' OR mla.name LIKE '%hybrid_mcq_started%' THEN 'Started'
    WHEN mla.name LIKE '%hybrid_game_completed%' OR mla.name LIKE '%hybrid_mcq_completed%' THEN 'Completed'
    ELSE NULL
  END AS event
FROM rl_dwh_prod.live.matomo_log_link_visit_action mllva
INNER JOIN rl_dwh_prod.live.matomo_log_action mla ON mllva.idaction_name = mla.idaction
INNER JOIN game_activity_mappings gam ON mllva.custom_dimension_2 = gam.activity_id
INNER JOIN rl_dwh_prod.live.matomo_log_action matomo_log_action1 ON mllva.idaction_url_ref = matomo_log_action1.idaction
WHERE (mla.name LIKE '%hybrid_game_started%' 
       OR mla.name LIKE '%hybrid_mcq_started%' 
       OR mla.name LIKE '%hybrid_game_completed%' 
       OR mla.name LIKE '%hybrid_mcq_completed%')
  AND gam.activity_id IS NOT NULL
  AND DATEADD(minute, 330, mllva.server_time) >= '2025-07-02'
"""


# Queries for mapped users calculation
MAPPED_USERS_QUERY_1 = """
SELECT DISTINCT "rl_dwh_prod"."live"."hybrid_users"."phone" 
FROM "rl_dwh_prod"."live"."hybrid_profiles" 
INNER JOIN "rl_dwh_prod"."live"."hybrid_profile_devices" ON "rl_dwh_prod"."live"."hybrid_profiles"."id" = "rl_dwh_prod"."live"."hybrid_profile_devices"."profile_id" 
INNER JOIN "rl_dwh_prod"."live"."hybrid_users" ON "rl_dwh_prod"."live"."hybrid_profiles"."hybrid_user_id" = "rl_dwh_prod"."live"."hybrid_users"."id", "rl_dwh_prod"."live"."activity_intervention" 
WHERE "rl_dwh_prod"."live"."hybrid_users"."phone" IS NOT NULL
"""

MAPPED_USERS_QUERY_2 = """
SELECT "rl_dwh_prod"."live"."covered_guardians"."group_id",
	"rl_dwh_prod"."live"."guardians"."phone" 
FROM "rl_dwh_prod"."live"."covered_guardians" 
INNER JOIN "rl_dwh_prod"."live"."guardians" ON "rl_dwh_prod"."live"."covered_guardians"."guardian_id" = "rl_dwh_prod"."live"."guardians"."id" 
WHERE "rl_dwh_prod"."live"."covered_guardians"."group_id" IS NOT NULL 
	AND "rl_dwh_prod"."live"."guardians"."deleted_at" IS NULL 
	AND "rl_dwh_prod"."live"."covered_guardians"."deleted_at" IS NULL 
	AND "rl_dwh_prod"."live"."covered_guardians"."covered" = 1
"""

# Query to get phone numbers for users in conversion funnel
# This joins matomo visitor data to hybrid_users to get phone numbers
USER_PHONE_QUERY = """
SELECT DISTINCT
    TO_HEX(mllva.idvisitor) AS idvisitor_hex,
    hu.phone
FROM rl_dwh_prod.live.matomo_log_link_visit_action mllva
INNER JOIN rl_dwh_prod.live.hybrid_profile_devices hpd ON CAST(TO_HEX(mllva.idvisitor) AS VARCHAR) = CAST(hpd.device_id AS VARCHAR)
INNER JOIN rl_dwh_prod.live.hybrid_profiles hp ON hpd.profile_id = hp.id
INNER JOIN rl_dwh_prod.live.hybrid_users hu ON hp.hybrid_user_id = hu.id
WHERE hu.phone IS NOT NULL
  AND DATEADD(minute, 330, mllva.server_time) >= '2025-07-02'
"""


def fetch_mapped_users_data() -> set:
    """Fetch mapped users (phones that appear in both queries) from Redshift
    
    Returns:
        set: Set of phone numbers that are mapped (appear in both queries)
    """
    if not PSYCOPG2_AVAILABLE:
        print("WARNING: psycopg2 not available. Cannot fetch mapped users data from Redshift.")
        print("  Install with: pip install psycopg2-binary")
        return set()
    
    print("=" * 60)
    print("FETCHING: Mapped Users Data from REDSHIFT")
    print("=" * 60)
    print(f"  Database Type: REDSHIFT (NOT MySQL/SQL)")
    print(f"  Host: {REDSHIFT_HOST}")
    print(f"  Database: {REDSHIFT_DATABASE}")
    print(f"  Port: {REDSHIFT_PORT}")
    print(f"  User: {REDSHIFT_USER}")
    print(f"  Connection Library: psycopg2 (PostgreSQL/Redshift driver)")
    
    # Retry configuration
    import time
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\n  [ACTION] Connecting to REDSHIFT (Attempt {attempt}/{max_retries})...")
            conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                connect_timeout=60,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            
            # Set statement timeout to 30 minutes for large queries
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '1800000'")  # 30 minutes in milliseconds
                conn.commit()
            
            print(f"  ✓ Successfully connected to REDSHIFT")
            
            # Fetch phones from query 1 (hybrid_users)
            print(f"  [ACTION] Executing query 1 (hybrid_users phones)...")
            df_query1 = pd.read_sql(MAPPED_USERS_QUERY_1, conn)
            phones_query1 = set(df_query1['phone'].dropna().astype(str).str.strip())
            print(f"  ✓ Query 1 returned {len(phones_query1)} unique phones from hybrid_users")
            
            # Fetch phones from query 2 (guardians)
            print(f"  [ACTION] Executing query 2 (guardians phones)...")
            df_query2 = pd.read_sql(MAPPED_USERS_QUERY_2, conn)
            phones_query2 = set(df_query2['phone'].dropna().astype(str).str.strip())
            print(f"  ✓ Query 2 returned {len(phones_query2)} unique phones from guardians")
            
            # Find mapped phones (phones that appear in both queries)
            mapped_phones = phones_query1.intersection(phones_query2)
            print(f"  ✓ Found {len(mapped_phones)} mapped phones (appear in both queries)")
            
            conn.close()
            print(f"  ✓ Connection closed")
            
            return mapped_phones
            
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            if attempt < max_retries:
                print(f"\n  [WARNING] Connection error on attempt {attempt}:")
                print(f"    {error_msg}")
                print(f"  [ACTION] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"\n  ERROR: Failed to connect to REDSHIFT after {max_retries} attempts:")
                print(f"    Error Type: OperationalError (connection issue)")
                print(f"    Error Message: {error_msg}")
                return set()
        except Exception as e:
            print(f"\n  ERROR: Failed to fetch mapped users data from REDSHIFT:")
            print(f"    Error Type: {type(e).__name__}")
            print(f"    Error Message: {str(e)}")
            if attempt < max_retries:
                print(f"  [ACTION] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                return set()
    
    return set()


def convert_hex_to_int(df: pd.DataFrame, hex_column: str = 'idvisitor_hex', output_column: str = 'idvisitor_converted') -> pd.DataFrame:
    """Convert hex string column to integer column in Python (handles large values)"""
    if hex_column not in df.columns:
        return df
    
    def hex_to_int(hex_str):
        """Convert hex string to integer, handling large values"""
        if pd.isna(hex_str) or hex_str is None or hex_str == '':
            return 0
        try:
            # Python's int() can handle arbitrarily large integers
            return int(str(hex_str), 16)
        except (ValueError, TypeError):
            return 0
    
    df[output_column] = df[hex_column].apply(hex_to_int)
    df = df.drop(columns=[hex_column])
    return df


def fetch_dataframe() -> pd.DataFrame:
    """Load main dataframe from conversion_funnel.csv file and process it"""
    print("\n" + "=" * 60)
    print("STEP 1: Loading data from conversion_funnel.csv")
    print("=" * 60)
    
    csv_file = 'conversion_funnel.csv'
    
    if not os.path.exists(csv_file):
        print(f"ERROR: File '{csv_file}' not found!")
        return pd.DataFrame()
    
    print(f"✓ File found: {csv_file}")
    file_size = os.path.getsize(csv_file) / (1024 * 1024)  # Size in MB
    print(f"✓ File size: {file_size:.2f} MB")
    
    try:
        print(f"\n[STEP 1.1] Reading CSV file in chunks...")
        sys.stdout.flush()
        
        # Read CSV file in chunks if it's large
        chunk_list = []
        chunk_size = 100000  # Read 100k rows at a time
        chunk_num = 0
        
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False):
            chunk_num += 1
            chunk_list.append(chunk)
            total_rows = sum(len(c) for c in chunk_list)
            print(f"  [Chunk {chunk_num}] Read {len(chunk):,} rows (Total: {total_rows:,} rows)")
            sys.stdout.flush()
        
        print(f"\n[STEP 1.2] Combining {len(chunk_list)} chunks...")
        sys.stdout.flush()
        df = pd.concat(chunk_list, ignore_index=True)
        print(f"✓ SUCCESS: Loaded {len(df):,} total records from CSV file")
        print(f"✓ Columns in CSV: {list(df.columns)}")
        sys.stdout.flush()
        
        print(f"\n[STEP 2] Processing duplicates and timezone adjustment...")
        sys.stdout.flush()
        
        # Handle duplicate idlink_va - keep first occurrence (sorted by server_time if available)
        initial_count = len(df)
        if 'idlink_va' in df.columns:
            print(f"  Checking for duplicates on idlink_va column...")
            sys.stdout.flush()
            # Sort by server_time if available to keep earliest record
            if 'server_time' in df.columns:
                print(f"  Converting server_time to datetime...")
                sys.stdout.flush()
                df['server_time'] = pd.to_datetime(df['server_time'], errors='coerce')
                print(f"  Sorting by server_time and removing duplicates...")
                sys.stdout.flush()
                df = df.sort_values('server_time').drop_duplicates(subset=['idlink_va'], keep='first')
            else:
                print(f"  Removing duplicates (no server_time for sorting)...")
                sys.stdout.flush()
                df = df.drop_duplicates(subset=['idlink_va'], keep='first')
            print(f"  ✓ Removed {initial_count - len(df):,} duplicate idlink_va records")
            sys.stdout.flush()
        else:
            print(f"  WARNING: 'idlink_va' column not found - skipping duplicate removal")
            sys.stdout.flush()
        
        # Add 5:30 hours (330 minutes) to server_time if it exists
        if 'server_time' in df.columns:
            print(f"  Adding 5:30 hours (330 minutes) to server_time...")
            sys.stdout.flush()
            df['server_time'] = df['server_time'] + pd.Timedelta(hours=5, minutes=30)
            print(f"  ✓ Added 5:30 hours to server_time")
            sys.stdout.flush()
        else:
            print(f"  WARNING: 'server_time' column not found - skipping timezone adjustment")
            sys.stdout.flush()
        
        print(f"\n[STEP 3] Categorizing events based on action names...")
        sys.stdout.flush()
        
        # Categorize events based on 'name' column (action name)
        if 'name' in df.columns:
            print(f"  Processing {len(df):,} records for event categorization...")
            sys.stdout.flush()
            
            def categorize_event(name):
                if pd.isna(name):
                    return None
                name_str = str(name)
                if '_started' in name_str:
                    return 'started'
                elif 'introduction_completed' in name_str and 'mid' not in name_str:
                    return 'introduction'
                elif '_mid_introduction' in name_str:
                    return 'mid_introduction'
                elif '_poll_completed' in name_str:
                    return 'parent_poll'
                elif 'action_completed' in name_str:
                    return 'questions'
                elif 'reward_completed' in name_str:
                    return 'rewards'
                elif 'question_completed' in name_str:
                    return 'validation'
                elif 'completed' in name_str and 'introduction' not in name_str and 'reward' not in name_str and 'question' not in name_str and 'mid_introduction' not in name_str and 'poll' not in name_str and 'action' not in name_str:
                    return 'completed'
                return None
            
            # Process in batches for progress tracking
            batch_size = 50000
            total_batches = (len(df) // batch_size) + 1
            events = []
            
            for i in range(0, len(df), batch_size):
                batch_num = (i // batch_size) + 1
                batch = df['name'].iloc[i:i+batch_size]
                batch_events = batch.apply(categorize_event)
                events.extend(batch_events)
                print(f"  [Batch {batch_num}/{total_batches}] Processed {min(i+batch_size, len(df)):,} / {len(df):,} records")
                sys.stdout.flush()
            
            df['event'] = events
            
            # Check event column values
            if len(df) > 0:
                print(f"\n[STEP 3.1] Event categorization summary:")
                sys.stdout.flush()
                event_counts = df['event'].value_counts(dropna=False)
                print(event_counts)
                null_events = df['event'].isna().sum()
                if null_events > 0:
                    print(f"  WARNING: {null_events:,} records have NULL event values")
                sys.stdout.flush()
        else:
            print(f"  WARNING: 'name' column not found - cannot categorize events")
            df['event'] = None
            sys.stdout.flush()
        
        print(f"\n[STEP 4] Processing idvisitor_converted column...")
        sys.stdout.flush()
        
        # Ensure idvisitor_converted column exists (convert from idvisitor if needed)
        if 'idvisitor_converted' not in df.columns and 'idvisitor' in df.columns:
            print(f"  Converting idvisitor to idvisitor_converted...")
            sys.stdout.flush()
            # Try to convert hex to decimal if needed
            try:
                df['idvisitor_converted'] = df['idvisitor'].apply(
                    lambda x: int(str(x), 16) if pd.notna(x) and isinstance(x, (str, int)) and str(x).startswith('0x') else x
                )
                print(f"  ✓ Converted idvisitor to idvisitor_converted")
            except Exception as e:
                print(f"  WARNING: Could not convert idvisitor: {e}")
                df['idvisitor_converted'] = df['idvisitor']
            sys.stdout.flush()
        elif 'idvisitor_converted' in df.columns:
            print(f"  ✓ idvisitor_converted column already exists")
            sys.stdout.flush()
        else:
            print(f"  WARNING: Neither idvisitor_converted nor idvisitor column found")
            sys.stdout.flush()
        
        print(f"\n[STEP 5] Final data summary:")
        print(f"  ✓ Final data shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        print(f"  ✓ Columns: {list(df.columns)}")
        sys.stdout.flush()
        
        return df
            
    except Exception as e:
        print(f"ERROR: Failed to load data from CSV file: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def fetch_score_dataframe() -> pd.DataFrame:
    """Fetch data for score distribution analysis using hybrid_games and hybrid_games_links tables
    
    Includes retry logic and extended timeouts for large queries.
    """
    if not PSYCOPG2_AVAILABLE:
        print("ERROR: psycopg2 not available. Cannot fetch score data from Redshift.")
        print("  Install with: pip install psycopg2-binary")
        return pd.DataFrame()
    
    print("=" * 60)
    print("FETCHING: Score Distribution Data from REDSHIFT")
    print("=" * 60)
    print(f"  Database Type: REDSHIFT (NOT MySQL/SQL)")
    print(f"  Host: {REDSHIFT_HOST}")
    print(f"  Database: {REDSHIFT_DATABASE}")
    print(f"  Port: {REDSHIFT_PORT}")
    print(f"  User: {REDSHIFT_USER}")
    print(f"  Connection Library: psycopg2 (PostgreSQL/Redshift driver)")
    
    # Retry configuration
    import time
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\n  [ACTION] Connecting to REDSHIFT (Attempt {attempt}/{max_retries})...")
            conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                connect_timeout=60,  # Increased from 30 to 60 seconds
                keepalives=1,  # Enable TCP keepalive
                keepalives_idle=30,  # Start keepalive after 30 seconds of idle
                keepalives_interval=10,  # Send keepalive every 10 seconds
                keepalives_count=5  # Number of keepalive packets before considering connection dead
            )
            print(f"  ✓ Successfully connected to REDSHIFT")
            
            # Set statement timeout (in milliseconds) - 30 minutes for large queries
            print(f"  [ACTION] Setting statement timeout to 30 minutes...")
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '1800000'")  # 30 minutes in milliseconds
            conn.commit()
            print(f"  ✓ Statement timeout set")
            
            print(f"  [ACTION] Executing query on REDSHIFT...")
            print(f"  [INFO] This may take several minutes for large datasets...")
            df = pd.read_sql(SCORE_DISTRIBUTION_QUERY, conn)
            conn.close()
            print(f"  ✓ Query executed successfully on REDSHIFT")
            print(f"  ✓ Connection closed")
            
            # Convert hex to int in Python (handles large values)
            if 'idvisitor_hex' in df.columns:
                print(f"  [ACTION] Converting hex to integer in Python...")
                df = convert_hex_to_int(df, 'idvisitor_hex', 'idvisitor_converted')
                print(f"  ✓ Converted idvisitor_hex to idvisitor_converted")
            
            print(f"SUCCESS: Fetched {len(df)} records from REDSHIFT")
            return df
            
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            error_msg = str(e)
            is_connection_error = (
                "server closed the connection" in error_msg.lower() or
                "connection already closed" in error_msg.lower() or
                "connection timeout" in error_msg.lower() or
                "terminated abnormally" in error_msg.lower()
            )
            
            if is_connection_error and attempt < max_retries:
                print(f"\n  [WARNING] Connection error on attempt {attempt}:")
                print(f"    {error_msg}")
                print(f"  [ACTION] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                print(f"\nERROR: Failed to connect/execute on REDSHIFT:")
                print(f"  Error Type: {type(e).__name__}")
                print(f"  Error Message: {error_msg}")
                if attempt >= max_retries:
                    print(f"  [ERROR] Max retries ({max_retries}) reached. Giving up.")
                print(f"  Check:")
                print(f"    - REDSHIFT credentials are correct in .env file")
                print(f"    - Network connectivity to Redshift cluster")
                print(f"    - Redshift cluster is running and accessible")
                print(f"    - Query may be too large - consider adding date filters")
                import traceback
                traceback.print_exc()
                return pd.DataFrame()
                
        except Exception as e:
            print(f"\nERROR: Failed to fetch score data from REDSHIFT:")
            print(f"  Error Type: {type(e).__name__}")
            print(f"  Error Message: {str(e)}")
            if attempt < max_retries:
                print(f"  [ACTION] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                import traceback
                traceback.print_exc()
                return pd.DataFrame()
    
    # Should not reach here, but return empty dataframe if we do
    return pd.DataFrame()


def parse_correct_selections_questions(custom_dim_1, game_name):
    """Parse correctSelections structure to extract question correctness (for "This or That" games)
    
    Checks two structures:
    1. roundDetails structure (for games like Quantitative Comparison)
    2. Nested gameData structure (same path as score distribution's parse_custom_dimension_1_correct_selections)
    """
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        data = json.loads(custom_dim_1)
        
        # Method 1: Check for roundDetails structure (for games like Quantitative Comparison)
        if 'roundDetails' in data and isinstance(data['roundDetails'], list):
            for round_detail in data['roundDetails']:
                if 'roundNumber' in round_detail:
                    question_num = round_detail['roundNumber']
                    cards = round_detail.get('cards', [])
                    selections = round_detail.get('selections', [])
                    
                    if cards and selections:
                        # Find the correct card (status = true)
                        correct_card_index = None
                        for idx, card in enumerate(cards):
                            if card.get('status') is True:
                                correct_card_index = idx
                                break
                        
                        # Get selected card
                        selected_card_index = None
                        if selections and len(selections) > 0:
                            selected_card_index = selections[0].get('card')
                        
                        # Determine correctness
                        is_correct = (selected_card_index is not None and 
                                    correct_card_index is not None and 
                                    selected_card_index == correct_card_index)
                        
                        results.append({
                            'question_number': question_num,
                            'is_correct': 1 if is_correct else 0,
                            'game_name': game_name
                        })
        
        # Method 2: Check nested gameData structure for roundDetails
        # Path: gameData[*] (where section="Action") -> gameData[*].gameData[*].roundDetails
        # This matches the structure shown in the example JSON
        if len(results) == 0 and 'gameData' in data and isinstance(data['gameData'], list):
            for game_data in data['gameData']:
                # Look for section="Action" (same as score distribution logic)
                if game_data.get('section') == 'Action' and 'gameData' in game_data and isinstance(game_data['gameData'], list):
                    for inner_game_data in game_data['gameData']:
                        # Check for roundDetails in the nested structure (this is the key!)
                        if 'roundDetails' in inner_game_data and isinstance(inner_game_data['roundDetails'], list):
                            for round_detail in inner_game_data['roundDetails']:
                                if 'roundNumber' in round_detail:
                                    question_num = round_detail['roundNumber']
                                    cards = round_detail.get('cards', [])
                                    selections = round_detail.get('selections', [])
                                    
                                    if cards and selections:
                                        # Find the correct card (status = true)
                                        correct_card_index = None
                                        for idx, card in enumerate(cards):
                                            if card.get('status') is True:
                                                correct_card_index = idx
                                                break
                                        
                                        # Get selected card
                                        selected_card_index = None
                                        if selections and len(selections) > 0:
                                            selected_card_index = selections[0].get('card')
                                        
                                        # Determine correctness
                                        is_correct = (selected_card_index is not None and 
                                                    correct_card_index is not None and 
                                                    selected_card_index == correct_card_index)
                                        
                                        results.append({
                                            'question_number': question_num,
                                            'is_correct': 1 if is_correct else 0,
                                            'game_name': game_name
                                        })
                        
                        # Also check for rounds array (alternative structure)
                        elif 'rounds' in inner_game_data and isinstance(inner_game_data['rounds'], list):
                            for round_idx, round_data in enumerate(inner_game_data['rounds'], 1):
                                if isinstance(round_data, dict):
                                    # Try to determine correctness from round data
                                    # Look for common patterns: isCorrect, correct, status, etc.
                                    is_correct = 0
                                    if 'isCorrect' in round_data:
                                        is_correct = 1 if round_data['isCorrect'] else 0
                                    elif 'correct' in round_data:
                                        is_correct = 1 if round_data['correct'] else 0
                                    elif 'status' in round_data:
                                        is_correct = 1 if round_data['status'] else 0
                                    elif 'selected' in round_data and 'correctOption' in round_data:
                                        is_correct = 1 if round_data.get('selected') == round_data.get('correctOption') else 0
                                    
                                    question_num = round_data.get('roundNumber', round_data.get('questionNumber', round_data.get('level', round_idx)))
                                    results.append({
                                        'question_number': int(question_num),
                                        'is_correct': is_correct,
                                        'game_name': game_name
                                    })
                        
                        # Also check for questions array
                        elif 'questions' in inner_game_data and isinstance(inner_game_data['questions'], list):
                            for q_idx, question_data in enumerate(inner_game_data['questions'], 1):
                                if isinstance(question_data, dict):
                                    is_correct = 0
                                    if 'isCorrect' in question_data:
                                        is_correct = 1 if question_data['isCorrect'] else 0
                                    elif 'correct' in question_data:
                                        is_correct = 1 if question_data['correct'] else 0
                                    
                                    question_num = question_data.get('questionNumber', question_data.get('number', q_idx))
                                    results.append({
                                        'question_number': int(question_num),
                                        'is_correct': is_correct,
                                        'game_name': game_name
                                    })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError) as e:
        return results


def extract_action_section_from_string(json_str):
    """Extract Action section from JSON string even if the rest is malformed.
    
    This function tries to find and extract just the Action section with its jsonData
    by searching for the pattern: {"section":"Action","jsonData":[...]}
    Uses bracket matching to properly extract nested JSON arrays.
    
    Tries multiple strategies:
    1. Look for "section":"Action" followed by "jsonData"
    2. Look for just "jsonData" (in case section key is malformed)
    3. Try to extract jsonData array using bracket matching
    """
    import re
    
    if not isinstance(json_str, str):
        return None
    
    # Strategy 1: Find "jsonData" that appears after "section":"Action"
    # Look for pattern: "section":"Action" ... "jsonData":[
    pattern1 = r'"section"\s*:\s*"Action"[^}]*?"jsonData"\s*:\s*\['
    match1 = re.search(pattern1, json_str, re.DOTALL)
    
    # Strategy 2: Find any "jsonData" key (in case section is malformed or missing)
    # Look for pattern: "jsonData":[
    pattern2 = r'"jsonData"\s*:\s*\['
    match2 = re.search(pattern2, json_str, re.DOTALL)
    
    # Use the first match found (prefer Strategy 1 if both match)
    match = match1 if match1 else match2
    
    if match:
        # Find the start of the jsonData array (the '[' character)
        start_pos = match.end() - 1  # -1 because we want to include the '['
        
        # Now find the matching closing bracket ']' using bracket counting
        bracket_count = 0
        in_string = False
        escape_next = False
        
        for i in range(start_pos, len(json_str)):
            char = json_str[i]
            
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if not in_string:
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        # Found the matching closing bracket
                        end_pos = i + 1
                        json_data_str = json_str[start_pos:end_pos]
                        try:
                            json_data = json.loads(json_data_str)
                            return json_data
                        except json.JSONDecodeError:
                            # Try cleaning the extracted string
                            try:
                                # Remove any trailing commas before closing bracket
                                json_data_str_clean = re.sub(r',\s*\]', ']', json_data_str)
                                json_data = json.loads(json_data_str_clean)
                                return json_data
                            except:
                                break
                        except:
                            break
        
        # If bracket matching failed, try a simpler regex approach as fallback
        # This handles simpler cases without deeply nested structures
        simple_pattern = r'"jsonData"\s*:\s*(\[[^\]]*(?:\[[^\]]*\][^\]]*)*\])'
        simple_match = re.search(simple_pattern, json_str, re.DOTALL)
        if simple_match:
            try:
                json_data_str = simple_match.group(1)
                # Try to clean common JSON errors
                json_data_str = re.sub(r',\s*\]', ']', json_data_str)  # Remove trailing commas
                json_data = json.loads(json_data_str)
                return json_data
            except:
                pass
    
    # Strategy 3: Try to find jsonData even if the format is slightly different
    # Look for jsonData with various spacing/quote issues
    pattern3 = r'["\']jsonData["\']\s*:\s*\['
    match3 = re.search(pattern3, json_str, re.DOTALL | re.IGNORECASE)
    if match3:
        start_pos = match3.end() - 1
        bracket_count = 0
        in_string = False
        escape_next = False
        
        for i in range(start_pos, len(json_str)):
            char = json_str[i]
            
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char in ('"', "'") and not escape_next:
                in_string = not in_string
                continue
            
            if not in_string:
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        end_pos = i + 1
                        json_data_str = json_str[start_pos:end_pos]
                        try:
                            json_data_str = re.sub(r',\s*\]', ']', json_data_str)
                            json_data = json.loads(json_data_str)
                            return json_data
                        except:
                            break
    
    return None


def parse_flow_stop_go_questions(custom_dim_1, game_name):
    """Parse flow structure to extract question correctness (for "Flow" games)
    
    Handles two structures:
    1. Nested: gameData[*] where section="Action" -> jsonData[*] -> userResponse[0].isCorrect
    2. Root level: section="Action" -> jsonData[*] -> userResponse[0].isCorrect (for Beginning Sounds games: Ma/Cha/Ba, Ka/Na/Ta, Ta/Va/Ga)
    
    This matches the logic used in score distribution's parse_custom_dimension_1_json_data
    but extracts per-question instead of total score.
    
    For each level in jsonData:
    - Read userResponse[0].isCorrect
    - If true → score = 1, if false → score = 0
    
    If JSON parsing fails, tries to extract Action section directly from the string.
    """
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        data = None
        json_data = None
        
        # Try to parse the full JSON first
        try:
            # Handle case where custom_dim_1 might already be a dict
            if isinstance(custom_dim_1, dict):
                data = custom_dim_1
            else:
                json_str = str(custom_dim_1).strip()
                # Remove any leading/trailing quotes if present
                if json_str.startswith('"') and json_str.endswith('"'):
                    json_str = json_str[1:-1]
                # Clean malformed JSON
                json_str = clean_malformed_json(json_str)
                data = json.loads(json_str)
        except (json.JSONDecodeError, ValueError, TypeError):
            # If full JSON parsing fails, try to extract just the Action section
            json_str = str(custom_dim_1).strip()
            if json_str.startswith('"') and json_str.endswith('"'):
                json_str = json_str[1:-1]
            json_str = clean_malformed_json(json_str)
            json_data = extract_action_section_from_string(json_str)
            if json_data:
                # Successfully extracted Action section, skip to processing jsonData
                pass
            else:
                # Could not extract Action section, return empty results
                return results
        
        # If we successfully parsed the full JSON, look for Action section
        if data is not None:
            action_section = None
            
            # Case 1: Check if Action section is at root level (for Beginning Sounds Ma/Cha/Ba)
            if isinstance(data, dict) and data.get('section') == 'Action' and 'jsonData' in data:
                action_section = data
                json_data = data['jsonData']
            # Case 2: Check nested structure (gameData[*] where section="Action")
            elif isinstance(data, dict) and 'gameData' in data and isinstance(data['gameData'], list):
                for game_data in data['gameData']:
                    if isinstance(game_data, dict) and game_data.get('section') == 'Action' and 'jsonData' in game_data:
                        action_section = game_data
                        json_data = game_data['jsonData']
                        break
        
        # Extract per-question from jsonData
        if json_data and isinstance(json_data, list):
            question_idx = 0
            for level_data in json_data:
                if not isinstance(level_data, dict):
                    continue
                
                # Extract per-question from userResponse
                user_responses = level_data.get('userResponse', [])
                if isinstance(user_responses, list) and len(user_responses) > 0:
                    # Use first userResponse
                    response = user_responses[0]
                    if isinstance(response, dict):
                        is_correct = response.get('isCorrect', False)
                        question_idx += 1
                        
                        # Get question number from level if available, otherwise use index
                        question_num = level_data.get('level', question_idx)
                        
                        results.append({
                            'question_number': int(question_num),
                            'is_correct': 1 if is_correct else 0,
                            'game_name': game_name
                        })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError) as e:
        return results


def parse_action_level_questions(custom_dim_1, game_name, level_number):
    """Parse action_level structure to extract question correctness (for "Action Level" games)"""
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        data = json.loads(custom_dim_1)
        
        # Check for options and chosenOption structure
        if 'options' in data and 'chosenOption' in data:
            chosen_option = data.get('chosenOption')
            
            if chosen_option is None:
                is_correct = 0
            else:
                options = data.get('options', [])
                if isinstance(options, list) and 0 <= chosen_option < len(options):
                    chosen_option_data = options[chosen_option]
                    if isinstance(chosen_option_data, dict):
                        is_correct = 1 if chosen_option_data.get('isCorrect', False) else 0
                    else:
                        is_correct = 0
                else:
                    is_correct = 0
            
            results.append({
                'question_number': level_number,
                'is_correct': is_correct,
                'game_name': game_name
            })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError) as e:
        return results


def parse_mcq_completed_questions(custom_dim_1, game_name):
    """Parse mcq_completed structure to extract per-question correctness from Action section
    
    Structure:
    {
      "section": "Action",
      "gameData": [
          {
            "options": [
                {"path": "o1.png", "isCorrect": false},
                {"path": "o2.png", "isCorrect": true},
                {"path": "o3.png", "isCorrect": false}
            ],
            "chosenOption": 1,
            ...
          },
          ...
      ]
    }
    
    For each question:
    - Look at chosenOption (index chosen by the user)
    - Look at options[chosenOption].isCorrect
    - If isCorrect == true, score = 1, else score = 0
    
    Returns list of question results with question_number and is_correct.
    """
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        # Look for Action section - can be at top level or inside gameData array
        action_section = None
        
        # Case 1: Action section at top level
        if isinstance(data, dict) and 'section' in data and data['section'] == 'Action':
            action_section = data
        # Case 2: Action section inside gameData array (similar to other game types)
        elif isinstance(data, dict) and 'gameData' in data and isinstance(data['gameData'], list):
            for item in data['gameData']:
                if isinstance(item, dict) and item.get('section') == 'Action':
                    action_section = item
                    break
        
        # Extract per-question correctness from Action section
        if action_section and 'gameData' in action_section and isinstance(action_section['gameData'], list):
            for question_idx, question in enumerate(action_section['gameData'], 1):
                if not isinstance(question, dict):
                    continue
                
                # Get options and chosenOption
                options = question.get('options', [])
                chosen_option = question.get('chosenOption')
                
                # Skip if no options or chosenOption is None
                if not isinstance(options, list) or len(options) == 0:
                    continue
                
                # Handle chosenOption - convert to int if needed, skip if None
                if chosen_option is None:
                    is_correct = 0
                else:
                    try:
                        chosen_option = int(chosen_option)
                    except (ValueError, TypeError):
                        is_correct = 0
                    else:
                        # Check if chosenOption is within bounds
                        if 0 <= chosen_option < len(options):
                            chosen_option_data = options[chosen_option]
                            if isinstance(chosen_option_data, dict):
                                # Check isCorrect - handle both boolean True and string "true"
                                is_correct_val = chosen_option_data.get('isCorrect', False)
                                is_correct = 1 if (is_correct_val is True or (isinstance(is_correct_val, str) and is_correct_val.lower() == 'true')) else 0
                            else:
                                is_correct = 0
                        else:
                            is_correct = 0
                
                results.append({
                    'question_number': question_idx,
                    'is_correct': is_correct,
                    'game_name': game_name
                })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return results


def parse_mcq_completed_questions_with_correct_option(custom_dim_1, game_name):
    """Parse mcq_completed structure to extract per-question correctness using correctOption
    
    This is for games like Positions that use chosenOption and correctOption instead of isCorrect.
    
    Structure:
    {
      "section": "Action",
      "gameData": [
          {
            "options": [...],
            "chosenOption": 1,
            "correctOption": 2,
            ...
          },
          ...
      ]
    }
    
    For each question:
    - Look at chosenOption (index chosen by the user)
    - Look at correctOption (the correct index)
    - If chosenOption == correctOption, score = 1, else score = 0
    
    Returns list of question results with question_number and is_correct.
    """
    results = []
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return results
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        # Look for Action section - can be at top level or inside gameData array
        action_section = None
        
        # Case 1: Action section at top level
        if isinstance(data, dict) and 'section' in data and data['section'] == 'Action':
            action_section = data
        # Case 2: Action section inside gameData array (similar to other game types)
        elif isinstance(data, dict) and 'gameData' in data and isinstance(data['gameData'], list):
            for item in data['gameData']:
                if isinstance(item, dict) and item.get('section') == 'Action':
                    action_section = item
                    break
        
        # Extract per-question correctness from Action section
        if action_section and 'gameData' in action_section and isinstance(action_section['gameData'], list):
            for question_idx, question in enumerate(action_section['gameData'], 1):
                if not isinstance(question, dict):
                    continue
                
                # Get chosenOption and correctOption
                chosen_option = question.get('chosenOption')
                correct_option = question.get('correctOption')
                
                # If either is None, score = 0
                if chosen_option is None or correct_option is None:
                    is_correct = 0
                else:
                    try:
                        chosen_option = int(chosen_option)
                        correct_option = int(correct_option)
                    except (ValueError, TypeError):
                        is_correct = 0
                    else:
                        # Compare chosenOption with correctOption
                        is_correct = 1 if chosen_option == correct_option else 0
                
                results.append({
                    'question_number': question_idx,
                    'is_correct': is_correct,
                    'game_name': game_name
                })
        
        return results
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return results


def get_game_type(game_name):
    """Map game name to its processing type"""
    game_type_mapping = {
        # correctSelections games
        'Relational Comparison': 'correctSelections',
        'Quantitative Comparison': 'correctSelections',
        'Relational Comparison II': 'correctSelections',
        'Number Comparison': 'correctSelections',
        'Primary Emotion Labelling': 'correctSelections',
        'Emotion Identification': 'correctSelections',
        'Identification of all emotions': 'correctSelections',
        'Beginning Sound Pa Cha Sa': 'correctSelections',
        
        # flow games
        'Revision Primary Colors': 'flow',
        'Revision Primary Shapes': 'flow',
        'Rhyming Words': 'flow',
        
        # action level games
        'Shape Circle': 'action_level',
        'Shape Triangle': 'action_level',
        'Shape Square': 'action_level',
        'Shape Rectangle': 'action_level',
        'Color Red': 'action_level',
        'Color Yellow': 'action_level',
        'Color Blue': 'action_level',
        'Numbers I': 'action_level',
        'Numbers II': 'action_level',
        'Numerals 1-10': 'action_level',
        'Beginning Sound Ma Ka La': 'action_level',
        'Beginning Sound Ba Ra Na': 'action_level',
        # Note: Beginning Sounds Ma/Cha/Ba is processed through game_completed (jsonData method)
    }
    return game_type_mapping.get(game_name, None)


def fetch_question_correctness_data() -> pd.DataFrame:
    """DEPRECATED: This function is no longer used. Use process_question_correctness() instead.
    
    This function has been replaced by process_question_correctness() which uses scores_data.csv
    instead of fetching from the database. Kept for backward compatibility only.
    """
    print("=" * 60)
    print("WARNING: fetch_question_correctness_data() is DEPRECATED")
    print("=" * 60)
    print("  This function is no longer used.")
    print("  Use process_question_correctness() instead, which uses scores_data.csv")
    print("=" * 60)
    return pd.DataFrame(columns=['game_name','question_number','correctness','percent','user_count','total_users'])


def parse_custom_dimension_1_correct_selections(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract correctSelections (for first query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        # Extract correctSelections from nested structure
        # Path: gameData[*].gameData[*].statistics.correctSelections
        if 'gameData' in data and len(data['gameData']) > 0:
            for game_data in data['gameData']:
                if 'gameData' in game_data and len(game_data['gameData']) > 0:
                    for inner_game_data in game_data['gameData']:
                        if 'statistics' in inner_game_data and 'correctSelections' in inner_game_data['statistics']:
                            correct_selections = inner_game_data['statistics']['correctSelections']
                            if correct_selections is not None:
                                return int(correct_selections)
        
        return 0
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0


def clean_malformed_json(json_str):
    """Clean malformed JSON strings by fixing common issues like consecutive double quotes"""
    if not isinstance(json_str, str):
        return json_str
    
    import re
    
    # Fix consecutive double quotes (common issue in the data)
    # Pattern: "value""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    # This happens when quotes get duplicated inside a string value
    # Replace all occurrences of 3+ consecutive quotes with a single quote
    # This is safe because JSON doesn't allow unescaped quotes inside strings anyway
    json_str = re.sub(r'"{3,}', '"', json_str)
    
    # Fix pattern where quotes appear right after a string value but before the closing quote
    # Pattern: "value""""more" -> "value""more"
    # This handles cases where quotes got duplicated between the value and the closing quote
    json_str = re.sub(r'([^\\"])"{2,}([^"\\])', r'\1"\2', json_str)
    
    # Also handle cases where quotes might be at the end of a string value
    # Pattern: "value"""" -> "value""
    json_str = re.sub(r'([^\\"])"{4,}"', r'\1""', json_str)
    
    # Fix other common issues
    # Remove any null bytes
    json_str = json_str.replace('\x00', '')
    
    # Remove any control characters except newlines and tabs
    json_str = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', json_str)
    
    return json_str


def parse_custom_dimension_1_json_data(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from jsonData (for second query)
    
    Handles two structures:
    1. Nested: gameData[*] where section="Action" -> jsonData[*] -> userResponse[0].isCorrect
    2. Root level: section="Action" -> jsonData[*] -> userResponse[0].isCorrect (for Beginning Sounds games: Ma/Cha/Ba, Ka/Na/Ta, Ta/Va/Ga)
    
    For each level in jsonData:
    - Read userResponse[0].isCorrect
    - If true → score = 1, if false → score = 0
    - Sum all level scores to get total_score
    """
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        # Handle case where custom_dim_1 might already be a dict
        if isinstance(custom_dim_1, dict):
            data = custom_dim_1
        else:
            # Parse JSON - try to handle malformed JSON
            json_str = str(custom_dim_1).strip()
            
            # Remove any leading/trailing quotes if present
            if json_str.startswith('"') and json_str.endswith('"'):
                json_str = json_str[1:-1]
            
            # Clean malformed JSON
            json_str = clean_malformed_json(json_str)
            
            try:
                data = json.loads(json_str)
            except json.JSONDecodeError:
                # Try additional fixes
                try:
                    # Try unescaping
                    json_str = json_str.encode().decode('unicode_escape')
                    data = json.loads(json_str)
                except:
                    # If full JSON parsing fails, try to extract just the Action section
                    json_data = extract_action_section_from_string(json_str)
                    if json_data:
                        # Successfully extracted Action section, process it directly
                        total_score = 0
                        for level_data in json_data:
                            if isinstance(level_data, dict) and 'userResponse' in level_data:
                                user_responses = level_data.get('userResponse', [])
                                if isinstance(user_responses, list) and len(user_responses) > 0:
                                    # Use first userResponse
                                    response = user_responses[0]
                                    if isinstance(response, dict) and 'isCorrect' in response:
                                        is_correct = response['isCorrect']
                                        # Handle both boolean True and string "true"
                                        if is_correct is True or (isinstance(is_correct, str) and is_correct.lower() == 'true'):
                                            total_score += 1
                        return total_score
                    # If we can't extract Action section, return 0
                    return 0
        
        total_score = 0
        
        # Case 1: Check if Action section is at root level (for Beginning Sounds Ma/Cha/Ba)
        if isinstance(data, dict) and data.get('section') == 'Action' and 'jsonData' in data:
            json_data = data['jsonData']
            if isinstance(json_data, list):
                for level_data in json_data:
                            if isinstance(level_data, dict) and 'userResponse' in level_data:
                                user_responses = level_data.get('userResponse', [])
                                if isinstance(user_responses, list) and len(user_responses) > 0:
                                    # Use first userResponse
                                    response = user_responses[0]
                                    if isinstance(response, dict) and 'isCorrect' in response:
                                        is_correct = response['isCorrect']
                                        # Handle both boolean True and string "true"
                                        if is_correct is True or (isinstance(is_correct, str) and is_correct.lower() == 'true'):
                                            total_score += 1
            return total_score
        
        # Case 2: Check nested structure (gameData[*] where section="Action")
        if 'gameData' in data and len(data['gameData']) > 0:
            for game_data in data['gameData']:
                # Look for section = "Action"
                if game_data.get('section') == 'Action' and 'jsonData' in game_data:
                    json_data = game_data['jsonData']
                    
                    if isinstance(json_data, list):
                        for level_data in json_data:
                            if isinstance(level_data, dict) and 'userResponse' in level_data:
                                user_responses = level_data.get('userResponse', [])
                                if isinstance(user_responses, list) and len(user_responses) > 0:
                                    # Use first userResponse
                                    response = user_responses[0]
                                    if isinstance(response, dict) and 'isCorrect' in response:
                                        is_correct = response['isCorrect']
                                        # Handle both boolean True and string "true"
                                        if is_correct is True or (isinstance(is_correct, str) and is_correct.lower() == 'true'):
                                            total_score += 1
            
            return total_score
        
        return 0
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0


def parse_custom_dimension_1_mcq_completed(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from mcq_completed games
    
    Structure:
    {
      "section": "Action",
      "gameData": [
          {
            "options": [
                {"path": "o1.png", "isCorrect": false},
                {"path": "o2.png", "isCorrect": true},
                {"path": "o3.png", "isCorrect": false}
            ],
            "chosenOption": 1,
            ...
          },
          ...
      ]
    }
    
    For each question:
    - Look at chosenOption (index chosen by the user)
    - Look at options[chosenOption].isCorrect
    - If isCorrect == true, score = 1, else score = 0
    - Sum all scores to get total score
    
    Note: Different questions can have different numbers of options.
    """
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        total_score = 0
        
        # Look for Action section - can be at top level or inside gameData array
        action_section = None
        
        # Case 1: Action section at top level
        if isinstance(data, dict) and 'section' in data and data['section'] == 'Action':
            action_section = data
        # Case 2: Action section inside gameData array (similar to other game types)
        elif isinstance(data, dict) and 'gameData' in data and isinstance(data['gameData'], list):
            for item in data['gameData']:
                if isinstance(item, dict) and item.get('section') == 'Action':
                    action_section = item
                    break
        
        # Extract scores from Action section
        if action_section and 'gameData' in action_section and isinstance(action_section['gameData'], list):
            for question in action_section['gameData']:
                if not isinstance(question, dict):
                    continue
                
                # Get options and chosenOption
                options = question.get('options', [])
                chosen_option = question.get('chosenOption')
                
                # Skip if no options or chosenOption is None
                if not isinstance(options, list) or len(options) == 0:
                    continue
                
                # Handle chosenOption - convert to int if needed, skip if None
                if chosen_option is None:
                    continue
                try:
                    chosen_option = int(chosen_option)
                except (ValueError, TypeError):
                    continue
                
                # Check if chosenOption is within bounds
                if 0 <= chosen_option < len(options):
                    chosen_option_data = options[chosen_option]
                    if isinstance(chosen_option_data, dict):
                        # Check isCorrect - handle both boolean True and string "true"
                        is_correct = chosen_option_data.get('isCorrect', False)
                        if is_correct is True or (isinstance(is_correct, str) and is_correct.lower() == 'true'):
                            total_score += 1
                        # else: score = 0 (already initialized)
        
        return total_score
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0


def parse_custom_dimension_1_mcq_completed_with_correct_option(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from mcq_completed games using correctOption
    
    This is for games like Positions that use chosenOption and correctOption instead of isCorrect.
    
    Structure:
    {
      "section": "Action",
      "gameData": [
          {
            "options": [...],
            "chosenOption": 1,
            "correctOption": 2,
            ...
          },
          ...
      ]
    }
    
    For each question:
    - Look at chosenOption (index chosen by the user)
    - Look at correctOption (the correct index)
    - If chosenOption == correctOption, score = 1, else score = 0
    - Sum all scores to get total score
    
    Note: Different questions can have different numbers of options.
    """
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        total_score = 0
        
        # Look for Action section - can be at top level or inside gameData array
        action_section = None
        
        # Case 1: Action section at top level
        if isinstance(data, dict) and 'section' in data and data['section'] == 'Action':
            action_section = data
        # Case 2: Action section inside gameData array (similar to other game types)
        elif isinstance(data, dict) and 'gameData' in data and isinstance(data['gameData'], list):
            for item in data['gameData']:
                if isinstance(item, dict) and item.get('section') == 'Action':
                    action_section = item
                    break
        
        # Extract scores from Action section
        if action_section and 'gameData' in action_section and isinstance(action_section['gameData'], list):
            for question in action_section['gameData']:
                if not isinstance(question, dict):
                    continue
                
                # Get chosenOption and correctOption
                chosen_option = question.get('chosenOption')
                correct_option = question.get('correctOption')
                
                # Skip if either is None
                if chosen_option is None or correct_option is None:
                    continue
                
                try:
                    chosen_option = int(chosen_option)
                    correct_option = int(correct_option)
                except (ValueError, TypeError):
                    continue
                
                # Compare chosenOption with correctOption
                if chosen_option == correct_option:
                    total_score += 1
                # else: score = 0 (already initialized)
        
        return total_score
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0


def parse_custom_dimension_1_action_games(custom_dim_1):
    """Parse custom_dimension_1 JSON to extract total score from action games (for third query)"""
    try:
        if pd.isna(custom_dim_1) or custom_dim_1 is None or custom_dim_1 == '' or custom_dim_1 == 'null':
            return 0
        
        # Parse JSON
        data = json.loads(custom_dim_1)
        
        # Extract score from action games structure
        # Structure: {"options": [{"path": "o1.png", "isCorrect": false}, ...], "chosenOption": 1, "totalTaps": 2, "time": 1754568484640}
        total_score = 0
        
        # Check if this is a single question record
        if 'options' in data and 'chosenOption' in data:
            chosen_option = data.get('chosenOption')
            
            # If chosenOption is null, score = 0
            if chosen_option is None:
                return 0
            
            # Check if chosenOption is within bounds and if the chosen option is correct
            options = data.get('options', [])
            if isinstance(options, list) and 0 <= chosen_option < len(options):
                chosen_option_data = options[chosen_option]
                if isinstance(chosen_option_data, dict) and chosen_option_data.get('isCorrect', False):
                    total_score = 1
                else:
                    total_score = 0
            else:
                total_score = 0
        
        return total_score
    except (json.JSONDecodeError, TypeError, AttributeError, KeyError, IndexError, ValueError):
        return 0


def get_game_name_from_custom_dimension_2(custom_dim_2):
    """Map custom_dimension_2 to game name"""
    game_mapping = {
        "12": 'Shape Circle',
        "24": 'Color Red',
        "28": 'Shape Triangle',
        "40": 'Color Yellow',
        "54": 'Numeracy I',
        "56": 'Numeracy II',
        "50": 'Relational Comparison',
        "52": 'Quantity Comparison',
        "60": 'Shape Square',
        "62": 'Revision Primary Colors',
        "58": 'Color Blue',
        "70": 'Relational Comparison II',
        "66": 'Rhyming Words Hindi',
        "68": 'Rhyming Words Marathi',
        "64": 'Revision Primary Shapes',
        "72": 'Number Comparison',
        "78": 'Primary Emotion I',
        "80": 'Primary Emotion II',
        "82": 'Shape Rectangle',
        "84": 'Numerals 1-10',
        "83": 'Numerals 1-10 Child',
        "76": 'Beginning Sound Ma Ka La Marathi',
        "74": 'Beginning Sound Ma Ka La Hindi',
        "88": 'Beginning Sound Pa Cha Sa Marathi',
        "86": 'Beginning Sound Pa Cha Sa Hindi',
        "94": 'Common Shapes',
        "96": 'Primary Colors'
    }
    return game_mapping.get(custom_dim_2, f'Game {custom_dim_2}')


def extract_per_question_correctness(df_score: pd.DataFrame) -> pd.DataFrame:
    """Extract per-question correctness across games using the same processing method as score distribution.
    
    This function processes each game dynamically, using the same logic as calculate_score_distribution_combined:
    - For game_completed: Tries both correct_selections and jsonData methods, picks the one that works
    - Also checks for roundDetails first (for games like Quantitative Comparison)
    - For action_level: Uses parse_custom_dimension_1_action_games (same as score distribution)

    Output columns:
    - game_name: str
    - idvisitor_converted: str/int
    - idvisit: int
    - session_instance: int (for action-level; 1 for others)
    - question_number: int (1-based)
    - is_correct: int (1 correct, 0 incorrect)
    """
    # Check for optional columns (language and game_code)
    has_language = 'language' in df_score.columns
    has_game_code = 'game_code' in df_score.columns
    
    if df_score.empty:
        base_columns = ['game_name', 'idvisitor_converted', 'idvisit', 'session_instance', 'question_number', 'is_correct']
        if has_language:
            base_columns.append('language')
        if has_game_code:
            base_columns.append('game_code')
        return pd.DataFrame(columns=base_columns)

    # Ensure expected columns exist
    cols_needed = ['game_name', 'idvisit', 'action_name', 'custom_dimension_1', 'idvisitor_converted', 'server_time']
    for c in cols_needed:
        if c not in df_score.columns:
            base_columns = ['game_name', 'idvisitor_converted', 'idvisit', 'session_instance', 'question_number', 'is_correct']
            if has_language:
                base_columns.append('language')
            if has_game_code:
                base_columns.append('game_code')
            return pd.DataFrame(columns=base_columns)

    # Parse timestamps
    df_score = df_score.copy()
    try:
        df_score['server_time'] = pd.to_datetime(df_score['server_time'])
    except Exception:
        pass

    # Note: We no longer exclude sorting games - they should be processed like other games
    
    # Helper function to create row dict with optional columns
    def create_question_row(game_name_val, idvisitor, idvisit, session_instance, question_number, is_correct, row_tuple=None):
        """Create a question row dict, including language and game_code (domain extracted) if available"""
        row_dict = {
            'game_name': game_name_val,
            'idvisitor_converted': idvisitor,
            'idvisit': idvisit,
            'session_instance': session_instance,
            'question_number': question_number,
            'is_correct': is_correct
        }
        if has_language and row_tuple is not None:
            row_dict['language'] = getattr(row_tuple, 'language', None)
        if has_game_code and row_tuple is not None:
            # Extract domain from game_code (e.g., HY-29-LL-06 -> LL)
            full_game_code = getattr(row_tuple, 'game_code', None)
            if full_game_code is not None and not pd.isna(full_game_code):
                row_dict['game_code'] = extract_domain_from_game_code(full_game_code)
            else:
                row_dict['game_code'] = None
        return row_dict

    print(f"\nProcessing per-question correctness for {df_score['game_name'].nunique()} unique games")
    print(f"  - Total records: {len(df_score):,}")
    if has_language:
        print(f"  - Language column found: will be preserved in output")
    if has_game_code:
        print(f"  - Game code column found: will be preserved in output")

    # Split by action types (same as score distribution)
    # Separate game_completed and mcq_completed for different processing methods
    # Games that should use mcq_completed method (Action section with gameData):
    # - Shape Rectangle: uses chosenOption/isCorrect method
    # - Numerals 1-10: uses chosenOption/isCorrect method
    # - Positions: uses chosenOption/isCorrect method
    # Note: "Beginning Sounds Ma/Cha/Ba", "Beginning Sounds Ka/Na/Ta", "Beginning Sounds Ta/Va/Ga"
    #       and "Beginning Sound Ba/Ra/Na" all have action_name like "beginning_sound_ma_cha_ba_hindi_hybrid_game_completed"
    #       (contains "hybrid_game_completed") and should use flow/jsonData method, so they go to game_completed_data
    games_to_use_mcq_completed_method = ['Shape Rectangle', 'Numerals 1-10', 'Positions']
    games_to_use_json_data_method = ['Beginning Sound Ba/Ra/Na', 'Beginning Sounds Ma/Cha/Ba', 'Beginning Sounds Ka/Na/Ta', 'Beginning Sounds Ta/Va/Ga']
    
    # Helper function for case-insensitive game name matching (handles whitespace and case variations)
    def _is_game_in_list(game_name, game_list):
        """Check if game_name matches any game in game_list (case-insensitive, handles whitespace)"""
        if pd.isna(game_name):
            return False
        game_name_clean = str(game_name).strip().lower()
        return any(game_name_clean == str(g).strip().lower() for g in game_list)
    
    # Create boolean masks for case-insensitive matching
    is_json_data_game = df_score['game_name'].apply(lambda x: _is_game_in_list(x, games_to_use_json_data_method))
    is_mcq_completed_game = df_score['game_name'].apply(lambda x: _is_game_in_list(x, games_to_use_mcq_completed_method))
    
    # Filter for game_completed_data:
    # - Includes action_name containing 'game_completed' (matches "hybrid_game_completed" too)
    # - Excludes action_name containing 'mcq_completed' (unless it's a jsonData game)
    # - Also includes mcq_completed records for jsonData games (to route them correctly)
    # - Excludes games that should use mcq_completed method
    game_completed_data = df_score[
        ((df_score['action_name'].str.contains('game_completed', na=False, case=False) & 
          ~df_score['action_name'].str.contains('mcq_completed', na=False, case=False)) |
         (df_score['action_name'].str.contains('mcq_completed', na=False, case=False) &
          is_json_data_game)) &
        ~is_mcq_completed_game
    ].copy()
    
    mcq_completed_data = df_score[
        (df_score['action_name'].str.contains('mcq_completed', na=False, case=False) &
         ~is_json_data_game) |
        (df_score['action_name'].str.contains('game_completed', na=False, case=False) &
         is_mcq_completed_game)
    ].copy()
    
    action_level_data = df_score[df_score['action_name'].str.contains('action_level', na=False)].copy()

    print(f"  - game_completed records: {len(game_completed_data):,}")
    print(f"  - mcq_completed records: {len(mcq_completed_data):,}")
    print(f"  - action_level records: {len(action_level_data):,}")
    print(f"  - Unique games in game_completed: {game_completed_data['game_name'].nunique()}")
    print(f"  - Unique games in mcq_completed: {mcq_completed_data['game_name'].nunique()}")
    print(f"  - Unique games in action_level: {action_level_data['game_name'].nunique()}")
    
    # Debug: Check if Beginning Sounds games are in the data
    beginning_sounds_games = ['Beginning Sound Ba/Ra/Na', 'Beginning Sounds Ma/Cha/Ba', 'Beginning Sounds Ka/Na/Ta', 'Beginning Sounds Ta/Va/Ga']
    print(f"\n  [DEBUG] Checking Beginning Sounds games in data:")
    for game in beginning_sounds_games:
        in_game_completed = game_completed_data[game_completed_data['game_name'].str.strip().str.lower() == game.strip().lower()]
        in_mcq_completed = mcq_completed_data[mcq_completed_data['game_name'].str.strip().str.lower() == game.strip().lower()]
        in_all = df_score[df_score['game_name'].str.strip().str.lower() == game.strip().lower()]
        print(f"    - {game}:")
        print(f"      In all data: {len(in_all):,} records")
        print(f"      In game_completed: {len(in_game_completed):,} records")
        print(f"      In mcq_completed: {len(in_mcq_completed):,} records")
        if len(in_all) > 0 and len(in_game_completed) == 0 and len(in_mcq_completed) == 0:
            # Show sample game names to see what the actual names are
            sample_names = in_all['game_name'].unique()[:3]
            print(f"      WARNING: Game exists in data but not in filtered sets!")
            print(f"      Sample actual game names: {list(sample_names)}")

    per_question_rows: list[dict] = []

    # Helper function to find matching game name (case-insensitive, handles variations)
    # Used only for action_level filtering
    def _find_game_method(game_name: str) -> str:
        """Find processing method for a game name (case-insensitive match) - used for action_level filtering only"""
        # Only check if it's action_level (games that should be in action_level_data)
        # Note: Beginning Sound Ba/Ra/Na, Beginning Sounds Ma/Cha/Ba, Ka/Na/Ta, and Ta/Va/Ga are processed through game_completed
        # (they come as game_completed records, not action_level records)
        action_level_games = {
            'Beginning Sounds Ma/Ka/La', 'Color Blue', 'Color Red', 'Color Yellow',
            'Numbers I', 'Numbers II', 'Numerals 1-10', 'Shape Circle', 'Shape Rectangle',
            'Shape Square', 'Shape Triangle', 'Positions', 'Sorting Primary Colors'
        }
        game_name_clean = str(game_name).strip().lower()
        for mapped_game in action_level_games:
            if game_name_clean == str(mapped_game).strip().lower():
                return 'action_level'
        return None

    # 1) Handle game_completed/mcq_completed - Process each game dynamically (same as score distribution)
    if not game_completed_data.empty:
        unique_games = game_completed_data['game_name'].nunique()
        total_game_completed_records = len(game_completed_data)
        print(f"\n  [STEP 1] Processing {unique_games} unique games from game_completed/mcq_completed")
        print(f"  - Total records: {total_game_completed_records:,}")
        print(f"  - Using dynamic method selection (same as score distribution)...")
        print(f"  - Games to process: {sorted(game_completed_data['game_name'].unique())}")
        
        import time
        step_start_time = time.time()
        
        # Process each game dynamically - try both methods and pick the best one (same as score distribution)
        games_processed = 0
        games_skipped = 0
        all_games_list = sorted(game_completed_data['game_name'].unique())
        
        for game_idx, game_name in enumerate(all_games_list, 1):
            print(f"\n    [GAME {game_idx}/{len(all_games_list)}] Processing: {game_name}")
            game_data = game_completed_data[game_completed_data['game_name'] == game_name].copy()
            
            # Skip action_level games in game_completed (they should be in action_level_data)
            if _find_game_method(game_name) == 'action_level':
                print(f"    [SKIP] {game_name}: Should be processed in action_level section")
                games_skipped += 1
                continue
            
            total_records = len(game_data)
            print(f"    - Testing methods for {game_name} ({total_records:,} records)...")
            
            # Try both methods on a sample to determine which works better (same as score distribution)
            # Method 1: correct_selections (roundDetails or nested gameData)
            correct_selections_count = 0
            correct_selections_total_questions = 0
            # Method 2: flow (gameData->jsonData)
            flow_count = 0
            flow_total_questions = 0
            
            # Test on up to 200 records or all records if less than 200 (increased from 100 for better coverage)
            test_sample_size = min(200, total_records)
            test_sample = game_data.head(test_sample_size)
            
            for row_tuple in test_sample.itertuples(index=False):
                raw = getattr(row_tuple, 'custom_dimension_1', None)
                if pd.isna(raw) or raw in (None, '', 'null'):
                    continue
                
                # Test Method 1: correct_selections
                try:
                    results = parse_correct_selections_questions(raw, game_name)
                    if len(results) > 0:
                        correct_selections_count += 1
                        correct_selections_total_questions += len(results)
                except Exception:
                    pass
                
                # Test Method 2: flow
                try:
                    results = parse_flow_stop_go_questions(raw, game_name)
                    if len(results) > 0:
                        flow_count += 1
                        flow_total_questions += len(results)
                except Exception:
                    pass
            
            # Games that should prefer flow method (jsonData structure, same as Beginning Sound Ba/Ra/Na)
            # These games have action_name like "beginning_sound_ma_cha_ba_hindi_hybrid_game_completed"
            # Includes: Beginning Sound Ba/Ra/Na, Beginning Sounds Ma/Cha/Ba, Ka/Na/Ta, Ta/Va/Ga
            games_prefer_flow = ['Beginning Sound Ba/Ra/Na', 'Beginning Sounds Ma/Cha/Ba', 'Beginning Sounds Ka/Na/Ta', 'Beginning Sounds Ta/Va/Ga']
            
            # Choose the method that produces more valid results (same logic as score distribution)
            # Prefer the method that extracts more questions overall, not just more records
            # For specific games, prefer flow if both methods work
            # Use case-insensitive matching to handle any name variations
            game_name_normalized = str(game_name).strip().lower()
            games_prefer_flow_normalized = [g.strip().lower() for g in games_prefer_flow]
            if game_name_normalized in games_prefer_flow_normalized and flow_total_questions > 0:
                processing_method = 'flow'
                print(f"    - {game_name}: Using flow method (preferred for this game, {flow_count} valid records, {flow_total_questions} questions in sample)")
            elif correct_selections_total_questions >= flow_total_questions and correct_selections_count > 0:
                processing_method = 'correct_selections'
                print(f"    - {game_name}: Using correct_selections method ({correct_selections_count} valid records, {correct_selections_total_questions} questions in sample)")
            elif flow_total_questions > 0:
                processing_method = 'flow'
                print(f"    - {game_name}: Using flow method ({flow_count} valid records, {flow_total_questions} questions in sample)")
            elif correct_selections_count > 0:
                # Fallback: use correct_selections if it has any valid records
                processing_method = 'correct_selections'
                print(f"    - {game_name}: Using correct_selections method (fallback: {correct_selections_count} valid records in sample)")
            elif flow_count > 0:
                # Fallback: use flow if it has any valid records
                processing_method = 'flow'
                print(f"    - {game_name}: Using flow method (fallback: {flow_count} valid records in sample)")
            else:
                print(f"    - {game_name}: No valid results found with either method in sample of {test_sample_size} records")
                print(f"      WARNING: This game will be skipped. Check if data structure matches expected format.")
                games_skipped += 1
                continue
            
            print(f"    [PROCESS] {game_name}: Using method '{processing_method}' ({total_records:,} records)")
            games_processed += 1
            
            # Process records using the determined method (use itertuples for better performance)
            records_processed = 0
            records_with_data = 0
            questions_extracted = 0
            progress_interval = max(1000, total_records // 10)  # Show progress every 10% or 1000 records
            
            import time
            start_time = time.time()
            
            for idx, row_tuple in enumerate(game_data.itertuples(index=False), 1):
                # Show progress at intervals
                if idx % progress_interval == 0 or idx == total_records:
                    elapsed = time.time() - start_time
                    rate = idx / elapsed if elapsed > 0 else 0
                    remaining = (total_records - idx) / rate if rate > 0 else 0
                    print(f"      [PROGRESS] {game_name}: {idx:,}/{total_records:,} records ({idx*100//total_records}%) | "
                          f"Processed: {records_processed:,} | Questions: {questions_extracted:,} | "
                          f"Rate: {rate:.0f} rec/s | ETA: {remaining:.0f}s", flush=True)
                
                # Get values from named tuple
                raw = getattr(row_tuple, 'custom_dimension_1', None)
                if pd.isna(raw) or raw in (None, '', 'null'):
                    continue
                
                game_name_val = getattr(row_tuple, 'game_name', game_name)
                idvisitor = getattr(row_tuple, 'idvisitor_converted', None)
                idvisit = getattr(row_tuple, 'idvisit', None)
                
                # Method 1: correct_selections (roundDetails)
                if processing_method == 'correct_selections':
                    try:
                        results = parse_correct_selections_questions(raw, game_name)
                        if len(results) > 0:
                            records_with_data += 1
                            questions_extracted += len(results)
                            for q_result in results:
                                per_question_rows.append(create_question_row(
                                    game_name_val, idvisitor, idvisit, 1,
                                    int(q_result['question_number']),
                                    int(q_result['is_correct']), row_tuple
                                ))
                        records_processed += 1
                    except Exception:
                        records_processed += 1
                        pass
                
                # Method 2: flow stop&go
                elif processing_method == 'flow':
                    try:
                        results = parse_flow_stop_go_questions(raw, game_name)
                        if len(results) > 0:
                            records_with_data += 1
                            questions_extracted += len(results)
                            for q_result in results:
                                per_question_rows.append(create_question_row(
                                    game_name_val, idvisitor, idvisit, 1,
                                    int(q_result['question_number']),
                                    int(q_result['is_correct']), row_tuple
                                ))
                        records_processed += 1
                    except Exception:
                        records_processed += 1
                        pass
            
            elapsed_total = time.time() - start_time
            print(f"      [OK] {game_name}: Completed in {elapsed_total:.1f}s | "
                  f"Records: {records_processed:,}/{total_records:,} | "
                  f"With data: {records_with_data:,} | "
                  f"Questions extracted: {questions_extracted:,}", flush=True)
                

        step_elapsed = time.time() - step_start_time
        total_questions = len([r for r in per_question_rows])
        print(f"\n  [STEP 1 SUMMARY] Completed in {step_elapsed:.1f}s")
        print(f"    - Processed: {games_processed} games")
        print(f"    - Skipped: {games_skipped} games")
        print(f"    - Total per-question records extracted: {total_questions:,}")
    
    # 1.5) Handle mcq_completed data (Action section with gameData - same as score distribution)
    if not mcq_completed_data.empty:
        print(f"\n  [STEP 1.5] Processing {mcq_completed_data['game_name'].nunique()} unique games from mcq_completed")
        print(f"    - Games: {sorted(mcq_completed_data['game_name'].unique())}")
        
        # Games that use correctOption instead of isCorrect
        # Note: Positions now uses isCorrect method, so this list is empty
        games_with_correct_option = []
        
        import time
        step_start_time = time.time()
        mcq_games_processed = 0
        mcq_records_processed = 0
        mcq_records_with_data = 0
        mcq_questions_extracted = 0
        
        for game_name in sorted(mcq_completed_data['game_name'].unique()):
            game_data = mcq_completed_data[mcq_completed_data['game_name'] == game_name].copy()
            total_records = len(game_data)
            print(f"\n    [GAME] Processing {game_name}: {total_records:,} records")
            mcq_games_processed += 1
            
            # Choose the appropriate parsing method (same as score distribution)
            if game_name in games_with_correct_option:
                print(f"      - Using correctOption method (chosenOption vs correctOption)")
                parse_func = parse_mcq_completed_questions_with_correct_option
            else:
                print(f"      - Using isCorrect method (options[chosenOption].isCorrect)")
                parse_func = parse_mcq_completed_questions
            
            # Process records
            progress_interval = max(1000, total_records // 10)
            start_time = time.time()
            game_records_processed = 0
            game_records_with_data = 0
            game_questions_extracted = 0
            
            for idx, row_tuple in enumerate(game_data.itertuples(index=False), 1):
                if idx % progress_interval == 0 or idx == total_records:
                    elapsed = time.time() - start_time
                    rate = idx / elapsed if elapsed > 0 else 0
                    remaining = (total_records - idx) / rate if rate > 0 else 0
                    print(f"      [PROGRESS] {game_name}: {idx:,}/{total_records:,} records ({idx*100//total_records}%) | "
                          f"Processed: {game_records_processed:,} | Questions: {game_questions_extracted:,} | "
                          f"Rate: {rate:.0f} rec/s | ETA: {remaining:.0f}s", flush=True)
                
                raw = getattr(row_tuple, 'custom_dimension_1', None)
                if pd.isna(raw) or raw in (None, '', 'null'):
                    continue
                
                game_name_val = getattr(row_tuple, 'game_name', game_name)
                idvisitor = getattr(row_tuple, 'idvisitor_converted', None)
                idvisit = getattr(row_tuple, 'idvisit', None)
                
                try:
                    results = parse_func(raw, game_name)
                    if len(results) > 0:
                        game_records_with_data += 1
                        game_questions_extracted += len(results)
                        for q_result in results:
                            per_question_rows.append(create_question_row(
                                game_name_val, idvisitor, idvisit, 1,
                                int(q_result['question_number']),
                                int(q_result['is_correct']), row_tuple
                            ))
                    game_records_processed += 1
                except Exception:
                    game_records_processed += 1
                    pass
            
            mcq_records_processed += game_records_processed
            mcq_records_with_data += game_records_with_data
            mcq_questions_extracted += game_questions_extracted
            
            elapsed_total = time.time() - start_time
            print(f"      [OK] {game_name}: Completed in {elapsed_total:.1f}s | "
                  f"Records: {game_records_processed:,}/{total_records:,} | "
                  f"With data: {game_records_with_data:,} | "
                  f"Questions extracted: {game_questions_extracted:,}", flush=True)
        
        step_elapsed = time.time() - step_start_time
        print(f"\n  [STEP 1.5 SUMMARY] Completed in {step_elapsed:.1f}s")
        print(f"    - Processed: {mcq_games_processed} games")
        print(f"    - Total per-question records extracted: {mcq_questions_extracted:,}")
    
    # 2) Handle action_level records (same as score distribution)
    if not action_level_data.empty:
        # Process all action_level games (same as score distribution - no filtering)
        unique_action_games = action_level_data['game_name'].nunique()
        print(f"\n  [STEP 2] Processing {unique_action_games} unique games from action_level")
        print(f"    - Games: {sorted(action_level_data['game_name'].unique())}")
        
        # Deduplicate (same as score distribution)
        before_dedup = len(action_level_data)
        print(f"    - Records before deduplication: {before_dedup:,}")
        if 'idlink_va' in action_level_data.columns:
            action_level_data = action_level_data.drop_duplicates(subset=['idlink_va'], keep='first')
            print(f"    - Using idlink_va for deduplication")
        else:
            action_level_data = action_level_data.drop_duplicates(
                subset=['idvisitor_converted', 'game_name', 'idvisit', 'server_time', 'custom_dimension_1'],
                keep='first'
            )
            print(f"    - Using composite key for deduplication")
        after_dedup = len(action_level_data)
        if before_dedup != after_dedup:
            print(f"    [OK] Removed {before_dedup - after_dedup:,} duplicate records ({before_dedup:,} -> {after_dedup:,})")
        else:
            print(f"    [OK] No duplicates found")
        
        # Sort and create session instances (same as score distribution)
        print(f"    - Sorting records and creating session instances...")
        action_level_data = action_level_data.sort_values(['idvisitor_converted', 'game_name', 'idvisit', 'server_time'])
        
        session_instances = []
        current_session = 1
        prev_user = prev_game = prev_visit = None
        prev_time = None
        
        # Use itertuples for better performance
        total_action_records = len(action_level_data)
        print(f"    - Processing {total_action_records:,} records to create session instances...")
        progress_interval = max(5000, total_action_records // 10)  # Show progress every 10% or 5000 records
        
        import time
        start_time = time.time()
        
        for idx, row_tuple in enumerate(action_level_data.itertuples(index=False), 1):
            # Show progress at intervals
            if idx % progress_interval == 0 or idx == total_action_records:
                elapsed = time.time() - start_time
                rate = idx / elapsed if elapsed > 0 else 0
                remaining = (total_action_records - idx) / rate if rate > 0 else 0
                print(f"      [PROGRESS] Session instances: {idx:,}/{total_action_records:,} ({idx*100//total_action_records}%) | "
                      f"Rate: {rate:.0f} rec/s | ETA: {remaining:.0f}s", flush=True)
            
            user = getattr(row_tuple, 'idvisitor_converted', None)
            game = getattr(row_tuple, 'game_name', None)
            visit = getattr(row_tuple, 'idvisit', None)
            time_val = getattr(row_tuple, 'server_time', None)
            
            if user != prev_user or game != prev_game or visit != prev_visit:
                current_session = 1
            elif prev_time is not None and (time_val - prev_time).total_seconds() > 300:
                current_session += 1
            
            session_instances.append(current_session)
            prev_user, prev_game, prev_visit, prev_time = user, game, visit, time_val
        
        elapsed_total = time.time() - start_time
        print(f"    [OK] Created session instances in {elapsed_total:.1f}s")
        
        action_level_data['session_instance'] = session_instances
        unique_sessions = action_level_data.groupby(['idvisitor_converted', 'game_name', 'idvisit', 'session_instance']).size()
        print(f"    [OK] Created {len(unique_sessions):,} unique game sessions")
        
        # Extract question number from action_name
        import re
        def _extract_level(action_name: str):
            try:
                m = re.search(r'action_level[_\- ]?(\d+)', str(action_name))
                if m:
                    return int(m.group(1))
            except Exception:
                pass
            return None
        
        levels = action_level_data['action_name'].apply(_extract_level)
        action_level_data['question_number'] = levels
        # Fallback numbering where level not found
        mask_missing = action_level_data['question_number'].isna()
        if mask_missing.any():
            action_level_data.loc[mask_missing, 'question_number'] = (
                action_level_data[mask_missing]
                .groupby(['idvisitor_converted', 'game_name', 'idvisit', 'session_instance'])
                .cumcount() + 1
            )
        
        # Compute correctness per record using parse_action_level_questions (same as score distribution)
        total_action_records = len(action_level_data)
        print(f"    - Parsing question scores from custom_dimension_1 using parse_action_level_questions...")
        print(f"    - Processing {total_action_records:,} records...")
        correct_count = 0
        incorrect_count = 0
        progress_interval = max(5000, total_action_records // 10)  # Show progress every 10% or 5000 records
        
        import time
        start_time = time.time()
        
        # Use itertuples for better performance
        for idx, row_tuple in enumerate(action_level_data.itertuples(index=False), 1):
            # Show progress at intervals
            if idx % progress_interval == 0 or idx == total_action_records:
                elapsed = time.time() - start_time
                rate = idx / elapsed if elapsed > 0 else 0
                remaining = (total_action_records - idx) / rate if rate > 0 else 0
                print(f"      [PROGRESS] Parsing: {idx:,}/{total_action_records:,} ({idx*100//total_action_records}%) | "
                      f"Correct: {correct_count:,} | Incorrect: {incorrect_count:,} | "
                      f"Rate: {rate:.0f} rec/s | ETA: {remaining:.0f}s", flush=True)
            
            try:
                custom_dim = getattr(row_tuple, 'custom_dimension_1', None)
                game_name_val = getattr(row_tuple, 'game_name', None)
                question_num = int(getattr(row_tuple, 'question_number', 0))
                
                results = parse_action_level_questions(custom_dim, game_name_val, question_num)
                if len(results) > 0:
                    q_result = results[0]  # Should only have one result per record
                    is_correct = q_result['is_correct']
                    if is_correct == 1:
                        correct_count += 1
                    else:
                        incorrect_count += 1
                    
                    per_question_rows.append(create_question_row(
                        game_name_val,
                        getattr(row_tuple, 'idvisitor_converted', None),
                        getattr(row_tuple, 'idvisit', None),
                        int(getattr(row_tuple, 'session_instance', 1)),
                        question_num,
                        int(is_correct),
                        row_tuple
                    ))
                else:
                    incorrect_count += 1
                    # Still add record with is_correct=0 if no results
                    per_question_rows.append(create_question_row(
                        game_name_val,
                        getattr(row_tuple, 'idvisitor_converted', None),
                        getattr(row_tuple, 'idvisit', None),
                        int(getattr(row_tuple, 'session_instance', 1)),
                        question_num,
                        0,
                        row_tuple
                    ))
            except Exception:
                incorrect_count += 1
                # Still add record with is_correct=0 if parsing fails
                try:
                    per_question_rows.append(create_question_row(
                        getattr(row_tuple, 'game_name', None),
                        getattr(row_tuple, 'idvisitor_converted', None),
                        getattr(row_tuple, 'idvisit', None),
                        int(getattr(row_tuple, 'session_instance', 1)),
                        int(getattr(row_tuple, 'question_number', 0)),
                        0,
                        row_tuple
                    ))
                except:
                    pass
        
        elapsed_total = time.time() - start_time
        print(f"    [OK] Parsed scores in {elapsed_total:.1f}s: {correct_count:,} correct (1), {incorrect_count:,} incorrect (0)")

        # Pre-compute unique game names as a set for O(1) lookup instead of calling .unique() in the comprehension
        action_level_game_names = set(action_level_data['game_name'].unique())
        print(f"    [OK] Extracted {len([r for r in per_question_rows if r.get('game_name') in action_level_game_names])} per-question records from action_level")
        print(f"\n  [STEP 2 SUMMARY] Processed {unique_action_games} action_level games")
    
    total_extracted = len(per_question_rows)
    print(f"\n  [FINAL] Total per-question records extracted: {total_extracted:,}")
    
    if not per_question_rows:
        return pd.DataFrame(columns=[
            'game_name', 'idvisitor_converted', 'idvisit', 'session_instance', 'question_number', 'is_correct'
        ])

    return pd.DataFrame.from_records(per_question_rows)


def calculate_score_distribution_combined(df_score):
    """Calculate score distribution using the new unified query with hybrid_games table"""
    print("Processing score distribution data...")
    print(f"  - Total records received: {len(df_score)}")
    
    if df_score.empty:
        print("WARNING: No score distribution data found")
        return pd.DataFrame()
    
    # Check for optional columns (language and game_code)
    has_language = 'language' in df_score.columns
    has_game_code = 'game_code' in df_score.columns
    
    if has_language:
        print(f"  - Language column found: will be included in output")
    if has_game_code:
        print(f"  - Game code column found: will extract domain and include in output")
    
    # The game_name is now directly available from the hybrid_games table
    # We need to determine the score calculation method based on the action_name
    combined_df = pd.DataFrame()
    
    # Separate data based on action type for different score calculation methods
    # Separate game_completed and mcq_completed (action_level is no longer used)
    # Note: Games that should use mcq_completed method (Action section with gameData):
    # - Shape Rectangle: uses chosenOption/isCorrect method
    # - Numerals 1-10: uses chosenOption/isCorrect method
    # - Positions: uses chosenOption/isCorrect method
    # Note: "Beginning Sounds Ma/Cha/Ba", "Beginning Sounds Ka/Na/Ta", "Beginning Sounds Ta/Va/Ga"
    #       and "Beginning Sound Ba/Ra/Na" all have action_name like "beginning_sound_ma_cha_ba_hindi_hybrid_game_completed"
    #       (contains "hybrid_game_completed") and should use jsonData method, so they go to game_completed_data
    games_to_use_mcq_completed_method = ['Shape Rectangle', 'Numerals 1-10', 'Positions']
    games_to_use_json_data_method = ['Beginning Sound Ba/Ra/Na', 'Beginning Sounds Ma/Cha/Ba', 'Beginning Sounds Ka/Na/Ta', 'Beginning Sounds Ta/Va/Ga']
    
    # Helper function for case-insensitive game name matching (handles whitespace and case variations)
    def _is_game_in_list(game_name, game_list):
        """Check if game_name matches any game in game_list (case-insensitive, handles whitespace)"""
        if pd.isna(game_name):
            return False
        game_name_clean = str(game_name).strip().lower()
        return any(game_name_clean == str(g).strip().lower() for g in game_list)
    
    # Create boolean masks for case-insensitive matching
    is_json_data_game = df_score['game_name'].apply(lambda x: _is_game_in_list(x, games_to_use_json_data_method))
    is_mcq_completed_game = df_score['game_name'].apply(lambda x: _is_game_in_list(x, games_to_use_mcq_completed_method))
    
    # Filter for game_completed_data:
    # - Includes action_name containing 'game_completed' (matches "hybrid_game_completed" too)
    # - Excludes action_name containing 'mcq_completed' (unless it's a jsonData game)
    # - Also includes mcq_completed records for jsonData games (to route them correctly)
    # - Excludes games that should use mcq_completed method
    game_completed_data = df_score[
        ((df_score['action_name'].str.contains('game_completed', na=False, case=False) & 
          ~df_score['action_name'].str.contains('mcq_completed', na=False, case=False)) |
         (df_score['action_name'].str.contains('mcq_completed', na=False, case=False) &
          is_json_data_game)) &
        ~is_mcq_completed_game
    ].copy()
    
    mcq_completed_data = df_score[
        (df_score['action_name'].str.contains('mcq_completed', na=False, case=False) &
         ~is_json_data_game) |
        (df_score['action_name'].str.contains('game_completed', na=False, case=False) &
         is_mcq_completed_game)
    ].copy()
    
    print(f"  - game_completed records: {len(game_completed_data)}")
    print(f"  - mcq_completed records: {len(mcq_completed_data)}")
    
    if len(game_completed_data) > 0:
        print(f"  - Unique games in game_completed: {game_completed_data['game_name'].nunique()}")
    if len(mcq_completed_data) > 0:
        print(f"  - Unique games in mcq_completed: {mcq_completed_data['game_name'].nunique()}")
    
    # Process game_completed data (correctSelections and jsonData games)
    if not game_completed_data.empty:
        print("  - Processing game_completed data...")
        print(f"    - Processing {game_completed_data['game_name'].nunique()} unique games")
        
        # Process each game individually to determine the correct score calculation method
        for game_name in game_completed_data['game_name'].unique():
            game_records = len(game_completed_data[game_completed_data['game_name'] == game_name])
            print(f"    - Processing {game_name}: {game_records} records")
            game_data = game_completed_data[game_completed_data['game_name'] == game_name].copy()
            
            # Try different score calculation methods and use the one that produces valid results
            # Method 1: correctSelections (for Relational Comparison, Quantity Comparison, etc.)
            game_data['total_score_correct'] = game_data['custom_dimension_1'].apply(parse_custom_dimension_1_correct_selections)
            correct_count = (game_data['total_score_correct'] > 0).sum()
            
            # Method 2: jsonData (for Revision games, Rhyming Words, Beginning Sound Ba/Ra/Na, etc.)
            game_data['total_score_json'] = game_data['custom_dimension_1'].apply(parse_custom_dimension_1_json_data)
            json_count = (game_data['total_score_json'] > 0).sum()
            
            # Games that should prefer jsonData method (same structure as Beginning Sound Ba/Ra/Na)
            # These games have action_name like "beginning_sound_ma_cha_ba_hindi_hybrid_game_completed"
            # Includes: Beginning Sound Ba/Ra/Na, Beginning Sounds Ma/Cha/Ba, Ka/Na/Ta, Ta/Va/Ga
            games_prefer_json_data = ['Beginning Sound Ba/Ra/Na', 'Beginning Sounds Ma/Cha/Ba', 'Beginning Sounds Ka/Na/Ta', 'Beginning Sounds Ta/Va/Ga']
            
            # Debug: For Beginning Sounds games, check a sample record if no scores found
            if game_name in games_prefer_json_data and json_count == 0 and correct_count == 0:
                # Try to debug by checking a sample record
                sample_records = game_data[game_data['custom_dimension_1'].notna()].head(5)
                if len(sample_records) > 0:
                    print(f"    - DEBUG: Checking sample record structure for {game_name}...")
                    for idx, row in sample_records.iterrows():
                        try:
                            raw_json_str = str(row['custom_dimension_1'])
                            # Try to clean the JSON string
                            # Remove any leading/trailing whitespace
                            raw_json_str = raw_json_str.strip()
                            # Check if it's already a dict (sometimes pandas stores it as dict)
                            if isinstance(row['custom_dimension_1'], dict):
                                sample_json = row['custom_dimension_1']
                            else:
                                # Clean malformed JSON before parsing
                                raw_json_str = clean_malformed_json(raw_json_str)
                                # Remove any leading/trailing quotes if present
                                if raw_json_str.startswith('"') and raw_json_str.endswith('"'):
                                    raw_json_str = raw_json_str[1:-1]
                                # Try parsing as JSON
                                sample_json = json.loads(raw_json_str)
                            
                            print(f"      Sample root keys: {list(sample_json.keys())[:10]}")
                            if 'gameData' in sample_json:
                                gd = sample_json['gameData']
                                print(f"      gameData found: {len(gd) if isinstance(gd, list) else 'not a list'} items")
                                if isinstance(gd, list) and len(gd) > 0:
                                    for i, item in enumerate(gd[:3]):
                                        if isinstance(item, dict):
                                            print(f"        Item {i}: section={item.get('section')}, has jsonData={'jsonData' in item}")
                                            if item.get('section') == 'Action' and 'jsonData' in item:
                                                json_data = item['jsonData']
                                                print(f"          jsonData type: {type(json_data)}, length: {len(json_data) if isinstance(json_data, list) else 'N/A'}")
                                                if isinstance(json_data, list) and len(json_data) > 0:
                                                    first_level = json_data[0]
                                                    print(f"          First level keys: {list(first_level.keys()) if isinstance(first_level, dict) else 'not a dict'}")
                                                    if isinstance(first_level, dict) and 'userResponse' in first_level:
                                                        ur = first_level['userResponse']
                                                        print(f"          userResponse type: {type(ur)}, length: {len(ur) if isinstance(ur, list) else 'N/A'}")
                            if 'section' in sample_json:
                                print(f"      Root section: {sample_json.get('section')}, has jsonData: {'jsonData' in sample_json}")
                                if sample_json.get('section') == 'Action' and 'jsonData' in sample_json:
                                    json_data = sample_json['jsonData']
                                    print(f"        Root jsonData type: {type(json_data)}, length: {len(json_data) if isinstance(json_data, list) else 'N/A'}")
                        except json.JSONDecodeError as e:
                            print(f"      JSON Parse Error at position {e.pos}: {str(e)[:200]}")
                            # Show a snippet of the JSON around the error
                            raw_str = str(row['custom_dimension_1'])
                            if len(raw_str) > 500:
                                # Show area around error if possible
                                error_pos = min(e.pos if hasattr(e, 'pos') else 0, len(raw_str))
                                start = max(0, error_pos - 100)
                                end = min(len(raw_str), error_pos + 100)
                                print(f"      JSON snippet around error (chars {start}-{end}): {raw_str[start:end]}")
                            else:
                                print(f"      Full JSON (first 500 chars): {raw_str[:500]}")
                            # Try to see if it's already a dict
                            if isinstance(row['custom_dimension_1'], dict):
                                print(f"      Note: custom_dimension_1 is already a dict, not a JSON string")
                                sample_json = row['custom_dimension_1']
                                print(f"      Dict keys: {list(sample_json.keys())[:10]}")
                        except Exception as e:
                            print(f"      Error parsing sample: {type(e).__name__}: {str(e)[:200]}")
                            # Check if it's already a dict
                            if isinstance(row['custom_dimension_1'], dict):
                                print(f"      Note: custom_dimension_1 is already a dict")
                                try:
                                    sample_json = row['custom_dimension_1']
                                    print(f"      Dict keys: {list(sample_json.keys())[:10]}")
                                except:
                                    pass
                        break  # Only check first sample
            
            # Choose the method that produces more valid scores
            # For specific games, prefer jsonData if both methods work
            # Use case-insensitive matching to handle any name variations
            game_name_normalized = str(game_name).strip().lower()
            games_prefer_json_data_normalized = [g.strip().lower() for g in games_prefer_json_data]
            if game_name_normalized in games_prefer_json_data_normalized and json_count > 0:
                print(f"    - {game_name}: Using jsonData method (preferred for this game, {json_count} valid scores)")
                game_data['total_score'] = game_data['total_score_json']
            elif correct_count >= json_count and correct_count > 0:
                print(f"    - {game_name}: Using correctSelections method ({correct_count} valid scores)")
                game_data['total_score'] = game_data['total_score_correct']
            elif json_count > 0:
                print(f"    - {game_name}: Using jsonData method ({json_count} valid scores)")
                game_data['total_score'] = game_data['total_score_json']
            else:
                print(f"    - {game_name}: No valid scores found, skipping")
                continue
            
            # Filter out zero scores and add to combined data
            game_data = game_data[game_data['total_score'] > 0]
            if not game_data.empty:
                # Select only needed columns for combined_df
                cols_to_keep = ['game_name', 'idvisitor_converted', 'idvisit', 'total_score']
                if has_language:
                    cols_to_keep.append('language')
                if has_game_code:
                    cols_to_keep.append('game_code')
                game_data = game_data[cols_to_keep].copy()
                
                valid_scores = len(game_data)
                score_range = f"{game_data['total_score'].min()}-{game_data['total_score'].max()}"
                print(f"      - Added {valid_scores} valid scores (range: {score_range})")
                combined_df = pd.concat([combined_df, game_data], ignore_index=True)
            else:
                print(f"      - No valid scores after filtering")
    
    # Process mcq_completed data (hybrid MCQ games with Action section)
    if not mcq_completed_data.empty:
        print("  - Processing mcq_completed data...")
        print(f"    - Processing {mcq_completed_data['game_name'].nunique()} unique games")
        print(f"    - Games in mcq_completed: {sorted(mcq_completed_data['game_name'].unique())}")
        
        # Games that use correctOption instead of isCorrect
        # Note: Positions now uses isCorrect method, so this list is empty
        games_with_correct_option = []
        
        # Process each game individually to determine the correct parsing method
        mcq_completed_data = mcq_completed_data.copy()
        mcq_completed_data['total_score'] = 0
        
        for game_name in sorted(mcq_completed_data['game_name'].unique()):
            game_records = len(mcq_completed_data[mcq_completed_data['game_name'] == game_name])
            print(f"    - Processing {game_name}: {game_records} records")
            
            game_mask = mcq_completed_data['game_name'] == game_name
            
            # Choose the appropriate parsing method
            if game_name in games_with_correct_option:
                print(f"      - {game_name}: Using correctOption method (chosenOption vs correctOption)")
                mcq_completed_data.loc[game_mask, 'total_score'] = mcq_completed_data.loc[game_mask, 'custom_dimension_1'].apply(
                    parse_custom_dimension_1_mcq_completed_with_correct_option
                )
            else:
                print(f"      - {game_name}: Using isCorrect method (options[chosenOption].isCorrect)")
                mcq_completed_data.loc[game_mask, 'total_score'] = mcq_completed_data.loc[game_mask, 'custom_dimension_1'].apply(
                    parse_custom_dimension_1_mcq_completed
                )
        
        # Log score parsing results by game
        for game_name in sorted(mcq_completed_data['game_name'].unique()):
            game_data = mcq_completed_data[mcq_completed_data['game_name'] == game_name]
            valid_scores = (game_data['total_score'] > 0).sum()
            zero_scores = (game_data['total_score'] == 0).sum()
            if valid_scores > 0:
                score_range = f"{game_data[game_data['total_score'] > 0]['total_score'].min()}-{game_data[game_data['total_score'] > 0]['total_score'].max()}"
                print(f"      - {game_name}: {valid_scores} valid (>0), {zero_scores} zero, range: {score_range}")
            else:
                print(f"      - {game_name}: {valid_scores} valid (>0), {zero_scores} zero - WARNING: No valid scores!")
        
        # Overall stats
        valid_scores = (mcq_completed_data['total_score'] > 0).sum()
        zero_scores = (mcq_completed_data['total_score'] == 0).sum()
        print(f"    - Overall parsed scores: {valid_scores} valid (>0), {zero_scores} zero scores")
        
        # Filter out zero scores
        mcq_completed_data = mcq_completed_data[mcq_completed_data['total_score'] > 0]
        
        if not mcq_completed_data.empty:
            # Select only needed columns for combined_df
            cols_to_keep = ['game_name', 'idvisitor_converted', 'idvisit', 'total_score']
            if has_language:
                cols_to_keep.append('language')
            if has_game_code:
                cols_to_keep.append('game_code')
            mcq_completed_data = mcq_completed_data[cols_to_keep].copy()
            
            valid_scores_count = len(mcq_completed_data)
            score_range = f"{mcq_completed_data['total_score'].min()}-{mcq_completed_data['total_score'].max()}"
            print(f"    - Added {valid_scores_count} valid scores (range: {score_range})")
            print(f"    - Games with valid scores: {sorted(mcq_completed_data['game_name'].unique())}")
            combined_df = pd.concat([combined_df, mcq_completed_data], ignore_index=True)
        else:
            print(f"    - No valid scores after filtering")
    
    # Process action_level data - REMOVED: No longer used
    # Action level processing has been removed as scores_data.csv now only contains game_completed and mcq_completed
    
    if combined_df.empty:
        print("WARNING: No score distribution data found")
        return pd.DataFrame()
    
    print(f"\n  - Combined data summary:")
    print(f"    - Total records: {len(combined_df)}")
    print(f"    - Unique games: {combined_df['game_name'].nunique()}")
    print(f"    - Unique users: {combined_df['idvisitor_converted'].nunique()}")
    print(f"    - Score range: {combined_df['total_score'].min()}-{combined_df['total_score'].max()}")
    
    # Extract domain from game_code if it exists
    if has_game_code and 'game_code' in combined_df.columns:
        print("  - Extracting domain from game_code...")
        combined_df['game_code'] = combined_df['game_code'].apply(extract_domain_from_game_code)
        print("  - Domain extraction complete")
    
    # Group by game and total score (and optionally language and game_code), then count distinct users
    # Each user-game-score combination is counted once
    print("\n  - Creating final score distribution...")
    groupby_cols = ['game_name', 'total_score']
    if has_language and 'language' in combined_df.columns:
        groupby_cols.append('language')
    if has_game_code and 'game_code' in combined_df.columns:
        groupby_cols.append('game_code')
    
    score_distribution = combined_df.groupby(groupby_cols)['idvisitor_converted'].nunique().reset_index()
    score_distribution.columns = groupby_cols + ['user_count']
    
    print(f"\nSUCCESS: Processed score distribution: {len(score_distribution)} records")
    print(f"  - Unique games in distribution: {score_distribution['game_name'].nunique()}")
    print(f"  - Score range in distribution: {score_distribution['total_score'].min()}-{score_distribution['total_score'].max()}")
    
    # Log summary by game
    print("\n  - Final distribution summary by game:")
    for game in sorted(score_distribution['game_name'].unique()):
        game_data = score_distribution[score_distribution['game_name'] == game]
        total_users = game_data['user_count'].sum()
        score_range = f"{game_data['total_score'].min()}-{game_data['total_score'].max()}"
        unique_scores = sorted(game_data['total_score'].unique().tolist())
        print(f"    - {game}: {total_users} total users, scores {score_range}, unique values: {unique_scores}")
    
    return score_distribution


def _distinct_count_ignore_blank(series: pd.Series) -> int:
    """Power BI DISTINCTCOUNTNOBLANK logic: ignore NULLs and empty strings"""
    if series.dtype == object:
        # For string columns: drop NULLs and empty strings
        cleaned = series.dropna()
        cleaned = cleaned[cleaned.astype(str).str.strip() != ""]
    else:
        # For numeric columns: just drop NULLs
        cleaned = series.dropna()
    return int(cleaned.nunique())


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build summary table with correct Power BI DISTINCTCOUNTNOBLANK logic
    For Users: Counts unique users who triggered each event at least once
    For Visits: Counts unique visits that triggered each event at least once
    For Instances: Counts total instances (idlink_va) for each event
    """
    print("Building summary statistics...")
    
    # Check if event column exists
    if 'event' not in df.columns:
        print("ERROR: 'event' column not found in dataframe")
        print(f"Available columns: {list(df.columns)}")
        return pd.DataFrame()
    
    # Filter out NULL/None events before grouping
    df_filtered = df[df['event'].notna()].copy()
    if df_filtered.empty:
        print("WARNING: No records with valid event values after filtering NULLs")
        return pd.DataFrame()
    
    print(f"Filtered to {len(df_filtered)} records with valid events (removed {len(df) - len(df_filtered)} NULL events)")
    
    # Group by event and compute distinct counts
    # This ensures each user is counted only once per event (if they triggered it at least once)
    print("Calculating distinct counts per event...")
    grouped = df_filtered.groupby('event').agg({
        'idvisitor_converted': _distinct_count_ignore_blank,  # Unique users per event
        'idvisit': _distinct_count_ignore_blank,              # Unique visits per event
        'idlink_va': _distinct_count_ignore_blank,            # Unique instances per event (total count)
    })
    
    # Log the counts for verification
    print("Event-wise distinct counts:")
    for event in grouped.index:
        print(f"  - {event}: {grouped.loc[event, 'idvisitor_converted']} unique users, "
              f"{grouped.loc[event, 'idvisit']} unique visits, "
              f"{grouped.loc[event, 'idlink_va']} instances")
    
    # Rename columns to match Power BI
    grouped.columns = ['Users', 'Visits', 'Instances']
    grouped = grouped.reset_index()
    grouped.rename(columns={'event': 'Event'}, inplace=True)
    
    # Ensure all funnel stages exist (fill missing with 0)
    # Order: started, introduction, questions, mid_introduction, validation, parent_poll, rewards, completed
    all_events = pd.DataFrame({'Event': ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']})
    grouped = all_events.merge(grouped, on='Event', how='left').fillna(0)
    
    # Convert to int and sort
    for col in ['Users', 'Visits', 'Instances']:
        grouped[col] = grouped[col].astype(int)
    
    grouped['Event'] = pd.Categorical(grouped['Event'], 
                                     categories=['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed'], 
                                     ordered=True)
    grouped = grouped.sort_values('Event')
    
    print(f"SUCCESS: Summary statistics: {len(grouped)} event types")
    print("Final summary data:")
    print(grouped.to_string(index=False))
    return grouped


def fetch_user_phone_mapping() -> pd.DataFrame:
    """Fetch mapping between idvisitor_converted and phone numbers from Redshift
    
    Returns:
        pd.DataFrame: DataFrame with columns 'idvisitor_converted' and 'phone'
    """
    if not PSYCOPG2_AVAILABLE:
        print("WARNING: psycopg2 not available. Cannot fetch user phone mapping from Redshift.")
        return pd.DataFrame(columns=['idvisitor_converted', 'phone'])
    
    print("=" * 60)
    print("FETCHING: User Phone Mapping from REDSHIFT")
    print("=" * 60)
    
    # Retry configuration
    import time
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\n  [ACTION] Connecting to REDSHIFT (Attempt {attempt}/{max_retries})...")
            conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                connect_timeout=60,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            
            # Set statement timeout to 30 minutes for large queries
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '1800000'")  # 30 minutes in milliseconds
                conn.commit()
            
            print(f"  ✓ Successfully connected to REDSHIFT")
            
            # Fetch user phone mapping
            print(f"  [ACTION] Executing user phone mapping query...")
            df_mapping = pd.read_sql(USER_PHONE_QUERY, conn)
            
            # Convert hex to int
            if 'idvisitor_hex' in df_mapping.columns:
                df_mapping = convert_hex_to_int(df_mapping, 'idvisitor_hex', 'idvisitor_converted')
            
            # Clean phone numbers (remove whitespace, convert to string)
            if 'phone' in df_mapping.columns:
                df_mapping['phone'] = df_mapping['phone'].astype(str).str.strip()
                df_mapping = df_mapping[df_mapping['phone'] != '']
                df_mapping = df_mapping[df_mapping['phone'] != 'None']
                df_mapping = df_mapping[df_mapping['phone'].notna()]
            
            print(f"  ✓ Query returned {len(df_mapping)} user-phone mappings")
            
            conn.close()
            print(f"  ✓ Connection closed")
            
            return df_mapping
            
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            if attempt < max_retries:
                print(f"\n  [WARNING] Connection error on attempt {attempt}:")
                print(f"    {error_msg}")
                print(f"  [ACTION] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"\n  ERROR: Failed to connect to REDSHIFT after {max_retries} attempts:")
                print(f"    Error Type: OperationalError (connection issue)")
                print(f"    Error Message: {error_msg}")
                return pd.DataFrame(columns=['idvisitor_converted', 'phone'])
        except Exception as e:
            print(f"\n  ERROR: Failed to fetch user phone mapping from REDSHIFT:")
            print(f"    Error Type: {type(e).__name__}")
            print(f"    Error Message: {str(e)}")
            if attempt < max_retries:
                print(f"  [ACTION] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                return pd.DataFrame(columns=['idvisitor_converted', 'phone'])
    
    return pd.DataFrame(columns=['idvisitor_converted', 'phone'])


def build_summary_with_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Build summary table with all combinations: overall, by domain, by language, and by both
    This allows the dashboard to filter by domain/language and get accurate distinct counts
    Also calculates percentage of mapped users for each event
    """
    print("Building summary statistics with domain and language grouping...")
    
    # Check if event column exists
    if 'event' not in df.columns:
        print("ERROR: 'event' column not found in dataframe")
        return pd.DataFrame()
    
    # Filter out NULL/None events before grouping
    df_filtered = df[df['event'].notna()].copy()
    if df_filtered.empty:
        print("WARNING: No records with valid event values after filtering NULLs")
        return pd.DataFrame()
    
    print(f"Filtered to {len(df_filtered)} records with valid events")
    
    # Skip fetching mapped users data from Redshift - using hardcoded values in dashboard instead
    # This saves time and avoids unnecessary Redshift queries
    print("Skipping mapped users data fetch (using hardcoded values in dashboard)...")
    mapped_phones = set()
    user_phone_mapping = pd.DataFrame()
    
    # Create empty mappings since we're not using them
    phone_to_mapped = {}
    idvisitor_to_phone = {}
    
    all_summaries = []
    
    # Helper function to calculate mapped users percentage for a group
    def calculate_mapped_users_percentage(group_df):
        """Calculate percentage of mapped users for a group"""
        if idvisitor_to_phone:
            users_with_phone = set()
            mapped_users = set()
            
            for idvisitor in group_df['idvisitor_converted'].dropna().unique():
                if idvisitor in idvisitor_to_phone:
                    phones = idvisitor_to_phone[idvisitor]
                    users_with_phone.add(idvisitor)
                    # Check if any phone is mapped
                    for phone in phones:
                        if phone in phone_to_mapped:
                            mapped_users.add(idvisitor)
                            break
            
            if len(users_with_phone) > 0:
                return (len(mapped_users) / len(users_with_phone)) * 100
        return 0.0
    
    # 1. Overall summary (domain='All', language='All')
    print("Calculating overall summary (domain='All', language='All')...")
    overall = df_filtered.groupby('event').agg({
        'idvisitor_converted': _distinct_count_ignore_blank,
        'idvisit': _distinct_count_ignore_blank,
        'idlink_va': _distinct_count_ignore_blank,
    })
    overall.columns = ['Users', 'Visits', 'Instances']
    overall = overall.reset_index()
    overall.rename(columns={'event': 'Event'}, inplace=True)
    overall['domain'] = 'All'
    overall['language'] = 'All'
    
    # Skip mapped users percentage calculation (using hardcoded values in dashboard)
    overall['Mapped_Users_Percentage'] = 0.0
    all_summaries.append(overall)
    
    # 2. By domain only (language='All')
    if 'domain' in df_filtered.columns:
        print("Calculating summary by domain (language='All')...")
        by_domain = df_filtered.groupby(['event', 'domain']).agg({
            'idvisitor_converted': _distinct_count_ignore_blank,
            'idvisit': _distinct_count_ignore_blank,
            'idlink_va': _distinct_count_ignore_blank,
        })
        by_domain.columns = ['Users', 'Visits', 'Instances']
        by_domain = by_domain.reset_index()
        by_domain.rename(columns={'event': 'Event'}, inplace=True)
        by_domain['language'] = 'All'
        # Remove rows where domain is null
        by_domain = by_domain[by_domain['domain'].notna()]
        
        # Skip mapped users percentage calculation (using hardcoded values in dashboard)
        by_domain['Mapped_Users_Percentage'] = 0.0
        all_summaries.append(by_domain)
    
    # 3. By language only (domain='All')
    if 'language' in df_filtered.columns:
        print("Calculating summary by language (domain='All')...")
        by_language = df_filtered.groupby(['event', 'language']).agg({
            'idvisitor_converted': _distinct_count_ignore_blank,
            'idvisit': _distinct_count_ignore_blank,
            'idlink_va': _distinct_count_ignore_blank,
        })
        by_language.columns = ['Users', 'Visits', 'Instances']
        by_language = by_language.reset_index()
        by_language.rename(columns={'event': 'Event'}, inplace=True)
        by_language['domain'] = 'All'
        # Remove rows where language is null
        by_language = by_language[by_language['language'].notna()]
        
        # Skip mapped users percentage calculation (using hardcoded values in dashboard)
        by_language['Mapped_Users_Percentage'] = 0.0
        all_summaries.append(by_language)
    
    # 4. By both domain and language
    if 'domain' in df_filtered.columns and 'language' in df_filtered.columns:
        print("Calculating summary by domain and language...")
        by_both = df_filtered.groupby(['event', 'domain', 'language']).agg({
            'idvisitor_converted': _distinct_count_ignore_blank,
            'idvisit': _distinct_count_ignore_blank,
            'idlink_va': _distinct_count_ignore_blank,
        })
        by_both.columns = ['Users', 'Visits', 'Instances']
        by_both = by_both.reset_index()
        by_both.rename(columns={'event': 'Event'}, inplace=True)
        # Remove rows where domain or language is null
        by_both = by_both[by_both['domain'].notna() & by_both['language'].notna()]
        
        # Calculate mapped users percentage
        # Skip mapped users percentage calculation (using hardcoded values in dashboard)
        by_both['Mapped_Users_Percentage'] = 0.0
        all_summaries.append(by_both)
    
    # Combine all summaries
    if all_summaries:
        combined = pd.concat(all_summaries, ignore_index=True)
        
        # Convert to int
        for col in ['Users', 'Visits', 'Instances']:
            combined[col] = combined[col].astype(int)
        
        # Round mapped users percentage to 2 decimal places
        if 'Mapped_Users_Percentage' in combined.columns:
            combined['Mapped_Users_Percentage'] = combined['Mapped_Users_Percentage'].round(2)
        
        # Sort
        sort_cols = ['Event']
        if 'domain' in combined.columns:
            sort_cols.append('domain')
        if 'language' in combined.columns:
            sort_cols.append('language')
        combined = combined.sort_values(sort_cols)
        
        print(f"SUCCESS: Summary statistics with filters: {len(combined)} combinations")
        print(f"  - Unique events: {combined['Event'].nunique()}")
        if 'domain' in combined.columns:
            print(f"  - Unique domains: {combined['domain'].nunique()}")
        if 'language' in combined.columns:
            print(f"  - Unique languages: {combined['language'].nunique()}")
        
        return combined
    else:
        return pd.DataFrame()


def preprocess_time_series_data_instances(df_instances: pd.DataFrame) -> pd.DataFrame:
    """DEPRECATED: Preprocess time series data for instances only - using created_at and distinct id counts
    This function is no longer used. Instances are now calculated from idlink_va in the combined query."""
    print("Preprocessing time series data for instances...")
    
    if df_instances.empty:
        print("WARNING: No instances data to process")
        return pd.DataFrame(columns=['period_label', 'game_name', 'instances', 'period_type'])
    
    # Convert created_at to datetime
    df_instances['created_at'] = pd.to_datetime(df_instances['created_at'])
    
    # Filter data to only include records from July 2nd, 2025 onwards
    july_2_2025 = pd.Timestamp('2025-07-02')
    df_instances = df_instances[df_instances['created_at'] >= july_2_2025].copy()
    print(f"Filtered instances data to July 2nd, 2025 onwards: {len(df_instances)} records")
    
    if df_instances.empty:
        print("WARNING: No instances data after filtering")
        return pd.DataFrame(columns=['period_label', 'game_name', 'instances', 'period_type'])
    
    # Get unique games
    unique_games = df_instances['game_name'].unique()
    print(f"Processing time series for {len(unique_games)} games")
    
    # Prepare time series data for different periods
    time_series_data = []
    
    # Process each game individually + "All Games" combined
    games_to_process = list(unique_games) + ['All Games']
    
    for game_name in games_to_process:
        if game_name == 'All Games':
            game_df = df_instances.copy()
        else:
            game_df = df_instances[df_instances['game_name'] == game_name].copy()
        
        if game_df.empty:
            continue
            
        print(f"  Processing time series for: {game_name}")
    
        # Daily aggregation - format: YYYY-MM-DD
        game_df['date'] = game_df['created_at'].dt.date
        if game_name == 'All Games':
            # For "All Games", aggregate across all games (don't group by game_name)
            daily_agg = game_df.groupby('date')['id'].nunique().reset_index()
            daily_agg.columns = ['period_label', 'instances']
            daily_agg['period_label'] = daily_agg['period_label'].astype(str)
            daily_agg['game_name'] = 'All Games'
        else:
            # For individual games, group by date and game_name
            daily_agg = game_df.groupby(['date', 'game_name'])['id'].nunique().reset_index()
            daily_agg.columns = ['period_label', 'game_name', 'instances']
            daily_agg['period_label'] = daily_agg['period_label'].astype(str)
        daily_agg['period_type'] = 'Day'
        time_series_data.extend(daily_agg.to_dict('records'))
        
        # Monthly aggregation - format: YYYY_MM (underscore, not hyphen)
        game_df['year'] = game_df['created_at'].dt.year
        game_df['month'] = game_df['created_at'].dt.month
        game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['month'].astype(str).str.zfill(2)
        if game_name == 'All Games':
            # For "All Games", aggregate across all games
            monthly_agg = game_df.groupby('period_label')['id'].nunique().reset_index()
            monthly_agg.columns = ['period_label', 'instances']
            monthly_agg['game_name'] = 'All Games'
        else:
            # For individual games, group by period_label and game_name
            monthly_agg = game_df.groupby(['period_label', 'game_name'])['id'].nunique().reset_index()
            monthly_agg.columns = ['period_label', 'game_name', 'instances']
        monthly_agg['period_type'] = 'Month'
        time_series_data.extend(monthly_agg.to_dict('records'))
            
        # Weekly aggregation - starts from Wednesday, format: YYYY_WW
        # Reset game_df to original for weekly processing
        if game_name == 'All Games':
            game_df = df_instances.copy()
        else:
            game_df = df_instances[df_instances['game_name'] == game_name].copy()
        # Shift date by -2 days before calculating week number (so Wednesday becomes Monday)
        game_df['shifted_date'] = game_df['created_at'] - pd.Timedelta(days=2)
        game_df['year'] = game_df['shifted_date'].dt.year
        # Use strftime('%W') which calculates week number with Monday as first day of week
        # This matches MySQL's WEEK() function behavior
        game_df['week'] = game_df['shifted_date'].dt.strftime('%W').astype(int)
        game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['week'].astype(str).str.zfill(2)
        
        if game_name == 'All Games':
            # For "All Games", aggregate across all games
            weekly_agg = game_df.groupby('period_label')['id'].nunique().reset_index()
            weekly_agg.columns = ['period_label', 'instances']
            weekly_agg['game_name'] = 'All Games'
        else:
            # For individual games, group by period_label and game_name
            weekly_agg = game_df.groupby(['period_label', 'game_name'])['id'].nunique().reset_index()
            weekly_agg.columns = ['period_label', 'game_name', 'instances']
        weekly_agg['period_type'] = 'Week'
        time_series_data.extend(weekly_agg.to_dict('records'))
            
    time_series_df = pd.DataFrame(time_series_data)
    print(f"SUCCESS: Time series instances data: {len(time_series_df)} records")
    print(f"  Daily records: {len(time_series_df[time_series_df['period_type'] == 'Day'])}")
    print(f"  Weekly records: {len(time_series_df[time_series_df['period_type'] == 'Week'])}")
    print(f"  Monthly records: {len(time_series_df[time_series_df['period_type'] == 'Month'])}")
    
    return time_series_df


def preprocess_time_series_data_visits_users(df_visits_users: pd.DataFrame) -> pd.DataFrame:
    """Preprocess time series data for instances, visits and users - using server_time
    Calculates Started and Completed separately for each metric
    Aggregates by game_code and language"""
    print("Preprocessing time series data for instances, visits and users (with Started/Completed)...")
    
    if df_visits_users.empty:
        print("WARNING: No time series data to process")
        return pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type', 'game_code', 'language'])
    
    # Convert server_time to datetime
    df_visits_users['server_time'] = pd.to_datetime(df_visits_users['server_time'])
    
    # Filter out NULL events
    df_visits_users = df_visits_users[df_visits_users['event'].notna()].copy()
    
    # Filter data to only include records from July 2nd, 2025 onwards
    july_2_2025 = pd.Timestamp('2025-07-02')
    df_visits_users = df_visits_users[df_visits_users['server_time'] >= july_2_2025].copy()
    print(f"Filtered time series data to July 2nd, 2025 onwards: {len(df_visits_users)} records")
    
    if df_visits_users.empty:
        print("WARNING: No time series data after filtering")
        return pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type', 'game_code', 'language'])
    
    # Check what columns we have
    print(f"  [INFO] Available columns after query: {list(df_visits_users.columns)}")
    
    # Transform language: if contains "mr-IN" then "mr", else "hi"
    if 'language' in df_visits_users.columns:
        print("  [ACTION] Transforming language column...")
        print(f"  [DEBUG] Language column sample values: {df_visits_users['language'].head(10).tolist()}")
        df_visits_users['language'] = df_visits_users['language'].apply(
            lambda x: 'mr' if pd.notna(x) and 'mr-IN' in str(x) else 'hi'
        )
        print(f"  [DEBUG] Language after transformation sample: {df_visits_users['language'].head(10).tolist()}")
        print("  [OK] Language transformation complete")
    else:
        print("  [WARNING] Language column not found in time series data")
        df_visits_users['language'] = 'hi'  # Default to 'hi' if not found
    
    # Extract game_code (domain) from full game_code
    if 'game_code' in df_visits_users.columns:
        print("  [ACTION] Extracting domain from game_code...")
        print(f"  [DEBUG] Game code column sample values: {df_visits_users['game_code'].head(10).tolist()}")
        df_visits_users['game_code'] = df_visits_users['game_code'].apply(extract_domain_from_game_code)
        print(f"  [DEBUG] Game code after extraction sample: {df_visits_users['game_code'].head(10).tolist()}")
        print("  [OK] Game code extraction complete")
    else:
        print("  [WARNING] Game code column not found in time series data")
        df_visits_users['game_code'] = None
    
    # Ensure game_code and language are strings (not None) for proper grouping
    # None values in groupby can cause issues, so we'll keep them as None but ensure they're handled
    if 'game_code' in df_visits_users.columns:
        # Keep None values but ensure they're properly typed
        df_visits_users['game_code'] = df_visits_users['game_code'].where(pd.notna(df_visits_users['game_code']), None)
    if 'language' in df_visits_users.columns:
        # Keep None values but ensure they're properly typed
        df_visits_users['language'] = df_visits_users['language'].where(pd.notna(df_visits_users['language']), None)
    
    # Get unique games
    unique_games = df_visits_users['game_name'].unique()
    print(f"Processing time series for {len(unique_games)} games")
    
    # Prepare time series data for different periods
    time_series_data = []
    
    # Process each game individually + "All Games" combined
    games_to_process = list(unique_games) + ['All Games']
    
    for game_name in games_to_process:
        if game_name == 'All Games':
            game_df = df_visits_users.copy()
        else:
            game_df = df_visits_users[df_visits_users['game_name'] == game_name].copy()
        
        if game_df.empty:
            continue
            
        print(f"  Processing time series for: {game_name}")
    
        # Daily aggregation - format: YYYY-MM-DD
        game_df['date'] = game_df['server_time'].dt.date
        for period_type, group_col in [('Day', 'date'), ('Month', None), ('Week', None)]:
            # Build group_by_cols including game_code and language
            base_group_cols = ['event', 'game_code', 'language']
            if period_type == 'Day':
                if game_name == 'All Games':
                    group_by_cols = ['date'] + base_group_cols
                else:
                    group_by_cols = ['date', 'game_name'] + base_group_cols
            elif period_type == 'Month':
                game_df['year'] = game_df['server_time'].dt.year
                game_df['month'] = game_df['server_time'].dt.month
                game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['month'].astype(str).str.zfill(2)
                if game_name == 'All Games':
                    group_by_cols = ['period_label'] + base_group_cols
                else:
                    group_by_cols = ['period_label', 'game_name'] + base_group_cols
            else:  # Week
                game_df['shifted_date'] = game_df['server_time'] - pd.Timedelta(days=2)
                game_df['year'] = game_df['shifted_date'].dt.year
                game_df['week'] = game_df['shifted_date'].dt.strftime('%W').astype(int)
                game_df['period_label'] = game_df['year'].astype(str) + '_' + game_df['week'].astype(str).str.zfill(2)
                if game_name == 'All Games':
                    group_by_cols = ['period_label'] + base_group_cols
                else:
                    group_by_cols = ['period_label', 'game_name'] + base_group_cols
            
            # Aggregate by event type, game_code, and language for each metric
            agg_df = game_df.groupby(group_by_cols).agg({
                'idlink_va': 'nunique',      # Instances
                'idvisit': 'nunique',        # Visits
                'idvisitor_converted': 'nunique'  # Users
            }).reset_index()
            
            # Rename the aggregated columns to match our metric names
            agg_df = agg_df.rename(columns={
                'idlink_va': 'instances',
                'idvisit': 'visits',
                'idvisitor_converted': 'users'
            })
            
            # Set period_label for Day period
            if period_type == 'Day':
                agg_df['period_label'] = agg_df['date'].astype(str)
                agg_df = agg_df.drop(columns=['date'])
            
            # Add game_name if it's not in the groupby (for All Games)
            if 'game_name' not in agg_df.columns:
                agg_df['game_name'] = game_name
            
            # Ensure game_code and language columns exist
            if 'game_code' not in agg_df.columns:
                agg_df['game_code'] = None
            else:
                # Keep None values as None (don't fill with 'Unknown')
                agg_df['game_code'] = agg_df['game_code'].where(pd.notna(agg_df['game_code']), None)
            
            if 'language' not in agg_df.columns:
                agg_df['language'] = None
            else:
                # Keep None values as None (don't fill with 'Unknown')
                agg_df['language'] = agg_df['language'].where(pd.notna(agg_df['language']), None)
            
            # Debug: Check what columns we have before reshaping
            print(f"    [DEBUG] Aggregated columns after rename: {list(agg_df.columns)}")
            print(f"    [DEBUG] Sample game_code values: {agg_df['game_code'].head(5).tolist() if 'game_code' in agg_df.columns else 'N/A'}")
            print(f"    [DEBUG] Sample language values: {agg_df['language'].head(5).tolist() if 'language' in agg_df.columns else 'N/A'}")
            
            # Reshape to long format: one row per metric-event combination
            for metric in ['instances', 'visits', 'users']:
                # Check if the metric column exists in agg_df
                if metric not in agg_df.columns:
                    print(f"    [WARNING] Metric '{metric}' not found in aggregated data, skipping...")
                    print(f"    [DEBUG] Available columns in agg_df: {list(agg_df.columns)}")
                    continue
                
                # Select the columns we need (including the metric column)
                base_cols = ['period_label', 'game_name', 'event', 'game_code', 'language']
                # Only include columns that exist in agg_df
                cols_to_select = [col for col in base_cols if col in agg_df.columns] + [metric]
                print(f"    [DEBUG] Selecting columns for metric '{metric}': {cols_to_select}")
                metric_df = agg_df[cols_to_select].copy()
                print(f"    [DEBUG] Columns after selection: {list(metric_df.columns)}")
                
                # Rename the metric column to 'count'
                if metric in metric_df.columns:
                    metric_df = metric_df.rename(columns={metric: 'count'})
                    print(f"    [DEBUG] Columns after rename: {list(metric_df.columns)}")
                else:
                    print(f"    [ERROR] Metric '{metric}' not in metric_df after selection!")
                    continue
                
                # Add any missing columns (should already be there from groupby, but just in case)
                if 'game_code' not in metric_df.columns:
                    metric_df['game_code'] = None
                if 'language' not in metric_df.columns:
                    metric_df['language'] = None
                if 'game_name' not in metric_df.columns:
                    metric_df['game_name'] = game_name
                if 'period_label' not in metric_df.columns:
                    metric_df['period_label'] = None
                if 'event' not in metric_df.columns:
                    metric_df['event'] = None
                
                # Ensure all required columns exist and in correct order
                # After rename, 'count' should exist, but verify
                if 'count' not in metric_df.columns:
                    print(f"    [ERROR] 'count' column not found after renaming '{metric}'")
                    continue
                
                # Reorder columns to ensure consistent structure
                final_cols = ['period_label', 'game_name', 'event', 'game_code', 'language', 'count']
                # Make sure all columns exist (they should after the checks above)
                missing_cols = [col for col in final_cols if col not in metric_df.columns]
                if missing_cols:
                    print(f"    [WARNING] Missing columns: {missing_cols}, adding with None values")
                    for col in missing_cols:
                        metric_df[col] = None
                
                metric_df = metric_df[final_cols]
                
                metric_df['metric'] = metric
                metric_df['period_type'] = period_type
                time_series_data.extend(metric_df.to_dict('records'))
    
    time_series_df = pd.DataFrame(time_series_data)
    print(f"SUCCESS: Time series data (with Started/Completed): {len(time_series_df)} records")
    print(f"  Daily records: {len(time_series_df[time_series_df['period_type'] == 'Day'])}")
    print(f"  Weekly records: {len(time_series_df[time_series_df['period_type'] == 'Week'])}")
    print(f"  Monthly records: {len(time_series_df[time_series_df['period_type'] == 'Month'])}")
    
    # Now create "All" aggregations for domain (game_code) and language, similar to conversion funnel
    print("\n  [ACTION] Creating 'All' aggregations for domain and language...")
    all_combinations = []
    
    # Filter out None values for aggregation
    time_series_df_clean = time_series_df[
        (time_series_df['game_code'].notna()) & 
        (time_series_df['language'].notna())
    ].copy()
    
    if not time_series_df_clean.empty:
        # 1. Overall summary (game_code='All', language='All')
        print("    [1/4] Calculating overall totals (game_code='All', language='All')...")
        overall = time_series_df_clean.groupby(['period_label', 'game_name', 'metric', 'event', 'period_type']).agg({
            'count': 'sum'
        }).reset_index()
        overall['game_code'] = 'All'
        overall['language'] = 'All'
        all_combinations.append(overall)
        print(f"      Generated {len(overall):,} overall records")
        
        # 2. By game_code only (language='All')
        if 'game_code' in time_series_df_clean.columns:
            print("    [2/4] Calculating by game_code (language='All')...")
            by_game_code = time_series_df_clean.groupby(['period_label', 'game_name', 'metric', 'event', 'period_type', 'game_code']).agg({
                'count': 'sum'
            }).reset_index()
            by_game_code['language'] = 'All'
            all_combinations.append(by_game_code)
            print(f"      Generated {len(by_game_code):,} game_code records")
        
        # 3. By language only (game_code='All')
        if 'language' in time_series_df_clean.columns:
            print("    [3/4] Calculating by language (game_code='All')...")
            by_language = time_series_df_clean.groupby(['period_label', 'game_name', 'metric', 'event', 'period_type', 'language']).agg({
                'count': 'sum'
            }).reset_index()
            by_language['game_code'] = 'All'
            all_combinations.append(by_language)
            print(f"      Generated {len(by_language):,} language records")
        
        # 4. By both game_code and language (already exists in time_series_df_clean)
        print("    [4/4] Using existing game_code+language combinations...")
        by_both = time_series_df_clean.copy()
        all_combinations.append(by_both)
        print(f"      Using {len(by_both):,} existing game_code+language records")
        
        # Combine all combinations
        if all_combinations:
            # Ensure all dataframes have the same columns in the same order
            base_cols = ['period_label', 'game_name', 'event', 'game_code', 'language', 'count', 'metric', 'period_type']
            reordered_combinations = []
            for df in all_combinations:
                # Only include columns that exist
                available_cols = [col for col in base_cols if col in df.columns]
                reordered_df = df[available_cols].copy()
                # Add missing columns with default values
                for col in base_cols:
                    if col not in reordered_df.columns:
                        if col == 'game_code':
                            reordered_df['game_code'] = 'All'
                        elif col == 'language':
                            reordered_df['language'] = 'All'
                # Reorder to match base_cols
                reordered_df = reordered_df[base_cols]
                reordered_combinations.append(reordered_df)
            
            time_series_df = pd.concat(reordered_combinations, ignore_index=True)
            print(f"  [OK] Combined all combinations: {len(time_series_df):,} total records")
        else:
            print("  [WARNING] No combinations generated, using original data")
    else:
        print("  [WARNING] No clean data for 'All' aggregations, using original data")
    
    # Debug: Check game_code and language values in final output
    if 'game_code' in time_series_df.columns:
        unique_game_codes = time_series_df['game_code'].unique()
        print(f"  [DEBUG] Unique game_code values in output: {sorted([str(x) for x in unique_game_codes if x is not None])}")
        print(f"  [DEBUG] Game_code value counts: {time_series_df['game_code'].value_counts().head(10).to_dict()}")
    else:
        print(f"  [WARNING] game_code column not in final output!")
    
    if 'language' in time_series_df.columns:
        unique_languages = time_series_df['language'].unique()
        print(f"  [DEBUG] Unique language values in output: {sorted([str(x) for x in unique_languages if x is not None])}")
        print(f"  [DEBUG] Language value counts: {time_series_df['language'].value_counts().head(10).to_dict()}")
    else:
        print(f"  [WARNING] language column not in final output!")
    
    return time_series_df


def fetch_hybrid_repeatability_data() -> pd.DataFrame:
    """Fetch repeatability data using Matomo data to count users who completed games"""
    print("\n" + "=" * 60)
    print("FETCHING: Repeatability Data from Redshift")
    print("=" * 60)
    
    if not PSYCOPG2_AVAILABLE:
        print("ERROR: psycopg2 not available. Cannot fetch repeatability data from Redshift.")
        print("  Install with: pip install psycopg2-binary")
        return pd.DataFrame()
    
    print(f"[STEP 1] Connecting to Redshift...")
    print(f"  Host: {REDSHIFT_HOST}")
    print(f"  Database: {REDSHIFT_DATABASE}")
    print(f"  Port: {REDSHIFT_PORT}")
    print(f"  User: {REDSHIFT_USER}")
    
    try:
        # Connect to Redshift
        print(f"  [ACTION] Establishing connection...")
        connection = psycopg2.connect(
            host=REDSHIFT_HOST,
            database=REDSHIFT_DATABASE,
            port=REDSHIFT_PORT,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            connect_timeout=30
        )
        print(f"  ✓ Successfully connected to Redshift")
        
        # New query: Count users who completed games (using completed events from Matomo)
        # Only count completed events, not started events
        # Note: In Redshift, idvisitor might already be in numeric format or we need to handle binary differently
        print(f"\n[STEP 2] Executing repeatability query...")
        print(f"  Query: Fetching completed game events from Redshift")
        repeatability_query = """
        SELECT 
            hg.game_name,
            TO_HEX(mllva.idvisitor) AS idvisitor_hex
        FROM rl_dwh_prod.live.hybrid_games hg
        INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl ON hg.id = hgl.game_id
        INNER JOIN rl_dwh_prod.live.matomo_log_link_visit_action mllva ON hgl.activity_id = mllva.custom_dimension_2
        INNER JOIN rl_dwh_prod.live.matomo_log_action mla ON mllva.idaction_name = mla.idaction
        WHERE (mla.name LIKE '%hybrid_game_completed%'
               OR mla.name LIKE '%hybrid_mcq_completed%')
          AND hgl.activity_id IS NOT NULL
        """
        
        print(f"  [DEBUG] Query to execute:")
        print(f"  {repeatability_query.strip()}")
        print(f"  [ACTION] Executing SQL query...")
        hybrid_df = pd.read_sql(repeatability_query, connection)
        connection.close()
        print(f"  ✓ Query executed successfully")
        print(f"  ✓ Connection closed")
        
        if hybrid_df.empty:
            print(f"\n[WARNING] No data found from Redshift query")
            print(f"  This could mean:")
            print(f"    - No completed events in the database")
            print(f"    - Query conditions not matching any records")
            return pd.DataFrame()
        
        print(f"\n[STEP 3] Processing query results...")
        print(f"  ✓ Fetched {len(hybrid_df):,} records from Redshift")
        
        # Convert hex string to numeric in Python (handles large values that STRTOL can't)
        if 'idvisitor_hex' in hybrid_df.columns:
            print(f"  [ACTION] Converting hex string to numeric in Python...")
            try:
                def hex_to_int(hex_str):
                    """Convert hex string to integer, handling large values"""
                    if pd.isna(hex_str) or hex_str is None or hex_str == '':
                        return 0
                    try:
                        # Python's int() can handle arbitrarily large integers
                        return int(str(hex_str), 16)
                    except (ValueError, TypeError):
                        return 0
                
                hybrid_df['idvisitor_converted'] = hybrid_df['idvisitor_hex'].apply(hex_to_int)
                # Drop the hex column
                hybrid_df = hybrid_df.drop(columns=['idvisitor_hex'])
                
                # Verify conversion
                sample_value = hybrid_df['idvisitor_converted'].iloc[0] if len(hybrid_df) > 0 else None
                print(f"  ✓ Sample idvisitor_converted value: {sample_value} (type: {type(sample_value).__name__})")
                
                # Verify we have unique values
                unique_count = hybrid_df['idvisitor_converted'].nunique()
                total_count = len(hybrid_df)
                print(f"  ✓ Unique idvisitor_converted values: {unique_count:,} out of {total_count:,} total")
                if unique_count == total_count:
                    print(f"  ✓ All values are unique (good!)")
                elif unique_count < total_count:
                    print(f"  ⚠ WARNING: Some duplicate idvisitor_converted values found")
                    print(f"    This might indicate a conversion issue")
            except Exception as e:
                print(f"  ERROR: Could not convert idvisitor_hex to numeric: {str(e)}")
                import traceback
                traceback.print_exc()
                return pd.DataFrame()
        
        print(f"  ✓ Unique users: {hybrid_df['idvisitor_converted'].nunique():,}")
        print(f"  ✓ Unique games: {hybrid_df['game_name'].nunique()}")
        print(f"  ✓ Sample data (first 5 rows):")
        print(hybrid_df.head(5).to_string())
        
        print(f"\n[STEP 4] Calculating repeatability metrics...")
        # Group by user and count distinct games played
        print(f"  [ACTION] Grouping by user and counting distinct games...")
        user_game_counts = hybrid_df.groupby('idvisitor_converted')['game_name'].nunique().reset_index()
        user_game_counts.columns = ['idvisitor_converted', 'games_played']
        print(f"  ✓ Calculated games played per user")
        print(f"  ✓ Total unique users: {len(user_game_counts):,}")
        
        # Group by games_played count and count users
        print(f"  [ACTION] Grouping by games_played count...")
        repeatability_data = user_game_counts.groupby('games_played').size().reset_index()
        repeatability_data.columns = ['games_played', 'user_count']
        print(f"  ✓ Calculated user counts per games_played")
        
        # Create complete range from 1 to max games played
        max_games = user_game_counts['games_played'].max() if not user_game_counts.empty else 0
        print(f"  ✓ Max games played by any user: {max_games}")
        
        if max_games > 0:
            print(f"  [ACTION] Creating complete range from 1 to {max_games}...")
            complete_range = pd.DataFrame({'games_played': range(1, max_games + 1)})
            repeatability_data = complete_range.merge(repeatability_data, on='games_played', how='left').fillna(0)
            repeatability_data['user_count'] = repeatability_data['user_count'].astype(int)
            print(f"  ✓ Created complete range with {len(repeatability_data)} rows")
        
        print(f"\n[STEP 5] Final repeatability data summary:")
        print(f"  ✓ Total rows: {len(repeatability_data)}")
        print(f"  ✓ Total users: {user_game_counts['idvisitor_converted'].nunique():,}")
        print(f"  ✓ Top 10 games_played counts:")
        print(repeatability_data.head(10).to_string())
        print(f"\n✓ SUCCESS: Repeatability data fetched and processed successfully")
        
        return repeatability_data
        
    except psycopg2.OperationalError as e:
        print(f"\n[ERROR] Failed to connect to Redshift:")
        print(f"  Error: {str(e)}")
        print(f"  Check:")
        print(f"    - Redshift credentials are correct")
        print(f"    - Network connectivity to Redshift")
        print(f"    - Redshift cluster is accessible")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    except psycopg2.ProgrammingError as e:
        print(f"\n[ERROR] SQL query error:")
        print(f"  Error: {str(e)}")
        print(f"  Check:")
        print(f"    - Table names and schema are correct")
        print(f"    - Column names exist in Redshift")
        print(f"    - SQL syntax is valid for Redshift")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    except Exception as e:
        print(f"\n[ERROR] Unexpected error while fetching repeatability data:")
        print(f"  Error type: {type(e).__name__}")
        print(f"  Error message: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def preprocess_repeatability_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess game repeatability data based on EXACT SQL query logic:
    
    The correct numbers should be:
    1 game: 15,846 users
    2 games: 10,776 users
    3 games: 6,009 users
    etc.
    
    This suggests the current logic is wrong. Let me implement the correct SQL query logic.
    """
    print("Preprocessing repeatability data using CORRECT SQL query logic...")
    
    # Filter for completed events only
    completed_events = df[df['event'] == 'Completed']
    
    if completed_events.empty:
        print("WARNING: No completed events found")
        return pd.DataFrame()
    
    print(f"DEBUG: Total completed events: {len(completed_events)}")
    print(f"DEBUG: Unique users in completed events: {completed_events['idvisitor_converted'].nunique()}")
    print(f"DEBUG: Unique games in completed events: {completed_events['game_name'].nunique()}")
    
    # The issue might be that we need to filter the data differently
    # Let me check what the actual data looks like
    print("DEBUG: Sample of completed events:")
    print(completed_events[['idvisitor_converted', 'game_name', 'event']].head(10))
    
    # Group by hybrid_profile_id (using idvisitor_converted as proxy)
    # Count distinct non-null values of game_name for each hybrid_profile_id
    user_game_counts = completed_events.groupby('idvisitor_converted')['game_name'].nunique().reset_index()
    user_game_counts.columns = ['hybrid_profile_id', 'games_played']
    
    print(f"DEBUG: User game counts sample:")
    print(user_game_counts.head(10))
    print(f"DEBUG: Games played distribution:")
    print(user_game_counts['games_played'].value_counts().sort_index().head(10))
    
    # Group by the count of distinct non-null game_name
    # Calculate CountDistinct_hybrid_profile_id for each distinct count value
    repeatability_data = user_game_counts.groupby('games_played').size().reset_index()
    repeatability_data.columns = ['games_played', 'user_count']
    
    print(f"DEBUG: Repeatability data before range completion:")
    print(repeatability_data.head(10))
    
    # Create complete range from 1 to max games played
    max_games = user_game_counts['games_played'].max()
    complete_range = pd.DataFrame({'games_played': range(1, max_games + 1)})
    repeatability_data = complete_range.merge(repeatability_data, on='games_played', how='left').fillna(0)
    repeatability_data['user_count'] = repeatability_data['user_count'].astype(int)
    
    print(f"SUCCESS: Repeatability data (SQL logic): {len(repeatability_data)} records")
    print(f"Max distinct games played: {max_games}")
    print(f"Total unique hybrid_profile_id: {user_game_counts['hybrid_profile_id'].nunique()}")
    print(f"FINAL DATA:")
    print(repeatability_data.head(10))
    return repeatability_data


def process_main_data() -> pd.DataFrame:
    """Process main dashboard data and game conversion numbers"""
    print("\n" + "=" * 60)
    print("PROCESSING: Main Dashboard Data")
    print("=" * 60)
    
    df_main = fetch_dataframe()
    if df_main.empty:
        print("ERROR: No main data found.")
        return pd.DataFrame()
    
    print(f"SUCCESS: Fetched {len(df_main)} records from main query")
    # Remove duplicates on idlink_va as requested
    initial_count = len(df_main)
    df_main = df_main.drop_duplicates(subset=['idlink_va'], keep='first')
    print(f"After removing duplicates on idlink_va: {len(df_main)} records (removed {initial_count - len(df_main)} duplicates)")
    df_main['date'] = pd.to_datetime(df_main['server_time']).dt.date
    
    # Extract domain from game_code if it exists
    if 'game_code' in df_main.columns:
        print(f"\n[DOMAIN EXTRACTION] Extracting domain from game_code...")
        sys.stdout.flush()
        df_main['domain'] = df_main['game_code'].apply(extract_domain_from_game_code)
        print(f"  ✓ Extracted domain for {df_main['domain'].notna().sum():,} records")
        print(f"  ✓ Unique domains: {df_main['domain'].dropna().unique().tolist()}")
        sys.stdout.flush()
    else:
        print(f"\n[DOMAIN EXTRACTION] WARNING: 'game_code' column not found - skipping domain extraction")
        sys.stdout.flush()
        df_main['domain'] = None
    
    # Create aggregated processed_data.csv by date, game, and event
    # This will be much smaller and enable date filtering
    print(f"\n[AGGREGATION] Creating aggregated processed_data.csv by date, game, and event...")
    sys.stdout.flush()
    
    # Filter out NULL events for aggregation
    df_main_valid = df_main[df_main['event'].notna()].copy()
    print(f"  Processing {len(df_main_valid):,} records with valid events for aggregation")
    sys.stdout.flush()
    
    if not df_main_valid.empty:
        # Group by date, game_name, and event, then calculate metrics
        aggregated_data = []
        
        # Process in batches to avoid memory issues
        batch_size = 100000
        total_batches = (len(df_main_valid) // batch_size) + 1
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(df_main_valid))
            batch_df = df_main_valid.iloc[start_idx:end_idx]
            
            # Group by date, game_name, event, domain, and language (if available)
            groupby_cols = ['date', 'game_name', 'event']
            if 'domain' in batch_df.columns:
                groupby_cols.append('domain')
            if 'language' in batch_df.columns:
                groupby_cols.append('language')
            
            grouped = batch_df.groupby(groupby_cols).agg({
                'idlink_va': 'count',  # Instances
                'idvisit': 'nunique',  # Visits (distinct)
                'idvisitor_converted': 'nunique'  # Users (distinct)
            }).reset_index()
            
            # Set column names based on available columns
            col_names = ['date', 'game_name', 'event']
            if 'domain' in batch_df.columns:
                col_names.append('domain')
            if 'language' in batch_df.columns:
                col_names.append('language')
            col_names.extend(['instances', 'visits', 'users'])
            grouped.columns = col_names
            aggregated_data.append(grouped)
            
            if total_batches > 1:
                print(f"  [Batch {batch_num + 1}/{total_batches}] Processed {end_idx:,} / {len(df_main_valid):,} records")
                sys.stdout.flush()
        
        # Combine all batches
        print(f"  Combining {len(aggregated_data)} batches...")
        sys.stdout.flush()
        aggregated_df = pd.concat(aggregated_data, ignore_index=True)
        
        # Final aggregation in case there are overlapping dates/games/events across batches
        print(f"  Performing final aggregation...")
        sys.stdout.flush()
        groupby_cols = ['date', 'game_name', 'event']
        if 'domain' in aggregated_df.columns:
            groupby_cols.append('domain')
        if 'language' in aggregated_df.columns:
            groupby_cols.append('language')
        
        processed_data_aggregated = aggregated_df.groupby(groupby_cols).agg({
            'instances': 'sum',
            'visits': 'sum',  # Sum of distinct counts (approximation, but works for our use case)
            'users': 'sum'    # Sum of distinct counts (approximation, but works for our use case)
        }).reset_index()
        
        # Convert date to string for CSV storage
        processed_data_aggregated['date'] = processed_data_aggregated['date'].astype(str)
        
        # Ensure numeric columns are integers
        for col in ['instances', 'visits', 'users']:
            processed_data_aggregated[col] = pd.to_numeric(processed_data_aggregated[col], errors='coerce').fillna(0).astype(int)
        
        print(f"  ✓ Aggregated to {len(processed_data_aggregated):,} rows (date × game × event combinations)")
        print(f"  ✓ Size reduction: {len(df_main):,} → {len(processed_data_aggregated):,} rows ({100 * (1 - len(processed_data_aggregated)/len(df_main)):.1f}% reduction)")
        sys.stdout.flush()
        
        # Save aggregated data
        print(f"\nSaving aggregated processed_data.csv ({len(processed_data_aggregated):,} rows)...")
        sys.stdout.flush()
        processed_data_aggregated.to_csv('data/processed_data.csv', index=False)
        print("✓ SUCCESS: Saved data/processed_data.csv (aggregated by date, game, event)")
        sys.stdout.flush()
        
        # Calculate file size
        file_size_mb = os.path.getsize('data/processed_data.csv') / (1024 * 1024)
        print(f"✓ File size: {file_size_mb:.2f} MB")
        sys.stdout.flush()
    else:
        print("  WARNING: No valid events found - creating empty processed_data.csv")
        # Include language column if it exists in df_main
        base_cols = ['date', 'game_name', 'event', 'instances', 'visits', 'users']
        if 'language' in df_main.columns:
            base_cols.insert(3, 'language')  # Insert after 'event'
        if 'domain' in df_main.columns:
            base_cols.insert(3, 'domain')  # Insert after 'event'
        processed_data_aggregated = pd.DataFrame(columns=base_cols)
        processed_data_aggregated.to_csv('data/processed_data.csv', index=False)
        print("✓ SUCCESS: Saved empty data/processed_data.csv")
        sys.stdout.flush()
    
    # Create and save game-specific conversion numbers
    # Track all funnel stages for each game
    game_conversion_data = []
    
    # Check if event column exists
    if 'event' not in df_main.columns:
        print("ERROR: 'event' column not found in main data")
        print(f"Available columns: {list(df_main.columns)}")
        return df_main
    
    # Filter out NULL events
    df_main_valid = df_main[df_main['event'].notna()].copy()
    print(f"Processing game conversion data: {len(df_main_valid)} records with valid events")
    
    for game in df_main_valid['game_name'].unique():
        if game != 'Unknown Game':
            game_data = df_main_valid[df_main_valid['game_name'] == game]
            
            # Get domain for this game (take first non-null domain if available)
            domain = None
            if 'domain' in game_data.columns:
                game_domains = game_data['domain'].dropna().unique()
                if len(game_domains) > 0:
                    domain = game_domains[0]
            
            # Get language for this game (take first non-null language if available)
            language = None
            if 'language' in game_data.columns:
                game_languages = game_data['language'].dropna().unique()
                if len(game_languages) > 0:
                    language = game_languages[0]
            
            # Calculate metrics for each funnel stage
            funnel_stages = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
            game_stats = {'game_name': game}
            if domain:
                game_stats['domain'] = domain
            if language:
                game_stats['language'] = language
            
            for stage in funnel_stages:
                stage_data = game_data[game_data['event'] == stage]
                game_stats[f'{stage}_users'] = stage_data['idvisitor_converted'].nunique() if 'idvisitor_converted' in stage_data.columns else 0
                game_stats[f'{stage}_visits'] = stage_data['idvisit'].nunique() if 'idvisit' in stage_data.columns else 0
                game_stats[f'{stage}_instances'] = len(stage_data)
            
            game_conversion_data.append(game_stats)
    
    print(f"Creating game conversion numbers for {len(game_conversion_data)} games...")
    sys.stdout.flush()
    game_conversion_df = pd.DataFrame(game_conversion_data)
    print(f"Saving game_conversion_numbers.csv...")
    sys.stdout.flush()
    game_conversion_df.to_csv('data/game_conversion_numbers.csv', index=False)
    print("✓ SUCCESS: Saved data/game_conversion_numbers.csv")
    sys.stdout.flush()
    
    return df_main


def extract_domain_from_game_code(game_code):
    """Extract domain from game code (e.g., HY-29-LL-06 -> LL)"""
    if pd.isna(game_code) or game_code is None or game_code == '':
        return None
    
    game_code_str = str(game_code)
    parts = game_code_str.split('-')
    # Pattern: HY-29-LL-06 -> domain is LL (3rd element, index 2)
    # Split by '-': ['HY', '29', 'LL', '06'] -> parts[2] = 'LL'
    if len(parts) >= 3:
        return parts[2]
    return None

def parse_event_from_name(name):
    """Parse event from action name (same logic as SQL CASE statement)"""
    if pd.isna(name) or name is None or name == '':
        return None
    
    name_str = str(name)
    if '_started' in name_str:
        return 'started'
    elif 'introduction_completed' in name_str and 'mid' not in name_str:
        return 'introduction'
    elif '_mid_introduction' in name_str:
        return 'mid_introduction'
    elif '_poll_completed' in name_str:
        return 'parent_poll'
    elif 'action_completed' in name_str:
        return 'questions'
    elif 'reward_completed' in name_str:
        return 'rewards'
    elif 'question_completed' in name_str:
        return 'validation'
    elif 'completed' in name_str and 'introduction' not in name_str and 'reward' not in name_str and 'question' not in name_str and 'mid_introduction' not in name_str and 'poll' not in name_str and 'action' not in name_str:
        return 'completed'
    return None


def process_summary_data(df_main: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Process summary statistics using conversion_funnel.csv directly"""
    print("\n" + "=" * 60)
    print("PROCESSING: Summary Statistics")
    print("=" * 60)
    
    if df_main is None:
        print("Loading main data from CSV...")
        # Load from conversion_funnel.csv (raw data format)
        csv_file = 'conversion_funnel.csv'
        if not os.path.exists(csv_file):
            print(f"  ERROR: {csv_file} not found")
            print(f"  Please run --main first to generate conversion_funnel.csv")
            return pd.DataFrame()
        
        print(f"  - Loading from {csv_file} (raw data format)")
        df_main = pd.read_csv(csv_file, low_memory=False)
        print(f"  - Loaded {len(df_main)} records")
        print(f"  - Columns: {list(df_main.columns)}")
    
    # Check if we need to create event column and rename idvisitor
    if 'event' not in df_main.columns:
        print("  - Event column not found, creating from 'name' column...")
        if 'name' not in df_main.columns:
            print(f"  ERROR: Neither 'event' nor 'name' column found")
            print(f"  Available columns: {list(df_main.columns)}")
            return pd.DataFrame()
        
        # Create event column from name using the same logic
        df_main['event'] = df_main['name'].apply(parse_event_from_name)
        print(f"  - Created event column from name column")
    
    # Rename idvisitor to idvisitor_converted if needed
    if 'idvisitor' in df_main.columns and 'idvisitor_converted' not in df_main.columns:
        print("  - Renaming 'idvisitor' to 'idvisitor_converted'...")
        df_main['idvisitor_converted'] = df_main['idvisitor']
    
    # Verify required columns exist
    required_cols = ['idvisitor_converted', 'idvisit', 'idlink_va', 'event']
    missing_cols = [col for col in required_cols if col not in df_main.columns]
    if missing_cols:
        print(f"  ERROR: Missing required columns: {missing_cols}")
        print(f"  Available columns: {list(df_main.columns)}")
        print(f"  Please run --main first to generate conversion_funnel.csv with required columns")
        return pd.DataFrame()
    
    # Ensure domain and language columns exist (extract if needed)
    if 'domain' not in df_main.columns and 'game_code' in df_main.columns:
        print("  - Extracting domain from game_code...")
        df_main['domain'] = df_main['game_code'].apply(extract_domain_from_game_code)
    
    # Build summary with domain and language grouping (includes overall summary)
    print("Building summary statistics with domain and language grouping...")
    sys.stdout.flush()
    summary_df = build_summary_with_filters(df_main)
    
    if summary_df.empty:
        print("  ERROR: Failed to build summary statistics")
        return pd.DataFrame()
    
    # Sort by Event, domain, language
    sort_cols = ['Event']
    if 'domain' in summary_df.columns:
        sort_cols.append('domain')
    if 'language' in summary_df.columns:
        sort_cols.append('language')
    summary_df = summary_df.sort_values(sort_cols)
    
    print(f"Saving summary_data.csv ({len(summary_df)} records)...")
    sys.stdout.flush()
    summary_df.to_csv('data/summary_data.csv', index=False)
    print(f"✓ SUCCESS: Saved data/summary_data.csv ({len(summary_df)} records)")
    print(f"  - Includes overall totals (domain='All', language='All')")
    print(f"  - Includes breakdowns by domain and language")
    sys.stdout.flush()
    
    return summary_df


def process_score_distribution(use_database: bool = False) -> pd.DataFrame:
    """Process score distribution data by fetching from Redshift database
    
    Args:
        use_database: If True, fetch directly from Redshift (always True now, CSV removed)
    """
    print("\n" + "=" * 60)
    print("PROCESSING: Score Distribution")
    print("=" * 60)
    
    df_score = pd.DataFrame()
    
    print("\nStep 1: Fetching score data from Redshift database...")
    df_score = fetch_score_dataframe()
    
    if df_score.empty:
        print(f"  [ERROR] No data fetched from Redshift")
        print(f"  [ERROR] Please ensure Redshift is accessible and the query returns data")
        score_distribution_df = pd.DataFrame(columns=['game_name', 'total_score', 'user_count'])
        score_distribution_df.to_csv('data/score_distribution_data.csv', index=False)
        return score_distribution_df
        
    # Check required columns
    print(f"  [INFO] Checking required columns...")
    required_cols = ['game_name', 'action_name', 'custom_dimension_1', 'idvisit', 'server_time']
    # idvisitor_hex OR idvisitor_converted must be present (database already converts it)
    has_visitor_id = 'idvisitor_hex' in df_score.columns or 'idvisitor_converted' in df_score.columns
    missing_cols = [col for col in required_cols if col not in df_score.columns]
    if missing_cols or not has_visitor_id:
        if missing_cols:
            print(f"  [ERROR] Missing required columns: {missing_cols}")
        if not has_visitor_id:
            print(f"  [ERROR] Missing visitor ID column (need either 'idvisitor_hex' or 'idvisitor_converted')")
        print(f"  [INFO] Available columns: {list(df_score.columns)}")
        score_distribution_df = pd.DataFrame(columns=['game_name', 'total_score', 'user_count'])
        score_distribution_df.to_csv('data/score_distribution_data.csv', index=False)
        return score_distribution_df
    print(f"  [OK] All required columns present")
    
    # Convert idvisitor_hex to idvisitor_converted if needed (only if hex exists and converted doesn't)
    if 'idvisitor_hex' in df_score.columns and 'idvisitor_converted' not in df_score.columns:
        print(f"  [ACTION] Converting idvisitor_hex to idvisitor_converted...")
        df_score = convert_hex_to_int(df_score, 'idvisitor_hex', 'idvisitor_converted')
        print(f"  [OK] Conversion complete")
    
    # Convert server_time to datetime if it's a string
    if 'server_time' in df_score.columns:
        try:
            print(f"  [ACTION] Converting server_time to datetime...")
            df_score['server_time'] = pd.to_datetime(df_score['server_time'])
            print(f"  [OK] Datetime conversion complete")
        except Exception as e:
            print(f"  [WARNING] Could not convert server_time: {e}")
    
    if df_score.empty:
        print("  [WARNING] No data found")
        score_distribution_df = pd.DataFrame(columns=['game_name', 'total_score', 'user_count'])
        score_distribution_df.to_csv('data/score_distribution_data.csv', index=False)
        return score_distribution_df
    
    # Print success message
    print(f"  [OK] Successfully loaded {len(df_score):,} records from Redshift database")
    print(f"  [INFO] Unique games in data: {df_score['game_name'].nunique()}")
    
    # Check for optional columns (language and game_code)
    has_language = 'language' in df_score.columns
    has_game_code = 'game_code' in df_score.columns
    if has_language:
        print(f"  [INFO] Language column found: will be included in output")
        # Transform language column: if contains "mr-IN" then "mr", else "hi"
        print(f"  [ACTION] Transforming language column...")
        df_score['language'] = df_score['language'].apply(
            lambda x: 'mr' if pd.notna(x) and 'mr-IN' in str(x) else 'hi'
        )
        print(f"  [OK] Language transformation complete")
    if has_game_code:
        print(f"  [INFO] Game code column found: will extract domain and include in output")
    
    print(f"\nStep 2: Calculating score distribution...")
    score_distribution_df = calculate_score_distribution_combined(df_score)
    
    if not score_distribution_df.empty:
        print(f"\nStep 3: Saving score distribution data...")
        score_distribution_df.to_csv('data/score_distribution_data.csv', index=False)
        print(f"SUCCESS: Saved data/score_distribution_data.csv ({len(score_distribution_df)} records)")
        print(f"  - File size: {len(score_distribution_df)} rows x {len(score_distribution_df.columns)} columns")
    else:
        print("WARNING: No score distribution data to save")
    
    print("\n" + "=" * 60)
    print("SCORE DISTRIBUTION PROCESSING COMPLETE")
    print("=" * 60 + "\n")
    
    return score_distribution_df


def fetch_valid_group_ids() -> List[int]:
    """Fetch valid group IDs from Redshift database based on the specified criteria"""
    if not PSYCOPG2_AVAILABLE:
        print("WARNING: psycopg2 not available. Cannot fetch group IDs from Redshift.")
        return []
    
    print("Fetching valid group IDs from Redshift database...")
    
    try:
        connection = psycopg2.connect(
            host=REDSHIFT_HOST,
            database=REDSHIFT_DATABASE,
            port=REDSHIFT_PORT,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            connect_timeout=30
        )
        
        group_query = """
        SELECT groups.id as group_id
        FROM rl_dwh_prod.live.groups 
        LEFT JOIN rl_dwh_prod.live.schools ON groups.school_id = schools.id 
        LEFT JOIN rl_dwh_prod.live.district_product ON groups.district_product_id = district_product.id 
        LEFT JOIN rl_dwh_prod.live.launches ON groups.launch_id = launches.id 
        LEFT JOIN rl_dwh_prod.live.organization_district_product ON groups.district_product_id = organization_district_product.district_product_id 
            AND groups.launch_id = organization_district_product.launch_id 
        LEFT JOIN rl_dwh_prod.live.districts ON district_product.district_id = districts.id 
        LEFT JOIN rl_dwh_prod.live.group_vnumber ON groups.id = group_vnumber.group_id 
        WHERE group_vnumber.role = 'CB' 
            AND schools.deleted_at IS NULL 
            AND launches.deleted_at IS NULL 
            AND groups.deleted_at IS NULL 
            AND groups.sunset_tag IS NULL 
            AND district_product.broad_tag IN ('ECE - Maharashtra', 'ECE - MP', 'ECE - Chandigarh', 'ECE - UP', 'ECE - RJ', 'ECE - Haryana')
        GROUP BY groups.id
        ORDER BY group_id
        """
        
        group_ids_df = pd.read_sql(group_query, connection)
        connection.close()
        
        group_ids = group_ids_df['group_id'].dropna().astype(int).tolist()
        print(f"SUCCESS: Fetched {len(group_ids)} valid group IDs")
        return group_ids
        
    except Exception as e:
        print(f"ERROR: Failed to fetch group IDs: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def fetch_rm_active_users(group_ids: List[int] = None) -> pd.DataFrame:
    """Load RM active users from CSV file"""
    print("Loading RM active users from CSV file...")
    
    # Check in root directory first, then data directory
    csv_path = 'RM_active_users_data.csv'
    if not os.path.exists(csv_path):
        csv_path = os.path.join('data', 'RM_active_users_data.csv')
    
    if not os.path.exists(csv_path):
        print(f"WARNING: RM_active_users_data.csv not found at {csv_path}")
        print("  Skipping RM active users data...")
        return pd.DataFrame()
    
    try:
        print(f"  Reading CSV file: {csv_path}")
        rm_df = pd.read_csv(csv_path)
        
        # Check if required columns exist
        required_columns = ['phone', 'group_id', 'sent_date']
        missing_columns = [col for col in required_columns if col not in rm_df.columns]
        if missing_columns:
            print(f"WARNING: Missing required columns in CSV: {missing_columns}")
            print(f"  Available columns: {list(rm_df.columns)}")
            return pd.DataFrame()
        
        # Filter by group_ids if provided
        if group_ids and len(group_ids) > 0:
            print(f"  Filtering by {len(group_ids)} group IDs...")
            rm_df = rm_df[rm_df['group_id'].isin(group_ids)]
        
        # Filter out null values
        rm_df = rm_df[
            rm_df['phone'].notna() & 
            rm_df['group_id'].notna() & 
            rm_df['sent_date'].notna()
        ].copy()
        
        # Filter by date (sent_date >= '2025-07-01')
        rm_df['sent_date'] = pd.to_datetime(rm_df['sent_date'], errors='coerce')
        rm_df = rm_df[rm_df['sent_date'] >= '2025-07-01'].copy()
        
        print(f"SUCCESS: Loaded {len(rm_df)} RM active user records from CSV")
        if not rm_df.empty:
            print(f"  Unique phones: {rm_df['phone'].nunique()}")
            print(f"  Date range: {rm_df['sent_date'].min()} to {rm_df['sent_date'].max()}")
        
        return rm_df
        
    except Exception as e:
        print(f"ERROR: Failed to load RM active users from CSV: {str(e)}")
        import traceback
        traceback.print_exc()
        print("  Continuing without RM active users data...")
        return pd.DataFrame()


def process_rm_active_users_time_series(rm_df: pd.DataFrame) -> pd.DataFrame:
    """Process RM active users data for time series (daily, weekly, monthly)"""
    if rm_df.empty:
        return pd.DataFrame()
    
    print("Processing RM active users time series data...")
    
    # Convert sent_date to datetime
    rm_df['sent_date'] = pd.to_datetime(rm_df['sent_date'])
    rm_df['date'] = rm_df['sent_date'].dt.date
    
    time_series_data = []
    
    # Daily aggregation
    daily_rm = rm_df.groupby('date')['phone'].nunique().reset_index()
    daily_rm.columns = ['date', 'rm_active_users']
    daily_rm['period_label'] = daily_rm['date'].astype(str)
    daily_rm['period_type'] = 'Day'
    daily_rm['game_name'] = 'All Games'
    daily_rm['metric'] = 'rm_active_users'
    daily_rm['event'] = 'RM Active Users'
    
    for _, row in daily_rm.iterrows():
        time_series_data.append({
            'period_label': row['period_label'],
            'game_name': row['game_name'],
            'metric': row['metric'],
            'event': row['event'],
            'count': int(row['rm_active_users']),
            'period_type': row['period_type'],
            'game_code': None,  # RM active users are not game-specific
            'language': None    # RM active users are not language-specific
        })
    
    # Weekly aggregation
    rm_df['shifted_date'] = rm_df['sent_date'] - pd.Timedelta(days=2)
    rm_df['year'] = rm_df['shifted_date'].dt.year
    rm_df['week'] = rm_df['shifted_date'].dt.strftime('%W').astype(int)
    rm_df['period_label'] = rm_df['year'].astype(str) + '_' + rm_df['week'].astype(str).str.zfill(2)
    
    weekly_rm = rm_df.groupby('period_label')['phone'].nunique().reset_index()
    weekly_rm.columns = ['period_label', 'rm_active_users']
    weekly_rm['period_type'] = 'Week'
    weekly_rm['game_name'] = 'All Games'
    weekly_rm['metric'] = 'rm_active_users'
    weekly_rm['event'] = 'RM Active Users'
    
    for _, row in weekly_rm.iterrows():
        time_series_data.append({
            'period_label': row['period_label'],
            'game_name': row['game_name'],
            'metric': row['metric'],
            'event': row['event'],
            'count': int(row['rm_active_users']),
            'period_type': row['period_type'],
            'game_code': None,  # RM active users are not game-specific
            'language': None    # RM active users are not language-specific
        })
    
    # Monthly aggregation
    rm_df['year'] = rm_df['sent_date'].dt.year
    rm_df['month'] = rm_df['sent_date'].dt.month
    rm_df['period_label'] = rm_df['year'].astype(str) + '_' + rm_df['month'].astype(str).str.zfill(2)
    
    monthly_rm = rm_df.groupby('period_label')['phone'].nunique().reset_index()
    monthly_rm.columns = ['period_label', 'rm_active_users']
    monthly_rm['period_type'] = 'Month'
    monthly_rm['game_name'] = 'All Games'
    monthly_rm['metric'] = 'rm_active_users'
    monthly_rm['event'] = 'RM Active Users'
    
    for _, row in monthly_rm.iterrows():
        time_series_data.append({
            'period_label': row['period_label'],
            'game_name': row['game_name'],
            'metric': row['metric'],
            'event': row['event'],
            'count': int(row['rm_active_users']),
            'period_type': row['period_type'],
            'game_code': None,  # RM active users are not game-specific
            'language': None    # RM active users are not language-specific
        })
    
    rm_time_series_df = pd.DataFrame(time_series_data)
    print(f"SUCCESS: Processed {len(rm_time_series_df)} RM active users time series records")
    return rm_time_series_df


def process_time_series(df_main: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Process time series data for instances, visits, and users
    All metrics are calculated from a single query using idlink_va for instances"""
    print("\n" + "=" * 60)
    print("PROCESSING: Time Series Data (Instances, Visits, Users)")
    print("=" * 60)
    
    # Fetch time series data from Redshift (single query for all metrics)
    print("=" * 60)
    print("FETCHING: Time Series Data from REDSHIFT")
    print("=" * 60)
    print(f"  Database Type: REDSHIFT (NOT MySQL/SQL)")
    print(f"  Host: {REDSHIFT_HOST}")
    print(f"  Database: {REDSHIFT_DATABASE}")
    print(f"  Port: {REDSHIFT_PORT}")
    print(f"  User: {REDSHIFT_USER}")
    print(f"  Connection Library: psycopg2 (PostgreSQL/Redshift driver)")
    
    df_time_series = pd.DataFrame()
    
    if not PSYCOPG2_AVAILABLE:
        print("ERROR: psycopg2 not available. Cannot fetch time series data from Redshift.")
        print("  Install with: pip install psycopg2-binary")
    else:
        try:
            print(f"\n  [ACTION] Connecting to REDSHIFT...")
            conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                connect_timeout=30
            )
            print(f"  ✓ Successfully connected to REDSHIFT")
            print(f"  [ACTION] Executing time series query on REDSHIFT...")
            df_time_series = pd.read_sql(TIME_SERIES_QUERY, conn)
            conn.close()
            print(f"  ✓ Query executed successfully on REDSHIFT")
            print(f"  ✓ Connection closed")
            
            # Convert hex to int in Python (handles large values)
            if 'idvisitor_hex' in df_time_series.columns:
                print(f"  [ACTION] Converting hex to integer in Python...")
                df_time_series = convert_hex_to_int(df_time_series, 'idvisitor_hex', 'idvisitor_converted')
                print(f"  ✓ Converted idvisitor_hex to idvisitor_converted")
            
            print(f"  ✓ Time series query returned {len(df_time_series)} records from REDSHIFT")
        except psycopg2.OperationalError as e:
            print(f"\n  ERROR: Failed to connect to REDSHIFT:")
            print(f"    Error Type: OperationalError (connection issue)")
            print(f"    Error Message: {str(e)}")
            print(f"    Check:")
            print(f"      - REDSHIFT credentials are correct in .env file")
            print(f"      - Network connectivity to Redshift cluster")
            print(f"      - Redshift cluster is running and accessible")
            import traceback
            traceback.print_exc()
        except Exception as e:
            print(f"\n  ERROR: Failed to fetch time series data from REDSHIFT:")
            print(f"    Error Type: {type(e).__name__}")
            print(f"    Error Message: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Process time series data (instances, visits, users all from same query)
    time_series_df = pd.DataFrame()
    if not df_time_series.empty:
        try:
            time_series_df = preprocess_time_series_data_visits_users(df_time_series)
        except Exception as e:
            print(f"WARNING: Failed to process time series data: {str(e)}")
            import traceback
            traceback.print_exc()
            time_series_df = pd.DataFrame()
    else:
        print("WARNING: No time series data to process")
        time_series_df = pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type'])
    
    # Load and process RM active users data from CSV (optional - don't block if it fails)
    print("\n" + "=" * 60)
    print("LOADING: RM Active Users Data from CSV (Optional)")
    print("=" * 60)
    rm_time_series_df = pd.DataFrame()
    
    try:
        # Optionally filter by group_ids if needed, but CSV should already be filtered
        group_ids = fetch_valid_group_ids()
        rm_df = fetch_rm_active_users(group_ids if group_ids else None)
        
        if not rm_df.empty:
            try:
                rm_time_series_df = process_rm_active_users_time_series(rm_df)
                print("SUCCESS: RM active users data processed and will be included in time series")
            except Exception as e:
                print(f"WARNING: Failed to process RM active users time series: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print("WARNING: No RM active users data loaded, continuing without it")
    except Exception as e:
        print(f"WARNING: Error during RM active users processing: {str(e)}")
        print("  Continuing with time series processing without RM active users...")
        import traceback
        traceback.print_exc()
    
    # Combine time series data with RM active users
    if not rm_time_series_df.empty:
        time_series_df = pd.concat([time_series_df, rm_time_series_df], ignore_index=True)
        print(f"Combined time series data: {len(time_series_df)} records (including RM active users)")
    
    # Sort by period_type, game_name, and period_label
    time_series_df = time_series_df.sort_values(['period_type', 'game_name', 'period_label'])
    
    # Save to CSV
    if not time_series_df.empty:
        time_series_df.to_csv('data/time_series_data.csv', index=False)
        print(f"SUCCESS: Saved data/time_series_data.csv ({len(time_series_df)} records)")
        print(f"  Columns: {list(time_series_df.columns)}")
        print(f"  Sample row: {time_series_df.iloc[0].to_dict() if len(time_series_df) > 0 else 'N/A'}")
    else:
        print("WARNING: No time series data to save")
        empty_df = pd.DataFrame(columns=['period_label', 'game_name', 'metric', 'event', 'count', 'period_type', 'game_code', 'language'])
        empty_df.to_csv('data/time_series_data.csv', index=False)
    
    return time_series_df


def process_repeatability(df_main: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Process repeatability data"""
    print("\n" + "=" * 60)
    print("PROCESSING: Repeatability Data")
    print("=" * 60)
    
    print(f"\n[STEP 1] Fetching repeatability data from Redshift...")
    repeatability_df = fetch_hybrid_repeatability_data()
    
    # Fallback to processed data if Redshift fetch fails
    if repeatability_df.empty:
        print(f"\n[STEP 2] Redshift data is empty, trying fallback to processed data...")
        print(f"  [ACTION] Loading processed_data.csv...")
        try:
            if df_main is None:
                if not os.path.exists('data/processed_data.csv'):
                    print(f"  ERROR: data/processed_data.csv not found")
                    print(f"  Cannot use fallback method")
                    print(f"  WARNING: No repeatability data to save")
                    return pd.DataFrame()
                
                print(f"  [ACTION] Reading data/processed_data.csv...")
                df_main = pd.read_csv('data/processed_data.csv')
                print(f"  ✓ Loaded {len(df_main):,} records from processed_data.csv")
                
                if 'server_time' in df_main.columns:
                    df_main['server_time'] = pd.to_datetime(df_main['server_time'])
                    print(f"  ✓ Converted server_time to datetime")
                else:
                    print(f"  WARNING: server_time column not found in processed_data.csv")
            
            print(f"  [ACTION] Processing repeatability data from processed_data.csv...")
            repeatability_df = preprocess_repeatability_data(df_main)
            
            if not repeatability_df.empty:
                print(f"  ✓ Successfully processed repeatability data from fallback source")
            else:
                print(f"  WARNING: Fallback processing returned empty data")
        except Exception as e:
            print(f"  ERROR: Fallback processing failed: {str(e)}")
            import traceback
            traceback.print_exc()
    else:
        print(f"\n[STEP 2] Redshift data fetched successfully, skipping fallback")
    
    # Save to CSV regardless of data source (Redshift or fallback)
    print(f"\n[STEP 3] Saving repeatability data to CSV...")
    if not repeatability_df.empty:
        print(f"  [ACTION] Writing to data/repeatability_data.csv...")
        repeatability_df.to_csv('data/repeatability_data.csv', index=False)
        file_size_kb = os.path.getsize('data/repeatability_data.csv') / 1024
        print(f"  ✓ SUCCESS: Saved data/repeatability_data.csv")
        print(f"    - Records: {len(repeatability_df):,}")
        print(f"    - File size: {file_size_kb:.2f} KB")
        print(f"    - Columns: {list(repeatability_df.columns)}")
        print(f"    - Sample data (first 5 rows):")
        print(repeatability_df.head(5).to_string())
    else:
        print(f"  WARNING: No repeatability data to save")
        print(f"  Creating empty CSV file...")
        empty_df = pd.DataFrame(columns=['games_played', 'user_count'])
        empty_df.to_csv('data/repeatability_data.csv', index=False)
        print(f"  ✓ Created empty data/repeatability_data.csv")

    print(f"\n✓ PROCESSING COMPLETE: Repeatability Data")
    return repeatability_df


def process_parent_poll() -> pd.DataFrame:
    """Process parent poll responses data from Excel file (NOT from database)"""
    import sys
    
    print("\n" + "=" * 60, flush=True)
    print("PROCESSING: Parent Poll Responses", flush=True)
    print("=" * 60, flush=True)
    print("NOTE: Reading from Excel file, NOT from database", flush=True)
    
    # Read poll data from CSV or Excel file (prefer CSV)
    csv_file = 'poll_responses_raw_data.csv'
    excel_file = 'poll_responses_raw_data.xlsx'
    df_poll = None
    
    # Try CSV first
    if os.path.exists(csv_file):
        print(f"\n[STEP 1] Reading parent poll data from CSV file: {csv_file}", flush=True)
        print("  This step reads the CSV file into memory...", flush=True)
        try:
            print("  [ACTION] Starting to read CSV file (this may take a moment for large files)...", flush=True)
            sys.stdout.flush()
            df_poll = pd.read_csv(csv_file, low_memory=False)
            print(f"  [SUCCESS] CSV file loaded successfully!", flush=True)
            print(f"  Total records loaded: {len(df_poll):,}", flush=True)
            sys.stdout.flush()
        except Exception as e:
            print(f"  ERROR: Failed to read CSV file: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Fallback to Excel if CSV not found or failed
    if df_poll is None and os.path.exists(excel_file):
        print(f"\n[STEP 1] Reading parent poll data from Excel file: {excel_file}", flush=True)
        print("  This step reads the Excel file into memory...", flush=True)
        try:
            print("  [ACTION] Starting to read Excel file (this may take a moment for large files)...", flush=True)
            sys.stdout.flush()
            df_poll = pd.read_excel(excel_file)
            print(f"  [SUCCESS] Excel file loaded successfully!", flush=True)
            print(f"  Total records loaded: {len(df_poll):,}", flush=True)
            sys.stdout.flush()
        except Exception as e:
            print(f"  ERROR: Failed to read Excel file: {str(e)}")
            import traceback
            traceback.print_exc()
    
    if df_poll is None:
        print(f"  ERROR: Neither '{csv_file}' nor '{excel_file}' found")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count', 'language', 'domain'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    if df_poll.empty:
        print("WARNING: No parent poll data found in file")
        # Create empty dataframe with expected headers
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count', 'language', 'domain'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    # Ensure required columns exist
    if 'custom_dimension_1' not in df_poll.columns:
        print("ERROR: 'custom_dimension_1' column not found in file")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count', 'language', 'domain'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    if 'game_name' not in df_poll.columns:
        print("ERROR: 'game_name' column not found in file")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count', 'language', 'domain'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    print(f"\n[STEP 2] Validating data structure...", flush=True)
    print(f"  Available columns: {list(df_poll.columns)}", flush=True)
    
    # Normalize column names (handle case variations and spaces)
    column_mapping = {}
    for col in df_poll.columns:
        col_lower = str(col).lower().strip()
        if col_lower in ['language', 'lanuagae']:  # Handle typo
            column_mapping['language'] = col
        elif col_lower in ['game_code', 'gamecode', 'game code']:
            column_mapping['game_code'] = col
        elif col_lower == 'custom_dimension_1':
            column_mapping['custom_dimension_1'] = col
        elif col_lower == 'game_name':
            column_mapping['game_name'] = col
    
    # Check for required columns
    has_language = 'language' in column_mapping
    has_game_code = 'game_code' in column_mapping
    
    if has_language:
        print(f"  [INFO] Language column found in raw data: '{column_mapping['language']}'", flush=True)
    else:
        print(f"  [WARNING] Language column not found - checking available columns...", flush=True)
        lang_cols = [c for c in df_poll.columns if 'lang' in str(c).lower()]
        if lang_cols:
            print(f"    Found potential language columns: {lang_cols}", flush=True)
    
    if has_game_code:
        print(f"  [INFO] game_code column found in raw data: '{column_mapping['game_code']}' - will extract domain", flush=True)
    else:
        print(f"  [WARNING] game_code column not found - checking available columns...", flush=True)
        game_code_cols = [c for c in df_poll.columns if 'game' in str(c).lower() and 'code' in str(c).lower()]
        if game_code_cols:
            print(f"    Found potential game_code columns: {game_code_cols}", flush=True)
    
    # Process each record
    processed_records = []
    debug_count = 0
    skipped_no_json = 0
    skipped_no_structure = 0
    records_with_poll_items = 0
    total_poll_items_found = 0
    encoding_errors = 0
    
    print(f"\n[STEP 3] Processing {len(df_poll):,} poll records...", flush=True)
    print(f"  This step extracts poll responses from JSON in custom_dimension_1 column", flush=True)
    print(f"  Progress will be shown every 10,000 records...", flush=True)
    sys.stdout.flush()
    
    for idx, row in df_poll.iterrows():
        try:
            # Get columns using normalized mapping or direct access
            custom_dim_1 = row.get(column_mapping.get('custom_dimension_1', 'custom_dimension_1'))
            game_name = row.get(column_mapping.get('game_name', 'game_name'))
            idvisit = row.get('idvisit')
            
            # Get language and game_code using normalized column names
            language = None
            if has_language:
                language_col = column_mapping.get('language')
                language = row.get(language_col) if language_col else None
                # Handle NaN/None
                if pd.isna(language):
                    language = None
            
            game_code = None
            if has_game_code:
                game_code_col = column_mapping.get('game_code')
                game_code = row.get(game_code_col) if game_code_col else None
                # Handle NaN/None
                if pd.isna(game_code):
                    game_code = None
            
            domain = None
            if game_code:
                domain = extract_domain_from_game_code(game_code)
            
            # Progress indicator
            if (idx + 1) % 10000 == 0:
                print(f"\n    [PROGRESS] {idx + 1:,}/{len(df_poll):,} records processed", flush=True)
                print(f"      - Records with poll items: {records_with_poll_items:,}", flush=True)
                print(f"      - Total poll responses extracted: {total_poll_items_found:,}", flush=True)
                print(f"      - Skipped (no JSON): {skipped_no_json:,}", flush=True)
                print(f"      - Skipped (no structure): {skipped_no_structure:,}", flush=True)
                sys.stdout.flush()
            
            # Parse JSON from custom_dimension_1
            if not custom_dim_1 or pd.isna(custom_dim_1):
                skipped_no_json += 1
                if debug_count < 3:
                    print(f"    [SKIP] Record {idx+1}: custom_dimension_1 is empty or NaN")
                    debug_count += 1
                continue
            
            try:
                poll_data = json.loads(custom_dim_1)
            except json.JSONDecodeError as e:
                skipped_no_json += 1
                if debug_count < 3:
                    print(f"    [SKIP] Record {idx+1}: JSON decode error - {str(e)[:50]}")
                    debug_count += 1
                continue
            
            if not isinstance(poll_data, dict):
                skipped_no_structure += 1
                if debug_count < 3:
                    print(f"    [SKIP] Record {idx+1}: poll_data is not a dict (type: {type(poll_data)})")
                    debug_count += 1
                continue
            
            # Look for poll data in the JSON structure
            # Poll data can be:
            # 1. At root level in a 'poll' key
            # 2. In gameData array (nested structure)
            # 3. At root level with options/chosenOption
            
            poll_items = []
            
            # Check for 'poll' key at root
            if 'poll' in poll_data:
                poll_section = poll_data.get('poll')
                if isinstance(poll_section, list):
                    poll_items.extend(poll_section)
                    if debug_count < 2:
                        print(f"    [FOUND] Record {idx+1}: 'poll' key at root (list with {len(poll_section)} items)")
                elif isinstance(poll_section, dict):
                    poll_items.append(poll_section)
                    if debug_count < 2:
                        print(f"    [FOUND] Record {idx+1}: 'poll' key at root (dict)")
            
            # Check if root has poll-like structure
            if 'options' in poll_data and 'chosenOption' in poll_data:
                poll_items.append(poll_data)
                if debug_count < 2:
                    print(f"    [FOUND] Record {idx+1}: Poll structure at root level")
            
            # Search through gameData array for poll responses
            # IMPORTANT: Only extract from "Poll" section, not from "Action" section
            game_data = poll_data.get('gameData', [])
            if isinstance(game_data, list):
                game_data_poll_count = 0
                for game_item in game_data:
                    if not isinstance(game_item, dict):
                        continue
                    
                    # Check if this is the "Poll" section
                    section = game_item.get('section', '')
                    if section.lower() == 'poll':
                        # This is the Poll section - extract poll questions from here
                        nested_game_data = game_item.get('gameData', [])
                        if isinstance(nested_game_data, list):
                            for question_idx, nested_item in enumerate(nested_game_data):
                                if isinstance(nested_item, dict) and 'options' in nested_item and 'chosenOption' in nested_item:
                                    # Add question number (1, 2, or 3) to the poll item
                                    nested_item['_poll_question_number'] = question_idx + 1
                                    poll_items.append(nested_item)
                                    game_data_poll_count += 1
                
                if game_data_poll_count > 0 and debug_count < 2:
                    print(f"    [FOUND] Record {idx+1}: Found {game_data_poll_count} poll items in Poll section")
            
            # If no poll items found, skip this record
            if not poll_items:
                skipped_no_structure += 1
                if debug_count < 5:
                    root_keys = list(poll_data.keys())[:10]
                    print(f"    [SKIP] Record {idx+1} ({game_name}): No poll structure found. Root keys: {root_keys}")
                    debug_count += 1
                continue
            
            # Track records with poll items
            records_with_poll_items += 1
            total_poll_items_found += len(poll_items)
            
            if debug_count < 2:
                print(f"    [PROCESSING] Record {idx+1} ({game_name}): Found {len(poll_items)} poll items")
            
            # Process each poll item found
            for poll_item_idx, poll_item in enumerate(poll_items):
                if not isinstance(poll_item, dict):
                    continue
                
                options = poll_item.get('options', [])
                chosen_option = poll_item.get('chosenOption')
                
                if isinstance(options, list) and len(options) > 0 and chosen_option is not None:
                    try:
                        chosen_option_idx = int(chosen_option)
                        if 0 <= chosen_option_idx < len(options):
                            selected_option = options[chosen_option_idx]
                            # Try different possible fields for option text
                            # Handle encoding issues by using safe string conversion
                            try:
                                # First try to get message field
                                message_field = selected_option.get('message', '')
                                
                                # If message is a dict (with language codes), extract a readable value
                                if isinstance(message_field, dict):
                                    # Try to get English first, then any available language
                                    option_message = (
                                        message_field.get('en', '') or
                                        message_field.get('en_US', '') or
                                        message_field.get('en_IN', '') or
                                        # If no English, get the first available value
                                        (list(message_field.values())[0] if message_field else '')
                                    )
                                elif message_field:
                                    option_message = message_field
                                else:
                                    # Fallback to other fields
                                    option_message = (
                                        selected_option.get('text', '') or 
                                        selected_option.get('label', '') or
                                        str(selected_option.get('path', '')) or
                                        str(selected_option.get('id', '')) or
                                        f"Option {chosen_option_idx + 1}"
                                    )
                                
                                # Ensure it's a string and handle encoding
                                if isinstance(option_message, bytes):
                                    option_message = option_message.decode('utf-8', errors='replace')
                                else:
                                    option_message = str(option_message)
                                
                                # Clean up the message - remove extra whitespace
                                option_message = option_message.strip()
                                
                                # If still empty or looks like a dict string, use option number
                                if not option_message or option_message.startswith('{') or option_message.startswith('['):
                                    option_message = f"Option {chosen_option_idx + 1}"
                                
                                # Replace any problematic characters for display
                                try:
                                    option_message = option_message.encode('utf-8', errors='replace').decode('utf-8')
                                except:
                                    option_message = f"Option {chosen_option_idx + 1}"
                            except Exception as e:
                                encoding_errors += 1
                                # If all else fails, use a safe representation
                                option_message = f"Option_{chosen_option_idx}"
                                if debug_count < 3:
                                    print(f"      [ENCODING ERROR] Record {idx+1}, Poll Item {poll_item_idx+1}: {str(e)[:50]}")
                                    debug_count += 1
                            
                            if option_message:
                                # Get question number from poll item (1, 2, or 3)
                                question_number = poll_item.get('_poll_question_number')
                                
                                # If we have a question number, use it
                                if question_number:
                                    question_text = f"Question {question_number}"
                                else:
                                    # Fallback: try to get from poll_item fields
                                    question_text = (
                                        poll_item.get('question', '') or 
                                        poll_item.get('questionText', '') or
                                        poll_item.get('questionId', '') or
                                        poll_item.get('question_id', '')
                                    )
                                    
                                    # If still no question text, use a generic identifier
                                    if not question_text:
                                        question_text = "Question (unknown)"
                                
                                record = {
                                    'game_name': game_name,
                                    'question': question_text,
                                    'option': option_message
                                }
                                # Add language and domain if available
                                if language is not None:
                                    record['language'] = language
                                if domain is not None:
                                    record['domain'] = domain
                                
                                processed_records.append(record)
                    except (ValueError, IndexError, TypeError):
                        continue
                
        except Exception as e:
            print(f"  WARNING: Error processing poll record {idx+1}: {str(e)}")
            if debug_count < 3:
                import traceback
                traceback.print_exc()
                debug_count += 1
            continue
    
    print(f"\n[STEP 4] Processing Summary:", flush=True)
    print(f"    - Total records processed: {len(df_poll):,}", flush=True)
    print(f"    - Records with poll items: {records_with_poll_items:,}", flush=True)
    print(f"    - Total poll items found: {total_poll_items_found:,}", flush=True)
    print(f"    - Skipped (no JSON): {skipped_no_json:,}", flush=True)
    print(f"    - Skipped (no poll structure): {skipped_no_structure:,}", flush=True)
    print(f"    - Encoding errors handled: {encoding_errors:,}", flush=True)
    print(f"    - Valid poll responses extracted: {len(processed_records):,}", flush=True)
    sys.stdout.flush()
    
    if not processed_records:
        print("\n  WARNING: No valid poll responses found after processing")
        poll_df = pd.DataFrame(columns=['game_name', 'question', 'option', 'count', 'language', 'domain'])
        poll_df.to_csv('data/poll_responses_data.csv', index=False)
        return poll_df
    
    # Convert to DataFrame
    print(f"\n[STEP 5] Converting to DataFrame...", flush=True)
    results_df = pd.DataFrame(processed_records)
    print(f"    Created DataFrame with {len(results_df)} rows", flush=True)
    
    # Aggregate: generate all combinations like summary_data.csv
    # 1. Overall totals (domain='All', language='All')
    # 2. By domain only (domain='CG', language='All')
    # 3. By language only (domain='All', language='hi')
    # 4. By both (domain='CG', language='hi')
    print(f"[STEP 6] Generating all combinations (overall, by domain, by language, by both)...", flush=True)
    sys.stdout.flush()
    
    # Fill NaN values in language and domain with 'Unknown' for grouping
    if 'language' in results_df.columns:
        results_df['language'] = results_df['language'].fillna('Unknown')
        print(f"  [INFO] Language column found - unique values: {results_df['language'].nunique()}", flush=True)
    if 'domain' in results_df.columns:
        results_df['domain'] = results_df['domain'].fillna('Unknown')
        print(f"  [INFO] Domain column found - unique values: {results_df['domain'].nunique()}", flush=True)
    
    all_combinations = []
    
    # 1. Overall totals (domain='All', language='All')
    print(f"  [1/4] Calculating overall totals (domain='All', language='All')...", flush=True)
    overall = results_df.groupby(['game_name', 'question', 'option']).size().reset_index(name='count')
    overall['domain'] = 'All'
    overall['language'] = 'All'
    all_combinations.append(overall)
    print(f"    Generated {len(overall):,} overall records", flush=True)
    
    # 2. By domain only (domain='CG', language='All')
    if 'domain' in results_df.columns:
        print(f"  [2/4] Calculating by domain only (language='All')...", flush=True)
        by_domain = results_df.groupby(['game_name', 'question', 'option', 'domain']).size().reset_index(name='count')
        by_domain['language'] = 'All'
        # Remove rows where domain is 'Unknown'
        by_domain = by_domain[by_domain['domain'] != 'Unknown']
        all_combinations.append(by_domain)
        print(f"    Generated {len(by_domain):,} domain-only records", flush=True)
    
    # 3. By language only (domain='All', language='hi')
    if 'language' in results_df.columns:
        print(f"  [3/4] Calculating by language only (domain='All')...", flush=True)
        by_language = results_df.groupby(['game_name', 'question', 'option', 'language']).size().reset_index(name='count')
        by_language['domain'] = 'All'
        # Remove rows where language is 'Unknown'
        by_language = by_language[by_language['language'] != 'Unknown']
        all_combinations.append(by_language)
        print(f"    Generated {len(by_language):,} language-only records", flush=True)
    
    # 4. By both (domain='CG', language='hi')
    if 'domain' in results_df.columns and 'language' in results_df.columns:
        print(f"  [4/4] Calculating by both domain and language...", flush=True)
        by_both = results_df.groupby(['game_name', 'question', 'option', 'domain', 'language']).size().reset_index(name='count')
        # Remove rows where domain or language is 'Unknown'
        by_both = by_both[(by_both['domain'] != 'Unknown') & (by_both['language'] != 'Unknown')]
        all_combinations.append(by_both)
        print(f"    Generated {len(by_both):,} domain+language records", flush=True)
    
    # Combine all combinations
    if all_combinations:
        # Ensure all dataframes have the same columns in the same order
        base_cols = ['game_name', 'question', 'option', 'count', 'domain', 'language']
        # Reorder columns for each dataframe
        reordered_combinations = []
        for df in all_combinations:
            # Only include columns that exist
            available_cols = [col for col in base_cols if col in df.columns]
            reordered_df = df[available_cols].copy()
            # Add missing columns with default values
            for col in base_cols:
                if col not in reordered_df.columns:
                    if col == 'domain':
                        reordered_df['domain'] = 'All'
                    elif col == 'language':
                        reordered_df['language'] = 'All'
            # Reorder to match base_cols
            reordered_df = reordered_df[base_cols]
            reordered_combinations.append(reordered_df)
        agg_df = pd.concat(reordered_combinations, ignore_index=True)
        
        print(f"  Total records after combining all combinations: {len(agg_df):,}", flush=True)
        if 'language' in agg_df.columns:
            print(f"  Unique languages: {sorted(agg_df['language'].unique())}", flush=True)
        if 'domain' in agg_df.columns:
            print(f"  Unique domains: {sorted(agg_df['domain'].dropna().unique())}", flush=True)
    else:
        # Fallback: basic aggregation if no language/domain columns
        print(f"  [FALLBACK] Basic aggregation (no language/domain columns)...", flush=True)
        agg_df = results_df.groupby(['game_name', 'question', 'option']).size().reset_index(name='count')
        agg_df['domain'] = 'All'
        agg_df['language'] = 'All'
    
    sys.stdout.flush()
    
    # Sort for consistent output
    sort_cols = ['game_name', 'question', 'option']
    if 'domain' in agg_df.columns:
        sort_cols.append('domain')
    if 'language' in agg_df.columns:
        sort_cols.append('language')
    agg_df = agg_df.sort_values(sort_cols)
    
    print(f"\n[STEP 7] Final Aggregation Summary:", flush=True)
    print(f"    - Unique records: {len(agg_df):,}", flush=True)
    print(f"    - Games: {agg_df['game_name'].nunique()}", flush=True)
    print(f"    - Questions: {agg_df['question'].nunique()}", flush=True)
    print(f"    - Total response count: {agg_df['count'].sum():,}", flush=True)
    
    # Show sample of games
    if len(agg_df) > 0:
        print(f"\n  Games with poll data:", flush=True)
        game_counts = agg_df.groupby('game_name')['count'].sum().sort_values(ascending=False)
        for game, count in game_counts.head(10).items():
            print(f"    - {game}: {count:,} responses", flush=True)
        if len(game_counts) > 10:
            print(f"    ... and {len(game_counts) - 10} more games", flush=True)
    
    # Save to CSV
    print(f"\n[STEP 8] Saving to data/poll_responses_data.csv...", flush=True)
    agg_df.to_csv('data/poll_responses_data.csv', index=False)
    print(f"  [SUCCESS] Saved data/poll_responses_data.csv ({len(agg_df)} records)", flush=True)
    sys.stdout.flush()
    
    return agg_df


def process_question_correctness(use_database: bool = False) -> pd.DataFrame:
    """Process question correctness data by fetching from Redshift database
    
    Uses the same query and processing method as score distribution for each game.
    Always fetches from Redshift (same as score distribution).
    
    Args:
        use_database: If True, fetch directly from Redshift (always True now, CSV removed)
    """
    print("\n" + "=" * 60)
    print("PROCESSING: Question Correctness Data")
    print("=" * 60)
    
    df_score = pd.DataFrame()
    
    print("\nStep 1: Fetching data from Redshift database...")
    df_score = fetch_score_dataframe()
    
    if df_score.empty:
        print(f"  [ERROR] No data fetched from Redshift")
        print(f"  [ERROR] Please ensure Redshift is accessible and the query returns data")
        question_correctness_df = pd.DataFrame(columns=['game_name','question_number','correctness','percent','user_count','total_users'])
        question_correctness_df.to_csv('data/question_correctness_data.csv', index=False)
        return question_correctness_df
        
    # Check required columns
    print(f"  [INFO] Checking required columns...")
    required_cols = ['game_name', 'action_name', 'custom_dimension_1', 'idvisit', 'server_time']
    # idvisitor_hex OR idvisitor_converted must be present (database already converts it)
    has_visitor_id = 'idvisitor_hex' in df_score.columns or 'idvisitor_converted' in df_score.columns
    missing_cols = [col for col in required_cols if col not in df_score.columns]
    if missing_cols or not has_visitor_id:
        if missing_cols:
            print(f"  [ERROR] Missing required columns: {missing_cols}")
        if not has_visitor_id:
            print(f"  [ERROR] Missing visitor ID column (need either 'idvisitor_hex' or 'idvisitor_converted')")
        print(f"  [INFO] Available columns: {list(df_score.columns)}")
        question_correctness_df = pd.DataFrame(columns=['game_name','question_number','correctness','percent','user_count','total_users'])
        question_correctness_df.to_csv('data/question_correctness_data.csv', index=False)
        return question_correctness_df
    print(f"  [OK] All required columns present")
    
    # Convert idvisitor_hex to idvisitor_converted if needed (only if hex exists and converted doesn't)
    if 'idvisitor_hex' in df_score.columns and 'idvisitor_converted' not in df_score.columns:
        print(f"  [ACTION] Converting idvisitor_hex to idvisitor_converted...")
        df_score = convert_hex_to_int(df_score, 'idvisitor_hex', 'idvisitor_converted')
        print(f"  [OK] Conversion complete")
    
    # Convert server_time to datetime if it's a string
    if 'server_time' in df_score.columns:
        try:
            print(f"  [ACTION] Converting server_time to datetime...")
            df_score['server_time'] = pd.to_datetime(df_score['server_time'])
            print(f"  [OK] Datetime conversion complete")
        except Exception as e:
            print(f"  [WARNING] Could not convert server_time: {e}")
    
    if df_score.empty:
        print("  [WARNING] No data found")
        question_correctness_df = pd.DataFrame(columns=['game_name','question_number','correctness','percent','user_count','total_users'])
        question_correctness_df.to_csv('data/question_correctness_data.csv', index=False)
        return question_correctness_df
    
    # Print success message
    print(f"  [OK] Successfully loaded {len(df_score):,} records from Redshift database")
    print(f"  [INFO] Unique games in data: {df_score['game_name'].nunique()}")
    
    # Check for optional columns (language and game_code) - same as score distribution
    has_language = 'language' in df_score.columns
    has_game_code = 'game_code' in df_score.columns
    if has_language:
        print(f"  [INFO] Language column found: will be included in output")
        # Transform language column: if contains "mr-IN" then "mr", else "hi" (same as score distribution)
        print(f"  [ACTION] Transforming language column...")
        df_score['language'] = df_score['language'].apply(
            lambda x: 'mr' if pd.notna(x) and 'mr-IN' in str(x) else 'hi'
        )
        print(f"  [OK] Language transformation complete")
    if has_game_code:
        print(f"  [INFO] Game code column found: will be included in output")
    
    print("\nStep 2: Extracting per-question correctness from score data...")
    print("  [INFO] Using same processing method as score distribution for each game...")
    print("  [INFO] This will process each game dynamically based on JSON structure...")
    per_question_df = extract_per_question_correctness(df_score)
    
    if per_question_df.empty:
        print("  [WARNING] No per-question correctness data extracted")
        print("  [WARNING] Check the logs above for processing details")
        question_correctness_df = pd.DataFrame(columns=['game_name','question_number','correctness','percent','user_count','total_users'])
        question_correctness_df.to_csv('data/question_correctness_data.csv', index=False)
        return question_correctness_df
    
    print(f"\n  [OK] Extracted {len(per_question_df):,} per-question records")
    print(f"  [INFO] Games with data: {per_question_df['game_name'].nunique()}")
    print(f"  [INFO] Unique questions: {per_question_df['question_number'].nunique()}")
    print(f"  [INFO] Games: {sorted(per_question_df['game_name'].unique())}")
    
    print("\nStep 3: Aggregating correctness by game and question...")
    
    # Check if language and game_code columns exist
    has_language_in_df = 'language' in per_question_df.columns
    has_game_code_in_df = 'game_code' in per_question_df.columns
    
    if has_language_in_df:
        print("  [INFO] Language column found: will be included in aggregation")
    if has_game_code_in_df:
        print("  [INFO] Game code column found: will be included in aggregation")
    
    print("  [ACTION] Calculating total users per question...")
    # Calculate total users per question (users who attempted the question)
    # Group by game_name, question_number, and optionally language and game_code
    groupby_cols = ['game_name', 'question_number']
    if has_language_in_df:
        groupby_cols.append('language')
    if has_game_code_in_df:
        groupby_cols.append('game_code')
    
    total_by_q = (
        per_question_df
        .groupby(groupby_cols)['idvisitor_converted']
        .nunique()
        .reset_index(name='total_users')
    )
    
    print(f"  [OK] Calculated total users for {len(total_by_q)} combinations")
    
    print("  [ACTION] Calculating correct and incorrect user counts...")
    # Calculate correct and incorrect user counts per question
    # Group by game_name, question_number, is_correct, and optionally language and game_code
    agg_groupby_cols = ['game_name', 'question_number', 'is_correct']
    if has_language_in_df:
        agg_groupby_cols.append('language')
    if has_game_code_in_df:
        agg_groupby_cols.append('game_code')
    
    agg = (
        per_question_df
        .groupby(agg_groupby_cols)['idvisitor_converted']
        .nunique()
        .reset_index(name='user_count')
    )
    
    print(f"  [OK] Calculated user counts for {len(agg)} combinations")
    
    # Merge to get total_users
    print("  [ACTION] Merging total users...")
    merge_on_cols = ['game_name', 'question_number']
    if has_language_in_df:
        merge_on_cols.append('language')
    if has_game_code_in_df:
        merge_on_cols.append('game_code')
    agg = agg.merge(total_by_q, on=merge_on_cols, how='left')
    
    # Calculate percentage
    print("  [ACTION] Calculating percentages...")
    agg['percent'] = (agg['user_count'] / agg['total_users'].where(agg['total_users'] > 0, 1) * 100).round(2)
    
    # Map is_correct to Correct/Incorrect
    agg['correctness'] = agg['is_correct'].map({1: 'Correct', 0: 'Incorrect'})
    
    # Select and order columns
    output_cols = ['game_name', 'question_number', 'correctness', 'percent', 'user_count', 'total_users']
    if has_language_in_df:
        output_cols.append('language')
    if has_game_code_in_df:
        output_cols.append('game_code')
    question_correctness_df = agg[output_cols].copy()
    print(f"  [OK] Aggregation complete")
    
    # Sort by game_name and question_number (and optionally language and game_code)
    print("  [ACTION] Sorting results...")
    sort_cols = ['game_name', 'question_number', 'correctness']
    if has_language_in_df:
        sort_cols.append('language')
    if has_game_code_in_df:
        sort_cols.append('game_code')
    question_correctness_df = question_correctness_df.sort_values(sort_cols)
    print(f"  [OK] Sorting complete")
    
    print("\nStep 4: Saving results to CSV...")
    question_correctness_df.to_csv('data/question_correctness_data.csv', index=False)
    print(f"  [OK] Question correctness data saved to data/question_correctness_data.csv")
    
    print("\n" + "=" * 60)
    print("QUESTION CORRECTNESS PROCESSING COMPLETE")
    print("=" * 60)
    print(f"  Total records: {len(question_correctness_df):,}")
    print(f"  Games: {question_correctness_df['game_name'].nunique()}")
    print(f"  Questions: {question_correctness_df['question_number'].nunique()}")
    print(f"  Correct records: {len(question_correctness_df[question_correctness_df['correctness'] == 'Correct']):,}")
    print(f"  Incorrect records: {len(question_correctness_df[question_correctness_df['correctness'] == 'Incorrect']):,}")
    print(f"  Games processed: {sorted(question_correctness_df['game_name'].unique())}")
    print("=" * 60)
    
    return question_correctness_df


def update_metadata(df_main: Optional[pd.DataFrame] = None):
    """Update metadata JSON file"""
    print("\n" + "=" * 60)
    print("UPDATING: Metadata")
    print("=" * 60)
    
    # Try to load existing metadata or create new one
    metadata_file = 'data/metadata.json'
    metadata = {}
    
    if os.path.exists(metadata_file):
        print("  Loading existing metadata...")
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        sys.stdout.flush()
    
    # Load data files to get current record counts
    record_counts = {}
    
    if df_main is not None:
        print("  Using df_main for record counts...")
        record_counts['main_data_records'] = len(df_main)
        if 'server_time' in df_main.columns:
            record_counts['data_date_range'] = {
                'start': str(df_main['server_time'].min()),
                'end': str(df_main['server_time'].max())
            }
        sys.stdout.flush()
    else:
        if os.path.exists('data/processed_data.csv'):
            print("  Reading processed_data.csv for record count (this may take a moment)...")
            sys.stdout.flush()
            # Just count lines instead of loading full CSV
            try:
                with open('data/processed_data.csv', 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f) - 1  # Subtract header
                record_counts['main_data_records'] = line_count
                print(f"  ✓ Counted {line_count:,} records in processed_data.csv")
            except Exception as e:
                print(f"  WARNING: Could not count records: {e}")
                record_counts['main_data_records'] = 0
            sys.stdout.flush()
    
    # Update record counts for each CSV - use line counting instead of loading
    print("  Counting records in other CSV files...")
    sys.stdout.flush()
    for csv_file, key in [
        ('data/summary_data.csv', 'summary_records'),
        ('data/score_distribution_data.csv', 'score_distribution_records'),
        ('data/time_series_data.csv', 'time_series_records'),
        ('data/repeatability_data.csv', 'repeatability_records'),
        ('data/question_correctness_data.csv', 'question_correctness_records'),
        ('data/poll_responses_data.csv', 'poll_responses_records'),
    ]:
        if os.path.exists(csv_file):
            try:
                # Count lines instead of loading full CSV
                with open(csv_file, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f) - 1  # Subtract header
                record_counts[key] = line_count
                print(f"    ✓ {key}: {line_count:,} records")
            except Exception as e:
                print(f"    ✗ Error counting {key}: {e}")
                record_counts[key] = 0
        else:
            record_counts[key] = 0
        sys.stdout.flush()
    
    # Update metadata
    print("  Updating metadata JSON...")
    sys.stdout.flush()
    metadata.update({
        'preprocessing_date': datetime.now().isoformat(),
        **record_counts
    })
    
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print("  ✓ SUCCESS: Saved data/metadata.json")
    sys.stdout.flush()
        

# Video Viewership Queries
VIDEO_BASE_QUERY = """
SELECT 
  mla.name,
  mla_ref.name AS name1,
  mllva.idvisitor,
  mllva.idvisit,
  mllva.idlink_va,
  mllva.server_time,
  mllva.custom_dimension_1,
  mllva.custom_dimension_2
FROM rl_dwh_prod.live.matomo_log_link_visit_action mllva
INNER JOIN rl_dwh_prod.live.matomo_log_action mla
  ON mllva.idaction_name = mla.idaction
INNER JOIN rl_dwh_prod.live.matomo_log_action mla_ref
  ON mllva.idaction_url_ref = mla_ref.idaction
WHERE (mla.name LIKE '%started' OR mla.name LIKE '%completed')
  AND mllva.server_time >= '2025-11-17'
"""

VIDEO_GAME_MAPPING_QUERY = """
SELECT 
  hg.game_name,
  hgl.activity_id
FROM rl_dwh_prod.live.hybrid_games hg
INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl
  ON hgl.game_id = hg.id
"""


def process_video_viewership() -> pd.DataFrame:
    """Process video viewership metrics by replicating Alteryx workflow
    
    Returns:
        pd.DataFrame: Final video viewership metrics with event counts, video engagement, and watch time analysis
    """
    print("\n" + "=" * 60)
    print("PROCESSING: Video Viewership Metrics")
    print("=" * 60)
    
    if not PSYCOPG2_AVAILABLE:
        print("ERROR: psycopg2 not available. Cannot fetch video viewership data from Redshift.")
        print("  Install with: pip install psycopg2-binary")
        return pd.DataFrame()
    
    # Step 1: Base interactions data
    print("\nStep 1: Fetching base interactions data...")
    import time
    max_retries = 3
    retry_delay = 5
    
    df_base = pd.DataFrame()
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  [ACTION] Connecting to REDSHIFT (Attempt {attempt}/{max_retries})...")
            conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                connect_timeout=60,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '1800000'")
                conn.commit()
            
            print(f"  ✓ Successfully connected to REDSHIFT")
            print(f"  [ACTION] Executing base interactions query...")
            df_base = pd.read_sql(VIDEO_BASE_QUERY, conn)
            print(f"  ✓ Fetched {len(df_base)} records")
            
            conn.close()
            break
            
        except Exception as e:
            if attempt < max_retries:
                print(f"  [WARNING] Error on attempt {attempt}: {str(e)}")
                print(f"  [ACTION] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                print(f"  ERROR: Failed to fetch base interactions data: {str(e)}")
                return pd.DataFrame()
    
    if df_base.empty:
        print("  ERROR: No base interactions data fetched")
        return pd.DataFrame()
    
    # Apply DISTINCT across all selected columns
    print("  [ACTION] Applying DISTINCT...")
    initial_count = len(df_base)
    df_base = df_base.drop_duplicates()
    print(f"  ✓ Removed {initial_count - len(df_base)} duplicate rows")
    
    # Filter out rows where name contains 'mid_intro'
    print("  [ACTION] Filtering out 'mid_intro' rows...")
    before_filter = len(df_base)
    df_base = df_base[~df_base['name'].str.contains('mid_intro', case=False, na=False)]
    print(f"  ✓ Removed {before_filter - len(df_base)} rows containing 'mid_intro'")
    print(f"  ✓ Remaining records: {len(df_base)}")
    
    # Step 2: Standardize language
    print("\nStep 2: Standardizing language...")
    df_base['language'] = df_base['name1'].apply(
        lambda x: 'mr' if pd.notna(x) and 'mr-IN' in str(x) else 'hi'
    )
    print(f"  ✓ Language standardized: {df_base['language'].value_counts().to_dict()}")
    
    # Step 3: Standardize event names
    print("\nStep 3: Standardizing event names...")
    def standardize_event_name(name):
        """Standardize event name based on patterns"""
        if pd.isna(name):
            return name
        name_str = str(name)
        if 'introduction' in name_str:
            return 'Intro'
        elif 'question_completed' in name_str:
            return 'Validation'
        elif 'action_completed' in name_str:
            return 'Questions'
        elif 'reward_completed' in name_str:
            return 'Rewards'
        elif 'game_completed' in name_str:
            return 'Completed'
        return name_str
    
    df_base['name'] = df_base['name'].apply(standardize_event_name)
    
    # Remove rows where name still contains '_'
    before_filter = len(df_base)
    df_base = df_base[~df_base['name'].str.contains('_', na=False)]
    print(f"  ✓ Removed {before_filter - len(df_base)} rows with '_' in name")
    print(f"  ✓ Event names standardized: {df_base['name'].value_counts().to_dict()}")
    
    # Step 4: Game mapping
    print("\nStep 4: Fetching game mapping...")
    df_game_mapping = pd.DataFrame()
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=REDSHIFT_HOST,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                connect_timeout=60,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '1800000'")
                conn.commit()
            
            df_game_mapping = pd.read_sql(VIDEO_GAME_MAPPING_QUERY, conn)
            print(f"  ✓ Fetched {len(df_game_mapping)} game mappings")
            
            conn.close()
            break
            
        except Exception as e:
            if attempt < max_retries:
                print(f"  [WARNING] Error on attempt {attempt}: {str(e)}")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                print(f"  ERROR: Failed to fetch game mapping: {str(e)}")
                return pd.DataFrame()
    
    if df_game_mapping.empty:
        print("  ERROR: No game mapping data fetched")
        return pd.DataFrame()
    
    # Special handling for "Beginning Sounds Pa/Ga/Ra" where activity_id is null
    # Set first null activity_id to 121 and second null activity_id to 122
    print("  [ACTION] Applying special handling for 'Beginning Sounds Pa/Ga/Ra' with null activity_id...")
    df_game_mapping = df_game_mapping.reset_index(drop=True)
    
    # Find rows where game_name is "Beginning Sounds Pa/Ga/Ra" and activity_id is null
    target_game_rows = df_game_mapping[
        (df_game_mapping['game_name'] == 'Beginning Sounds Pa/Ga/Ra') & 
        (df_game_mapping['activity_id'].isna())
    ].copy()
    
    print(f"  [DEBUG] Found {len(target_game_rows)} rows with 'Beginning Sounds Pa/Ga/Ra' and null activity_id")
    
    if len(target_game_rows) >= 1:
        # Get the index of the first row
        first_idx = target_game_rows.index[0]
        print(f"  [DEBUG] Setting first row (index {first_idx}) activity_id to 121")
        df_game_mapping.loc[first_idx, 'activity_id'] = 121
    
    if len(target_game_rows) >= 2:
        # Get the index of the second row
        second_idx = target_game_rows.index[1]
        print(f"  [DEBUG] Setting second row (index {second_idx}) activity_id to 122")
        df_game_mapping.loc[second_idx, 'activity_id'] = 122
    
    if len(target_game_rows) > 2:
        print(f"  [WARNING] Found {len(target_game_rows)} rows with null activity_id for 'Beginning Sounds Pa/Ga/Ra', only updated first 2")
    
    print(f"  ✓ Applied special handling: set activity_id to 121 and 122 for 'Beginning Sounds Pa/Ga/Ra' with null activity_id")
    
    # Debug: Check if there are duplicate activity_ids after modification
    duplicate_activity_ids = df_game_mapping[df_game_mapping.duplicated(subset=['activity_id'], keep=False)]
    if not duplicate_activity_ids.empty:
        print(f"  [WARNING] Found duplicate activity_ids after modification:")
        print(f"    {duplicate_activity_ids[['game_name', 'activity_id']].to_string()}")
    
    # Join with base data
    print("  [ACTION] Joining game mapping with base data...")
    before_join = len(df_base)
    
    # Debug: Check original data types and sample values
    print(f"  [DEBUG] df_base['custom_dimension_2'] dtype: {df_base['custom_dimension_2'].dtype}")
    print(f"  [DEBUG] df_game_mapping['activity_id'] dtype: {df_game_mapping['activity_id'].dtype}")
    print(f"  [DEBUG] Sample custom_dimension_2 values: {df_base['custom_dimension_2'].dropna().head(10).tolist()}")
    print(f"  [DEBUG] Sample activity_id values: {df_game_mapping['activity_id'].head(10).tolist()}")
    print(f"  [DEBUG] Unique custom_dimension_2 count: {df_base['custom_dimension_2'].nunique()}")
    print(f"  [DEBUG] Unique activity_id count: {df_game_mapping['activity_id'].nunique()}")
    
    # Convert both columns to the same type (string) for merging
    # First, convert to numeric if possible, then to string to ensure exact match
    # This handles cases where one might be "121" and the other 121.0
    df_base['custom_dimension_2'] = pd.to_numeric(df_base['custom_dimension_2'], errors='coerce')
    df_base = df_base[df_base['custom_dimension_2'].notna()]  # Remove NaN after conversion
    df_base['custom_dimension_2'] = df_base['custom_dimension_2'].astype(int).astype(str)
    
    df_game_mapping['activity_id'] = pd.to_numeric(df_game_mapping['activity_id'], errors='coerce')
    df_game_mapping = df_game_mapping[df_game_mapping['activity_id'].notna()]  # Remove NaN after conversion
    df_game_mapping['activity_id'] = df_game_mapping['activity_id'].astype(int).astype(str)
    
    # Debug: Check converted values
    print(f"  [DEBUG] After conversion - Sample custom_dimension_2: {df_base['custom_dimension_2'].head(10).tolist()}")
    print(f"  [DEBUG] After conversion - Sample activity_id: {df_game_mapping['activity_id'].head(10).tolist()}")
    
    # Check for overlapping values
    base_values = set(df_base['custom_dimension_2'].unique())
    mapping_values = set(df_game_mapping['activity_id'].unique())
    overlap = base_values.intersection(mapping_values)
    print(f"  [DEBUG] Overlapping values count: {len(overlap)}")
    if len(overlap) > 0:
        print(f"  [DEBUG] Sample overlapping values: {list(overlap)[:10]}")
    else:
        print(f"  [WARNING] No overlapping values found!")
        print(f"  [DEBUG] Sample base values: {list(base_values)[:10]}")
        print(f"  [DEBUG] Sample mapping values: {list(mapping_values)[:10]}")
    
    df_base = df_base.merge(
        df_game_mapping,
        left_on='custom_dimension_2',
        right_on='activity_id',
        how='inner'
    )
    print(f"  ✓ Joined: {before_join} -> {len(df_base)} records")
    
    # Metric Block A (Point 1): Event counts
    print("\nMetric Block A: Calculating event counts...")
    # Deduplicate on idlink_va
    df_dedup = df_base.drop_duplicates(subset=['idlink_va'])
    print(f"  ✓ Deduplicated: {len(df_base)} -> {len(df_dedup)} records")
    
    # Pivot (cross-tab): Rows (game_name, language), Columns (name), Values (COUNT(idlink_va))
    print("  [ACTION] Creating pivot table...")
    metric_a = df_dedup.groupby(['game_name', 'language', 'name']).size().reset_index(name='count')
    metric_a_pivot = metric_a.pivot_table(
        index=['game_name', 'language'],
        columns='name',
        values='count',
        fill_value=0
    ).reset_index()
    print(f"  ✓ Pivot created: {len(metric_a_pivot)} rows")
    print(f"  ✓ Columns: {list(metric_a_pivot.columns)}")
    
    # Metric Block B (Point 2): Video engagement
    print("\nMetric Block B: Calculating video engagement...")
    import json
    
    def parse_json_for_video(custom_dim_1):
        """Check if custom_dimension_1 contains 'video' in JSON"""
        if pd.isna(custom_dim_1) or custom_dim_1 is None:
            return False
        try:
            data = json.loads(custom_dim_1)
            # Check if any value in the JSON contains 'video'
            json_str = json.dumps(data).lower()
            return 'video' in json_str
        except:
            return False
    
    df_video = df_base[df_base['custom_dimension_1'].apply(parse_json_for_video)].copy()
    print(f"  ✓ Filtered to {len(df_video)} records with 'video' in JSON")
    
    if not df_video.empty:
        metric_b = df_video.groupby(['game_name', 'language'])['idlink_va'].nunique().reset_index(name='Video Started')
        print(f"  ✓ Video engagement calculated: {len(metric_b)} rows")
    else:
        metric_b = pd.DataFrame(columns=['game_name', 'language', 'Video Started'])
        print(f"  ⚠ No video engagement data found")
    
    # Point 3: Join Metric Block A and B
    print("\nPoint 3: Joining Metric Block A and B...")
    print(f"  [DEBUG] Metric A games: {sorted(metric_a_pivot['game_name'].unique().tolist())}")
    print(f"  [DEBUG] Metric B games: {sorted(metric_b['game_name'].unique().tolist()) if not metric_b.empty else 'N/A'}")
    point_3 = metric_a_pivot.merge(metric_b, on=['game_name', 'language'], how='left')
    point_3['Video Started'] = point_3['Video Started'].fillna(0).astype(int)
    print(f"  ✓ Joined on game_name and language: {len(point_3)} rows")
    print(f"  [DEBUG] Point 3 games: {sorted(point_3['game_name'].unique().tolist())}")
    
    # Metric Block C (Point 4): Watch time analysis
    print("\nMetric Block C: Calculating watch time analysis...")
    
    def extract_watch_time(custom_dim_1):
        """Extract watch time value from JSON where key contains 'spentwatching'"""
        if pd.isna(custom_dim_1) or custom_dim_1 is None:
            return None
        try:
            data = json.loads(custom_dim_1)
            json_str = json.dumps(data).lower()
            if 'spentwatching' in json_str:
                # Recursively search for the value
                def find_value(obj, target_key):
                    """Recursively find value for key containing target_key"""
                    if isinstance(obj, dict):
                        for key, value in obj.items():
                            if target_key.lower() in str(key).lower():
                                if isinstance(value, (int, float)):
                                    return float(value)
                                elif isinstance(value, str):
                                    try:
                                        return float(value)
                                    except:
                                        pass
                            if isinstance(value, (dict, list)):
                                result = find_value(value, target_key)
                                if result is not None:
                                    return result
                    elif isinstance(obj, list):
                        for item in obj:
                            result = find_value(item, target_key)
                            if result is not None:
                                return result
                    return None
                
                value = find_value(data, 'spentwatching')
                if value is not None:
                    return float(value)
                
                # Fallback: use regex to find numeric value after spentwatching
                import re
                matches = re.findall(r'spentwatching["\']?\s*:\s*(\d+\.?\d*)', json_str, re.IGNORECASE)
                if matches:
                    return float(matches[0])
            return None
        except:
            return None
    
    df_watch_time = df_base.copy()
    df_watch_time['watch_time_value'] = df_watch_time['custom_dimension_1'].apply(extract_watch_time)
    df_watch_time = df_watch_time[df_watch_time['watch_time_value'].notna()].copy()
    print(f"  ✓ Filtered to {len(df_watch_time)} records with watch time data")
    
    # Debug: Check which games have watch time data
    if not df_watch_time.empty:
        print(f"  [DEBUG] Games with watch time data: {sorted(df_watch_time['game_name'].unique().tolist())}")
        print(f"  [DEBUG] Languages with watch time data: {sorted(df_watch_time['language'].unique().tolist())}")
        
        # Check specifically for "Beginning Sounds Pa/Ga/Ra"
        target_game = 'Beginning Sounds Pa/Ga/Ra'
        if target_game in df_watch_time['game_name'].values:
            print(f"  [DEBUG] Found '{target_game}' in watch time data: {len(df_watch_time[df_watch_time['game_name'] == target_game])} records")
        else:
            print(f"  [WARNING] '{target_game}' NOT found in watch time data!")
            # Check for similar names
            similar_games = [g for g in df_watch_time['game_name'].unique() if 'Pa' in str(g) or 'Ga' in str(g) or 'Ra' in str(g)]
            if similar_games:
                print(f"  [DEBUG] Similar game names found: {similar_games}")
    
    if not df_watch_time.empty:
        # Average watch time - group by game_name and language
        metric_c_avg = df_watch_time.groupby(['game_name', 'language'])['watch_time_value'].mean().reset_index(name='Average')
        print(f"  [DEBUG] Metric C Avg: {len(metric_c_avg)} rows, games: {sorted(metric_c_avg['game_name'].unique().tolist())}")
        
        # Count distinct idlink_va where value <= 10 (min) - group by game_name and language
        metric_c_min = df_watch_time[df_watch_time['watch_time_value'] <= 10].groupby(['game_name', 'language'])['idlink_va'].nunique().reset_index(name='Min')
        print(f"  [DEBUG] Metric C Min: {len(metric_c_min)} rows, games: {sorted(metric_c_min['game_name'].unique().tolist())}")
        
        # Count distinct idlink_va where value >= 200 (max) - group by game_name and language
        metric_c_max = df_watch_time[df_watch_time['watch_time_value'] >= 200].groupby(['game_name', 'language'])['idlink_va'].nunique().reset_index(name='Max')
        print(f"  [DEBUG] Metric C Max: {len(metric_c_max)} rows, games: {sorted(metric_c_max['game_name'].unique().tolist())}")
        
        # Join all three metrics on game_name and language
        print(f"  [ACTION] Joining Metric C components on game_name and language...")
        metric_c = metric_c_avg.merge(metric_c_min, on=['game_name', 'language'], how='left')
        metric_c = metric_c.merge(metric_c_max, on=['game_name', 'language'], how='left')
        metric_c['Min'] = metric_c['Min'].fillna(0).astype(int)
        metric_c['Max'] = metric_c['Max'].fillna(0).astype(int)
        print(f"  ✓ Watch time analysis calculated: {len(metric_c)} rows")
        print(f"  [DEBUG] Final Metric C games: {sorted(metric_c['game_name'].unique().tolist())}")
        print(f"  [DEBUG] Final Metric C languages: {sorted(metric_c['language'].unique().tolist())}")
    else:
        metric_c = pd.DataFrame(columns=['game_name', 'language', 'Average', 'Min', 'Max'])
        print(f"  ⚠ No watch time data found")
    
    # Final Output: Join Point 3 and Point 4
    # Use INNER join so we only get games that are in both Point 3 and Metric C
    # This naturally filters to only games with watch time data
    print("\nFinal Output: Joining Point 3 and Point 4...")
    print(f"  [DEBUG] Games in Point 3: {len(point_3)} rows, unique games: {sorted(point_3['game_name'].unique().tolist())}")
    print(f"  [DEBUG] Games in Metric C: {len(metric_c)} rows, unique games: {sorted(metric_c['game_name'].unique().tolist()) if not metric_c.empty else 'N/A'}")
    final_output = point_3.merge(metric_c, on=['game_name', 'language'], how='inner')
    final_output['Average'] = final_output['Average'].fillna(0)
    final_output['Min'] = final_output['Min'].fillna(0).astype(int)
    final_output['Max'] = final_output['Max'].fillna(0).astype(int)
    
    # Ensure all expected columns exist (fill missing event columns with 0)
    expected_event_cols = ['Intro', 'Validation', 'Questions', 'Rewards', 'Completed']
    for col in expected_event_cols:
        if col not in final_output.columns:
            final_output[col] = 0
    
    # Rename columns to match expected format (Started, Questions, Rewards)
    # Based on the existing CSV, it seems 'Intro' should be 'Started'
    if 'Intro' in final_output.columns:
        final_output = final_output.rename(columns={'Intro': 'Started'})
    
    # The INNER join should naturally filter to only games in Metric C
    # Filter to only 'hi' language (as per expected output)
    print(f"\nFiltering to 'hi' language only (INNER join already filtered games)...")
    before_lang_filter = len(final_output)
    final_output = final_output[final_output['language'] == 'hi']
    print(f"  ✓ Language filter: {before_lang_filter} -> {len(final_output)} rows")
    print(f"  ✓ Games in final output: {sorted(final_output['game_name'].unique().tolist())}")
    
    # Select only required columns: game_name, language, Started, Questions, Rewards, Video Started, Average, Min, Max
    required_cols = ['game_name', 'language', 'Started', 'Questions', 'Rewards', 'Video Started', 'Average', 'Min', 'Max']
    # Check which columns exist
    available_cols = [col for col in required_cols if col in final_output.columns]
    final_output = final_output[available_cols]
    
    # Rename 'language' to 'lanuagae' (typo as per expected output)
    if 'language' in final_output.columns:
        final_output = final_output.rename(columns={'language': 'lanuagae'})
    
    # Sort by game_name
    final_output = final_output.sort_values(['game_name']).reset_index(drop=True)
    
    print(f"  ✓ Final output: {len(final_output)} rows")
    print(f"  ✓ Columns: {list(final_output.columns)}")
    print(f"  ✓ Games: {final_output['game_name'].unique().tolist()}")
    
    # Save to CSV
    output_path = 'data/video_viewership_data.csv'
    print(f"\nSaving video viewership data to {output_path}...")
    final_output.to_csv(output_path, index=False)
    print(f"✓ SUCCESS: Saved {output_path} ({len(final_output)} records)")
    
    return final_output


def main():
    """Main preprocessing function with modular processing options"""
    print("=" * 60)
    print("STARTING PREPROCESSING SCRIPT")
    print("=" * 60)
    sys.stdout.flush()
    
    parser = argparse.ArgumentParser(
        description='Preprocess data for Matomo Events Dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all visuals (default behavior)
  python preprocess_data.py --all

  # Process only score distribution
  python preprocess_data.py --score-distribution

  # Process only question correctness
  python preprocess_data.py --question-correctness

  # Process only parent poll responses
  python preprocess_data.py --parent-poll

  # Process multiple visuals
  python preprocess_data.py --time-series --repeatability

Available visuals:
  --main               Main dashboard data and game conversion numbers
  --summary            Summary statistics
  --score-distribution Score distribution
  --time-series        Time series data
  --repeatability      Repeatability data
  --question-correctness Question correctness data
  --parent-poll        Parent poll responses data
  --video-viewership   Video viewership metrics
  --all                Process all visuals (default if no flags provided)
  --metadata           Update metadata file
        """
    )
    
    parser.add_argument('--main', action='store_true', help='Process main dashboard data')
    parser.add_argument('--summary', action='store_true', help='Process summary statistics')
    parser.add_argument('--score-distribution', action='store_true', help='Process score distribution')
    parser.add_argument('--time-series', action='store_true', help='Process time series data')
    parser.add_argument('--repeatability', action='store_true', help='Process repeatability data')
    parser.add_argument('--question-correctness', action='store_true', help='Process question correctness data')
    parser.add_argument('--parent-poll', action='store_true', help='Process parent poll responses data')
    parser.add_argument('--video-viewership', action='store_true', help='Process video viewership metrics')
    parser.add_argument('--all', action='store_true', help='Process all visuals (default)')
    parser.add_argument('--metadata', action='store_true', help='Update metadata file')
    parser.add_argument('--use-database', action='store_true', help='Fetch score data directly from Redshift instead of using scores_data.csv')
    
    args = parser.parse_args()
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Determine what to process
    process_all = args.all or not any([
        args.main, args.summary, args.score_distribution,
        args.time_series, args.repeatability, args.question_correctness, args.parent_poll, args.video_viewership
    ])
    
    if process_all:
        print("=" * 60)
        print("PROCESSING ALL VISUALS")
        print("=" * 60)
        print("\nNote: Use specific flags to process individual visuals:")
        print("  python preprocess_data.py --question-correctness")
        print("  python preprocess_data.py --score-distribution --time-series")
        print("  See --help for more options.\n")
    
    try:
        df_main = None
        
        # Process main data if requested or if processing all
        if args.main or process_all:
            df_main = process_main_data()
        
        # Process summary if requested or if processing all
        if args.summary or process_all:
            process_summary_data(df_main)
        
        # Process score distribution if requested or if processing all
        if args.score_distribution or process_all:
            process_score_distribution(use_database=args.use_database)
        
        # Process time series if requested or if processing all
        if args.time_series or process_all:
            process_time_series(df_main)
        
        # Process repeatability if requested or if processing all
        if args.repeatability or process_all:
            process_repeatability(df_main)
        
        # Process question correctness if requested or if processing all
        if args.question_correctness or process_all:
            process_question_correctness(use_database=args.use_database)
        
        # Process parent poll if requested or if processing all
        if args.parent_poll or process_all:
            process_parent_poll()
        
        # Process video viewership if requested or if processing all
        if args.video_viewership or process_all:
            process_video_viewership()
        
        # Update metadata if requested or if processing all
        if args.metadata or process_all:
            print("\n[FINAL STEP] Updating metadata...")
            sys.stdout.flush()
            update_metadata(df_main)
        
        print("\n" + "=" * 60)
        print("PREPROCESSING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("All processed data saved to 'data/' directory")
        print("Ready for deployment to Render!")
        print("\nNext steps:")
        print("1. Commit and push the updated data/ directory to GitHub")
        print("2. Render will automatically redeploy with the latest data")
        sys.stdout.flush()
        print("\n✓ Script execution finished. Exiting...")
        sys.stdout.flush()
        
        # Explicitly exit to ensure script terminates
        sys.exit(0)
        
    except Exception as e:
        print(f"\nERROR during preprocessing: {str(e)}")
        import traceback
        traceback.print_exc()
        print("Please check your database connection and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
