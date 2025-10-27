#!/usr/bin/env python3
"""
Debug script to understand the conversion metrics calculation
"""

import pandas as pd
import pymysql
from dotenv import load_dotenv
import os

load_dotenv()

# Database configuration
HOST = os.getenv('DB_HOST')
PORT = int(os.getenv('DB_PORT', 3306))
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
DATABASE = os.getenv('DB_NAME')

def _distinct_count_ignore_blank(series):
    """Equivalent to DISTINCTCOUNTNOBLANK in Power BI"""
    return series.dropna().nunique()

def debug_conversion_metrics():
    """Debug the conversion metrics calculation"""
    print("Fetching raw data for debugging...")
    
    sql_query = """
    SELECT 
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
      `matomo_log_link_visit_action`.`idlink_va`,
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
        print(f"Fetched {len(df)} records from database")
        
        # 1. Total metrics (across all games)
        print("\n=== TOTAL METRICS (All Games Combined) ===")
        total_grouped = df.groupby('event').agg({
            'idvisitor_converted': _distinct_count_ignore_blank,
            'idvisit': _distinct_count_ignore_blank, 
            'idlink_va': _distinct_count_ignore_blank,
        })
        print("Total unique counts:")
        print(total_grouped)
        
        # 2. Game-specific metrics
        print("\n=== GAME-SPECIFIC METRICS ===")
        valid_games = df[~df['game_name'].isin(['Unknown Game', '0', 0]) & df['game_name'].notna()]['game_name'].unique()
        print(f"Valid games: {list(valid_games)}")
        
        game_totals = []
        for game in valid_games:
            game_data = df[df['game_name'] == game]
            game_grouped = game_data.groupby('event').agg({
                'idvisitor_converted': _distinct_count_ignore_blank,
                'idvisit': _distinct_count_ignore_blank, 
                'idlink_va': _distinct_count_ignore_blank,
            })
            print(f"\n{game}:")
            print(game_grouped)
            game_totals.append(game_grouped)
        
        # 3. Sum of game-specific metrics
        print("\n=== SUM OF GAME-SPECIFIC METRICS ===")
        if game_totals:
            combined_game_totals = pd.concat(game_totals).groupby('event').sum()
            print("Sum of all game-specific counts:")
            print(combined_game_totals)
        
        # 4. Check for user overlap between games
        print("\n=== USER OVERLAP ANALYSIS ===")
        started_users_by_game = {}
        completed_users_by_game = {}
        
        for game in valid_games:
            game_data = df[df['game_name'] == game]
            started_users = set(game_data[game_data['event'] == 'Started']['idvisitor_converted'].dropna())
            completed_users = set(game_data[game_data['event'] == 'Completed']['idvisitor_converted'].dropna())
            started_users_by_game[game] = started_users
            completed_users_by_game[game] = completed_users
            print(f"{game}: {len(started_users)} started users, {len(completed_users)} completed users")
        
        # Check overlaps
        all_started_users = set()
        all_completed_users = set()
        
        for game, users in started_users_by_game.items():
            all_started_users.update(users)
        for game, users in completed_users_by_game.items():
            all_completed_users.update(users)
        
        print(f"\nTotal unique started users across all games: {len(all_started_users)}")
        print(f"Total unique completed users across all games: {len(all_completed_users)}")
        
        # Check if this matches our total metrics
        total_started = total_grouped.loc['Started', 'idvisitor_converted']
        total_completed = total_grouped.loc['Completed', 'idvisitor_converted']
        print(f"Total metrics started users: {total_started}")
        print(f"Total metrics completed users: {total_completed}")
        
        if len(all_started_users) == total_started:
            print("✅ Total metrics match unique users across all games")
        else:
            print("❌ Total metrics do NOT match unique users across all games")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_conversion_metrics()
