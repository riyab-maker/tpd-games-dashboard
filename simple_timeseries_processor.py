#!/usr/bin/env python3
"""
Simple Time Series Data Processor (No Emojis for Windows Compatibility)
"""

import os
import pandas as pd
import pymysql
from datetime import datetime
from dotenv import load_dotenv
import sys

# Load environment variables
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

def fetch_timeseries_data() -> pd.DataFrame:
    """Fetch time series data from database"""
    print("Fetching time series data...")
    
    # SQL Query for time series data
    sql_query = """
    SELECT 
      `matomo_log_link_visit_action`.`idlink_va`,
      CONV(HEX(`matomo_log_link_visit_action`.`idvisitor`), 16, 10) AS idvisitor_converted,
      `matomo_log_link_visit_action`.`idvisit`,
      DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) AS server_time,
      `matomo_log_link_visit_action`.`idaction_name`,
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
    AND DATE_ADD(`matomo_log_link_visit_action`.`server_time`, INTERVAL 330 MINUTE) >= '2025-07-02'
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
        print(f"SUCCESS: Fetched {len(df)} records from database")
        return df
        
    except Exception as e:
        print(f"ERROR: Error fetching data: {e}")
        sys.exit(1)

def process_timeseries_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Process time series metrics for all games combined"""
    print("Processing time series metrics...")
    
    # Convert server_time to datetime
    df['server_time'] = pd.to_datetime(df['server_time'])
    
    # Create time period columns
    df['day'] = df['server_time'].dt.date
    df['week'] = df['server_time'].dt.to_period('W').dt.start_time.dt.date
    df['month'] = df['server_time'].dt.to_period('M').dt.start_time.dt.date
    
    all_metrics = []
    
    # Process each time period
    for period_type in ['day', 'week', 'month']:
        print(f"  Processing {period_type} data...")
        
        # Group by time period and event
        grouped = df.groupby([period_type, 'event']).agg({
            'idvisitor_converted': _distinct_count_ignore_blank,
            'idvisit': _distinct_count_ignore_blank,
            'idlink_va': _distinct_count_ignore_blank,
        }).reset_index()
        
        # Pivot to get Started/Completed as columns
        pivot = grouped.pivot(index=period_type, columns='event', values=['idvisitor_converted', 'idvisit', 'idlink_va'])
        pivot.columns = [f"{col[1]}_{col[0]}" for col in pivot.columns]
        pivot = pivot.reset_index()
        
        # Rename columns to match expected format
        column_mapping = {
            'Started_idvisitor_converted': 'started_users',
            'Completed_idvisitor_converted': 'completed_users',
            'Started_idvisit': 'started_visits',
            'Completed_idvisit': 'completed_visits',
            'Started_idlink_va': 'started_instances',
            'Completed_idlink_va': 'completed_instances',
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in pivot.columns:
                pivot[new_col] = pivot[old_col].fillna(0).astype(int)
            else:
                pivot[new_col] = 0
        
        # Add time period type and format
        pivot['period_type'] = period_type.title()
        pivot['time_period'] = pivot[period_type].astype(str)
        
        # Select final columns
        final_cols = ['period_type', 'time_period', 'started_users', 'completed_users', 
                     'started_visits', 'completed_visits', 'started_instances', 'completed_instances']
        pivot = pivot[final_cols]
        
        all_metrics.append(pivot)
    
    # Combine all time periods
    combined_df = pd.concat(all_metrics, ignore_index=True)
    
    # Sort by period type and time
    combined_df = combined_df.sort_values(['period_type', 'time_period'])
    
    print(f"SUCCESS: Time series metrics: {len(combined_df)} records")
    return combined_df

def save_processed_data(total_metrics: pd.DataFrame):
    """Save processed data to CSV files"""
    print("Saving processed data...")
    
    # Ensure processed_data directory exists
    os.makedirs('processed_data', exist_ok=True)
    
    # Save total metrics
    total_file = 'processed_data/timeseries_total.csv'
    total_metrics.to_csv(total_file, index=False)
    print(f"SUCCESS: Saved total time series to {total_file}")
    
    # Print sample data for verification
    print("\nSample Time Series Data (Month view):")
    month_data = total_metrics[total_metrics['period_type'] == 'Month'].head(5)
    print(month_data.to_string(index=False))

def main():
    """Main processing function"""
    print("Starting Time Series Data Processing...")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Fetch raw data
        df = fetch_timeseries_data()
        
        # Process total metrics
        total_metrics = process_timeseries_metrics(df)
        
        # Save processed data
        save_processed_data(total_metrics)
        
        print(f"\nTime series processing completed successfully!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"ERROR: Error in time series processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
