#!/usr/bin/env python3
"""
Transform existing time series data to new structure
"""
import pandas as pd
import os

def transform_time_series_data():
    """Transform the existing time series data to new structure"""
    print("🚀 Transforming time series data...")
    
    try:
        # Read existing data
        print("Reading existing time series data...")
        df = pd.read_csv('data/time_series_data.csv')
        print(f"✅ Read {len(df)} records")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Sample data:\n{df.head()}")
        
        # Create new structure
        print("Creating new data structure...")
        
        # The new structure should have:
        # - time_period, period_type, game_name (existing)
        # - visits, users, instances (new columns)
        # - completed_visits, completed_users, completed_instances (for compatibility)
        # - started_visits, started_users, started_instances (set to 0)
        
        new_df = df.copy()
        
        # Map columns to new structure
        # For the new logic, we want to show completed events as the main metrics
        new_df['visits'] = new_df['completed_visits']
        new_df['users'] = new_df['completed_users'] 
        new_df['instances'] = new_df['completed_instances']
        
        # Add completed columns for compatibility
        new_df['completed_visits'] = new_df['visits']
        new_df['completed_users'] = new_df['users']
        new_df['completed_instances'] = new_df['instances']
        
        # Add started columns (set to 0 since we're only tracking completed events)
        new_df['started_visits'] = 0
        new_df['started_users'] = 0
        new_df['started_instances'] = 0
        
        # Reorder columns
        column_order = [
            'time_period', 'period_type', 'game_name',
            'visits', 'users', 'instances',
            'started_visits', 'started_users', 'started_instances',
            'completed_visits', 'completed_users', 'completed_instances'
        ]
        
        new_df = new_df[column_order]
        
        # Save transformed data
        print("Saving transformed data...")
        new_df.to_csv('data/time_series_data.csv', index=False)
        print("✅ Saved transformed data/time_series_data.csv")
        
        # Show sample data
        print("\nSample transformed data:")
        print(new_df.head(10))
        print(f"\nNew columns: {new_df.columns.tolist()}")
        print(f"Total records: {len(new_df)}")
        
        # Show some statistics
        print(f"\nStatistics:")
        print(f"Total visits: {new_df['visits'].sum()}")
        print(f"Total users: {new_df['users'].sum()}")
        print(f"Total instances: {new_df['instances'].sum()}")
        print(f"Unique games: {new_df['game_name'].nunique()}")
        
        print("✅ Transformation completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    transform_time_series_data()
