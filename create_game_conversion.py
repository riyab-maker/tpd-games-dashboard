#!/usr/bin/env python3
"""
Create game conversion data with all 25 games
"""
import pandas as pd

# Read the time series data to get all games
ts_df = pd.read_csv('data/time_series_data.csv')
games = ts_df['game_name'].unique()

# Create game conversion data for all games
game_conversion_data = []
for game in games:
    game_data = ts_df[ts_df['game_name'] == game]
    
    # Calculate totals
    total_visits = game_data['visits'].sum()
    total_users = game_data['users'].sum()
    total_instances = game_data['instances'].sum()
    
    game_conversion_data.append({
        'game_name': game,
        'started_users': 0,  # Not tracking started events in new structure
        'completed_users': total_users,
        'started_visits': 0,  # Not tracking started events in new structure
        'completed_visits': total_visits,
        'started_instances': 0,  # Not tracking started events in new structure
        'completed_instances': total_instances
    })

# Create DataFrame and save
df = pd.DataFrame(game_conversion_data)
df.to_csv('data/game_conversion_numbers.csv', index=False)

print(f"Created game conversion data with {len(df)} games")
print("Sample data:")
print(df.head(10))
