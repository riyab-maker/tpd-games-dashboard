# Query Comparison: Hybrid Dashboard vs TPD Games Dashboard

## Key Differences

### 1. Pattern Matching
- **Hybrid Dashboard**: Uses `'%hybrid_game_started%'` and `'%hybrid_mcq_started%'`
- **TPD Dashboard**: Uses `'%game_started%'` and `'%mcq_started%'` (simpler, without 'hybrid_' prefix)

### 2. Activity ID Filtering
- **Hybrid Dashboard**: Uses CAST to INTEGER: `CAST(mllva.custom_dimension_2 AS INTEGER) IN (149, 150, 160, 166)`
- **TPD Dashboard**: Uses string comparison: `custom_dimension_2 IN ('149','150','160','166')`

### 3. Date Filters
- **Hybrid Dashboard Time Series**: `DATEADD(minute, 330, mllva.server_time) >= '2025-07-02'`
- **Hybrid Dashboard Repeatability**: `mllva.server_time >= '2025-07-01'`
- **TPD Dashboard Both**: `DATEADD(minute, 330, mllva.server_time) >= '2025-01-03'`

### 4. Language Join
- **Hybrid Dashboard Repeatability**: Uses `INNER JOIN` for language (excludes NULL idaction_url_ref)
- **TPD Dashboard Repeatability**: Uses `LEFT JOIN` for language (includes NULL idaction_url_ref)

### 5. Game Mappings
- **Hybrid Dashboard**: 
  - Uses `hybrid_games` and `hybrid_games_links` tables
  - Includes normalization logic for renamed games
  - Includes unmapped game mappings (Primary Emotion Labelling, Patterns, etc.)
- **TPD Dashboard**: 
  - No game tables used
  - Direct CASE statement mapping for 4 activity IDs
  - No normalization or unmapped game mappings

### 6. Query Structure
- **Hybrid Dashboard Repeatability**: 
  - Complex CTE structure with `raw_data`, `normalized_activity_ids`, and `game_activity_mappings`
  - Joins normalized activity IDs with game mappings
- **TPD Dashboard Repeatability**: 
  - Simple direct query
  - Direct CASE statement for game names

## Files Created

1. **TPD_TIME_SERIES_QUERY.sql** - TPD Games Dashboard time series query
2. **TPD_REPEATABILITY_QUERY.sql** - TPD Games Dashboard repeatability query
3. **HYBRID_DASHBOARD_REPEATABILITY_QUERY.sql** - Current Hybrid Dashboard repeatability query (for reference)
