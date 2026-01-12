-- Hybrid Dashboard - Repeatability Query (Current Version)
-- This is the query currently used in the Hybrid Dashboard
-- Date: server_time >= '2025-07-01'
-- Uses '%hybrid_game_completed%' and '%hybrid_mcq_completed%' patterns
-- Includes normalization logic and game_activity_mappings with hybrid_games tables

WITH raw_data AS (
  -- Get raw data with language information
  SELECT 
    mllva.custom_dimension_2 AS original_activity_id,
    TO_HEX(mllva.idvisitor) AS idvisitor_hex,
    matomo_log_action1.name AS language
  FROM rl_dwh_prod.live.matomo_log_link_visit_action mllva
  INNER JOIN rl_dwh_prod.live.matomo_log_action mla ON mllva.idaction_name = mla.idaction
  INNER JOIN rl_dwh_prod.live.matomo_log_action matomo_log_action1 ON mllva.idaction_url_ref = matomo_log_action1.idaction
  WHERE (mla.name LIKE '%hybrid_game_completed%'
         OR mla.name LIKE '%hybrid_mcq_completed%')
    AND mllva.server_time >= '2025-07-01'
    AND mllva.custom_dimension_2 IS NOT NULL 
    AND mllva.custom_dimension_2 != ''
),
normalized_activity_ids AS (
  -- Normalize activity_id based on original activity_id and language
  -- Cast original_activity_id to INTEGER for consistent type handling
  SELECT 
    idvisitor_hex,
    language,
    CASE 
      -- Patterns AB: set to 123 for both hi and mr
      WHEN CAST(original_activity_id AS INTEGER) IN (
        SELECT DISTINCT hgl.activity_id 
        FROM rl_dwh_prod.live.hybrid_games hg
        INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl ON hg.id = hgl.game_id
        WHERE hg.game_name = 'Patterns AB'
      ) THEN 123
      -- Patterns ABC, AAB: set to 125 for both hi and mr
      WHEN CAST(original_activity_id AS INTEGER) IN (
        SELECT DISTINCT hgl.activity_id 
        FROM rl_dwh_prod.live.hybrid_games hg
        INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl ON hg.id = hgl.game_id
        WHERE hg.game_name = 'Patterns ABC, AAB'
      ) THEN 125
      -- Beginning Sounds Pa/Cha/Sa: hi -> 121, mr -> 122
      WHEN CAST(original_activity_id AS INTEGER) IN (
        SELECT DISTINCT hgl.activity_id 
        FROM rl_dwh_prod.live.hybrid_games hg
        INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl ON hg.id = hgl.game_id
        WHERE hg.game_name = 'Beginning Sound Pa Cha Sa'
      ) AND language = 'hi' THEN 121
      WHEN CAST(original_activity_id AS INTEGER) IN (
        SELECT DISTINCT hgl.activity_id 
        FROM rl_dwh_prod.live.hybrid_games hg
        INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl ON hg.id = hgl.game_id
        WHERE hg.game_name = 'Beginning Sound Pa Cha Sa'
      ) AND language = 'mr' THEN 122
      -- Keep original activity_id for all other cases (cast to INTEGER)
      ELSE CAST(original_activity_id AS INTEGER)
    END AS normalized_activity_id
  FROM raw_data
),
game_activity_mappings AS (
  -- Existing mappings from hybrid_games and hybrid_games_links
  -- Exclude Primary Emotion Labelling games and additional unmapped games from main query
  SELECT DISTINCT
    hg.game_name,
    hg.game_code,
    hgl.activity_id
  FROM rl_dwh_prod.live.hybrid_games hg
  INNER JOIN rl_dwh_prod.live.hybrid_games_links hgl ON hg.id = hgl.game_id
  WHERE hg.id NOT IN (37, 38, 39, 40, 43, 44, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66)  -- Exclude Primary Emotion Labelling games and additional unmapped games
  
  UNION ALL
  
  -- Missing game-activity mappings (Primary Emotion Labelling games - only in unmapped section)
  SELECT 
    'Primary Emotion Labelling : Happy' AS game_name,
    'HY-37-SEL-08' AS game_code,
    140 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Sad' AS game_name,
    'HY-38-SEL-08' AS game_code,
    130 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Anger' AS game_name,
    'HY-39-SEL-08' AS game_code,
    131 AS activity_id
  UNION ALL
  SELECT 
    'Primary Emotion Labelling : Fear' AS game_name,
    'HY-40-SEL-08' AS game_code,
    132 AS activity_id
  UNION ALL
  
  -- Missing game-activity mappings (Additional unmapped games)
  SELECT 
    'Rhyming Words II' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 43 LIMIT 1) AS game_code,
    138 AS activity_id
  UNION ALL
  SELECT 
    'Rhyming Words III' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 44 LIMIT 1) AS game_code,
    139 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds Ka/Ma/La' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 50 LIMIT 1) AS game_code,
    141 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds Pa/Cha/Sa' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 51 LIMIT 1) AS game_code,
    142 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds Ba/Ra/Na' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 52 LIMIT 1) AS game_code,
    143 AS activity_id
  UNION ALL
  SELECT 
    'Rhyming Words 1' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 53 LIMIT 1) AS game_code,
    136 AS activity_id
  UNION ALL
  SELECT 
    'Akshar ka' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 54 LIMIT 1) AS game_code,
    145 AS activity_id
  UNION ALL
  SELECT 
    'Akshar ma' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 55 LIMIT 1) AS game_code,
    146 AS activity_id
  UNION ALL
  SELECT 
    'Akshar cha' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 56 LIMIT 1) AS game_code,
    147 AS activity_id
  UNION ALL
  SELECT 
    'Akshar ba' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 57 LIMIT 1) AS game_code,
    148 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds Ta/Va/Ga' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 58 LIMIT 1) AS game_code,
    151 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds Ma/Ka/Cha' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 59 LIMIT 1) AS game_code,
    152 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds Ba/Pa/Sa' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 60 LIMIT 1) AS game_code,
    153 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds La/Ta/Va' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 61 LIMIT 1) AS game_code,
    154 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sounds Ra/Na/Ga' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 62 LIMIT 1) AS game_code,
    155 AS activity_id
  UNION ALL
  SELECT 
    'Akshar ka V2' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 63 LIMIT 1) AS game_code,
    156 AS activity_id
  UNION ALL
  SELECT 
    'Akshar ma V2' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 64 LIMIT 1) AS game_code,
    157 AS activity_id
  UNION ALL
  SELECT 
    'Akshar cha V2' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 65 LIMIT 1) AS game_code,
    158 AS activity_id
  UNION ALL
  SELECT 
    'Akshar ba V2' AS game_name,
    (SELECT hg.game_code FROM rl_dwh_prod.live.hybrid_games hg WHERE hg.id = 66 LIMIT 1) AS game_code,
    161 AS activity_id
  UNION ALL
  
  -- Normalized activity_id mappings for renamed games
  -- These ensure the normalized activity_ids exist in game_activity_mappings
  -- Patterns AB: normalized to 123 (for both hi and mr)
  SELECT 
    'Patterns AB' AS game_name,
    'HY-12-CG-07' AS game_code,
    123 AS activity_id
  UNION ALL
  -- Patterns ABC, AAB: normalized to 125 (for both hi and mr)
  SELECT 
    'Patterns ABC, AAB' AS game_name,
    'HY-16-CG-10' AS game_code,
    125 AS activity_id
  UNION ALL
  -- Beginning Sounds Pa/Cha/Sa: normalized to 121 (hi) and 122 (mr)
  -- Get game_code from hybrid_games table
  SELECT 
    'Beginning Sound Pa Cha Sa' AS game_name,
    (SELECT hg.game_code 
     FROM rl_dwh_prod.live.hybrid_games hg 
     WHERE hg.game_name = 'Beginning Sound Pa Cha Sa' 
     LIMIT 1) AS game_code,
    121 AS activity_id
  UNION ALL
  SELECT 
    'Beginning Sound Pa Cha Sa' AS game_name,
    (SELECT hg.game_code 
     FROM rl_dwh_prod.live.hybrid_games hg 
     WHERE hg.game_name = 'Beginning Sound Pa Cha Sa' 
     LIMIT 1) AS game_code,
    122 AS activity_id
)
SELECT DISTINCT
    gam.game_name,
    nai.idvisitor_hex
FROM normalized_activity_ids nai
INNER JOIN game_activity_mappings gam ON nai.normalized_activity_id = gam.activity_id
WHERE gam.activity_id IS NOT NULL
