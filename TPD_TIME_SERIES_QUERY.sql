-- TPD Games Dashboard - Time Series Query
-- Filters: custom_dimension_2 IN ('149','150','160','166')
-- Date: server_time >= '2026-01-03' (with timezone adjustment)
-- Uses simpler pattern matching (without 'hybrid_' prefix)

SELECT 
  mllva.idlink_va,
  TO_HEX(mllva.idvisitor) AS idvisitor_hex,
  mllva.idvisit,
  DATEADD(minute, 330, mllva.server_time) AS server_time,
  mllva.idaction_name,
  mllva.custom_dimension_2,
  mla.name AS action_name,
  matomo_log_action1.name AS language,
  CASE 
    WHEN mla.name LIKE '%game_started%' OR mla.name LIKE '%mcq_started%' THEN 'Started'
    WHEN mla.name LIKE '%game_completed%' OR mla.name LIKE '%mcq_completed%' THEN 'Completed'
    ELSE NULL
  END AS event,
  CASE 
    WHEN CAST(mllva.custom_dimension_2 AS INTEGER) = 166 THEN 'significance_of_early_years_2'
    WHEN CAST(mllva.custom_dimension_2 AS INTEGER) = 150 THEN 'significance_of_early_years_1'
    WHEN CAST(mllva.custom_dimension_2 AS INTEGER) = 160 THEN 'language_development_1'
    ELSE 'redirected to emotional_development'
  END AS game_name
FROM rl_dwh_prod.live.matomo_log_link_visit_action mllva
INNER JOIN rl_dwh_prod.live.matomo_log_action mla ON mllva.idaction_name = mla.idaction
INNER JOIN rl_dwh_prod.live.matomo_log_action matomo_log_action1 ON mllva.idaction_url_ref = matomo_log_action1.idaction
WHERE (mla.name LIKE '%game_started%' 
       OR mla.name LIKE '%mcq_started%' 
       OR mla.name LIKE '%game_completed%' 
       OR mla.name LIKE '%mcq_completed%')
  AND DATEADD(minute, 330, mllva.server_time) >= '2026-01-03'
  AND custom_dimension_2 IN ('149','150','160','166')
