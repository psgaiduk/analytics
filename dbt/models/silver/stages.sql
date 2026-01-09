SELECT
    season_id,
    ROW_NUMBER() OVER(PARTITION BY season_id ORDER BY MIN(start_time)) AS stage_num,
    stage AS stage_name,
    location,
    MIN(start_time) AS start_stage,
    MAX(start_time) AS end_stage,
    COUNT(*) AS count_races,
    (stage = 'OG__') AS is_olympic_games,
    (stage = 'CH__') AS is_world_championship,
    CASE
        WHEN MIN(status_id) > 1 AND MAX(status_id) = 11 THEN 'finished'
        WHEN min(status_id) = 1 AND MAX(status_id) = 1 THEN 'planned'
        ELSE 'current'
    END AS stage_status
FROM {{ ref('competition') }}
GROUP BY season_id, stage, location