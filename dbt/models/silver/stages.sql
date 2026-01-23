SELECT
    season_id,
    ROW_NUMBER() OVER(PARTITION BY season_id ORDER BY MIN(start_time)) AS stage_num,
    e.event_id AS event_id,
    e.description AS event_name,
    nat,
    nat_long,
    location,
    MIN(start_time) AS start_stage,
    MAX(start_time) AS end_stage,
    COUNT(*) AS count_races,
    (lower(e.description) LIKE '%olympic%') AS is_olympic_games,
    (lower(e.description) LIKE '%championships%') AS is_world_championship,
    CASE
        WHEN MIN(status_id) > 1 AND MAX(status_id) = 11 THEN 'finished'
        WHEN min(status_id) = 1 AND MAX(status_id) = 1 THEN 'planned'
        ELSE 'current'
    END AS stage_status
FROM {{ ref('competitions') }} AS c
JOIN {{ ref('events') }} AS e USING(event_id)
GROUP BY season_id, e.event_id, e.description, nat, nat_long, location