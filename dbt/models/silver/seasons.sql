SELECT
    season_id,
    MIN(start_date) AS start_season,
    MAX(end_date) AS end_season,
    COUNT(*) AS count_events,
    COUNT(DISTINCT nat) AS count_countries,
    MAX(description ILIKE '%olympic%') AS is_olympic_season,
    MAX(description ILIKE '%shampionship%') AS is_championship_season,
    YEAR(start_season) AS start_year_season,
    YEAR(end_season) AS end_year_season
FROM {{ ref('events') }}
GROUP BY season_id