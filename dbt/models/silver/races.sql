SELECT
    race_id,
    status_text,
    cat_id,
    discipline_id,
    start_time,
    description,
    short_description,
    start_mode,
    nr_shootings,
    shooting_positions,
    s.stage_num,
    c.location,
    s.is_olympic_games,
    s.is_world_championship,
    c.season_id,
    CASE
        WHEN cat_id = 'MX' THEN 'Смешанная'
        WHEN cat_id = 'SM' THEN 'Мужская'
        WHEN cat_id = 'SW' THEN 'Женская'
    END gender,
    CASE
        WHEN discipline_id = 'RL' AND cat_id = 'MX' THEN 'Смешанная эстафета'
        WHEN discipline_id = 'SP' THEN 'Спринт'
        WHEN discipline_id = 'PU' THEN 'Преследование'
        WHEN discipline_id IN ('IN', 'SI') THEN 'Индивидуальная'
        WHEN discipline_id = 'SR' THEN 'Одиночная эстафета'
        WHEN discipline_id = 'MS' THEN 'Масс-старт'
        WHEN discipline_id = 'TM' THEN 'Командная гонка'
        ELSE 'Эстафета'
    END race_type
FROM {{ ref('competition') }} AS c
JOIN {{ ref('stages') }} AS s ON c.stage = s.stage_name AND c.season_id = s.season_id