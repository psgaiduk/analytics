WITH 
    -- Константа для личного зачета (World Cup)
    -- (discipline_id IN ('IN', 'SP', 'PU', 'MS', 'SI') OR (discipline_id IN ('TM', 'RL', 'SR') AND is_team = 1)) AS is_wc_race,
    -- Пока не решил как учитывать командные гонки в личном зачете, поэтому сейчас считаю очки и в командных гонках
    -- для участников команды тоже, выше код, который учитывает только для команд
    discipline_id IN ('IN', 'SP', 'PU', 'MS', 'SI', 'TM', 'RL', 'SR') AS is_wc_race,
    -- Константа для Кубка Наций (Nations Cup)
    -- Очки дают только: Индивидуалка (IN), Спринт (SP), Эстафеты (RL, SR, TM)
    (discipline_id IN ('IN', 'SP') OR (discipline_id IN ('TM', 'RL', 'SR') AND is_team = 1)) AS is_nc_race
SELECT
    *,
    -- Номер спортсмена в команде для личных гонок (учитываем только тех, у кого есть ранг)
    IF(discipline_id IN ('IN', 'SP', 'PU', 'MS', 'SI') AND rank IS NOT NULL,
        row_number() OVER (PARTITION BY race_id, nationality ORDER BY rank ASC),
        1
    ) AS athlete_team_num,
    -- 1. ЛИЧНЫЙ ЗАЧЕТ (World Cup)
    multiIf(
        rank IS NULL OR rank = 0 OR NOT is_wc_race, 0,
        rank = 1, 90,
        rank = 2, 75,
        rank = 3, 60,
        rank = 4, 50,
        rank = 5, 45,
        rank = 6, 40,
        rank BETWEEN 7 AND 9, 50 - (2 * rank),
        rank BETWEEN 10 AND 21, 41 - rank,
        discipline_id = 'MS' AND rank BETWEEN 22 AND 30, 58 - (2 * rank),
        discipline_id != 'MS' AND rank BETWEEN 22 AND 40, 41 - rank,
        0
    ) AS new_wc,

    -- 2. КУБОК НАЦИЙ (Nations Cup)
    multiIf(
        rank IS NULL OR rank = 0 OR NOT is_nc_race, 0,
        -- Эстафеты и командная гонка (RL, TM)
        discipline_id IN ('RL', 'TM'), 
            multiIf(
                rank = 1, 420,
                rank = 2, 390,
                rank = 3, 360,
                rank = 4, 330,
                rank = 5, 310,
                rank = 6, 290, 
                rank = 7, 270,
                rank = 8, 250,
                rank = 9, 230, 
                ank = 10, 220, 
                rank BETWEEN 11 AND 15, 320 - (rank * 10),
                0
            ),
        -- Сингл-микст (SR)
        discipline_id = 'SR', 
            multiIf(
                rank = 1,210,
                rank = 2, 195,
                rank = 3, 180,
                rank = 4, 165,
                rank = 5, 155,
                rank = 6, 145, 
                rank = 7, 135,
                rank = 8, 125,
                rank = 9, 115,
                rank = 10, 110,
                0
            ),
        -- Личные гонки (IN, SP)
        discipline_id IN ('IN', 'SP'),
            multiIf(
                rank = 1, 160,
                rank = 2, 154,
                rank = 3, 148, 
                rank BETWEEN 4 AND 10, 143 - ((rank - 4) * 3), 
                rank BETWEEN 11 AND 80, 124 - ((rank - 11) * 2), 
                rank BETWEEN 81 AND 160, 80 - (rank - 81),
                0
            ),
        0
    ) AS new_nc
FROM {{ ref('result') }}
JOIN {{ ref('races') }} USING(race_id)