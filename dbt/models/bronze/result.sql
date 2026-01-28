SELECT
    CAST(StartOrder AS Int32) AS start_order,
    CAST(ResultOrder AS Int32) AS result_order,
    nullIf(IRM, 'None') AS irm,
    IBUId AS ibu_id,
    (IsTeam != 'False') AS is_team,
    Name AS name,
    ShortName AS short_name,
    FamilyName AS family_name,
    GivenName AS given_name,
    Nat AS nationality,
    toInt32OrNull(Bib) AS bib,
    nullIf(Leg, 'None') AS leg,
    toInt32OrNull(Rank) AS rank,
    Shootings AS shootings,
    toInt32OrNull(ShootingTotal) AS shooting_total,
    nullIf(RunTime, 'None') AS run_time,
    date_diff('millisecond', toDateTime64('1970-01-01 00:00:00', 1), parseDateTime64BestEffortOrNull(concat('1970-01-01 00:', TotalTime), 1)) / 1000.0 AS total_time_seconds,
    concat(formatDateTime(toDateTime(floor(total_time_seconds)), '%H:%i:%s'), '.', toString(toInt32(round((total_time_seconds - floor(total_time_seconds)) * 10)))) AS total_time,
    CASE 
        WHEN Behind = '0.0' OR Behind IS NULL THEN 0.0
        WHEN position(Behind, ':') > 0 THEN 
            toInt32(substring(replaceRegexpAll(Behind, '^\+', ''), 1, position(replaceRegexpAll(Behind, '^\+', ''), ':') - 1)) * 60 + 
            toFloat64(substring(replaceRegexpAll(Behind, '^\+', ''), position(replaceRegexpAll(Behind, '^\+', ''), ':') + 1))
        ELSE 
            toFloat64OrNull(replaceRegexpAll(Behind, '^\+', ''))
    END AS behind_seconds,
    if(behind_seconds IS NULL, NULL,
        concat(
            formatDateTime(toDateTime(floor(behind_seconds)), '%H:%i:%s'),
            '.',
            toString(toInt32(round((behind_seconds - floor(behind_seconds)) * 10)))
        )
    ) AS behind,
    toInt32OrNull(WC) AS wc,
    toInt32OrNull(NC) AS nc,
    toInt32OrNull(NOC) AS noc,
    nullIf(StartTime, 'None') AS start_time,
    StartInfo AS start_info,
    toInt32(StartRow) AS start_row,
    toInt32(StartLane) AS start_lane,
    nullIf(BibColor, 'None') AS bib_color,
    nullIf(StartGroup, 'None') AS start_group,
    PursuitStartDistance AS pursuit_start_distance,
    race_id
FROM {{ source('biathlon_raw', 'results') }}

UNION ALL

SELECT
    CAST(StartOrder AS Int32) AS start_order,
    CAST(ResultOrder AS Int32) AS result_order,
    nullIf(IRM, 'None') AS irm,
    nullIf(IBUId, 'nan') AS ibu_id,
    (IsTeam != 'False') AS is_team,
    Name AS name,
    ShortName AS short_name,
    FamilyName AS family_name,
    GivenName AS given_name,
    Nat AS nationality,
    toInt32OrNull(Bib) AS bib,
    nullIf(Leg, 'nan') AS leg,
    toInt32OrNull(Rank) AS rank,
    Shootings AS shootings,
    toInt32OrNull(ShootingTotal) AS shooting_total,
    nullIf(RunTime, 'nan') AS run_time,
    date_diff('millisecond', toDateTime64('1970-01-01 00:00:00', 1), parseDateTime64BestEffortOrNull(concat('1970-01-01 00:', TotalTime), 1)) / 1000.0 AS total_time_seconds,
    concat(
        formatDateTime(toDateTime(floor(ifNull(total_time_seconds, 0))), '%H:%i:%s'), 
        '.', 
        toString(CAST(round((ifNull(total_time_seconds, 0) - floor(ifNull(total_time_seconds, 0))) * 10) AS Int32))
    ) AS total_time,
    CASE 
        WHEN Behind = '0.0' OR Behind IS NULL THEN 0.0
        WHEN position(Behind, ':') > 0 THEN 
            toInt32(substring(replaceRegexpAll(Behind, '^\+', ''), 1, position(replaceRegexpAll(Behind, '^\+', ''), ':') - 1)) * 60 + 
            toFloat64(substring(replaceRegexpAll(Behind, '^\+', ''), position(replaceRegexpAll(Behind, '^\+', ''), ':') + 1))
        ELSE 
            toFloat64OrNull(replaceRegexpAll(Behind, '^\+', ''))
    END AS behind_seconds,
    if(behind_seconds IS NULL OR isNaN(behind_seconds), 
        NULL,
        concat(
            formatDateTime(toDateTime(floor(behind_seconds)), '%H:%i:%s'),
            '.',
            toString(CAST(round((behind_seconds - floor(behind_seconds)) * 10) AS Int32))
        )
    ) AS behind,
    toInt32OrNull(WC) AS wc,
    toInt32OrNull(NC) AS nc,
    toInt32OrNull(NOC) AS noc,
    nullIf(StartTime, 'nan') AS start_time,
    StartInfo AS start_info,
    toInt32(StartRow) AS start_row,
    toInt32(StartLane) AS start_lane,
    nullIf(BibColor, 'nan') AS bib_color,
    nullIf(StartGroup, 'nan') AS start_group,
    nullIf(PursuitStartDistance, 'nan') AS pursuit_start_distance,
    race_id
FROM {{ source('biathlon_raw', 'results_og') }}