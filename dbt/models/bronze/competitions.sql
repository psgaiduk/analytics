{{ config(
    materialized='view'
) }}

SELECT
    RaceId AS race_id,
    nullIf(km, '') AS km,
    catId As cat_id,
    DisciplineId AS discipline_id,
    CAST(StatusId AS Int64) AS status_id,
    StatusText AS status_text,
    ScheduleStatus AS schedule_status,
    nullIf(ResultStatus, 'NONE') AS result_status,
    (HasLiveData != 'False') AS has_live_data,
    (IsLive != 'False') AS is_live,
    parseDateTime64BestEffort(StartTime, 3) AS start_time,
    Description AS description,
    ShortDescription AS short_description,
    Location AS location,
    nullIf(ResultsCredit, 'None') AS results_credit,
    nullIf(TimingCredit, 'None') AS timing_credit,
    (HasAnalysis != 'False') AS has_analysis,
    StartMode AS start_mode,
    CAST(NrShootings AS Int8) AS nr_shootings,
    CAST(NrSpareRounds AS Int8) AS nr_spare_rounds,
    (HasSpareRounds != 'False') AS has_spare_rounds,
    CAST(PenaltySeconds AS Int32) AS penalty_seconds,
    CAST(NrLegs AS Int8) AS nr_legs,
    ShootingPositions AS shooting_positions,
    CAST(LocalUTCOffset AS Int32) AS local_utc_offset,
    RSC AS rsc,
    nullIf(GenderOrder, 'None') AS gender_order,
    season_id,
    event_id,
    parseDateTime64BestEffort(updated_at, 3) AS updated_at
FROM {{ source('biathlon_raw', 'competitions') }}

