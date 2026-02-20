from enum import Enum


class CompetitionTable(str, Enum):
    """Fields of competition table in database."""

    EVENT_ID = "event_id"
    SEASON_ID = "season_id"
    START_TIME = "StartTime"
    STATUS_ID = "StatusId"
    RACE_ID = "RaceId"
    UPDATED_AT = "updated_at"
