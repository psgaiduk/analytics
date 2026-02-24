from enum import Enum


class EventsTable(Enum):
    """Fields of events table in database."""

    EVENT_ID = "EventId"
    START_DATE = "StartDate"
    END_DATE = "EndDate"
    SEASON_ID = "SeasonId"
