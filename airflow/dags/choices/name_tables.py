from enum import Enum


class TableNames(str, Enum):
    """Name of tables in database."""

    BIATHLON_EVENTS = "biathlon_raw.events"
    BIATHLON_COMPETITION = "biathlon_raw.competition"
    BIATHLON_RESULT = "biathlon_raw.result"
    BIATHLON_ANALYTICS_RESULT = "biathlon_raw.analytics_result"
