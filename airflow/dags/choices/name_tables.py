from enum import Enum


class TableNames(str, Enum):
    """Name of tables in database."""

    BIATHLON_EVENTS = "biathlon_raw.events"
    BIATHLON_COMPETITION = "biathlon_raw.competitions"
    BIATHLON_RESULT = "biathlon_raw.results"
    BIATHLON_ANALYTICS_RESULT = "biathlon_raw.analytics_results"

    BIATHLON_OG_EVENTS = "biathlon_raw.events_og"
    BIATHLON_OG_COMPETITION = "biathlon_raw.competitions_og"
    BIATHLON_OG_RESULT = "biathlon_raw.results_og"
