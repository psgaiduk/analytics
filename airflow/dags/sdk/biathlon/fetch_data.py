from datetime import datetime
from logging import getLogger
from time import sleep

from pandas import DataFrame
from requests import get
from requests.exceptions import Timeout, ConnectionError, HTTPError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from constants import BIATHLON_RESULTS_URL


RETRYABLE_ERRORS = (Timeout, ConnectionError, HTTPError)

log = getLogger(__name__)


class BiathlonEventsFetcher:
    """Fetch events for season."""

    def __init__(self, rt: int, season_id: str):
        """Init.

        Args:
            rt (int): rt for biathlon results
            season_id (int): id of season.
        """
        self.rt = rt
        self.season_id = season_id

    def fetch(self) -> DataFrame:
        """Fetch events for this season.

        Returns:
            DataFrame: competitions on this season.
        """

        log.info(f"Fetching competitions: rt={self.rt}, season_id={self.season_id}")
        events = self._get_events()
        events_df = DataFrame(events)
        if events_df.empty:
            log.warning(f"Season {self.season_id} has no events.")
            return DataFrame()
        events_df["updated_at"] = datetime.now()
        log.info(f"Fetched {len(events_df)} rows")

        return events_df

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying get events from api (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
    def _get_events(self):
        url = f"{BIATHLON_RESULTS_URL}/Events?RT={self.rt}&SeasonId={self.season_id}"
        response = get(url, timeout=30)
        log.info(f"Status code for season_id {self.season_id} {response.status_code}")

        if response.status_code != 200:
            log.error(f"API Error {response.status_code}: {response.text}")
            response.raise_for_status()

        return response.json()


class BiathlonCompetitionsFetcher:
    """Fetch competition for event."""

    def __init__(self, rt: int, season_id: str):
        """Init.

        Args:
            rt (int): rt for biathlon results
            season_id (int): id of season.
        """
        self.rt = rt
        self.season_id = season_id

    def fetch(self, event_id: str) -> DataFrame:
        """Fetch competitions for this event.

        Args:
            event_id (str): id of event.

        Returns:
            DataFrame: competitions for this event.
        """

        log.info(f"Fetching competitions for event: {event_id}")

        data = self._get_stage(event_id=event_id)
        if not data:
            return DataFrame()

        competition = DataFrame(data)
        if competition.empty:
            log.warning(f"Event {event_id} has no competitions.")
            return DataFrame()
        competition["updated_at"] = datetime.now()
        competition["season_id"] = self.season_id
        competition["event_id"] = event_id
        sleep(1)

        log.info(f"Fetched {len(competition)} rows")
        return competition

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying get competitions from api (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
    def _get_stage(self, event_id: str):
        url = f"{BIATHLON_RESULTS_URL}/Competitions?RT={self.rt}&EventId={event_id}"

        response = get(url, timeout=30)
        log.info(f"Status code for event_id {event_id}: {response.status_code}")

        if response.status_code != 200:
            log.error(f"Error response: {response.text}")
            response.raise_for_status()
            return

        return response.json()


class BiathlonResultsFetcher:
    """Fetch results from biathlonresults.com."""

    def __init__(self, rt: int):
        """Init.

        Args:
            rt (int): rt for biathlonresults.com.
        """
        self.rt = rt

    def fetch(self, race_id: str) -> tuple[DataFrame, dict]:
        """Fetch results from biathlon results.

        Args:
            season_id (int): season id.
            race_id (str): race id.
            rt (int): rt.

        Returns:
            list: [results, analytics_results]
        """

        log.info(f"start get results for race {race_id}")

        results = self._get_results(race_id=race_id)
        if results is None:
            log.warning(f"Race {race_id} has no results.")
            return DataFrame()

        results_df = DataFrame(results["Results"])
        results_df["race_id"] = race_id
        return results_df, results["Competition"]

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying get results from api (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
    def _get_results(self, race_id: str) -> dict:
        url = f"{BIATHLON_RESULTS_URL}/Results?RT={self.rt}&RaceId={race_id}"
        response = get(url, timeout=30)
        log.info(f"Status code for race_id {race_id}: {response.status_code}")
        if response.status_code != 200:
            log.error(f"Get error response: {response.text}")
            response.raise_for_status()
            return
        return response.json()


class BiathlonAnalyticsResultsFetcher:
    """Fetch analytics result from biathlonresults.com."""

    def __init__(self, rt: int, race_id: str):
        """Init.

        Args:
            rt (int): rt for biathlonresults.com.
            race_id (str): id of race.
        """
        self.rt = rt
        self.race_id = race_id

    def fetch(self, type_id: str, type_name: str) -> DataFrame:
        """Fetch analytics result from biathlonresults.com.

        Args:
            type_id (str): Id of analytics type.
            type_name (str): pretty name of analytics type.

        Returns:
            DataFrame: analytics result.
        """

        analytic_results = self._get_analytics_results(type_id=type_id)
        if analytic_results is None:
            log.warning(f"No analytics data for race_id = {self.race_id} and type_id {type_id}")
            return DataFrame()

        analytics_result_df = DataFrame(analytic_results["Results"])
        analytics_result_df["race_id"] = self.race_id
        analytics_result_df["type_id"] = type_id
        analytics_result_df["type_name"] = type_name
        sleep(0.5)

        return analytics_result_df

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying get analytics result from api (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
    def _get_analytics_results(self, type_id: str) -> DataFrame:
        analytics_url = f"{BIATHLON_RESULTS_URL}/AnalyticResults?RaceId={self.race_id}&TypeId={type_id}"
        response = get(analytics_url, timeout=30)
        response.raise_for_status()
        log.info(f"Status code for type_id {type_id}: {response.status_code}")
        if response.status_code != 200:
            log.error(f"Get error response: {response.text}")
            return None
        return response.json()
