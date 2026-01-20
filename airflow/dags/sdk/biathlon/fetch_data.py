from datetime import datetime
from logging import getLogger
from time import sleep

from pandas import DataFrame, concat
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

    def __init__(self, rt: int):
        """Init.

        Args:
            rt (int): rt for biathlon results
        """
        self.rt = rt

    def fetch(self, event_id: str) -> DataFrame:
        """Fetch competitions for this event.

        Args:
            event_id (str): id of event.

        Returns:
            DataFrame: competitions for this event.
        """

        log.info(f"Fetching competitions: {event_id}")
        log.info(f"Stages: {self.stages}")

        data = self._get_stage(event_id=event_id)
        if not data:
            return DataFrame()

        competition = DataFrame(data)
        competition["updated_at"] = datetime.now()
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
            self.log.error(f"Error response: {response.text}")
            response.raise_for_status()
            return

        return response.json()


class BiathlonResultsFetcher:
    """Загружает данные соревнований с biathlonresults.com."""

    def fetch(self, season_id: int, race_id: str, rt: int) -> list:
        """Fetch results and analytics results from biathlon results.

        Args:
            season_id (int): season id.
            race_id (str): race id.
            rt (int): rt.

        Returns:
            list: [results, analytics_results]
        """

        self.race_id = race_id
        self.rt = rt
        self.season_id = season_id
        log.info(f"start get results for season {season_id} and race {race_id}")
        analytic_results = DataFrame()

        results = self._get_results()
        if results is None or results.empty:
            log.warning(f"Race {race_id} has no results.")
            return DataFrame(), DataFrame()
        sleep(1)

        for type_id, type_name in self._get_analytics_type():
            analytic_results_df = self._get_analytics_results(type_id=type_id, type_name=type_name)
            if analytic_results_df is None or analytic_results_df.empty:
                continue
            analytic_results = concat([analytic_results, analytic_results_df], ignore_index=True)
            sleep(1)

        return [results, analytic_results]

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying get results from api (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
    def _get_results(self):
        url = f"{BIATHLON_RESULTS_URL}/Results?RT={self.rt}&RaceId={self.race_id}"
        response = get(url, timeout=30)
        log.info(f"Status code for race_id {self.race_id}: {response.status_code}")
        if response.status_code != 200:
            log.error(f"Get error response: {response.text}")
            return
        data = response.json()
        if not data:
            log.warning(f"No analytics data for race_id = {self.race_id}")
            return DataFrame()

        self.competition = data["Competition"]
        df = DataFrame(data["Results"])
        df["race_id"] = self.race_id
        df["rt"] = self.rt
        df["season_id"] = self.season_id
        return df

    def _get_analytics_type(self):
        legs = int(self.competition.get("NrLegs", 0))
        shootings = int(self.competition.get("NrShootings", 0))
        analytic_types = [
            ["CRST", "Total Course Time"],
            ["RNGT", "Total Range Time"],
            ["STTM", "Total Shooting Time"],
            ["SKIT", "Ski Time"],
        ]

        if legs:
            analytic_types.extend([[f"FI{i + 1}L", f"Results Les {i + 1}"] for i in range(legs)])
            analytic_types.extend([[f"CRST{i + 1}", f"Course Time Leg {i + 1}"] for i in range(legs)])
            analytic_types.extend([[f"RNGT{i + 1}T", f"Range Time Leg {i + 1}"] for i in range(legs)])
        else:
            legs = 1

        analytic_types.extend([[f"CRS{i + 1}", f"Course Time Lap {i + 1}"] for i in range((shootings + 1) * legs)])
        analytic_types.extend([[f"RNG{i + 1}", f"Range Time {i + 1}"] for i in range(shootings * legs)])
        analytic_types.extend([[f"S{i + 1}TM", f"Shooting Time {i + 1}"] for i in range(shootings * legs)])
        log.info(f"analytic_types: {analytic_types}")
        return analytic_types

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying get analytics result from api (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
    def _get_analytics_results(self, type_name: str, type_id: str) -> DataFrame:
        analytics_url = f"{BIATHLON_RESULTS_URL}/AnalyticResults?RaceId={self.race_id}&TypeId={type_id}"
        response = get(analytics_url, timeout=30)
        response.raise_for_status()
        log.info(f"Status code for type_id {type_id}: {response.status_code}")
        if response.status_code != 200:
            log.error(f"Get error response: {response.text}")
            return DataFrame()
        analytics_data = response.json()
        if not analytics_data:
            log.warning(f"No analytics data for race_id = {self.race_id} and type_id {type_id}")
            return DataFrame()

        df = DataFrame(analytics_data["Results"])
        df["race_id"] = self.race_id
        df["type_id"] = type_id
        df["type_name"] = type_name
        df["season_id"] = self.season_id

        log.debug(f"data: {df.head(2)}")
        return df
