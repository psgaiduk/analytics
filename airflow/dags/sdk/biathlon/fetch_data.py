from logging import getLogger
from time import sleep
from typing import List

from pandas import DataFrame, concat
from requests import get

from constants import BIATHLON_RESULTS_URL


log = getLogger(__name__)


class BiathlonCompetitionsFetcher:
    """
    Загружает список соревнований с biathlonresults.com.
    """

    stages: List[str] = ["CH__", "OG__"] + [f"CP{i}" if i > 9 else f"CP0{i}" for i in range(1, 20)]

    def __init__(self) -> None:
        """Init."""
        self.base_url = BIATHLON_RESULTS_URL
        self.sleep_seconds = 1

    def fetch(self, rt: int, season_id: int) -> DataFrame:
        """Fetch competitions for this season.

        Args:
            rt (int): rt for biathlon results
            season_id (int): id of season.

        Returns:
            DataFrame: competitions on this season.
        """
        self.rt = rt
        self.season_id = season_id
        results = DataFrame()

        self.log.info(f"Fetching competitions: rt={rt}, season_id={season_id}")
        self.log.info(f"Stages: {self.stages}")

        for stage in self.stages:
            data = self._get_stage()
            if not data:
                continue

            df = DataFrame(data)
            df["season_id"] = season_id
            df["stage"] = stage
            df["rt"] = rt
            results = concat([results, df], ignore_index=True)
            sleep(self.sleep_seconds)

        self.log.info("Fetched %s rows", len(results))
        return results

    def _get_stage(self, stage: str):
        event_id = f"BT{self.season_id}SWRL{stage}"
        url = f"{self.base_url}/Competitions?RT={self.rt}&EventId={event_id}"

        response = get(url, timeout=30)
        self.log.info(
            "Status code for event_id %s: %s",
            event_id,
            response.status_code,
        )

        if response.status_code != 200:
            self.log.error("Error response: %s", response.text)
            return

        return response.json()
