from datetime import date
from logging import getLogger

log = getLogger(__name__)


def generate_season_id_func() -> str:
    """
    Генерирует season_id по текущей дате.

    Returns:
        str: season_id
    """
    log.info("Generate season_id")
    today = date.today()
    year = today.year % 100
    log.debug(f"today = {today} year = {year}")
    if today.month < 7:
        log.debug("month less 7")
        season_id = f"{year - 1}{year}"
    else:
        log.debug("Month after 6")
        season_id = f"{year}{year + 1}"
    return season_id
