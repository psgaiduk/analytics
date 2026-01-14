def get_season_id_func(year_end: int) -> str:
    """Get season_id from year_end

    Args:
        year_end (int): Year end of season (e.g., 22 for 2022-2023 season)

    Returns:
        str: season_id (e.g., "2223" for 2022-2023 season)
    """
    next_year = (year_end + 1) % 100
    return f"{year_end:02d}{next_year:02d}"
