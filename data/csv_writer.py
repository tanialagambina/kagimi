from typing import List, Dict
import pandas as pd

from src.config import OUTPUT_CSV, TODAY_DATETIME
from src.parsing import parse_lat_lon, parse_date_to_datetime


def write_csv(units: List[Dict]):
    """
    Docstring for write_csv

    :param units: Description
    :type units: List[Dict]
    """
    rows = []
    for u in units:
        lat, lon = parse_lat_lon(u["coordinates"])
        rows.append(
            {
                **u,
                "latitude": lat,
                "longitude": lon,
                "snapshot_datetime": TODAY_DATETIME,
                "earliest_move_in_datetime": parse_date_to_datetime(
                    u.get("earliest_move_in_date")
                ),
            }
        )

    df = pd.DataFrame(rows)
    df.sort_values("unit_id", inplace=True)
    df.to_csv(OUTPUT_CSV, index=False)
