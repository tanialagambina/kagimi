from datetime import datetime
from typing import Tuple


def parse_lat_lon(wkt: str) -> Tuple[float | None, float | None]:
    try:
        inner = wkt.replace("POINT(", "").replace(")", "")
        lat, lon = map(float, inner.split())
        return lat, lon
    except Exception:
        return None, None


def parse_date_to_datetime(value: str | None) -> str | None:
    if not value:
        return None
    return datetime.fromisoformat(value).isoformat()
