from typing import Dict, List
import time
import random
import requests

from src.config import (
    API_URL,
    FILTERS,
    PAGINATION,
    REQUEST_BEHAVIOUR,
    HEADERS,
)


def build_params(
    *,
    check_in: str,
    check_out: str,
    offset: int,
) -> Dict:
    return {
        "layouts": ",".join(FILTERS["layouts"]),
        "check_in": check_in,
        "check_out": check_out,
        "min_price": FILTERS["min_price"],
        "max_price": FILTERS["max_price"],
        "gcc_id": FILTERS["gcc_id"],
        "limit": PAGINATION["limit"],
        "offset": offset,
    }


def fetch_units_page(
    *,
    check_in: str,
    check_out: str,
    offset: int,
) -> Dict:
    response = requests.get(
        API_URL,
        params=build_params(
            check_in=check_in,
            check_out=check_out,
            offset=offset,
        ),
        headers=HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def polite_sleep():
    time.sleep(
        random.uniform(
            REQUEST_BEHAVIOUR["min_delay_sec"],
            REQUEST_BEHAVIOUR["max_delay_sec"],
        )
    )


def fetch_all_units(
    *,
    check_in: str,
    check_out: str,
) -> List[Dict]:
    """
    Fetch all units for the given date range,
    enforcing size filters locally as a safety net.
    """
    all_units: Dict[int, Dict] = {}
    offset = PAGINATION["start_offset"]

    min_size = FILTERS.get("size_square_meters_min")
    max_size = FILTERS.get("size_square_meters_max")

    for page in range(1, PAGINATION["max_pages"] + 1):
        data = fetch_units_page(
            check_in=check_in,
            check_out=check_out,
            offset=offset,
        )
        items = data.get("items", [])

        if not items:
            break

        for item in items:
            size = item.get("size_square_meters")

            # Defensive size filtering
            if min_size is not None and size < min_size:
                continue
            if max_size is not None and size > max_size:
                continue

            all_units[item["unit_id"]] = item

        if len(items) < PAGINATION["limit"]:
            break

        offset += PAGINATION["limit"]
        polite_sleep()

    return list(all_units.values())

