import time
import random
import requests
from typing import Dict, List

from .config import API_URL, FILTERS, PAGINATION, HEADERS, REQUEST_BEHAVIOUR


def build_params(offset: int) -> Dict:
    return {
        "layouts": ",".join(FILTERS["layouts"]),
        "check_in": FILTERS["check_in"],
        "check_out": FILTERS["check_out"],
        "min_price": FILTERS["min_price"],
        "max_price": FILTERS["max_price"],
        "gcc_id": FILTERS["gcc_id"],
        "limit": PAGINATION["limit"],
        "offset": offset,
    }


def fetch_units_page(offset: int) -> Dict:
    r = requests.get(
        API_URL,
        params=build_params(offset),
        headers=HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def polite_sleep():
    time.sleep(
        random.uniform(
            REQUEST_BEHAVIOUR["min_delay_sec"],
            REQUEST_BEHAVIOUR["max_delay_sec"],
        )
    )


def fetch_all_units() -> List[Dict]:
    all_units = {}
    offset = PAGINATION["start_offset"]

    for page in range(1, PAGINATION["max_pages"] + 1):
        data = fetch_units_page(offset)
        items = data.get("items", [])

        if not items:
            break

        for item in items:
            all_units[item["unit_id"]] = item

        if len(items) < PAGINATION["limit"]:
            break

        offset += PAGINATION["limit"]
        polite_sleep()

    return list(all_units.values())
