import time
import random
import requests
import pandas as pd
from typing import Dict, List

API_URL = "https://ywzjnepacv.ap-northeast-1.awsapprunner.com/v1/units"

FILTERS = {
    "layouts": [
        "1DK",
        "1LDK",
        "1LDKS",
        "2K",
        "2DK",
        "2LDK",
        "3DK",
        "3LDK",
    ],
    "check_in": "2026-09-28",
    "check_out": "2027-03-28",
    "min_price": 95_000,
    "max_price": 380_000,
    "gcc_id": 101,  # Tokyo
}

PAGINATION = {
    "limit": 12,        # matches site behaviour
    "start_offset": 0,
}

REQUEST_BEHAVIOUR = {
    "min_delay_sec": 1.0,   # be polite
    "max_delay_sec": 2.5,
}

OUTPUT_CSV = "hmlet_units.csv"

# --------------------------------------------------
# HEADERS
# --------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://hmlet.com/",
}


# --------------------------------------------------
# API HELPERS
# --------------------------------------------------

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
    params = build_params(offset)
    response = requests.get(
        API_URL,
        params=params,
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


# --------------------------------------------------
# MAIN SCRAPE LOGIC
# --------------------------------------------------

def fetch_all_units() -> List[Dict]:
    all_units = {}
    offset = PAGINATION["start_offset"]
    page = 1

    while True:
        data = fetch_units_page(offset)
        items = data.get("items", [])

        if not items:
            print(f"Page {page}: 0 units — stopping.")
            break

        added = 0
        for item in items:
            uid = item["unit_id"]
            if uid not in all_units:
                all_units[uid] = item
                added += 1

        print(
            f"Page {page}: {len(items)} units, "
            f"added {added}, total {len(all_units)}"
        )

        offset += PAGINATION["limit"]
        page += 1

        polite_sleep()

    return list(all_units.values())


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------

def main():
    units = fetch_all_units()

    df = pd.DataFrame(units)
    df.sort_values("unit_id", inplace=True)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved {len(df)} units → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
