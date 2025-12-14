from datetime import date, datetime
from pathlib import Path

API_URL = "https://ywzjnepacv.ap-northeast-1.awsapprunner.com/v1/units"

FILTERS = {
    "layouts": [
        "1DK", "1LDK", "1LDKS",
        "2K", "2DK", "2LDK",
        "3DK", "3LDK",
    ],
    "check_in": "2026-09-29",
    "check_out": "2027-03-28",
    "min_price": 95_000,
    "max_price": 380_000,
    "gcc_id": 101,
}

PAGINATION = {
    "limit": 12,
    "start_offset": 0,
    "max_pages": 50,
}

REQUEST_BEHAVIOUR = {
    "min_delay_sec": 1.0,
    "max_delay_sec": 2.5,
}

TODAY_DATE = date.today()
TODAY_DATETIME = datetime.combine(
    TODAY_DATE, datetime.min.time()
).isoformat()

OUTPUT_DIR = Path("out")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = OUTPUT_DIR / f"hmlet_units_{TODAY_DATE.isoformat()}.csv"
DB_PATH = OUTPUT_DIR / "hmlet_units.sqlite"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://hmlet.com/",
}
