from datetime import date, datetime, timezone, timedelta
from pathlib import Path

API_URL = "https://ywzjnepacv.ap-northeast-1.awsapprunner.com/v1/units"

FILTERS = {
    "layouts": [
        "1LDK","2LDK","3LDK",
    ],
    "min_price": 95_000,
    "max_price": 380_000,
    "gcc_id": 101,
    "size_square_meters_min": 36,
    "size_square_meters_max": None,
}


MAIN_CHECK_IN = date.fromisoformat("2026-10-01")
CHECK_OUT = "2027-03-29"

SUGGESTION_WINDOW_DAYS = 15  # 1 week

QUERIES = []

# Primary query
QUERIES.append({
    "name": "main",
    "check_in": MAIN_CHECK_IN.isoformat(),
    "check_out": CHECK_OUT,
    "is_primary": True,
})

# Secondary queries: day -1 to day -14
for delta in range(1, SUGGESTION_WINDOW_DAYS + 1):
    d = MAIN_CHECK_IN - timedelta(days=delta)
    QUERIES.append({
        "name": f"minus_{delta}_days",
        "check_in": d.isoformat(),
        "check_out": CHECK_OUT,
        "is_primary": False,
    })

PAGINATION = {
    "limit": 12,
    "start_offset": 0,
    "max_pages": 50,
}

REQUEST_BEHAVIOUR = {
    "min_delay_sec": 1.0,
    "max_delay_sec": 2.5,
}

SNAPSHOT_DATETIME = datetime.now(timezone.utc).isoformat(timespec="seconds")

OUTPUT_DIR = Path("out")
OUTPUT_DIR.mkdir(exist_ok=True)
TODAY_DATE = date.today()
TODAY_DATETIME = datetime.combine(
    TODAY_DATE, datetime.min.time()
).isoformat()


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
