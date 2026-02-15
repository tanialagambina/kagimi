import sqlite3
from pathlib import Path
from datetime import date, datetime


#--------------------------------------------------
# CONFIG & CONSTANTS
#--------------------------------------------------

DB_PATH = Path("out/hmlet_units.sqlite")
SEPARATOR = "────────────────────────\n"
SUB_SEPARATOR = "· · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · ·\n"


# --------------------------------------------------
# URL HELPERS
# --------------------------------------------------

def build_unit_url(property_id: int, unit_id: int, check_in: str, check_out: str) -> str:
    return (
        f"https://hmlet.com/en/property/{property_id}/units/{unit_id}/detail"
        f"?check_in={check_in}&check_out={check_out}"
    )


def days_earlier(primary_check_in: str, suggested_check_in: str) -> int:
    primary = date.fromisoformat(primary_check_in)
    suggested = date.fromisoformat(suggested_check_in)
    return (primary - suggested).days

def most_expensive_unit_url(units, check_in=None, check_out=None):
    """
    Returns a Hmlet URL for the most expensive unit.

    units → list of API unit dicts
    """

    if not units:
        return None

    unit = max(units, key=lambda u: u["list_price"])

    if check_in and check_out:
        return build_unit_url(
            unit["property_id"],
            unit["unit_id"],
            check_in,
            check_out,
        )

    return (
        f"https://hmlet.com/en/property/{unit['property_id']}"
        f"/units/{unit['unit_id']}/detail"
    )

def build_all_unit_urls(units, check_in=None, check_out=None):
    """
    Returns URLs for ALL units.

    units → list of API unit dicts
    """

    if not units:
        return []

    urls = []

    for unit in units:

        if check_in and check_out:
            url = build_unit_url(
                unit["property_id"],
                unit["unit_id"],
                check_in,
                check_out,
            )
        else:
            url = (
                f"https://hmlet.com/en/property/{unit['property_id']}"
                f"/units/{unit['unit_id']}/detail"
            )

        urls.append((unit, url))

    return urls



# --------------------------------------------------
# DB HELPERS
# --------------------------------------------------

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_primary_query(conn):
    return conn.execute(
        """
        SELECT query_id, check_in_date, check_out_date
        FROM queries
        WHERE is_primary = 1
        """
    ).fetchone()


def fetch_units_for_snapshot(conn, snapshot_datetime: str, query_id: int):
    return conn.execute(
        """
        SELECT
            s.unit_id,
            s.price_jpy,
            u.property_id,
            u.property_name_en,
            u.layout,
            u.city_en,
            u.size_square_meters,
            u.unit_number
        FROM availability_snapshots s
        JOIN units u ON u.unit_id = s.unit_id
        WHERE s.snapshot_datetime = ?
          AND s.query_id = ?
        """,
        (snapshot_datetime, query_id),
    ).fetchall()


def fetch_secondary_only_units_for_snapshot(
    conn,
    snapshot_datetime: str,
    primary_query_id: int,
):
    return conn.execute(
        """
        SELECT
            s.unit_id,
            u.property_id,
            u.property_name_en,
            u.layout,
            u.city_en,
            u.size_square_meters,
            u.unit_number,
            s.price_jpy,
            MAX(q.check_in_date) AS check_in_date
        FROM availability_snapshots s
        JOIN units u ON u.unit_id = s.unit_id
        JOIN queries q ON q.query_id = s.query_id
        WHERE s.snapshot_datetime = ?
          AND q.is_primary = 0
          AND s.unit_id NOT IN (
              SELECT unit_id
              FROM availability_snapshots
              WHERE snapshot_datetime = ?
                AND query_id = ?
          )
        GROUP BY s.unit_id
        """,
        (snapshot_datetime, snapshot_datetime, primary_query_id),
    ).fetchall()

# --------------------------------------------------
# SCHEMA MANAGEMENT
# --------------------------------------------------

def initialise_schema(conn):
    """
    Ensures required tables exist.
    Safe to run every time.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS property_snapshots (
            snapshot_datetime TEXT,
            property_id INTEGER,
            property_name_en TEXT,
            property_name_ja TEXT,
            available_room_count INTEGER,
            minimum_list_price INTEGER,

            PRIMARY KEY (snapshot_datetime, property_id)
        )
        """
    )

    conn.commit()


# --------------------------------------------------
# PROPERTY SNAPSHOT HELPERS
# --------------------------------------------------

def insert_property_snapshot(conn, snapshot_dt, properties):
    conn.executemany(
        """
        INSERT OR REPLACE INTO property_snapshots (
            snapshot_datetime,
            property_id,
            property_name_en,
            property_name_ja,
            available_room_count,
            minimum_list_price
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_dt,
                p["property_id"],
                p["property_name_en"],
                p["property_name_ja"],
                p["available_room_count"],
                p["minimum_list_price"],
            )
            for p in properties
        ],
    )
    conn.commit()


def get_last_two_property_snapshots(conn):
    rows = conn.execute(
        """
        SELECT DISTINCT snapshot_datetime
        FROM property_snapshots
        ORDER BY snapshot_datetime DESC
        LIMIT 2
        """
    ).fetchall()

    if len(rows) < 2:
        return None, None

    return rows[0]["snapshot_datetime"], rows[1]["snapshot_datetime"]


def fetch_properties_for_snapshot(conn, snapshot_dt):
    rows = conn.execute(
        """
        SELECT *
        FROM property_snapshots
        WHERE snapshot_datetime = ?
        """,
        (snapshot_dt,),
    ).fetchall()

    return {row["property_id"]: row for row in rows}

# --------------------------------------------------
# SORTING
# --------------------------------------------------

def sort_secondary_rows(rows, primary_check_in):
    return sorted(
        rows,
        key=lambda r: (
            days_earlier(primary_check_in, r["check_in_date"]),
            r["price_jpy"] if r["price_jpy"] is not None else 10**18,
            -(r["size_square_meters"] or 0),
        ),
    )

# --------------------------------------------------
# PROPERTY DIFF LOGIC
# --------------------------------------------------

def compare_property_snapshots(latest, previous):
    latest_ids = set(latest.keys())
    previous_ids = set(previous.keys())

    new_properties = latest_ids - previous_ids

    return new_properties

# --------------------------------------------------
# WEEKLY PROPERTY DISCOVERY
# --------------------------------------------------

def fetch_properties_opened_this_week(conn):
    """
    Properties that appeared for the first time in the last 7 days.
    """

    return conn.execute(
        """
        SELECT DISTINCT
            p.property_id,
            p.property_name_en,
            p.property_name_ja,
            p.available_room_count,
            p.minimum_list_price
        FROM property_snapshots p

        WHERE p.snapshot_datetime >= datetime('now', '-7 days')

          AND p.property_id NOT IN (
              SELECT property_id
              FROM property_snapshots
              WHERE snapshot_datetime < datetime('now', '-7 days')
          )

        ORDER BY p.minimum_list_price ASC
        """
    ).fetchall()

def unit_floor(unit_number):
    """
    Converts unit number → floor label.

    Rules:
    1 / 2 → Basement
    101 → 1st floor
    502 → 5th floor
    1003 → 10th floor
    """

    if unit_number is None:
        return None

    unit_str = str(unit_number).strip()

    if not unit_str:
        return None

    if len(unit_str) == 1:
        return 1

    if len(unit_str) >= 3:
        floor_part = unit_str[:-2]

        if floor_part.isdigit():
            return int(floor_part)

    return None


def ordinal(n):
    """
    1 → 1st
    2 → 2nd
    3 → 3rd
    """

    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")

    return f"{n}{suffix}"

def filter_out_first_floor(rows, debug=False):

    filtered = []

    for r in rows:
        floor = unit_floor(r["unit_number"])

        if floor == 1:
            if debug:
                print(f"Skipping 1st floor unit {r['unit_id']}")
            continue

        filtered.append(r)

    return filtered


def build_google_maps_search(property_name):
    if not property_name:
        return None

    query = f"Hmlet {property_name}"
    return f"https://www.google.com/maps?q={query.replace(' ', '+')}"