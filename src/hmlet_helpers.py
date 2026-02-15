import sqlite3
from pathlib import Path
from datetime import date


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
            u.size_square_meters
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
