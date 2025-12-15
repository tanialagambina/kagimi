import sqlite3
from typing import List, Dict

from src.config import DB_PATH, SNAPSHOT_DATETIME
from src.parsing import parse_lat_lon, parse_date_to_datetime



def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS units (
            unit_id INTEGER PRIMARY KEY,
            property_id INTEGER,
            property_name_en TEXT,
            property_name_ja TEXT,
            unit_number INTEGER,
            layout TEXT,
            size_square_meters REAL,
            city_en TEXT,
            city_ja TEXT,
            coordinates TEXT,
            latitude REAL,
            longitude REAL
        );

        CREATE TABLE IF NOT EXISTS availability_snapshots (
            snapshot_datetime DATETIME NOT NULL,
            query_id INTEGER NOT NULL,
            unit_id INTEGER NOT NULL,
            price_jpy INTEGER,
            earliest_move_in_datetime DATETIME,
            reviews INTEGER,
            rating REAL,
            PRIMARY KEY (snapshot_datetime, query_id, unit_id),
            FOREIGN KEY (unit_id) REFERENCES units(unit_id),
            FOREIGN KEY (query_id) REFERENCES queries(query_id)
        );

        CREATE TABLE IF NOT EXISTS queries (
            query_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            check_in_date DATE NOT NULL,
            check_out_date DATE NOT NULL,
            is_primary BOOLEAN NOT NULL
        );
        """
    )


def upsert_units(conn: sqlite3.Connection, units: List[Dict]):
    sql = """
    INSERT INTO units (
        unit_id, property_id, property_name_en, property_name_ja,
        unit_number, layout, size_square_meters,
        city_en, city_ja, coordinates, latitude, longitude
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(unit_id) DO UPDATE SET
        property_id = excluded.property_id,
        property_name_en = excluded.property_name_en,
        property_name_ja = excluded.property_name_ja,
        unit_number = excluded.unit_number,
        layout = excluded.layout,
        size_square_meters = excluded.size_square_meters,
        city_en = excluded.city_en,
        city_ja = excluded.city_ja,
        coordinates = excluded.coordinates,
        latitude = excluded.latitude,
        longitude = excluded.longitude;
    """

    rows = []
    for u in units:
        lat, lon = parse_lat_lon(u["coordinates"])
        rows.append((
            u["unit_id"],
            u["property_id"],
            u["property_name_en"],
            u["property_name_ja"],
            u.get("unit_number"),
            u["layout"],
            u["size_square_meters"],
            u["city_en"],
            u["city_ja"],
            u["coordinates"],
            lat,
            lon,
        ))

    conn.executemany(sql, rows)


def upsert_snapshots(
    conn: sqlite3.Connection,
    units: list[dict],
    *,
    query_id: int,
):
    sql = """
    INSERT INTO availability_snapshots (
        snapshot_datetime,
        query_id,
        unit_id,
        price_jpy,
        earliest_move_in_datetime,
        reviews,
        rating
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(snapshot_datetime, query_id, unit_id) DO UPDATE SET
        price_jpy = excluded.price_jpy,
        earliest_move_in_datetime = excluded.earliest_move_in_datetime,
        reviews = excluded.reviews,
        rating = excluded.rating;
    """

    rows = [
        (
            SNAPSHOT_DATETIME,
            query_id,  # ✅ THIS WAS MISSING
            u["unit_id"],
            u["list_price"],
            parse_date_to_datetime(u.get("earliest_move_in_date")),
            u["total_reviews"],
            u["overall_score"],
        )
        for u in units
    ]

    conn.executemany(sql, rows)



def get_or_create_query(
    conn: sqlite3.Connection,
    name: str,
    check_in_date: str,
    check_out_date: str,
    is_primary: bool,
) -> int:
    """
    Returns query_id for the given query definition.
    Creates the query if it does not already exist.
    """

    row = conn.execute(
        """
        SELECT query_id
        FROM queries
        WHERE name = ?
          AND check_in_date = ?
          AND check_out_date = ?
        """,
        (name, check_in_date, check_out_date),
    ).fetchone()

    if row:
        return row["query_id"]

    cursor = conn.execute(
        """
        INSERT INTO queries (
            name,
            check_in_date,
            check_out_date,
            is_primary
        ) VALUES (?, ?, ?, ?)
        """,
        (name, check_in_date, check_out_date, int(is_primary)),
    )

    return cursor.lastrowid

