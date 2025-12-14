from src.api import fetch_all_units
from data.csv_writer import write_csv
from data.sqlite_db import (
    get_connection,
    init_db,
    upsert_units,
    upsert_snapshots,
)


def main():
    print("Fetching units...")
    units = fetch_all_units()

    write_csv(units)
    print("CSV written")

    conn = get_connection()
    init_db(conn)

    upsert_units(conn, units)
    upsert_snapshots(conn, units)

    conn.commit()
    conn.close()

    print(f"Done ✔ ({len(units)} units)")


if __name__ == "__main__":
    main()
