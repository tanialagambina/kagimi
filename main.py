from src.api import fetch_all_units
from src.config import QUERIES
from data.csv_writer import write_csv
from data.sqlite_db import (
    get_connection,
    init_db,
    upsert_units,
    upsert_snapshots,
    get_or_create_query,
)


def main():
    conn = get_connection()
    init_db(conn)

    all_units_seen = {}

    for query in QUERIES:
        print(
            f"\nFetching units for query '{query['name']}' "
            f"({query['check_in']} → {query['check_out']})"
        )

        # 1️⃣ Get query_id
        query_id = get_or_create_query(
            conn,
            name=query["name"],
            check_in_date=query["check_in"],
            check_out_date=query["check_out"],
            is_primary=query["is_primary"],
        )

        # 2️⃣ Fetch units for THIS query
        units = fetch_all_units(
            check_in=query["check_in"],
            check_out=query["check_out"],
        )

        print(f"  → {len(units)} units found")

        # 3️⃣ Store global units (deduplicated)
        upsert_units(conn, units)

        # 4️⃣ Store snapshots FOR THIS QUERY
        upsert_snapshots(conn, units, query_id=query_id)

        # Track for CSV (optional combined output)
        for u in units:
            all_units_seen[u["unit_id"]] = u

    # Optional: write combined CSV
    write_csv(list(all_units_seen.values()))
    print("\nCSV written")

    conn.commit()
    conn.close()

    print(f"\nDone ✔ ({len(all_units_seen)} unique units across all queries)")


if __name__ == "__main__":
    main()
