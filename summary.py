import sqlite3
from pathlib import Path
from datetime import date
from src.emailer import send_email


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

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


def get_latest_snapshot_datetime(conn):
    row = conn.execute(
        """
        SELECT snapshot_datetime
        FROM availability_snapshots
        ORDER BY snapshot_datetime DESC
        LIMIT 1
        """
    ).fetchone()
    return row["snapshot_datetime"] if row else None


def fetch_units_for_snapshot(conn, snapshot_datetime: str, query_id: int):
    rows = conn.execute(
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

    return rows


def fetch_secondary_only_units_for_snapshot(
    conn,
    snapshot_datetime: str,
    primary_query_id: int,
):
    """
    "Secondary-only" = units that are available in ANY non-primary query snapshot,
    but NOT available in the primary query snapshot, at the same snapshot_datetime.

    Note: This groups by unit_id and uses MAX(check_in_date) just like your alerts script.
    """
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
# OUTPUT
# --------------------------------------------------


def build_roundup_message(
    snapshot_dt: str,
    primary_check_in: str,
    primary_check_out: str,
    primary_units_rows: list,
    secondary_rows: list,
) -> str:
    lines = []
    lines.append("🗼 Your Hmlet Weekly Roundup\n")
    lines.append("A summary of availability for your preferred and alternative dates, could one of these be your future home?\n")
    lines.append("☕ Grab a drink and take a moment to browse this week's available units!\n")
    lines.append(f"Query dates: {primary_check_in} → {primary_check_out}\n")
    lines.append(SUB_SEPARATOR)

    # Sort primary units by price then size (nice predictable order)
    primary_units_rows_sorted = sorted(
        primary_units_rows,
        key=lambda r: (r["price_jpy"] if r["price_jpy"] is not None else 10**18, -(r["size_square_meters"] or 0)),
    )

    lines.append("🏠 Available units in your primary time range:\n")
    if not primary_units_rows_sorted:
        lines.append("  (No units available for the primary dates in this snapshot.)\n")
    else:
        for r in primary_units_rows_sorted:
            url = build_unit_url(r["property_id"], r["unit_id"], primary_check_in, primary_check_out)
            lines.append(
                f"▪ [Unit {r['unit_id']}] {r['property_name_en']} | {r['layout']} | "
                f"{r['size_square_meters']} m² | {r['city_en']} | 💴 ¥{r['price_jpy']:,}\n"
                f"  ➡️ {url}\n"
            )

    lines.append(SUB_SEPARATOR)
    # Secondary suggestions ordered by "how many days earlier" (smallest first)
    # i.e. 1 day earlier first, 15 days earlier last
    secondary_sorted = sorted(
        secondary_rows,
        key=lambda r: (
            days_earlier(primary_check_in, r["check_in_date"]),  # delta asc
            r["price_jpy"] if r["price_jpy"] is not None else 10**18,  # then cheaper first
            -(r["size_square_meters"] or 0),  # then larger first
        ),
    )

    lines.append("💡 Have you also considered these properties?\n")
    lines.append("They are also available if you start your lease slightly earlier!\n")
    lines.append("ℹ️ You can pay for the extra days at the start of the lease, but physically move in on your preferred date.\n")
    if not secondary_sorted:
        lines.append("  (No secondary suggestions in this snapshot.)\n")
    else:
        for r in secondary_sorted:
            delta = days_earlier(primary_check_in, r["check_in_date"])
            url = build_unit_url(r["property_id"], r["unit_id"], r["check_in_date"], primary_check_out)
            lines.append(
                f"▪ [Unit {r['unit_id']}] {r['property_name_en']} | {r['layout']} | "
                f"{r['size_square_meters']} m² | {r['city_en']} | 💴 ¥{r['price_jpy']:,}\n"
                f"  📆 {delta} days earlier ({r['check_in_date']})\n"
                f"  ➡️ {url}\n"
            )
    lines.append(SUB_SEPARATOR)
    lines.append(f"\n\nSnapshot taken at: {snapshot_dt}\n")
    lines.append("Generated by your friendly Hmlet unit availability bot 🤖\n")
    lines.append("Have a great day! 🌞\n")

    return "\n".join(lines)



# --------------------------------------------------
# MAIN
# --------------------------------------------------


def main():
    if not DB_PATH.exists():
        print("Database not found — run scraper first.")
        return

    conn = get_connection()

    primary = get_primary_query(conn)
    if not primary:
        print("No primary query defined.")
        conn.close()
        return

    primary_query_id = primary["query_id"]
    primary_check_in = primary["check_in_date"]
    primary_check_out = primary["check_out_date"]

    snapshot_dt = get_latest_snapshot_datetime(conn)
    if not snapshot_dt:
        print("No snapshots exist yet — nothing to summarise.")
        conn.close()
        return

    primary_units = fetch_units_for_snapshot(conn, snapshot_dt, primary_query_id)
    secondary_units = fetch_secondary_only_units_for_snapshot(conn, snapshot_dt, primary_query_id)

    message = build_roundup_message(
        snapshot_dt=snapshot_dt,
        primary_check_in=primary_check_in,
        primary_check_out=primary_check_out,
        primary_units_rows=primary_units,
        secondary_rows=secondary_units,
    )

    print(message)

    send_email(
        subject="📬 Hmlet Weekly Roundup",
        body=message,
    )

    conn.close()


if __name__ == "__main__":
    main()
