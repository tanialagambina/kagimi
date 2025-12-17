import sqlite3
from pathlib import Path
from datetime import date
from src.emailer import send_email


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

DB_PATH = Path("out/hmlet_units.sqlite")


# --------------------------------------------------
# URL HELPERS
# --------------------------------------------------

def build_unit_url(property_id: int, unit_id: int, check_in: str, check_out: str) -> str:
    return (
        f"https://hmlet.com/en/property/{property_id}/units/{unit_id}/detail"
        f"?check_in={check_in}&check_out={check_out}"
    )

def days_earlier(primary_check_in: str, suggested_check_in: str) -> int:
    """
    Returns how many days earlier the suggested date is vs the primary date.
    """
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
    """
    Returns the primary query row.
    """
    return conn.execute(
        """
        SELECT query_id, check_in_date, check_out_date
        FROM queries
        WHERE is_primary = 1
        """
    ).fetchone()


def get_last_two_snapshots(conn):
    """
    Returns (latest_snapshot_datetime, previous_snapshot_datetime)
    """
    rows = conn.execute(
        """
        SELECT DISTINCT snapshot_datetime
        FROM availability_snapshots
        ORDER BY snapshot_datetime DESC
        LIMIT 2
        """
    ).fetchall()

    if len(rows) < 2:
        return None, None

    return rows[0]["snapshot_datetime"], rows[1]["snapshot_datetime"]


def fetch_units_for_snapshot(conn, snapshot_datetime: str, query_id: int):
    """
    Returns dict: unit_id -> snapshot row (for a specific query)
    """
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

    return {row["unit_id"]: row for row in rows}


def fetch_secondary_only_units(conn, snapshot_datetime: str, primary_query_id: int):
    """
    Returns ONE row per unit:
    the closest (latest) secondary check-in date (via MAX) where the unit is available
    but it is NOT in the primary query results.
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
        ORDER BY check_in_date DESC, s.price_jpy
        """,
        (snapshot_datetime, snapshot_datetime, primary_query_id),
    ).fetchall()

def fetch_secondary_only_units_for_snapshot(
    conn,
    snapshot_datetime: str,
    primary_query_id: int,
):
    """
    Secondary-query units NOT in primary query for a given snapshot.
    One row per unit, using latest possible check-in date.
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
# DIFF LOGIC
# --------------------------------------------------

def compare_snapshots(latest, previous):
    latest_ids = set(latest.keys())
    previous_ids = set(previous.keys())

    new_units = latest_ids - previous_ids
    removed_units = previous_ids - latest_ids
    common_units = latest_ids & previous_ids

    price_changes = [
        uid
        for uid in common_units
        if latest[uid]["price_jpy"] != previous[uid]["price_jpy"]
    ]

    return new_units, removed_units, price_changes

def diff_suggestions(latest_rows, previous_rows):
    """
    Return only NEW secondary-query suggestions
    """
    latest_by_id = {row["unit_id"]: row for row in latest_rows}
    previous_ids = {row["unit_id"] for row in previous_rows}

    return [
        row
        for uid, row in latest_by_id.items()
        if uid not in previous_ids
    ]

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------

def build_alert_message(
    latest_dt: str,
    previous_dt: str,
    latest: dict,
    previous: dict,
    new_units: set,
    removed_units: set,
    price_changes: list,
    suggestions,
    primary_check_in: str,
    primary_check_out: str,
) -> str:
    lines = []

    lines.append("HMLET ALERTS\n")
    lines.append(f"Previous run: {previous_dt}")
    lines.append(f"Latest run:   {latest_dt}\n")

    main_query_changed = bool(new_units or removed_units or price_changes)

    # ---------------- MAIN QUERY RESULTS ----------------

    if new_units:
        lines.append("🆕 NEW UNITS (Main search)")
        for uid in sorted(new_units):
            u = latest[uid]
            url = build_unit_url(
                property_id=u["property_id"],
                unit_id=uid,
                check_in=primary_check_in,
                check_out=primary_check_out,
            )
            lines.append(
                f"+ [Unit {uid}] {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | {u['city_en']} | "
                f"¥{u['price_jpy']:,}\n"
                f"  {url}"
            )
        lines.append("")

    if removed_units:
        lines.append("❌ REMOVED UNITS (Main search)")
        for uid in sorted(removed_units):
            u = previous[uid]
            url = build_unit_url(
                property_id=u["property_id"],
                unit_id=uid,
                check_in=primary_check_in,
                check_out=primary_check_out,
            )
            lines.append(
                f"- [Unit {uid}] {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | {u['city_en']} | "
                f"¥{u['price_jpy']:,}\n"
                f"  {url}"
            )
        lines.append("")

    if price_changes:
        lines.append("💰 PRICE CHANGES (Main search)")
        for uid in sorted(price_changes):
            l = latest[uid]
            p = previous[uid]
            diff = l["price_jpy"] - p["price_jpy"]
            arrow = "⬆️" if diff > 0 else "⬇️"
            url = build_unit_url(
                property_id=l["property_id"],
                unit_id=uid,
                check_in=primary_check_in,
                check_out=primary_check_out,
            )
            lines.append(
                f"{arrow} [Unit {uid}] {l['property_name_en']} | {l['layout']} | "
                f"{l['size_square_meters']} m² | "
                f"¥{p['price_jpy']:,} → ¥{l['price_jpy']:,}\n"
                f"  {url}"
            )
        lines.append("")

    if not main_query_changed:
        lines.append("✅ No changes in your main search\n")

    # ---------------- SECONDARY QUERY SUGGESTIONS ----------------

    if suggestions:
        lines.append("💡 Have you also considered…")
        lines.append(
            "These homes aren’t available for your selected move in date, "
            "but have just become available if you started your lease slightly earlier.\n"
            "For each unit, this is the latest move-in date we found (closest to your main date):\n"
        )

        for s in suggestions:
            suggested_check_in = s["check_in_date"]  # MAX() from SQL
            url = build_unit_url(
                property_id=s["property_id"],
                unit_id=s["unit_id"],
                check_in=suggested_check_in,
                check_out=primary_check_out,
            )
            delta_days = days_earlier(primary_check_in, suggested_check_in)

            lines.append(
                f"+ [Unit {s['unit_id']}] {s['property_name_en']} | {s['layout']} | "
                f"{s['size_square_meters']} m² | {s['city_en']} | "
                f"¥{s['price_jpy']:,}\n"
                f"  → Available if you moved in {delta_days} days earlier "
                f"(on {suggested_check_in})\n"
                f"  👀➡️ {url}\n"
            )

        lines.append("")

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

    latest_dt, previous_dt = get_last_two_snapshots(conn)
    if not latest_dt or not previous_dt:
        print("Only one snapshot exists — nothing to compare yet.")
        conn.close()
        return

    latest = fetch_units_for_snapshot(conn, latest_dt, primary_query_id)
    previous = fetch_units_for_snapshot(conn, previous_dt, primary_query_id)

    new_units, removed_units, price_changes = compare_snapshots(latest, previous)

    latest_suggestions = fetch_secondary_only_units_for_snapshot(
    conn, latest_dt, primary_query_id
    )

    previous_suggestions = fetch_secondary_only_units_for_snapshot(
        conn, previous_dt, primary_query_id
    )

    suggestions = diff_suggestions(
        latest_suggestions,
        previous_suggestions,
    )


    message = build_alert_message(
        latest_dt=latest_dt,
        previous_dt=previous_dt,
        latest=latest,
        previous=previous,
        new_units=new_units,
        removed_units=removed_units,
        price_changes=price_changes,
        suggestions=suggestions,
        primary_check_in=primary_check_in,
        primary_check_out=primary_check_out,
    )

    print(message)

    if new_units or removed_units or price_changes or suggestions:
        send_email(
            subject="🏠 Hmlet property update",
            body=message,
        )

    conn.close()


if __name__ == "__main__":
    main()
