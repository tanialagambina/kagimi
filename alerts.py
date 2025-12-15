import sqlite3
from pathlib import Path
from datetime import date
from src.emailer import send_email


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

DB_PATH = Path("out/hmlet_units.sqlite")


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
        SELECT query_id, check_in_date
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
    One row per unit:
    closest (latest) check-in date before the main date
    """
    return conn.execute(
        """
        SELECT
            s.unit_id,
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


# --------------------------------------------------
# OUTPUT
# --------------------------------------------------

def build_alert_message(
    latest_dt,
    previous_dt,
    primary_check_in: str,
    latest,
    previous,
    new_units,
    removed_units,
    price_changes,
    suggestions,
) -> str:
    lines = []

    primary_date = date.fromisoformat(primary_check_in)

    lines.append("HMLET ALERTS\n")
    lines.append(f"Previous run: {previous_dt}")
    lines.append(f"Latest run:   {latest_dt}\n")

    main_query_changed = bool(new_units or removed_units or price_changes)

    # ---------------- MAIN QUERY RESULTS ----------------

    if new_units:
        lines.append("🆕 NEW UNITS (Main search)")
        for uid in sorted(new_units):
            u = latest[uid]
            lines.append(
                f"+ [Unit {uid}] {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | {u['city_en']} | "
                f"¥{u['price_jpy']:,}"
            )
        lines.append("")

    if removed_units:
        lines.append("❌ REMOVED UNITS (Main search)")
        for uid in sorted(removed_units):
            u = previous[uid]
            lines.append(
                f"- [Unit {uid}] {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | {u['city_en']} | "
                f"¥{u['price_jpy']:,}"
            )
        lines.append("")

    if price_changes:
        lines.append("💰 PRICE CHANGES (Main search)")
        for uid in sorted(price_changes):
            l = latest[uid]
            p = previous[uid]
            arrow = "⬆️" if l["price_jpy"] > p["price_jpy"] else "⬇️"
            lines.append(
                f"{arrow} [Unit {uid}] {l['property_name_en']} | {l['layout']} | "
                f"{l['size_square_meters']} m² | "
                f"¥{p['price_jpy']:,} → ¥{l['price_jpy']:,}"
            )
        lines.append("")

    if not main_query_changed:
        lines.append("✅ No changes in your main search\n")

    # ---------------- SECONDARY QUERY SUGGESTIONS ----------------

    if suggestions:
        lines.append("💡 HAVE YOU ALSO CONSIDERED…")
        lines.append(
            "These homes aren’t available for your main dates,\n"
            "but WOULD be if you moved in slightly earlier:\n"
        )

        for s in suggestions:
            suggestion_date = date.fromisoformat(s["check_in_date"])
            days_earlier = (primary_date - suggestion_date).days

            lines.append(
                f"+ [Unit {s['unit_id']}] {s['property_name_en']} | {s['layout']} | "
                f"{s['size_square_meters']} m² | {s['city_en']} | "
                f"¥{s['price_jpy']:,}\n"
                f"  → Available if you moved in on {s['check_in_date']} "
                f"({days_earlier} days earlier)"
            )

        lines.append("")

    if not main_query_changed and not suggestions:
        lines.append("ℹ️ No changes detected across any searches")

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

    latest_dt, previous_dt = get_last_two_snapshots(conn)
    if not latest_dt or not previous_dt:
        print("Only one snapshot exists — nothing to compare yet.")
        conn.close()
        return

    latest = fetch_units_for_snapshot(conn, latest_dt, primary_query_id)
    previous = fetch_units_for_snapshot(conn, previous_dt, primary_query_id)

    new_units, removed_units, price_changes = compare_snapshots(
        latest, previous
    )

    suggestions = fetch_secondary_only_units(
        conn, latest_dt, primary_query_id
    )

    message = build_alert_message(
        latest_dt,
        previous_dt,
        primary_check_in,
        latest,
        previous,
        new_units,
        removed_units,
        price_changes,
        suggestions,
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
