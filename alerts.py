import sqlite3
from pathlib import Path
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


def fetch_units_for_snapshot(conn, snapshot_datetime: str):
    """
    Returns dict: unit_id -> snapshot row
    """
    rows = conn.execute(
        """
        SELECT
            s.unit_id,
            s.price_jpy,
            s.earliest_move_in_datetime,
            u.property_name_en,
            u.layout,
            u.city_en,
            u.size_square_meters
        FROM availability_snapshots s
        JOIN units u ON u.unit_id = s.unit_id
        WHERE s.snapshot_datetime = ?
        """,
        (snapshot_datetime,),
    ).fetchall()

    return {row["unit_id"]: row for row in rows}

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
    latest,
    previous,
    new_units,
    removed_units,
    price_changes,
) -> str:
    lines = []

    lines.append("HMLET ALERTS\n")
    lines.append(f"Previous run: {previous_dt}")
    lines.append(f"Latest run:   {latest_dt}\n")

    if new_units:
        lines.append("🆕 NEW UNITS")
        for uid in sorted(new_units):
            u = latest[uid]
            lines.append(
                f"+ {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | "
                f"{u['city_en']} | ¥{u['price_jpy']:,}"
            )
        lines.append("")

    if removed_units:
        lines.append("❌ REMOVED UNITS")
        for uid in sorted(removed_units):
            u = previous[uid]
            lines.append(
                f"- {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | "
                f"{u['city_en']} | ¥{u['price_jpy']:,}"
            )
        lines.append("")

    if price_changes:
        lines.append("💰 PRICE CHANGES")
        for uid in sorted(price_changes):
            l = latest[uid]
            p = previous[uid]
            arrow = "⬆️" if l["price_jpy"] > p["price_jpy"] else "⬇️"
            lines.append(
                f"{arrow} {l['property_name_en']} | {l['layout']} | "
                f"{l['size_square_meters']} m² | "
                f"¥{p['price_jpy']:,} → ¥{l['price_jpy']:,}"
            )
        lines.append("")

    if not (new_units or removed_units or price_changes):
        lines.append("✅ No changes detected")

    return "\n".join(lines)


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    if not DB_PATH.exists():
        print("Database not found — run scraper first.")
        return

    conn = get_connection()

    latest_dt, previous_dt = get_last_two_snapshots(conn)

    if not latest_dt or not previous_dt:
        print("Only one snapshot exists — nothing to compare yet.")
        conn.close()
        return

    latest = fetch_units_for_snapshot(conn, latest_dt)
    previous = fetch_units_for_snapshot(conn, previous_dt)

    new_units, removed_units, price_changes = compare_snapshots(
        latest, previous
    )

    message = build_alert_message(
        latest_dt,
        previous_dt,
        latest,
        previous,
        new_units,
        removed_units,
        price_changes,
    )

    print(message)

    if new_units or removed_units or price_changes:
        send_email(
            subject="🏠 Hmlet property update",
            body=message,
        )

    conn.close()


if __name__ == "__main__":
    main()
