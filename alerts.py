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


def build_unit_url(
    property_id: int, unit_id: int, check_in: str, check_out: str
) -> str:
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


def get_last_two_snapshots(conn):
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


def diff_secondary_suggestions(
    latest_rows,
    previous_rows,
    latest_main_units,
):
    """
    Secondary suggestion diffs with stability:
    - NEW: unit_id appears for first time
    - REMOVED: unit_id fully disappears (not just moved into main)
    - PRICE CHANGE: same unit_id, different price
    """
    latest = {row["unit_id"]: row for row in latest_rows}
    previous = {row["unit_id"]: row for row in previous_rows}

    latest_ids = set(latest.keys())
    previous_ids = set(previous.keys())

    # New suggestions (same as before, stable)
    new_units = {uid: latest[uid] for uid in latest_ids - previous_ids}

    # Removed suggestions ONLY if unit no longer exists anywhere
    removed_units = {
        uid: previous[uid]
        for uid in previous_ids - latest_ids
        if uid not in latest_main_units
    }

    price_changes = [
        (latest[uid], previous[uid])
        for uid in latest_ids & previous_ids
        if latest[uid]["price_jpy"] != previous[uid]["price_jpy"]
    ]

    return new_units, removed_units, price_changes



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
    new_suggestions: dict,
    removed_suggestions: dict,
    suggestion_price_changes: list,
    primary_check_in: str,
    primary_check_out: str,
) -> str:
    lines = []

    lines.append("HMLET ALERTS\n")
    lines.append(f"Previous run: {previous_dt}")
    lines.append(f"Latest run:   {latest_dt}\n")

    # ---------------- MAIN QUERY ----------------

    if new_units:
        lines.append("✨ NEW UNITS detected in your time range ✨\n")
        for uid in sorted(new_units):
            u = latest[uid]
            url = build_unit_url(
                u["property_id"], uid, primary_check_in, primary_check_out
            )
            lines.append(
                f"+ [Unit {uid}] {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | {u['city_en']} | ¥{u['price_jpy']:,}\n"
                f"  👀➡️ {url}\n"
            )

    if removed_units:
        lines.append("❌ REMOVED UNITS detected in your time range\n")
        for uid in sorted(removed_units):
            u = previous[uid]
            url = build_unit_url(
                u["property_id"], uid, primary_check_in, primary_check_out
            )
            lines.append(
                f"- [Unit {uid}] {u['property_name_en']} | {u['layout']} | "
                f"{u['size_square_meters']} m² | {u['city_en']} | ¥{u['price_jpy']:,}\n"
                f"  👀➡️ {url}\n"
            )

    if price_changes:
        lines.append("💰 PRICE CHANGES detected in your time range\n")
        for uid in sorted(price_changes):
            l = latest[uid]
            p = previous[uid]
            arrow = "⬆️" if l["price_jpy"] > p["price_jpy"] else "⬇️"
            url = build_unit_url(
                l["property_id"], uid, primary_check_in, primary_check_out
            )
            lines.append(
                f"{arrow} [Unit {uid}] {l['property_name_en']} | {l['layout']} | "
                f"{l['size_square_meters']} m² | "
                f"¥{p['price_jpy']:,} → ¥{l['price_jpy']:,}\n"
                f"  👀➡️ {url}\n"
            )

    if not (new_units or removed_units or price_changes):
        lines.append("✅ No changes in your main search\n")

    # ---------------- SECONDARY SUGGESTIONS ----------------

    if new_suggestions:
        lines.append(
            "💡 Have you also considered these properties?\nThey have just become available if you start your lease slightly earlier:\n"
        )
        for s in new_suggestions.values():
            delta = days_earlier(primary_check_in, s["check_in_date"])
            url = build_unit_url(
                s["property_id"], s["unit_id"], s["check_in_date"], primary_check_out
            )
            lines.append(
                f"+ [Unit {s['unit_id']}] {s['property_name_en']} | {s['layout']} | "
                f"{s['size_square_meters']} m² | {s['city_en']} | ¥{s['price_jpy']:,}\n"
                f"  → {delta} days earlier ({s['check_in_date']})\n"
                f"  👀➡️ {url}\n"
            )

    if removed_suggestions:
        lines.append("❌ Removed properties detected from earlier move-in options:\n")
        for s in removed_suggestions.values():
            url = build_unit_url(
                s["property_id"], s["unit_id"], s["check_in_date"], primary_check_out
            )
            lines.append(
                f"- [Unit {s['unit_id']}] {s['property_name_en']} | {s['layout']} | "
                f"{s['size_square_meters']} m² | {s['city_en']} | ¥{s['price_jpy']:,}\n"
                f"  👀➡️ {url}\n"
            )

    if suggestion_price_changes:
        lines.append("💰 Price changes detected for earlier move in options:\n")
        for l, p in suggestion_price_changes:
            arrow = "⬆️" if l["price_jpy"] > p["price_jpy"] else "⬇️"
            url = build_unit_url(
                l["property_id"], l["unit_id"], l["check_in_date"], primary_check_out
            )
            lines.append(
                f"{arrow} [Unit {l['unit_id']}] {l['property_name_en']} | {l['layout']} | "
                f"{l['size_square_meters']} m²\n"
                f"  ¥{p['price_jpy']:,} → ¥{l['price_jpy']:,}\n"
                f"  👀➡️ {url}\n"
            )

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
        return

    primary_query_id = primary["query_id"]
    primary_check_in = primary["check_in_date"]
    primary_check_out = primary["check_out_date"]

    latest_dt, previous_dt = get_last_two_snapshots(conn)
    if not latest_dt or not previous_dt:
        print("Only one snapshot exists — nothing to compare yet.")
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

    (
        new_suggestions,
        removed_suggestions,
        suggestion_price_changes,
    ) = diff_secondary_suggestions(
        latest_suggestions,
        previous_suggestions,
        latest_main_units=set(latest.keys()),
    )


    # FIX: explicit single gate for email sending
    changes_detected = any([
        new_units,
        removed_units,
        price_changes,
        new_suggestions,
        removed_suggestions,
        suggestion_price_changes,
    ])

    if not changes_detected:
        print("No changes detected — email not sent.")
        conn.close()
        return

    message = build_alert_message(
        latest_dt,
        previous_dt,
        latest,
        previous,
        new_units,
        removed_units,
        price_changes,
        new_suggestions,
        removed_suggestions,
        suggestion_price_changes,
        primary_check_in,
        primary_check_out,
    )

    print(message)

    send_email(
        subject="🏠 Hmlet property update",
        body=message,
    )

    conn.close()


if __name__ == "__main__":
    main()
