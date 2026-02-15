from datetime import datetime
from src.api import fetch_properties
from src.emailer import send_email
from src.hmlet_helpers import (
    get_connection,
    initialise_schema,
    insert_property_snapshot,
    get_last_two_property_snapshots,
    fetch_properties_for_snapshot,
    compare_property_snapshots,
    SUB_SEPARATOR,
)



# --------------------------------------------------
# OUTPUT
# --------------------------------------------------

def build_property_alert_message(new_properties, latest):
    lines = []
    lines.append("🗼 New Hmlet Properties Detected in Tokyo!\n")
    lines.append(SUB_SEPARATOR)

    for pid in sorted(new_properties):
        p = latest[pid]

        lines.append(
            f"▪ {p['property_name_en']} ({p['property_name_ja']})\n"
            f"  🏠 Rooms Available: {p['available_room_count']}\n"
            f"  💴 From ¥{p['minimum_list_price']:,}\n"
        )

    return "\n".join(lines)


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    conn = get_connection()
    initialise_schema(conn)


    snapshot_dt = datetime.utcnow().isoformat()

    properties = fetch_properties()

    insert_property_snapshot(conn, snapshot_dt, properties)

    latest_dt, previous_dt = get_last_two_property_snapshots(conn)

    if not latest_dt or not previous_dt:
        print("Only one property snapshot exists — baseline established.")
        conn.close()
        return

    latest = fetch_properties_for_snapshot(conn, latest_dt)
    previous = fetch_properties_for_snapshot(conn, previous_dt)

    new_properties = compare_property_snapshots(latest, previous)

    if not new_properties:
        print("✅ No new properties detected.")
        conn.close()
        return

    message = build_property_alert_message(new_properties, latest)

    print(message)

    send_email(
        subject="✨ New Hmlet Properties!",
        body=message,
    )

    conn.close()


if __name__ == "__main__":
    main()
