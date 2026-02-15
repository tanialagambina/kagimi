from datetime import datetime
from src.api import fetch_properties, fetch_units_for_property
from src.emailer import send_email
from src.hmlet_helpers import (
    get_connection,
    initialise_schema,
    insert_property_snapshot,
    get_last_two_property_snapshots,
    fetch_properties_for_snapshot,
    compare_property_snapshots,
    build_all_unit_urls,
    unit_floor,
    ordinal,
    build_google_maps_search,
    SUB_SEPARATOR,
    SEPARATOR,
    BOT_SIGN_OFF,
)



# --------------------------------------------------
# OUTPUT
# --------------------------------------------------


def build_property_alert_message(new_properties, latest, latest_dt):
    lines = []
    lines.append("🗼🎉 New buildings opened in Tokyo!")
    lines.append(
        "These buildings have just opened — explore available units:\n"
    )
    lines.append(SUB_SEPARATOR)

    for pid in sorted(new_properties):
        p = latest[pid]

        units = fetch_units_for_property(p["property_id"])
        unit_urls = build_all_unit_urls(units)

        maps_link = build_google_maps_search(p["property_name_en"])

        lines.append(
            f"🏢 [Property {p['property_id']}] "
            f"{p['property_name_en']} ({p['property_name_ja']})\n"
            f"💴 From ¥{p['minimum_list_price']:,}\n"
            f"📍 {maps_link}\n"
        )

        if not unit_urls:
            lines.append("  (No units currently available for selected filters)\n")
        else:
            for unit, url in unit_urls:
                lines.append(
                    f"▪ [Unit {unit['unit_id']}] "
                    f"{unit['layout']} | {ordinal(unit_floor(unit['unit_number']))} floor | {unit['size_square_meters']} m² | "
                    f"💴 ¥{unit['list_price']:,}\n"
                    f"  ➡️ {url}\n"
                )

        lines.append(SEPARATOR)

    lines.append(SUB_SEPARATOR)
    lines.append(f"Snapshot taken at: {latest_dt}\n")
    lines.append(BOT_SIGN_OFF)

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

    message = build_property_alert_message(new_properties, latest, latest_dt)

    print(message)

    send_email(
        subject="✨ New Hmlet Properties!",
        body=message,
    )

    conn.close()


if __name__ == "__main__":
    main()
