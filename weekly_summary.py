from src.emailer import send_email
from src.hmlet_helpers import (
    get_connection,
    get_primary_query,
    build_unit_url,
    days_earlier,
    fetch_units_for_snapshot,
    fetch_secondary_only_units_for_snapshot,
    fetch_properties_opened_this_week,
    build_all_unit_urls,
    DB_PATH,
    SUB_SEPARATOR,
    SEPARATOR,
)
from src.api import fetch_units_for_property

# --------------------------------------------------
# SNAPSHOT HELPERS
# --------------------------------------------------


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


# --------------------------------------------------
# OUTPUT
# --------------------------------------------------


def build_roundup_message(
    snapshot_dt: str,
    primary_check_in: str,
    primary_check_out: str,
    primary_units_rows: list,
    secondary_rows: list,
    new_properties_this_week: list,
) -> str:
    lines = []
    lines.append("🗼 Your Hmlet Weekly Roundup\n")
    lines.append(
        "A summary of availability for your preferred and alternative dates, could one of these be your future home?\n"
    )
    lines.append(
        "☕ Grab a drink and take a moment to browse this week's available units!\n"
    )
    lines.append(f"Query dates: {primary_check_in} → {primary_check_out}\n")
    lines.append(SUB_SEPARATOR)

    # Sort primary units by price then size (nice predictable order)
    primary_units_rows_sorted = sorted(
        primary_units_rows,
        key=lambda r: (
            r["price_jpy"] if r["price_jpy"] is not None else 10**18,
            -(r["size_square_meters"] or 0),
        ),
    )

    lines.append("🏠 Available units in your primary time range:\n")
    if not primary_units_rows_sorted:
        lines.append("  (No units available for the primary dates in this snapshot.)\n")
    else:
        for r in primary_units_rows_sorted:
            url = build_unit_url(
                r["property_id"], r["unit_id"], primary_check_in, primary_check_out
            )
            lines.append(
                f"▪ [Unit {r['unit_id']}] {r['property_name_en']} | {r['layout']} | 🔑 {r['unit_number']} | "
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
            (
                r["price_jpy"] if r["price_jpy"] is not None else 10**18
            ),  # then cheaper first
            -(r["size_square_meters"] or 0),  # then larger first
        ),
    )

    lines.append("💡 Have you also considered these properties?")
    lines.append("They are available if you start your lease slightly earlier!")
    lines.append(
        "ℹ️ You can pay for the extra days at the start of the lease, but physically move in on your preferred date.\n"
    )
    if not secondary_sorted:
        lines.append("  (No secondary suggestions in this snapshot.)\n")
    else:
        for r in secondary_sorted:
            delta = days_earlier(primary_check_in, r["check_in_date"])
            url = build_unit_url(
                r["property_id"], r["unit_id"], r["check_in_date"], primary_check_out
            )
            lines.append(
                f"▪ [Unit {r['unit_id']}] {r['property_name_en']} | {r['layout']} | 🔑 {r['unit_number']} | "
                f"{r['size_square_meters']} m² | {r['city_en']} | 💴 ¥{r['price_jpy']:,}\n"
                f"  📆 {delta} days earlier ({r['check_in_date']})\n"
                f"  ➡️ {url}\n"
            )

        if new_properties_this_week:
            lines.append(SUB_SEPARATOR)
            lines.append("🎉 New buildings opened this week:\n")
            lines.append(
                "While there may not be availability for your dates yet, "
                "they're worth keeping an eye on as there could be soon!\n"
            )

            for p in sorted(new_properties_this_week, key=lambda r: r["minimum_list_price"]):

                units = fetch_units_for_property(p["property_id"])
                unit_urls = build_all_unit_urls(units)
                unit_urls = sorted(unit_urls, key=lambda x: x[0]["list_price"])


                lines.append(
                    f"🏢 [Property {p['property_id']}] "
                    f"{p['property_name_en']} ({p['property_name_ja']})\n"
                    f"💴 From ¥{p['minimum_list_price']:,}\n"
                )

                if not unit_urls:
                    lines.append("  (No units currently available)\n")
                else:
                    for unit, url in unit_urls:
                        lines.append(
                            f"▪ [Unit {unit['unit_id']}] "
                            f"{unit['layout']} | 🔑 {unit['unit_number']} | {unit['size_square_meters']} m² | "
                            f"💴 ¥{unit['list_price']:,}\n"
                            f"  ➡️ {url}\n"
                        )

                lines.append(SEPARATOR)



    lines.append(SUB_SEPARATOR)
    lines.append(f"Snapshot taken at: {snapshot_dt}\n")
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
    secondary_units = fetch_secondary_only_units_for_snapshot(
        conn, snapshot_dt, primary_query_id
    )
    new_properties_this_week = fetch_properties_opened_this_week(conn)

    message = build_roundup_message(
        snapshot_dt=snapshot_dt,
        primary_check_in=primary_check_in,
        primary_check_out=primary_check_out,
        primary_units_rows=primary_units,
        secondary_rows=secondary_units,
        new_properties_this_week=new_properties_this_week,
    )

    print(message)

    send_email(
        subject="📬 Hmlet Weekly Roundup",
        body=message,
    )

    conn.close()


if __name__ == "__main__":
    main()
