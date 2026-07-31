"""Write the demo's raw order feed.

`--late-day` rewrites one day's rows with a newer `updated_at`, which is how the
demo produces a single stale partition without touching the others.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

DATA = Path(__file__).parent / "data" / "orders.csv"
START = date(2024, 1, 1)
DAYS = 4
REGIONS = ("north", "south")
HEADER = ["order_id", "order_date", "region", "amount", "updated_at"]


def rows(late_day: date | None) -> list[list[str]]:
    rng = random.Random(17)
    out: list[list[str]] = []
    order_id = 1
    for offset in range(DAYS):
        day = START + timedelta(days=offset)
        loaded_at = datetime(2024, 1, 5, 6, 0, 0) + timedelta(minutes=offset)
        if late_day is not None and day == late_day:
            loaded_at = datetime(2024, 1, 6, 9, 30, 0)
        for region in REGIONS:
            for _ in range(rng.randint(3, 6)):
                out.append(
                    [
                        str(order_id),
                        day.isoformat(),
                        region,
                        f"{rng.uniform(10, 500):.2f}",
                        loaded_at.isoformat(sep=" "),
                    ]
                )
                order_id += 1
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--late-day",
        default=None,
        help="ISO date whose rows get a newer updated_at (e.g. 2024-01-02)",
    )
    args = parser.parse_args()
    late = date.fromisoformat(args.late_day) if args.late_day else None

    DATA.parent.mkdir(parents=True, exist_ok=True)
    with DATA.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows(late))
    print(f"wrote {DATA}" + (f" (late arrivals on {late})" if late else ""))


if __name__ == "__main__":
    main()
