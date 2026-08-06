#!/usr/bin/env python

from spec import CASES_FILE


def main() -> int:
    print(
        "Rust script datasources are not implemented yet; benchmark cases are "
        f"shared from {CASES_FILE}."
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
