"""Repro: a compound boolean (`and` / `or`) in an `auto` (derived-concept)
assignment fails to parse, while the same expression parses fine inside a `?`
inline filter or a `where` clause.

    auto x <- flag = 1;             # OK  (single comparison)
    auto x <- flag + 1;             # OK  (arithmetic)
    auto x <- flag = 1 and id = 1;  # FAIL InvalidSyntaxException
    auto x <- d between a and b;    # FAIL (between is a compound predicate)
    sum(v ? flag = 1 and id = 1)    # OK  (compound bool allowed in ? filter)
    where flag = 1 and id = 1 ...   # OK  (compound bool allowed in where)

Workaround: inline the compound condition in the `?` filter / `where` rather than
naming it as a derived concept. Surfaced writing tpc_ds query05.preql (wanted
`auto in_sale <- (date between ...) and dim is not null;`).
"""

from trilogy.parsing.parse_engine_v2 import parse_text

PRELUDE = """
key id int;
property id.d date;
property id.flag int;
property id.v int;
datasource t (id, d, flag, v) grain (id)
query '''select 1 as id, date '2020-01-01' as d, 1 as flag, 10 as v''';
"""

CASES = [
    ("auto: arithmetic", "auto x <- v + 1;\nselect x;"),
    ("auto: single comparison", "auto x <- flag = 1;\nselect x;"),
    ("auto: compound bool (and)", "auto x <- flag = 1 and id = 1;\nselect x;"),
    (
        "auto: between",
        "auto x <- d between '2020-01-01'::date and '2020-12-31'::date;\nselect x;",
    ),
    ("? filter: compound bool", "select sum(v ? flag = 1 and id = 1) as s;"),
    ("where: compound bool", "where flag = 1 and id = 1 select id;"),
]


def main() -> None:
    for label, body in CASES:
        try:
            parse_text(PRELUDE + "\n" + body)
            print(f"  OK    {label}")
        except Exception as e:
            exp = next(
                (ln.strip() for ln in str(e).splitlines() if "expected" in ln), ""
            )
            print(f"  FAIL  {label}  [{type(e).__name__}] {exp[:70]}")


if __name__ == "__main__":
    main()
