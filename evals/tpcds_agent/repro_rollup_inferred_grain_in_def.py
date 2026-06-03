"""Repro: arithmetic combining two `@def` aggregate calls whose body uses the
inferred-grain `by rollup ()` recurses (RecursionError building the output
concept). The same shape works with explicit rollup keys, or when the two
aggregates are written inline instead of through a function.

Needs all three: (1) the aggregate is defined in a `def`, (2) its body uses the
empty-paren inferred-grain `by rollup ()`, (3) two such calls are combined with
an arithmetic operator (`+` / `-`) in one select column. Surfaced in tpc_ds
query05.preql's `profit` column (`@windowed(net_profit,…) - @windowed(loss,…)`)."""

from trilogy import Dialects, Environment

PRELUDE = """
key id int;
property id.g string;
property id.v int;
property id.w int;
datasource t (id, g, v, w) grain (id)
query '''select 1 as id, 'a' as g, 10 as v, 1 as w
   union all select 2, 'a', 20, 2
   union all select 3, 'b', 5, 3''';
"""

CASES = [
    (
        "def + rollup() + single call",
        "def f(m) -> sum(m) by rollup ();\nselect g, @f(v) as a;",
    ),
    (
        "def + rollup() + subtraction  (BUG)",
        "def f(m) -> sum(m) by rollup ();\nselect g, @f(v) - @f(w) as a;",
    ),
    (
        "def + rollup() + addition     (BUG)",
        "def f(m) -> sum(m) by rollup ();\nselect g, @f(v) + @f(w) as a;",
    ),
    (
        "def + EXPLICIT key + subtraction (fix)",
        "def f(m) -> sum(m) by rollup g;\nselect g, @f(v) - @f(w) as a;",
    ),
    (
        "INLINE rollup() + subtraction (control)",
        "select g, (sum(v) by rollup ()) - (sum(w) by rollup ()) as a;",
    ),
]


def main() -> None:
    for label, body in CASES:
        try:
            ex = Dialects.DUCK_DB.default_executor(
                environment=Environment(working_path=".")
            )
            ex.generate_sql(PRELUDE + body)
            print(f"  OK    {label}")
        except Exception as e:
            print(f"  FAIL  {label}  [{type(e).__name__}]")


if __name__ == "__main__":
    main()
