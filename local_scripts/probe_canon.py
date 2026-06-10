import os

os.environ["TRILOGY_V4_DISCOVERY"] = "1"
from trilogy.core.models.environment import Environment


def show(title, text, concept):
    env = Environment()
    env.parse(text, persist=True)
    benv = env.materialize_for_select()
    c = benv.concepts[concept]
    print(f"\n=== {title} ===")
    print(
        f"{concept}: deriv={c.derivation} addr={c.address} canon={c.canonical_address} pseud={set(c.pseudonyms)}"
    )
    print("materialized_canonical:", concept in {a for a in benv.materialized_canonical_concepts})
    print("  mat_canon set:", sorted(benv.materialized_canonical_concepts))
    for n, d in benv.datasources.items():
        print(f"DS {n} grain={d.grain.components} non_partial_for={d.non_partial_for}")
        print(f"    partial_concepts={[c.address for c in d.partial_concepts]}")
        for col in d.columns:
            print(
                f"    col alias={col.alias} concept={col.concept.address} canon={col.concept.canonical_address} mods={col.modifiers} is_complete={col.is_complete}"
            )


show(
    "merge-onto-key (const)",
    """key orid int;
auto orid_2 <- unnest([1,2,3,4,5]);
property orid.val int;
datasource orders ( ~orid, val ) grain (orid)
query '''select 1 as orid, 10 as val union all select 2, 20''';
merge orid into ~orid_2;""",
    "orid_2",
)
