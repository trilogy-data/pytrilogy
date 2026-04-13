import os
import sys

if "--v1" in sys.argv:
    os.environ["TRILOGY_FORCE_V1_PARSER"] = "1"

from trilogy.constants import CONFIG, ParserVersion

if os.environ.get("TRILOGY_FORCE_V1_PARSER") == "1":
    CONFIG.parser_version = ParserVersion.V1

from trilogy.core.models.environment import Environment

if "--v1" in sys.argv:
    from trilogy.parsing.parse_engine import parse_text
else:
    from trilogy.parsing.parse_engine_v2 import parse_text

env = Environment()
env, r = parse_text(
    """
key a int;
key b int;
key wrapper struct<a,b>;
key array_struct list<wrapper>;

auto unnest_array<-unnest(array_struct);
merge unnest_array into wrapper;

datasource struct_array (
    array_struct: array_struct
)
grain (array_struct)
query '''
select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
'''
;

SELECT
    wrapper.a,
    b
order by
    wrapper.a asc
;
""",
    env,
)

print("=== parse env (address: value_id, grain) ===")
for addr in [
    "unnest_array.a",
    "unnest_array.b",
    "wrapper.a",
    "wrapper.b",
    "local.unnest_array",
    "local.wrapper",
    "local.a",
    "local.b",
]:
    v = env.concepts.data.get(addr)
    if v is not None:
        print(f"  {addr}: id={id(v)} grain={v.grain} pseudonyms={v.pseudonyms}")

print()
# Build environment
build = env.materialize_for_select()
print("=== build env (address: value_id, grain) ===")
for addr in [
    "unnest_array.a",
    "unnest_array.b",
    "wrapper.a",
    "wrapper.b",
    "local.unnest_array",
    "local.wrapper",
    "local.a",
    "local.b",
]:
    v = build.concepts.data.get(addr)
    if v is not None:
        print(f"  {addr}: id={id(v)} grain={v.grain} pseudonyms={v.pseudonyms}")

print()
print("=== build alias_origin_lookup ===")
for k, v in build.alias_origin_lookup.items():
    if "__preql" in k:
        continue
    print(f"  {k}: id={id(v)} grain={v.grain} pseudonyms={v.pseudonyms}")
