from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
import trilogy.core.processing.discovery_validation as dv

orig = dv._conditions_met


def patched(stack, found_addresses, mandatory_with_filter, conditions):
    res = orig(stack, found_addresses, mandatory_with_filter, conditions)
    if not res and conditions is not None:
        print("=== _conditions_met returned False ===")
        print("conditional:", conditions.conditional)
        print("found_addresses:", sorted(found_addresses))
        print("mandatory_with_filter:", [c.address for c in mandatory_with_filter])
        for node in stack:
            resolved = node.resolve()
            print(f"  node {node}")
            print(f"    preexisting_conditions: {node.preexisting_conditions}")
            print(f"    outputs: {[c.address for c in resolved.output_concepts]}")
            print(f"    derivations: {[(c.address, str(c.derivation)) for c in resolved.output_concepts]}")
    return res


dv._conditions_met = patched

env = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
eng = Dialects.DUCK_DB.default_executor(environment=env)
try:
    print(eng.generate_sql(Path("repro.preql").read_text())[-1])
except Exception as e:
    print("ERROR:", type(e).__name__, str(e)[:200])
