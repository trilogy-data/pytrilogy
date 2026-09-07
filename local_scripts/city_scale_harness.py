"""Scale a partitioned model to N cities and time hydration and planning.

Clones the sf_tree_reporting `data/raw` model (one rollup source plus
per-city `complete where` partitions over a shared dedup lineage) into
`<out>/raw_<N>` with synthetic cities cloned from the San Francisco template,
then reports cold hydration, the first-query baseline materialization and warm
per-query planning at each size. Every term that grows with datasource count
per query shows up here long before it shows up at the real city count.

    python -m local_scripts.city_scale_harness --source <data/raw>
    python -m local_scripts.city_scale_harness --source <data/raw> --sizes 36 288
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.parsing.parse_engine_v2 import clear_parse_cache, parse_text

CONTEXT = """import tree_enrichment;
import tree_info;
import ecoregion_info;
import std.display;

constant active_city <- 'USSFO';
constant active_city_ecoregion <- 423;
constant active_city_usda_zone <- 10;
constant active_city_biome <- 'Mediterranean Forests, Woodlands & Scrub';
constant active_city_realm <- 'nearctic';

property native_locality_bucket <- CASE
    WHEN ecoregion_id=active_city_ecoregion THEN 'Native'
    WHEN biome = active_city_biome THEN 'Same biome, non-native'
    WHEN realm = active_city_realm THEN 'Native Region, Different Biome'
  else 'Non-Native, Different Biome'
end::string;

auto dominance_rank <- rank(species) over (order by count(tree_id) by species desc, species asc);
auto cumulative_tree_share_pct <- ((sum count(tree_id) by species order by dominance_rank asc) / (count(tree_id) by *))::float::percent;
"""

QUERIES = {
    "city_count": "SELECT count(tree_id) as total_trees WHERE city = 'USSFO';",
    "city_species": "SELECT species, count(tree_id) as tree_count WHERE city = 'USSFO' ORDER BY tree_count DESC LIMIT 100;",
    "city_rows": "SELECT latitude, longitude WHERE latitude IS NOT NULL AND longitude IS NOT NULL and city = 'USSFO';",
    "all_species": "SELECT species, count(tree_id) as tree_count_all ORDER BY tree_count_all DESC LIMIT 100;",
}


def real_city_count(source: Path) -> int:
    text = (source / "core.preql").read_text(encoding="utf-8")
    match = re.search(r"key city enum<string>\[(.*?)\];", text, re.DOTALL)
    if match is None:
        raise ValueError("core.preql has no `key city enum<string>[...]` declaration")
    return match.group(1).count("'") // 2


def generate(source: Path, n: int, out_root: Path) -> Path:
    out = out_root / f"raw_{n}"
    if out.exists():
        shutil.rmtree(out)
    shutil.copytree(
        source,
        out,
        ignore=shutil.ignore_patterns(
            "__pycache__", "tests", "*.parquet", "enrichment"
        ),
    )
    codes = [f"zz{i:03d}" for i in range(n - real_city_count(source))]
    template = (source / "ussfo" / "sf_tree_info.preql").read_text(encoding="utf-8")
    for code in codes:
        city_dir = out / code
        city_dir.mkdir()
        body = (
            template.replace("ussfo", code)
            .replace("USSFO", code.upper())
            .replace("sf_", f"{code}_")
        )
        (city_dir / f"{code}_tree_info.preql").write_text(body, encoding="utf-8")
        for script in ("sf_update_time.py", "sf_osm_probe.py", "sf_tree_info.py"):
            shutil.copy(
                source / "ussfo" / script, city_dir / script.replace("sf_", f"{code}_")
            )
    if not codes:
        return out

    def edit(name: str, transform) -> None:
        path = out / name
        path.write_text(transform(path.read_text(encoding="utf-8")), encoding="utf-8")

    edit(
        "tree_common.preql",
        lambda t: t
        + "".join(
            f"property <*>.{c}_data_updated_through datetime;\n"
            f"property <*>.{c}_osm_data_updated_through datetime;\n"
            for c in codes
        ),
    )
    edit(
        "community_tree_info.preql",
        lambda t: t
        + "".join(
            f"property <*>.{c}_community_data_updated_through datetime;\n"
            for c in codes
        ),
    )

    def core(t: str) -> str:
        t = re.sub(
            r"(key city enum<string>\[.*?)(\];)",
            lambda m: m.group(1)
            + ", "
            + ", ".join(f"'{c.upper()}'" for c in codes)
            + m.group(2),
            t,
            count=1,
            flags=re.DOTALL,
        )
        return t.replace(
            "\nend; # mapped",
            "\n"
            + "".join(f"    when city = '{c.upper()}' then 423\n" for c in codes)
            + "end; # mapped",
            1,
        )

    edit("core.preql", core)

    def tree_info(t: str) -> str:
        imports = re.findall(r"^import \w+\.\w+_tree_info;\n", t, re.MULTILINE)
        t = t.replace(
            imports[-1],
            imports[-1] + "".join(f"import {c}.{c}_tree_info;\n" for c in codes),
            1,
        )
        merges = re.findall(r"^merge \w+_source into data_source;\n", t, re.MULTILINE)
        t = t.replace(
            merges[-1],
            merges[-1]
            + "".join(f"merge {c}_source into data_source;\n" for c in codes),
            1,
        )
        return re.sub(
            r"(\w+_published_data_updated_through)\);",
            lambda m: m.group(1)
            + ",\n"
            + ",\n".join(f"{c}_published_data_updated_through" for c in codes)
            + ");",
            t,
            count=1,
        )

    edit("tree_info.preql", tree_info)
    edit(
        "tree_dedup.preql",
        lambda t: t.replace(
            "\n) AS t(city",
            ",\n"
            + ",\n".join(f"    ('{c.upper()}', 0.00008983, 0.00011361)" for c in codes)
            + "\n) AS t(city",
            1,
        ),
    )
    return out


def measure(raw: Path) -> dict[str, float]:
    clear_parse_cache()
    env = Environment(working_path=raw)
    start = time.perf_counter()
    parse_text(CONTEXT, env)
    timings = {"hydrate": time.perf_counter() - start}
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    for label, query in QUERIES.items():
        start = time.perf_counter()
        executor.generate_sql(query)
        timings[label] = time.perf_counter() - start
    start = time.perf_counter()
    executor.generate_sql(QUERIES["city_species"])
    timings["city_species_repeat"] = time.perf_counter() - start
    timings["datasources"] = len(env.datasources)
    return timings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help="the partitioned model to clone (an sf_tree_reporting data/raw)",
    )
    parser.add_argument(
        "--out", type=Path, default=Path(__file__).resolve().parent / "city_scale_runs"
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=[36, 72, 144, 288])
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    columns = ["hydrate", *QUERIES, "city_species_repeat"]
    print("cities  datasources  " + "  ".join(f"{c:>19s}" for c in columns))
    for n in args.sizes:
        raw = generate(args.source, n, args.out)
        t = measure(raw)
        print(
            f"{n:6d}  {int(t['datasources']):11d}  "
            + "  ".join(f"{t[c]:18.3f}s" for c in columns),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
