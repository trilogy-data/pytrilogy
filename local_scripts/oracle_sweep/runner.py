from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.util
import json
import os
import subprocess
import sys
import tarfile
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from io import BytesIO
from itertools import groupby
from multiprocessing import get_context
from pathlib import Path
from typing import Any

# Nothing here imports `trilogy` at module level: a worker puts the package
# root of the ref under test on `sys.path` first.

REPO = Path(__file__).resolve().parents[2]
CACHE = REPO / ".cache" / "oracle_sweep"
FAMILIES = {
    "first": ("tests.discovery.test_oracle_sweep_laws", "shape"),
    "spellings": ("tests.discovery.test_oracle_spellings_laws", "shape2"),
    "expressions": ("tests.discovery.test_oracle_expressions_laws", "shape3"),
}
GENERATOR_FILES = [
    "tests/discovery/test_rollup_resolution_laws.py",
    "tests/discovery/test_oracle_sweep_laws.py",
    "tests/discovery/test_oracle_spellings_laws.py",
]
# A family's own generator, beside the shared ones: adding a family leaves the
# others' cached records valid.
FAMILY_FILES = {"expressions": ["tests/discovery/test_oracle_expressions_laws.py"]}
# Bump when a record's fields or meaning change: cached records are keyed on it.
RECORD_VERSION = 1
MODELS = ("base", "summary")
# Seeds per task. Fixed, so a `--carry-state` run does not depend on `--jobs`.
CHUNK = 250
BAD = ("wrong", "raise", "invalid")
# Working-tree result files kept: one per state of the source swept.
TREE_STATES = 6
NATIVE = "_preql_import_resolver"

Record = dict[str, Any]
Key = tuple[int, str]

_STATE: dict[str, Any] = {}


def shape_fn(family: str) -> Any:
    module, name = FAMILIES[family]
    return getattr(importlib.import_module(module), name)


def _init_worker(root: str | None) -> None:
    if root:
        sys.path.insert(0, root)
    import trilogy

    if root and not Path(trilogy.__file__).resolve().is_relative_to(Path(root)):
        raise RuntimeError(f"trilogy imported from {trilogy.__file__}, not {root}")


def _executor(model: str) -> tuple[Any, Any]:
    if model not in _STATE:
        from copy import deepcopy

        from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY
        from trilogy import Dialects

        executor = Dialects.DUCK_DB.default_executor()
        executor.parse_text(BASE + (SUMMARY if model == "summary" else ""))
        _STATE[model] = (executor, deepcopy(executor.environment))
    return _STATE[model]


def sql_facts(sql: str) -> Record:
    return {
        "sql": hashlib.sha1(sql.encode()).hexdigest()[:12],
        "summary": sql.count('as "tree_summary"'),
        "scans": sql.count('as "trees"'),
        "joins": sql.count(" JOIN "),
        "ctes": sql.count(" as (\n"),
    }


def run_one(
    query: str,
    expected: list[tuple],
    model: str,
    fresh: bool = True,
    proven: str | None = None,
) -> tuple[Record, str | None, list[tuple] | None]:
    """One query against one model: its record, its SQL and its rows.

    SQL hashing to `proven` already matched the oracle, so it is not run."""
    from copy import deepcopy

    from tests.discovery.test_oracle_sweep_laws import REJECTED, normalize

    executor, pristine = _executor(model)
    if fresh:
        # A select defines its outputs and CTE names in the environment; a copy
        # of the parsed model plans as a fresh executor does, at 1ms.
        executor.environment = deepcopy(pristine)
    try:
        sql = executor.generate_sql(query)[-1]
    except Exception as e:
        status = "rejected" if REJECTED in str(e) else "raise"
        return {"status": status, "error": _error(e)}, None, None
    record = sql_facts(sql)
    if record["sql"] == proven:
        return {**record, "status": "ok"}, sql, None
    try:
        rows = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    except Exception as e:
        return {**record, "status": "invalid", "error": _error(e)}, sql, None
    ok = normalize(rows) == normalize(expected)
    return {**record, "status": "ok" if ok else "wrong"}, sql, rows


def _error(e: Exception) -> str:
    return f"{type(e).__name__}: {' '.join(str(e).split())}"[:240]


Task = tuple[str, list[int], list[str], bool, dict[Key, str]]


def _run_chunk(task: Task) -> list[Record]:
    family, seeds, models, carry, proven = task
    shape = shape_fn(family)
    out: list[Record] = []
    for model in models:
        if carry and model in _STATE:
            del _STATE[model]
        for seed in seeds:
            query, expected = shape(seed)
            record, _, _ = run_one(
                query, expected, model, not carry, proven.get((seed, model))
            )
            out.append({"seed": seed, "model": model, **record})
    return out


def _queries(task: tuple[str, list[int]]) -> list[str]:
    family, seeds = task
    shape = shape_fn(family)
    return [shape(seed)[0] for seed in seeds]


def _chunks(seeds: list[int], aligned: bool = False) -> Iterator[list[int]]:
    """`aligned` cuts at multiples of CHUNK, whatever seeds are asked for."""
    if aligned:
        for _, chunk in groupby(seeds, key=lambda s: s // CHUNK):
            yield list(chunk)
        return
    for i in range(0, len(seeds), CHUNK):
        yield seeds[i : i + CHUNK]


def parse_seeds(spec: str) -> list[int]:
    """`0:90000`, `5,17,100:200`, or `@file` of whitespace-separated seeds."""
    if spec.startswith("@"):
        return sorted({int(s) for s in Path(spec[1:]).read_text().split()})
    seeds: set[int] = set()
    for part in spec.split(","):
        lo, _, hi = part.partition(":")
        seeds.update(range(int(lo), int(hi)) if hi else [int(lo)])
    return sorted(seeds)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout.strip()


def extract_ref(ref: str) -> tuple[str, Path]:
    """The `trilogy` package of `ref`, extracted once per commit."""
    sha = _git("rev-parse", f"{ref}^{{commit}}")
    root = CACHE / "src" / sha
    if not (root / "trilogy").is_dir():
        blob = subprocess.run(
            ["git", "archive", "--format=tar", sha, "trilogy"],
            cwd=REPO,
            check=True,
            capture_output=True,
        ).stdout
        staging = root.with_name(f"{sha}.{os.getpid()}")
        with tarfile.open(fileobj=BytesIO(blob)) as tar:
            tar.extractall(staging, filter="data")
        root.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(root)
    return sha, root


def _digest(files: Iterable[Path], seed: str = "") -> str:
    digest = hashlib.sha1(seed.encode())
    for path in sorted(files):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


def generator_hash(family: str = "") -> str:
    """The generators, the record format, and the native planner every side runs."""
    spec = importlib.util.find_spec(NATIVE)
    native = [
        Path(d) for d in (spec.submodule_search_locations if spec else None) or []
    ]
    files = [f for d in native for f in d.rglob("*") if f.is_file()]
    return _digest(
        [
            *(REPO / name for name in GENERATOR_FILES + FAMILY_FILES.get(family, [])),
            *files,
        ],
        str(RECORD_VERSION),
    )


def tree_hash() -> str:
    """The working tree's `trilogy` sources: one cache file per state of them."""
    skip = {"__pycache__", "target"}
    files = (
        f
        for f in (REPO / "trilogy").rglob("*")
        if f.is_file() and not skip.intersection(f.relative_to(REPO).parts)
    )
    return _digest(files)


def _prune_tree_states(results: Path) -> None:
    stale = sorted(results.glob("tree-*.jsonl"), key=lambda f: f.stat().st_mtime)
    for path in stale[:-TREE_STATES]:
        path.unlink(missing_ok=True)
        path.with_suffix(".lock").unlink(missing_ok=True)


@contextmanager
def _locked(path: Path) -> Iterator[None]:
    """Two sweeps writing one result file lose both."""
    try:
        import fcntl
    except ImportError:  # Windows: no advisory lock
        yield
        return
    with path.with_suffix(".lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SystemExit(
                f"another sweep is writing {path.name}; wait for it"
            ) from None
        yield


def read_records(path: Path) -> dict[Key, Record]:
    if not path.exists():
        return {}
    records = []
    with path.open() as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # blank, or the line a killed sweep was writing
    return {(r["seed"], r["model"]): r for r in records}


def append_records(path: Path, records: Iterable[Record]) -> None:
    with path.open("a") as f:
        f.writelines(json.dumps(r, sort_keys=True) + "\n" for r in records)


def write_records(path: Path, records: Iterable[Record]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        for r in sorted(records, key=lambda r: (r["seed"], r["model"])):
            f.write(json.dumps(r, sort_keys=True) + "\n")
    tmp.replace(path)


def _progress(done: int, total: int, started: float) -> None:
    elapsed = time.perf_counter() - started
    eta = elapsed / done * (total - done) if done else 0.0
    print(
        f"\r  {done}/{total} seeds  {elapsed:6.0f}s  eta {eta:5.0f}s",
        end="",
        file=sys.stderr,
        flush=True,
    )


def sweep(
    family: str,
    seeds: list[int],
    models: Iterable[str] = MODELS,
    jobs: int = 0,
    ref: str | None = None,
    carry: bool = False,
    quiet: bool = False,
    proven: dict[Key, Record] | None = None,
    cache: bool = True,
) -> dict[Key, Record]:
    """Records for `seeds` x `models`, on the working tree or on `ref`.

    Records are cached per commit for a ref and per state of `trilogy/` for
    the working tree, and written as each chunk finishes: only seeds a side has
    not run cost anything, and a killed sweep resumes. A query two seeds share
    is planned once unless `carry`, and SQL a `proven` record already matched to
    the oracle is not executed again."""
    models = list(models)
    jobs = jobs or os.cpu_count() or 1
    root: Path | None = None
    if ref:
        state, root = extract_ref(ref)
    else:
        state = f"tree-{tree_hash()}"
    right = {k: r["sql"] for k, r in (proven or {}).items() if r["status"] == "ok"}
    if not cache:
        fresh = _sweep(family, seeds, models, jobs, root, carry, quiet, right, None)
        return {(s, m): fresh[(s, m)] for s in seeds for m in models}
    name = f"{state}-{family}-{generator_hash(family)}{'-carry' if carry else ''}.jsonl"
    path = CACHE / "results" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with _locked(path):
        known = read_records(path)
        wanted = [s for s in seeds if any((s, m) not in known for m in models)]
        if carry:
            # a seed's record depends on the seeds its chunk ran before it
            stale = {s // CHUNK for s in wanted}
            wanted = [s for s in seeds if s // CHUNK in stale]
        if wanted:
            known.update(
                _sweep(family, wanted, models, jobs, root, carry, quiet, right, path)
            )
            write_records(path, known.values())
            _prune_tree_states(path.parent)
    return {(s, m): known[(s, m)] for s in seeds for m in models}


def _sweep(
    family: str,
    seeds: list[int],
    models: list[str],
    jobs: int,
    root: Path | None,
    carry: bool,
    quiet: bool,
    proven: dict[Key, str],
    checkpoint: Path | None,
) -> dict[Key, Record]:
    started = time.perf_counter()
    pool = ProcessPoolExecutor(
        max_workers=jobs,
        mp_context=get_context("spawn"),
        initializer=_init_worker,
        initargs=(str(root) if root else None,),
    )
    with pool:
        twins: dict[int, list[int]] = {}
        todo = seeds
        if not carry:
            first: dict[str, int] = {}
            texts = pool.map(_queries, [(family, c) for c in _chunks(seeds)])
            for seed, query in zip(seeds, (q for chunk in texts for q in chunk)):
                twins.setdefault(first.setdefault(query, seed), []).append(seed)
            todo = sorted(twins)
        out: dict[Key, Record] = {}
        tasks: list[Task] = [
            (
                family,
                c,
                models,
                carry,
                {(s, m): proven[(s, m)] for s in c for m in models if (s, m) in proven},
            )
            for c in _chunks(todo, aligned=carry)
        ]
        done = 0
        for task, records in zip(tasks, pool.map(_run_chunk, tasks)):
            # a query's record stands for every seed that spells it
            records = [
                {**r, "seed": seed}
                for r in records
                for seed in twins.get(r["seed"], [r["seed"]])
            ]
            out.update({(r["seed"], r["model"]): r for r in records})
            if checkpoint:
                append_records(checkpoint, records)
            done += len(task[1])
            if not quiet:
                _progress(done, len(todo), started)
    if not quiet:
        print(file=sys.stderr)
    return out


def summarize(records: dict[Key, Record]) -> Record:
    status = Counter(r["status"] for r in records.values())
    planned = [r for r in records.values() if "sql" in r]
    return {
        "queries": len(records),
        **{s: status.get(s, 0) for s in ("ok", "rejected", *BAD)},
        "summary_reads": sum(1 for r in planned if r["summary"]),
        "scans": sum(r["scans"] for r in planned),
        "joins": sum(r["joins"] for r in planned),
        "ctes": sum(r["ctes"] for r in planned),
    }


def _bad(records: dict[Key, Record], key: Key) -> bool:
    return records[key]["status"] in BAD


def compare(base: dict[Key, Record], new: dict[Key, Record]) -> Record:
    """What moved between two runs, over the queries both hold."""
    keys = sorted(base.keys() & new.keys())
    moved = [k for k in keys if base[k]["status"] != new[k]["status"]]
    both = [k for k in keys if "sql" in base[k] and "sql" in new[k]]
    lost = [k for k in both if base[k]["summary"] and not new[k]["summary"]]

    def grew(field: str, among: list[Key]) -> list[Key]:
        return [k for k in among if new[k][field] > base[k][field]]

    return {
        "broken": [k for k in moved if _bad(new, k)],
        "fixed": [k for k in moved if _bad(base, k) and not _bad(new, k)],
        "still_bad": [k for k in keys if _bad(base, k) and k not in moved],
        "sql_changed": [k for k in both if base[k]["sql"] != new[k]["sql"]],
        "summary_gained": [
            k for k in both if new[k]["summary"] and not base[k]["summary"]
        ],
        "summary_lost": lost,
        # a read lost beside the same fact scans is not a cost
        "summary_lost_more_scans": grew("scans", lost),
        "more_joins": grew("joins", both),
        "fewer_joins": [k for k in both if new[k]["joins"] < base[k]["joins"]],
        "more_scans": grew("scans", both),
    }


def _label(keys: list[Key], show: int) -> str:
    text = " ".join(f"{seed}:{model}" for seed, model in keys[:show])
    return text + (f" ... (+{len(keys) - show})" if len(keys) > show else "")


def report(
    base: dict[Key, Record],
    new: dict[Key, Record],
    family: str | None = None,
    show: int = 20,
) -> bool:
    """Print the comparison; True when nothing went wrong, raising or invalid."""
    a, b = summarize(base), summarize(new)
    print(f"{'':16}{'base':>10}{'new':>10}{'delta':>9}")
    for field in a:
        print(f"{field:16}{a[field]:>10}{b[field]:>10}{b[field] - a[field]:>+9}")
    moved = compare(base, new)
    shape = shape_fn(family) if family else None
    for name, keys in moved.items():
        print(f"{name}: {len(keys)}  {_label(keys, show)}")
        if shape and name in ("broken", "summary_lost_more_scans"):
            for seed, model in keys[:show]:
                detail = new[(seed, model)].get("error", "")
                print(f"    {seed}:{model}  {shape(seed)[0]}  {detail}")
    return not moved["broken"]


def show_seed(family: str, seed: int, models: Iterable[str], ref: str | None) -> None:
    _init_worker(str(extract_ref(ref)[1]) if ref else None)
    query, expected = shape_fn(family)(seed)
    from tests.discovery.test_oracle_sweep_laws import normalize

    print(query)
    print("expected:", normalize(expected))
    for model in models:
        record, sql, rows = run_one(query, expected, model)
        print(f"\n-- {model}: {record}")
        if rows is not None:
            print("rows:    ", normalize(rows))
        if sql:
            print(sql)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m local_scripts.oracle_sweep")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--family", choices=list(FAMILIES), default="spellings")
        p.add_argument("--seeds", default="0:4000")
        p.add_argument("--models", default=",".join(MODELS))
        p.add_argument("--jobs", type=int, default=0, help="default: every core")
        p.add_argument(
            "--carry-state",
            action="store_true",
            help=f"keep each select's definitions for the next, per {CHUNK} seeds",
        )
        p.add_argument("--no-cache", action="store_true", help="run every seed again")

    run = sub.add_parser("run", help="sweep the working tree, or --ref")
    common(run)
    run.add_argument("--ref", help="a commit to sweep instead (cached)")
    run.add_argument("--out", type=Path)

    ab = sub.add_parser("ab", help="sweep --base and the working tree, and compare")
    common(ab)
    ab.add_argument("--base", default="origin/main")
    ab.add_argument("--out", type=Path, help="the working tree's records")
    ab.add_argument("--show", type=int, default=20)

    diff = sub.add_parser("diff", help="compare two record files")
    diff.add_argument("base", type=Path)
    diff.add_argument("new", type=Path)
    diff.add_argument("--family", choices=list(FAMILIES))
    diff.add_argument("--show", type=int, default=20)

    show = sub.add_parser("show", help="one seed: query, oracle rows, SQL, rows")
    show.add_argument("seed", type=int)
    show.add_argument("--family", choices=list(FAMILIES), default="spellings")
    show.add_argument("--models", default=",".join(MODELS))
    show.add_argument("--ref")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "diff":
        ok = report(
            read_records(args.base), read_records(args.new), args.family, args.show
        )
        return 0 if ok else 1
    models = args.models.split(",")
    if args.command == "show":
        show_seed(args.family, args.seed, models, args.ref)
        return 0
    seeds = parse_seeds(args.seeds)
    kwargs = {
        "models": models,
        "jobs": args.jobs,
        "carry": args.carry_state,
        "cache": not args.no_cache,
    }
    if args.command == "run":
        records = sweep(args.family, seeds, ref=args.ref, **kwargs)
        if args.out:
            write_records(args.out, records.values())
        print(json.dumps(summarize(records)))
        return 0 if not any(r["status"] in BAD for r in records.values()) else 1
    print(f"base {args.base}", file=sys.stderr)
    base = sweep(args.family, seeds, ref=args.base, **kwargs)
    print("working tree", file=sys.stderr)
    new = sweep(args.family, seeds, proven=base, **kwargs)
    write_records(args.out or CACHE / f"last-{args.family}.jsonl", new.values())
    return 0 if report(base, new, args.family, args.show) else 1
