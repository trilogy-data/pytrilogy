"""Foreign-key inference for `trilogy ingest`.

Two stages, per specs/ingest_fk_inference.md:

  Stage 1 — candidate generation by fuzzy name match, on *canonical* names
            (prefix already stripped by ``canonicalize_names``).
  Stage 2 — value-overlap verification ("sniffing") against the warehouse.

Output is the same ``{table: {column: "ref_table.ref_column"}}`` structure
``parse_foreign_keys`` produces, so accepted edges feed the existing
``apply_foreign_key_references`` path unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from trilogy.scripts.display import print_info, print_warning
from trilogy.scripts.ingest_helpers.formatting import (
    canonicalize_names,
    canonicolize_name,
)

if TYPE_CHECKING:
    from trilogy.core.models.datasource import Datasource

# Canonical-name suffixes that mark a column as a candidate foreign key.
# Underscore-qualified forms are a strong signal (TPC-DS `*_sk`); the bare
# tokens catch glued forms (TPC-H `custkey`).
FK_SUFFIXES = ("_sk", "_id", "_key", "_fk")
_FK_SUFFIX_TOKENS = ("sk", "id", "key", "fk")
# Stage 1 confidence by match kind — orders candidates only; Stage 2 value
# overlap is the real filter.
_CONFIDENCE: dict[str, float] = {"exact": 1.0, "suffix": 0.85, "stem": 0.7}
# Stage 2: accept a non-complete match when at least this fraction of sampled
# from-values are found in the referenced key (tolerates dirty/sampled data).
SUBSET_OVERLAP_THRESHOLD = 0.95
# Bound on distinct from-values pulled per containment check.
DEFAULT_SNIFF_SAMPLE = 50_000
# Shortest name stem considered meaningful for a fuzzy entity match.
_MIN_STEM_LEN = 3


@dataclass
class TableFKInfo:
    """The per-table introspection facts the matcher and sniffer need."""

    name: str  # ingest source key / foreign-key-map key
    sql_relation: str  # quoted, queryable relation for value sniffing
    raw_columns: list[str]  # raw column names, original order
    raw_to_canonical: dict[str, str]
    key_raw_columns: list[str]  # raw names of the table's grain key columns

    @property
    def single_key_raw(self) -> str | None:
        """The sole single-column key, if the table has exactly one."""
        return self.key_raw_columns[0] if len(self.key_raw_columns) == 1 else None

    @property
    def single_key_canonical(self) -> str | None:
        key = self.single_key_raw
        return self.raw_to_canonical.get(key) if key is not None else None


@dataclass
class FKCandidate:
    """A proposed reference edge before value verification."""

    from_table: str
    from_column: str  # raw
    to_table: str
    to_column: str  # raw
    match_kind: str  # "exact" | "suffix" | "stem"

    @property
    def confidence(self) -> float:
        return _CONFIDENCE[self.match_kind]

    @property
    def target_ref(self) -> str:
        return f"{self.to_table}.{self.to_column}"


@dataclass
class InferredFK:
    """An accepted FK edge, ready to feed the application path."""

    from_table: str
    from_column: str  # raw
    to_table: str
    to_column: str  # raw
    match_kind: str
    overlap: float | None  # None when sniffing was skipped (fast level)

    @property
    def target_ref(self) -> str:
        return f"{self.to_table}.{self.to_column}"


def _fk_stem(canonical: str) -> str | None:
    """The entity stem of an FK-like canonical name, or None if not FK-like.

    Returns "" for a bare key token (``id``/``sk``/``key``/``fk``), which has
    no stem but can still match a target by exact name.
    """
    for suffix in FK_SUFFIXES:
        if canonical.endswith(suffix) and len(canonical) > len(suffix):
            return canonical[: -len(suffix)]
    for token in _FK_SUFFIX_TOKENS:
        if canonical == token:
            return ""
        if canonical.endswith(token):
            stem = canonical[: -len(token)]
            if len(stem) >= _MIN_STEM_LEN:
                return stem
    return None


def _stem_related(a: str, b: str) -> bool:
    """True if two name stems plausibly denote the same entity.

    Covers exact match, naive singular/plural, and abbreviation (one stem a
    substring of the other, e.g. ``addr`` ↔ ``address``).
    """
    if len(a) < _MIN_STEM_LEN or len(b) < _MIN_STEM_LEN:
        return False
    if a == b or a.rstrip("s") == b.rstrip("s"):
        return True
    short, long = sorted((a, b), key=len)
    return short in long


def _match_kind(
    from_canonical: str, from_stem: str, to_key_canonical: str, to_table: str
) -> str | None:
    """Classify how a column name matches a candidate target key, or None."""
    if from_canonical == to_key_canonical:
        return "exact"
    # Suffix match needs a *qualified* target key (``date_sk``, not a bare
    # ``id``/``sk``) so any ``*_id`` column doesn't latch onto every table.
    if "_" in to_key_canonical and from_canonical.endswith("_" + to_key_canonical):
        return "suffix"
    if from_stem:
        to_key_stem = _fk_stem(to_key_canonical)
        if to_key_stem and _stem_related(from_stem, to_key_stem):
            return "stem"
        if _stem_related(from_stem, canonicolize_name(to_table)):
            return "stem"
    return None


def _candidates_for_column(
    src: TableFKInfo, raw_column: str, from_stem: str, tables: list[TableFKInfo]
) -> list[FKCandidate]:
    """Stage 1 for one column: best candidate per target table, ranked."""
    from_canonical = src.raw_to_canonical[raw_column]
    best: dict[str, tuple[str, str]] = {}  # to_table -> (kind, to_column)
    for target in tables:
        if target.name == src.name:
            continue  # skip self-references
        to_column = target.single_key_raw
        to_key = target.single_key_canonical
        if to_column is None or to_key is None:
            continue  # only single-column-keyed tables can be FK targets
        kind = _match_kind(from_canonical, from_stem, to_key, target.name)
        if kind is None:
            continue
        current = best.get(target.name)
        if current is None or _CONFIDENCE[kind] > _CONFIDENCE[current[0]]:
            best[target.name] = (kind, to_column)
    candidates = [
        FKCandidate(src.name, raw_column, to_table, to_column, kind)
        for to_table, (kind, to_column) in best.items()
    ]
    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates


def generate_candidates(
    tables: list[TableFKInfo],
) -> dict[tuple[str, str], list[FKCandidate]]:
    """Stage 1: propose reference targets for every FK-like column.

    Returns ``{(from_table, from_column): [candidates...]}`` with candidates
    ranked by descending confidence.
    """
    out: dict[tuple[str, str], list[FKCandidate]] = {}
    for src in tables:
        identity = src.single_key_raw
        for raw_column in src.raw_columns:
            if raw_column == identity:
                continue  # a table's own key is its identity, not an FK
            from_stem = _fk_stem(src.raw_to_canonical[raw_column])
            if from_stem is None:
                continue  # not FK-like
            candidates = _candidates_for_column(src, raw_column, from_stem, tables)
            if candidates:
                out[(src.name, raw_column)] = candidates
    return out


def _rollback(executor: Any) -> None:
    """Clear an aborted transaction so later sniff queries still run."""
    try:
        executor.connection.rollback()
    except Exception:
        pass


def _containment_sql(
    quote: Any,
    from_relation: str,
    from_column: str,
    to_relation: str,
    to_column: str,
    sample: int,
) -> str:
    fq = quote(from_column)
    tq = quote(to_column)
    return (
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN t.v IS NULL THEN 1 ELSE 0 END) AS unmatched FROM "
        f"(SELECT DISTINCT {fq} AS v FROM {from_relation} "
        f"WHERE {fq} IS NOT NULL LIMIT {sample}) f "
        f"LEFT JOIN (SELECT DISTINCT {tq} AS v FROM {to_relation}) t "
        "ON f.v = t.v"
    )


def measure_overlap(
    executor: Any,
    src: TableFKInfo,
    candidate: FKCandidate,
    target: TableFKInfo,
    sample: int = DEFAULT_SNIFF_SAMPLE,
) -> float | None:
    """Stage 2: fraction of sampled from-values found in the referenced key.

    Returns a value in [0, 1], or None when the check can't run (incompatible
    types, empty source column).
    """
    sql = _containment_sql(
        executor.generator.safe_quote,
        src.sql_relation,
        candidate.from_column,
        target.sql_relation,
        candidate.to_column,
        sample,
    )
    try:
        rows = executor.execute_raw_sql(sql).fetchall()
    except Exception as e:
        print_warning(
            f"FK sniff skipped for {candidate.from_table}.{candidate.from_column}"
            f" -> {candidate.target_ref}: {e}"
        )
        _rollback(executor)
        return None
    if not rows or not rows[0] or not rows[0][0]:
        return None
    total = rows[0][0]
    unmatched = rows[0][1] or 0
    return (total - unmatched) / total


def _reverse_coverage(
    executor: Any,
    by_name: dict[str, TableFKInfo],
    candidate: FKCandidate,
    sample: int,
) -> float:
    """Fraction of the *referenced key's* values found in the referencing
    column — high for the true parent (its key is densely used), low for an
    incidental superset table (e.g. customer_demographics over hdemo_sk).
    """
    swapped = FKCandidate(
        candidate.to_table,
        candidate.to_column,
        candidate.from_table,
        candidate.from_column,
        candidate.match_kind,
    )
    coverage = measure_overlap(
        executor,
        by_name[candidate.to_table],
        swapped,
        by_name[candidate.from_table],
        sample,
    )
    return coverage if coverage is not None else 0.0


def _break_overlap_tie(
    executor: Any,
    by_name: dict[str, TableFKInfo],
    contenders: list[tuple[FKCandidate, float]],
    sample: int,
) -> tuple[FKCandidate, float]:
    """Among candidates tied on forward containment, pick the true parent by
    reverse coverage."""
    return max(
        contenders,
        key=lambda pair: _reverse_coverage(executor, by_name, pair[0], sample),
    )


def _verify_column(
    executor: Any,
    by_name: dict[str, TableFKInfo],
    candidates: list[FKCandidate],
    sample: int,
) -> InferredFK | None:
    """Stage 2 for one column: sniff candidates, keep the best that passes."""
    measured: list[tuple[FKCandidate, float]] = []
    for i, candidate in enumerate(candidates):  # confidence-ordered
        overlap = measure_overlap(
            executor,
            by_name[candidate.from_table],
            candidate,
            by_name[candidate.to_table],
            sample,
        )
        if overlap is None:
            continue
        measured.append((candidate, overlap))
        # A complete match can't be beaten by any *lower*-confidence candidate
        # still unmeasured; only stop once the remaining tier drops off.
        next_confidence = (
            candidates[i + 1].confidence if i + 1 < len(candidates) else -1.0
        )
        if overlap >= 1.0 and next_confidence < candidate.confidence:
            break

    passing = [pair for pair in measured if pair[1] >= SUBSET_OVERLAP_THRESHOLD]
    if not passing:
        return None
    best_overlap = max(overlap for _, overlap in passing)
    contenders = [p for p in passing if p[1] >= best_overlap - 1e-9]
    # Prefer the strongest name match among equal-overlap candidates; only
    # value-sniff a tie-break when even that is ambiguous.
    top_confidence = max(c.confidence for c, _ in contenders)
    top = [p for p in contenders if p[0].confidence >= top_confidence]
    if len(top) == 1:
        candidate, overlap = top[0]
    else:
        candidate, overlap = _break_overlap_tie(executor, by_name, top, sample)
    return InferredFK(
        candidate.from_table,
        candidate.from_column,
        candidate.to_table,
        candidate.to_column,
        candidate.match_kind,
        overlap,
    )
    return None


def _resolve_target_conflicts(inferred: list[InferredFK]) -> list[InferredFK]:
    """Drop all-but-best when several columns of one table resolve to the same
    target concept — wiring them all would collapse distinct columns (a
    role-playing dimension) onto a single concept.
    """
    groups: dict[tuple[str, str, str], list[InferredFK]] = {}
    for fk in inferred:
        groups.setdefault((fk.from_table, fk.to_table, fk.to_column), []).append(fk)
    kept: list[InferredFK] = []
    for group in groups.values():
        if len(group) == 1:
            kept.append(group[0])
            continue
        group.sort(key=lambda f: f.overlap or 0.0, reverse=True)
        kept.append(group[0])
        for dropped in group[1:]:
            print_info(
                f"Skipping inferred FK {dropped.from_table}.{dropped.from_column}"
                f" -> {dropped.target_ref}: another column already references it"
                " (role-playing dimension; use --fks to wire explicitly)"
            )
    return kept


def infer_foreign_keys(
    tables: list[TableFKInfo],
    executor: Any,
    level: str,
    sample_size: int = DEFAULT_SNIFF_SAMPLE,
) -> list[InferredFK]:
    """Run Stage 1 (and Stage 2 for ``full``) and return accepted FK edges.

    ``level`` is ``off`` (nothing), ``fast`` (name match only) or ``full``
    (name match plus value sniffing).
    """
    if level == "off" or len(tables) < 2:
        return []
    candidates = generate_candidates(tables)
    by_name = {t.name: t for t in tables}
    accepted: list[InferredFK] = []
    for edges in candidates.values():
        if level == "fast":
            best = edges[0]  # highest confidence
            accepted.append(
                InferredFK(
                    best.from_table,
                    best.from_column,
                    best.to_table,
                    best.to_column,
                    best.match_kind,
                    None,
                )
            )
        else:
            verified = _verify_column(executor, by_name, edges, sample_size)
            if verified is not None:
                accepted.append(verified)
    return _resolve_target_conflicts(accepted)


def build_table_fk_info(
    name: str, datasource: "Datasource", dialect: Any
) -> TableFKInfo:
    """Derive a TableFKInfo from an ingested datasource (before FK wiring)."""
    raw_columns = [c.alias for c in datasource.columns if isinstance(c.alias, str)]
    raw_to_canonical = canonicalize_names(raw_columns)
    canonical_to_raw = {canonical: raw for raw, canonical in raw_to_canonical.items()}
    key_raw_columns = [
        canonical_to_raw[component]
        for component in datasource.grain.component_order
        if component in canonical_to_raw
    ]
    address = datasource.address
    location = address if isinstance(address, str) else address.location
    return TableFKInfo(
        name=name,
        sql_relation=dialect.safe_quote(location),
        raw_columns=raw_columns,
        raw_to_canonical=raw_to_canonical,
        key_raw_columns=key_raw_columns,
    )


def merge_fk_maps(
    inferred: list[InferredFK], explicit: dict[str, dict[str, str]]
) -> dict[str, dict[str, str]]:
    """Combine inferred edges with explicit ``--fks``; explicit wins per column."""
    merged: dict[str, dict[str, str]] = {}
    for fk in inferred:
        merged.setdefault(fk.from_table, {})[fk.from_column] = fk.target_ref
    for table, columns in explicit.items():
        merged.setdefault(table, {}).update(columns)
    return merged
