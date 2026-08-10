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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from trilogy.constants import DEFAULT_NAMESPACE, logger
from trilogy.scripts.display import print_info, print_warning
from trilogy.scripts.ingest_helpers.formatting import (
    canonicalize_names,
    canonicolize_name,
)
from trilogy.scripts.ingest_helpers.introspection import (
    FILE_ADDRESS_TYPES,
    IntrospectionLevel,
    file_introspection_source,
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
# An FK column is considered NON-partial (complete) only when reverse coverage
# — fraction of the parent's keys observed in the child — is at or above this
# threshold. A small slack swallows sampling noise; anything visibly short of
# full coverage stays partial so source-selection plays it safe.
COMPLETE_REVERSE_THRESHOLD = 0.999
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
    # Sample-unique single columns beyond the elected grain (candidate keys in
    # the relational sense). Unverified: full-table uniqueness is checked
    # lazily, only for columns an FK actually name-matched.
    alternate_key_raw_columns: list[str] = field(default_factory=list)
    # Lazily-populated full-table uniqueness verdicts, raw column -> bool.
    unique_verdicts: dict[str, bool] = field(default_factory=dict)

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
    # Target is a sample-unique alternate key, not the verified grain — the
    # edge must pass a full-table uniqueness check before acceptance.
    alternate: bool = False

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
    # True when the parent's key has values that don't appear in the child
    # column — i.e. the child is a strict subset and the source datasource
    # cannot stand in for the parent as a complete source of that concept.
    # Defaults conservatively to True; full-mode sniffing flips it to False
    # when reverse coverage is ≥ ``COMPLETE_REVERSE_THRESHOLD``.
    partial: bool = True
    # Set on role-playing-dimension conflicts so multiple FKs from one table
    # can import the dim under distinct names (e.g. ``first_shipto_date``).
    role_alias: str | None = None

    @property
    def target_ref(self) -> str:
        base = f"{self.to_table}.{self.to_column}"
        return f"{base}@{self.role_alias}" if self.role_alias else base

    def binding(self) -> FKBinding:
        return FKBinding(target_ref=self.target_ref, partial=self.partial)


@dataclass
class FKBinding:
    """Per-column FK wiring instructions handed to ``apply_foreign_key_references``.

    ``target_ref`` is ``table.column`` or ``table.column@role_alias`` (the same
    string produced by ``InferredFK.target_ref`` and parsed by the apply layer).
    ``partial`` controls whether the rewritten datasource column receives a
    ``~`` modifier — set when the child is a strict subset of the parent's
    key, cleared when reverse-coverage proves the FK is complete.
    """

    target_ref: str
    partial: bool = True


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

    Covers exact match, naive singular/plural, abbreviation (one stem a
    substring of the other, e.g. ``addr`` ↔ ``address``), and token-aware
    matches for compound stems (``current_addr`` against ``address``: the
    ``addr`` token abbreviates ``address``).
    """
    if len(a) < _MIN_STEM_LEN or len(b) < _MIN_STEM_LEN:
        return False
    if a == b or a.rstrip("s") == b.rstrip("s"):
        return True
    short, long = sorted((a, b), key=len)
    if short in long:
        return True
    # Compound from-stems (e.g. ``current_addr``, ``bill_addr``) carry the
    # entity name in one of their underscore-delimited tokens — check those
    # against the shorter (target-key) stem the same way.
    for token in long.split("_"):
        if len(token) < _MIN_STEM_LEN:
            continue
        if token == short or token.rstrip("s") == short.rstrip("s") or token in short:
            return True
    return False


def _match_kind(
    from_canonical: str,
    from_stem: str,
    to_key_canonical: str,
    to_table: str | None,
) -> str | None:
    """Classify how a column name matches a candidate target key, or None.

    ``to_table`` enables the stem-to-table-name rule (``customer_id`` ->
    ``customers.id``). It is only sound when the target column is the table's
    single identity; pass None for alternate keys, where the table name says
    nothing about which of several unique columns is meant.
    """
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
        if to_table is not None and _stem_related(
            from_stem, canonicolize_name(to_table)
        ):
            return "stem"
    return None


def _target_key_options(target: TableFKInfo) -> list[tuple[str, bool]]:
    """(raw column, is_alternate) FK-target options a table offers: its single
    grain key (verified unique) plus sample-unique alternate keys."""
    options: list[tuple[str, bool]] = []
    single = target.single_key_raw
    if single is not None:
        options.append((single, False))
    for alt in target.alternate_key_raw_columns:
        if alt != single:
            options.append((alt, True))
    return options


def _candidates_for_column(
    src: TableFKInfo, raw_column: str, from_stem: str, tables: list[TableFKInfo]
) -> list[FKCandidate]:
    """Stage 1 for one column: best candidate per (target table, target column),
    ranked. The grain option is tried first so an equal-confidence alternate
    never displaces it."""
    from_canonical = src.raw_to_canonical[raw_column]
    # (to_table, to_column) -> (kind, alternate)
    best: dict[tuple[str, str], tuple[str, bool]] = {}
    for target in tables:
        if target.name == src.name:
            continue  # skip self-references
        for to_column, is_alternate in _target_key_options(target):
            to_key = target.raw_to_canonical.get(to_column)
            if to_key is None:
                continue
            kind = _match_kind(
                from_canonical,
                from_stem,
                to_key,
                None if is_alternate else target.name,
            )
            if kind is None:
                continue
            current = best.get((target.name, to_column))
            if current is None or _CONFIDENCE[kind] > _CONFIDENCE[current[0]]:
                best[(target.name, to_column)] = (kind, is_alternate)
    candidates = [
        FKCandidate(src.name, raw_column, to_table, to_column, kind, alternate)
        for (to_table, to_column), (kind, alternate) in best.items()
    ]
    # Highest confidence first; grain targets ahead of alternates on ties.
    candidates.sort(key=lambda c: (-c.confidence, c.alternate))
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


@dataclass
class CompositeFKCandidate:
    """A proposed reference onto a composite-keyed table: one (from, to)
    column pair per target key component, accepted or rejected as a unit."""

    from_table: str
    to_table: str
    pairs: list[tuple[str, str]]  # (from_raw, to_raw), one per key component
    match_kinds: list[str]  # per-pair "exact" | "suffix"


def _component_match(
    src: TableFKInfo, target: TableFKInfo, to_column: str
) -> tuple[str, str] | None:
    """The single child column matching one composite key component, with its
    match kind — or None when the component has no match or an ambiguous one
    (``home_team_id``/``away_team_id`` both matching ``team_id``: a
    role-playing composite we decline to guess at).

    Only exact/suffix name matches apply: stem fuzz per component would
    multiply false positives across the tuple, and the table-name rule is
    meaningless when the key has several columns.
    """
    to_key = target.raw_to_canonical[to_column]
    exact: list[str] = []
    suffix: list[str] = []
    for raw in src.raw_columns:
        from_canonical = src.raw_to_canonical[raw]
        if from_canonical == to_key:
            exact.append(raw)
        elif "_" in to_key and from_canonical.endswith("_" + to_key):
            suffix.append(raw)
    pool = exact or suffix
    if len(pool) != 1:
        return None
    return pool[0], ("exact" if exact else "suffix")


def generate_composite_candidates(
    tables: list[TableFKInfo],
) -> list[CompositeFKCandidate]:
    """Stage 1 for composite-keyed targets: a table qualifies as a candidate
    parent iff *every* component of its key matches a distinct child column."""
    out: list[CompositeFKCandidate] = []
    for src in tables:
        for target in tables:
            if target.name == src.name:
                continue
            key_cols = target.key_raw_columns
            if len(key_cols) < 2:
                continue  # single-key targets are the ordinary path
            pairs: list[tuple[str, str]] = []
            kinds: list[str] = []
            for to_column in key_cols:
                matched = _component_match(src, target, to_column)
                if matched is None:
                    break
                pairs.append((matched[0], to_column))
                kinds.append(matched[1])
            if len(pairs) != len(key_cols):
                continue
            from_cols = {from_col for from_col, _ in pairs}
            if len(from_cols) != len(pairs):
                continue  # one child column claimed twice — not a real tuple
            if from_cols == set(src.key_raw_columns):
                # The matched columns ARE the child's own grain: a peer table
                # at the same grain, not a dimension (mirrors the single-key
                # "own key is its identity" rule). A proper subset stays valid
                # (an N:1 rollup like batting -> appearances).
                continue
            out.append(CompositeFKCandidate(src.name, target.name, pairs, kinds))
    return out


def _rollback(executor: Any) -> None:
    """Clear an aborted transaction so later sniff queries still run."""
    try:
        executor.connection.rollback()
    except Exception as e:
        logger.debug("Rollback after failed FK sniff query also failed: %s", e)


def _containment_sql(
    quote: Any,
    from_relation: str,
    from_columns: list[str],
    to_relation: str,
    to_columns: list[str],
    sample: int,
) -> str:
    """Tuple containment: distinct child tuples LEFT JOINed to parent key
    tuples on every component. A constant marker column detects misses — a
    joined value being NULL can't, since key components may themselves be
    nullable. A single-column key is just the one-component case.
    """
    f_select = ", ".join(f"{quote(c)} AS v{i}" for i, c in enumerate(from_columns))
    f_not_null = " AND ".join(f"{quote(c)} IS NOT NULL" for c in from_columns)
    t_select = ", ".join(f"{quote(c)} AS v{i}" for i, c in enumerate(to_columns))
    on = " AND ".join(f"f.v{i} = t.v{i}" for i in range(len(from_columns)))
    return (
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN t._m IS NULL THEN 1 ELSE 0 END) AS unmatched FROM "
        f"(SELECT DISTINCT {f_select} FROM {from_relation} "
        f"WHERE {f_not_null} LIMIT {sample}) f "
        f"LEFT JOIN (SELECT DISTINCT {t_select}, 1 AS _m FROM {to_relation}) t "
        f"ON {on}"
    )


def _measure_containment(
    executor: Any,
    src: TableFKInfo,
    target: TableFKInfo,
    from_columns: list[str],
    to_columns: list[str],
    label: str,
    sample: int,
) -> float | None:
    """Fraction of sampled child tuples found in the parent's key, or None when
    the check can't run (incompatible types, empty source column).

    ``label`` names the edge in the skip warning — the only thing that differs
    between the single-column and composite callers.
    """
    sql = _containment_sql(
        executor.generator.safe_quote,
        src.sql_relation,
        from_columns,
        target.sql_relation,
        to_columns,
        sample,
    )
    try:
        rows = executor.execute_raw_sql(sql).fetchall()
    except Exception as e:
        print_warning(f"FK sniff skipped for {label}: {e}")
        _rollback(executor)
        return None
    if not rows or not rows[0] or not rows[0][0]:
        return None
    total = rows[0][0]
    unmatched = rows[0][1] or 0
    return float((total - unmatched) / total)


def measure_overlap(
    executor: Any,
    src: TableFKInfo,
    candidate: FKCandidate,
    target: TableFKInfo,
    sample: int = DEFAULT_SNIFF_SAMPLE,
) -> float | None:
    """Stage 2: fraction of sampled from-values found in the referenced key."""
    return _measure_containment(
        executor,
        src,
        target,
        [candidate.from_column],
        [candidate.to_column],
        f"{candidate.from_table}.{candidate.from_column} -> {candidate.target_ref}",
        sample,
    )


def measure_composite_overlap(
    executor: Any,
    src: TableFKInfo,
    candidate: CompositeFKCandidate,
    target: TableFKInfo,
    sample: int = DEFAULT_SNIFF_SAMPLE,
) -> float | None:
    """Stage 2 for a composite candidate: fraction of sampled child tuples
    found among the parent's key tuples, or None when the check can't run."""
    return _measure_containment(
        executor,
        src,
        target,
        [from_col for from_col, _ in candidate.pairs],
        [to_col for _, to_col in candidate.pairs],
        f"{candidate.from_table} -> {candidate.to_table} (composite)",
        sample,
    )


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


def _alternate_key_is_unique(
    executor: Any, target: TableFKInfo, raw_column: str
) -> bool:
    """Full-relation uniqueness of an alternate-key target, cached per column.

    Sample uniqueness got the column nominated; only full-table uniqueness
    makes it a real key — accepting a non-unique target would manufacture a
    fan-out join. NULLs collapse into one group and correctly read non-unique.
    """
    cached = target.unique_verdicts.get(raw_column)
    if cached is not None:
        return cached
    quoted = executor.generator.safe_quote(raw_column)
    sql = (
        f"SELECT MAX(_n) FROM "
        f"(SELECT COUNT(*) AS _n FROM {target.sql_relation} GROUP BY {quoted}) _g"
    )
    try:
        rows = executor.execute_raw_sql(sql).fetchall()
        verdict = bool(rows) and rows[0][0] == 1
    except Exception as e:
        print_warning(
            f"Uniqueness check skipped for alternate key "
            f"{target.name}.{raw_column}: {e}"
        )
        _rollback(executor)
        verdict = False
    target.unique_verdicts[raw_column] = verdict
    return verdict


def _verify_column(
    executor: Any,
    by_name: dict[str, TableFKInfo],
    candidates: list[FKCandidate],
    sample: int,
) -> InferredFK | None:
    """Stage 2 for one column: sniff candidates, keep the best that passes."""
    measured: list[tuple[FKCandidate, float]] = []
    for i, candidate in enumerate(candidates):  # confidence-ordered
        if candidate.alternate and not _alternate_key_is_unique(
            executor, by_name[candidate.to_table], candidate.to_column
        ):
            continue
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
    # A verified grain target outranks an alternate key on an otherwise-even tie.
    grain_top = [p for p in top if not p[0].alternate]
    if grain_top:
        top = grain_top
    if len(top) == 1:
        candidate, overlap = top[0]
    else:
        candidate, overlap = _break_overlap_tie(executor, by_name, top, sample)
    reverse = _reverse_coverage(executor, by_name, candidate, sample)
    partial = reverse < COMPLETE_REVERSE_THRESHOLD
    return InferredFK(
        candidate.from_table,
        candidate.from_column,
        candidate.to_table,
        candidate.to_column,
        candidate.match_kind,
        overlap,
        partial=partial,
    )


def _resolve_target_conflicts(
    inferred: list[InferredFK],
    by_name: dict[str, TableFKInfo],
) -> list[InferredFK]:
    """Role-alias FKs that share a target so the imports stay distinct.

    When several columns of one table resolve to the same target concept,
    wiring them all to the same import would collapse the columns onto a
    single concept (a role-playing dimension). Instead, each FK in the
    conflict group gets a ``role_alias`` derived from the FROM column's
    canonical name (minus its FK suffix), so the dim is imported under that
    role-specific name. Singleton FKs are left untouched and keep the bare
    target-table alias.
    """
    groups: dict[tuple[str, str, str], list[InferredFK]] = {}
    for fk in inferred:
        groups.setdefault((fk.from_table, fk.to_table, fk.to_column), []).append(fk)
    kept: list[InferredFK] = []
    for group in groups.values():
        if len(group) == 1:
            kept.append(group[0])
            continue
        src = by_name[group[0].from_table]
        for fk in group:
            canonical = src.raw_to_canonical[fk.from_column]
            fk.role_alias = _fk_stem(canonical) or canonical
            kept.append(fk)
            print_info(
                f"Role-aliasing inferred FK {fk.from_table}.{fk.from_column}"
                f" -> {fk.to_table}.{fk.to_column} (imported as `{fk.role_alias}`)"
            )
    return kept


def _verify_composite(
    executor: Any,
    by_name: dict[str, TableFKInfo],
    candidate: CompositeFKCandidate,
    sample: int,
) -> list[InferredFK] | None:
    """Stage 2 for a composite candidate: tuple containment gates the whole
    group; per-component reverse coverage then sets each binding's partial
    flag (the ``~`` modifier describes single-concept coverage)."""
    overlap = measure_composite_overlap(
        executor,
        by_name[candidate.from_table],
        candidate,
        by_name[candidate.to_table],
        sample,
    )
    if overlap is None or overlap < SUBSET_OVERLAP_THRESHOLD:
        return None
    group: list[InferredFK] = []
    for (from_col, to_col), kind in zip(candidate.pairs, candidate.match_kinds):
        component = FKCandidate(
            candidate.from_table, from_col, candidate.to_table, to_col, kind
        )
        reverse = _reverse_coverage(executor, by_name, component, sample)
        group.append(
            InferredFK(
                candidate.from_table,
                from_col,
                candidate.to_table,
                to_col,
                "composite",
                overlap,
                partial=reverse < COMPLETE_REVERSE_THRESHOLD,
            )
        )
    return group


def infer_foreign_keys(
    tables: list[TableFKInfo],
    executor: Any,
    level: IntrospectionLevel,
    sample_size: int = DEFAULT_SNIFF_SAMPLE,
) -> list[InferredFK]:
    """Run Stage 1 (and Stage 2 for ``FULL``) and return accepted FK edges."""
    if level is IntrospectionLevel.OFF or len(tables) < 2:
        return []
    candidates = generate_candidates(tables)
    by_name = {t.name: t for t in tables}
    accepted: list[InferredFK] = []
    for edges in candidates.values():
        if level is IntrospectionLevel.FAST:
            # Fast mode runs no queries, so alternate-key targets (unverified
            # uniqueness) are off the table entirely.
            named = [e for e in edges if not e.alternate]
            if not named:
                continue
            best = named[0]  # highest confidence
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

    # Composite-keyed targets, after single-column edges so those take
    # precedence: a column already bound to one concept cannot also carry a
    # composite component, and mixing single + composite links onto one
    # target would double-import it.
    claimed = {(fk.from_table, fk.from_column) for fk in accepted}
    targeted = {(fk.from_table, fk.to_table) for fk in accepted}
    for comp in generate_composite_candidates(tables):
        edge_label = f"{comp.from_table} -> {comp.to_table}"
        if (comp.from_table, comp.to_table) in targeted:
            print_info(
                f"Skipping composite FK {edge_label}: a single-column FK "
                "already links these tables"
            )
            continue
        if any((comp.from_table, from_col) in claimed for from_col, _ in comp.pairs):
            print_info(
                f"Skipping composite FK {edge_label}: a component column is "
                "already bound elsewhere"
            )
            continue
        if level is IntrospectionLevel.FAST:
            # The composite target key is the table's verified grain, so
            # name-only acceptance is as sound as the single-key fast path.
            group = [
                InferredFK(comp.from_table, f, comp.to_table, t, "composite", None)
                for f, t in comp.pairs
            ]
        else:
            maybe_group = _verify_composite(executor, by_name, comp, sample_size)
            if maybe_group is None:
                continue
            group = maybe_group
        accepted.extend(group)
        claimed.update((fk.from_table, fk.from_column) for fk in group)
        targeted.add((comp.from_table, comp.to_table))
    return _resolve_target_conflicts(accepted, by_name)


def _grain_key_columns(name: str, datasource: Datasource) -> list[str]:
    """Raw column names behind a datasource's grain.

    Grain components are concept *addresses*, and the datasource already states
    which column backs which address — so this is a lookup through its own
    bindings, not a name match. The ``local.``-prefixed and bare spellings of a
    default-namespace address name one concept (the same fallback
    ``Environment.__getitem__`` does), so either resolves.

    Resolution is all-or-nothing. Dropping the components that don't match
    leaves a *shorter* key, and a composite grain silently reduced to one
    column reads as a single identity: the table would then advertise an FK
    target that is not unique, and every reference onto it would fan out. No
    key at all is the safe answer, and it only costs this table its place as
    an FK target.
    """
    by_address = {
        c.concept.address: c.alias
        for c in datasource.columns
        if isinstance(c.alias, str)
    }
    resolved: list[str] = []
    for component in datasource.grain.component_order:
        raw = by_address.get(component) or by_address.get(
            f"{DEFAULT_NAMESPACE}.{component}"
        )
        if raw is None:
            print_warning(
                f"Datasource {name!r} declares grain component {component!r},"
                " which is bound to none of its columns; skipping it as a"
                " foreign key target."
            )
            return []
        resolved.append(raw)
    return resolved


def build_table_fk_info(
    name: str,
    datasource: Datasource,
    dialect: Any,
    alternate_keys: list[str] | None = None,
) -> TableFKInfo:
    """Derive a TableFKInfo from an ingested datasource (before FK wiring).

    ``alternate_keys`` are raw names of sample-unique single columns beyond
    the elected grain (see ``_target_key_options``).
    """
    raw_columns = [c.alias for c in datasource.columns if isinstance(c.alias, str)]
    raw_to_canonical = canonicalize_names(raw_columns)
    key_raw_columns = _grain_key_columns(name, datasource)
    address = datasource.address
    if isinstance(address, str):
        sql_relation = dialect.safe_quote(address)
    elif address.type in FILE_ADDRESS_TYPES:
        # File-backed datasources sniff through DuckDB's read_* functions —
        # their location is a path/URL, not a quotable relation name.
        sql_relation = file_introspection_source(address.location, address.type)
    else:
        sql_relation = dialect.safe_quote(address.location)
    known = set(raw_columns)
    return TableFKInfo(
        name=name,
        sql_relation=sql_relation,
        raw_columns=raw_columns,
        raw_to_canonical=raw_to_canonical,
        key_raw_columns=key_raw_columns,
        alternate_key_raw_columns=[
            a for a in (alternate_keys or []) if a in known and a not in key_raw_columns
        ],
        # The elected grain was verified (or DB-declared) at ingest time.
        unique_verdicts=dict.fromkeys(key_raw_columns, True),
    )


def merge_fk_maps(
    inferred: list[InferredFK], explicit: dict[str, dict[str, FKBinding]]
) -> dict[str, dict[str, FKBinding]]:
    """Combine inferred edges with explicit ``--fks``; explicit wins per column."""
    merged: dict[str, dict[str, FKBinding]] = {}
    for fk in inferred:
        merged.setdefault(fk.from_table, {})[fk.from_column] = fk.binding()
    for table, columns in explicit.items():
        merged.setdefault(table, {}).update(columns)
    return merged


def enrich_explicit_fks_partial(
    explicit_fk_map: dict[str, dict[str, FKBinding]],
    by_name: dict[str, TableFKInfo],
    executor: Any,
    sample: int = DEFAULT_SNIFF_SAMPLE,
) -> None:
    """Resolve the partial flag on explicit FKs via reverse-coverage sniffing.

    Explicit ``--fks`` arrive with the conservative ``partial=True`` default.
    When we have an executor (full mode), check whether the parent's key is
    fully covered by the child column; flip to ``partial=False`` when it is.
    Tables not present in ``by_name`` (e.g. the user named a non-ingested
    table) are left as-is. Mutates ``explicit_fk_map`` in place.
    """
    for from_table, columns in explicit_fk_map.items():
        src = by_name.get(from_table)
        if src is None:
            continue
        for from_column, binding in columns.items():
            target_ref = binding.target_ref
            if "@" in target_ref:
                target_ref = target_ref.rsplit("@", 1)[0]
            to_table, to_column = target_ref.rsplit(".", 1)
            target = by_name.get(to_table)
            if target is None:
                continue
            candidate = FKCandidate(
                from_table, from_column, to_table, to_column, "exact"
            )
            reverse = _reverse_coverage(executor, by_name, candidate, sample)
            if reverse >= COMPLETE_REVERSE_THRESHOLD:
                binding.partial = False
