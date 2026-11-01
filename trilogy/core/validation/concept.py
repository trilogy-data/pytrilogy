from trilogy import Environment, Executor
from trilogy.core.enums import Derivation, FunctionType, Modifier
from trilogy.core.exceptions import (
    ConceptModelValidationError,
    DatasourceColumnBindingData,
    DatasourceColumnBindingError,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import CompiledCTE
from trilogy.core.validation.common import (
    SAMPLE_LIMIT,
    ExpectationType,
    ValidationTest,
    easy_query,
    grain_check_address,
)


def _is_summed(check: BuildConcept) -> bool:
    lineage = check.lineage
    if isinstance(lineage, BuildAggregateWrapper):
        lineage = lineage.function
    return isinstance(lineage, BuildFunction) and lineage.operator == FunctionType.SUM


def _is_complete(concept: BuildConcept, datasource: BuildDatasource) -> bool:
    return next(
        x for x in datasource.columns if x.concept.address == concept.address
    ).is_complete


def _partial_binding_error(
    concept: BuildConcept, datasource: BuildDatasource, message: str
) -> DatasourceColumnBindingError:
    return DatasourceColumnBindingError(
        address=datasource.identifier,
        errors=[
            DatasourceColumnBindingData(
                address=concept.address,
                value=None,
                value_type=concept.datatype,
                value_modifiers=[Modifier.PARTIAL],
                actual_type=concept.datatype,
                actual_modifiers=concept.modifiers,
            )
        ],
        message=message,
    )


def missing_values_sql(
    exec: Executor,
    env: Environment,
    concept: BuildConcept,
    complete: BuildDatasource,
    others: list[BuildDatasource],
) -> str:
    """How many values of ``concept`` the ``others`` hold that ``complete``
    lacks: an anti-join of the union of their distinct values."""
    generator = exec.generator
    ctes: list[CompiledCTE] = []
    names: list[str] = []
    for idx, datasource in enumerate([complete, *others]):
        compiled = generator.generate_ctes(
            easy_query(
                concepts=[concept],
                datasource=datasource,
                env=env,
                grain=BuildGrain(components={concept.address}),
                limit=None,
                # one datasource imported twice shares a name
                name=f"complete_source_{idx}",
            )
        )
        name = f"complete_values_{idx}"
        ctes += [
            *compiled[:-1],
            CompiledCTE(name=name, statement=compiled[-1].statement),
        ]
        names.append(generator.quote(name) if generator.QUOTE_CTE_NAMES else name)
    column = generator.quote(concept.safe_address)
    held = " UNION ".join(f"SELECT {column} AS v FROM {name}" for name in names[1:])
    full_select = (
        f"SELECT COUNT(*) AS missing FROM ({held}) held WHERE held.v IS NOT NULL "
        f"AND NOT EXISTS (SELECT 1 FROM {names[0]} c WHERE c.{column} = held.v)"
    )
    return generator.render_sql_template(
        recursive=False, output=None, full_select=full_select, ctes=ctes
    )


def _validate_summed(
    concept: BuildConcept,
    check: BuildConcept,
    sources: list[BuildDatasource],
    env: Environment,
    exec: Executor | None,
) -> list[ValidationTest]:
    """An additive metric's complete sources must agree on its total; one below
    the largest is missing rows."""
    results: list[ValidationTest] = []
    seen: dict[str, int] = {}
    for datasource in sources:
        query = easy_query(concepts=[check], datasource=datasource, env=env, limit=1)
        if exec is None:
            results.append(
                ValidationTest(
                    raw_query=query,
                    check_type=ExpectationType.ROWCOUNT,
                    expected=f"equal_max_{concept.safe_address}",
                    result=None,
                    ran=False,
                )
            )
            continue
        rows = exec.execute_raw_sql(exec.generate_sql(query)[-1]).fetchall()
        seen[datasource.name] = (rows[0][0] if rows else 0) or 0
    if exec is None:
        return results
    max_seen = max(seen.values(), default=0)
    for datasource in sources:
        err = None
        if seen[datasource.name] < max_seen:
            err = _partial_binding_error(
                concept,
                datasource,
                f"Concept {concept.address} is missing values in datasource "
                f"{datasource.name} (max grain check value {max_seen}, datasource "
                f"value {seen[datasource.name]}) but is not marked as partial.",
            )
        results.append(
            ValidationTest(
                check_type=ExpectationType.ROWCOUNT,
                expected=str(max_seen),
                result=err,
                ran=True,
            )
        )
    return results


def validate_multi_datasource_concept(
    concept: BuildConcept,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None = None,
) -> list[ValidationTest]:
    """A complete binding is a claim the planner trusts: reading the concept
    from that source alone yields every value. So it must hold every value any
    other source holds, partial ones included. Equal distinct counts do not
    show that ({1, 2} beside {2, 3}), so the check is set containment. An
    additive aggregate metric instead compares SUM totals across its complete
    sources."""
    check = build_env.concepts.get(grain_check_address(concept))
    if check is None:
        return []
    sources = [
        datasource
        for datasource in build_env.datasources.values()
        if concept.address in {c.address for c in datasource.concepts}
    ]
    if len(sources) <= 1:
        return []
    completes = [ds for ds in sources if _is_complete(concept, ds)]
    if _is_summed(check):
        return _validate_summed(concept, check, completes, env, exec)
    results: list[ValidationTest] = []
    for complete in completes:
        others = [ds for ds in sources if ds is not complete]
        if exec is None:
            results.append(
                ValidationTest(
                    check_type=ExpectationType.ROWCOUNT,
                    expected=f"contains_all_{concept.safe_address}",
                    result=None,
                    ran=False,
                )
            )
            continue
        sql = missing_values_sql(exec, env, concept, complete, others)
        rows = exec.execute_raw_sql(sql).fetchall()
        missing = rows[0][0] if rows else 0
        err = None
        if missing:
            err = _partial_binding_error(
                concept,
                complete,
                f"Concept {concept.address} is bound complete in datasource "
                f"{complete.name}, but {missing} of its values held by "
                f"{', '.join(ds.name for ds in others)} are missing there. Mark "
                "the binding partial (~) or add the missing rows.",
            )
        results.append(
            ValidationTest(
                generated_query=sql,
                check_type=ExpectationType.ROWCOUNT,
                expected="0",
                result=err,
                ran=True,
            )
        )
    return results


def _bound(datasource: BuildDatasource) -> set[str]:
    return {column.concept.address for column in datasource.columns}


def binding_sources(
    concept: BuildConcept, build_env: BuildEnvironment
) -> list[tuple[frozenset[str], list[BuildDatasource]]]:
    """Each row-determinant set some table binds `concept` beside, with every
    table binding those determinants and `concept`. Each binding claims the
    determinants determine the concept model-wide (`Binding.closure_sound`),
    and one table's grain check proves it for its own rows only."""
    datasources = list(build_env.datasources.values())
    claims: dict[frozenset[str], None] = {}
    for datasource in datasources:
        determinants = datasource.row_determinants
        if determinants and concept.address in _bound(datasource) - determinants:
            claims.setdefault(determinants, None)
    out = []
    for determinants in claims:
        needed = determinants | {concept.address}
        sources = [ds for ds in datasources if needed <= _bound(ds)]
        if len(sources) > 1:
            out.append((determinants, sources))
    return out


def binding_violation_sql(
    exec: Executor,
    env: Environment,
    concept: BuildConcept,
    determinants: list[BuildConcept],
    sources: list[BuildDatasource],
) -> str:
    """Determinant values the union of `sources` pairs with two values of
    `concept`, NULL counting as one (`Binding.cross_check_exact`)."""
    generator = exec.generator
    concepts = [*determinants, concept]
    ctes: list[CompiledCTE] = []
    names: list[str] = []
    for idx, datasource in enumerate(sources):
        compiled = generator.generate_ctes(
            easy_query(
                concepts=concepts,
                datasource=datasource,
                env=env,
                grain=BuildGrain(components={c.address for c in concepts}),
                limit=None,
                name=f"binding_source_{idx}",
            )
        )
        name = f"binding_rows_{idx}"
        ctes += [
            *compiled[:-1],
            CompiledCTE(name=name, statement=compiled[-1].statement),
        ]
        names.append(generator.quote(name) if generator.QUOTE_CTE_NAMES else name)
    keys = ", ".join(generator.quote(c.safe_address) for c in determinants)
    column = generator.quote(concept.safe_address)
    held = " UNION ".join(f"SELECT {keys}, {column} FROM {name}" for name in names)
    full_select = (
        f"SELECT {keys} FROM ({held}) held GROUP BY {keys} "
        f"HAVING COUNT(DISTINCT {column}) > 1 "
        f"OR (COUNT({column}) > 0 AND COUNT(*) > COUNT({column}))"
    )
    return generator.render_sql_template(
        recursive=False, output=None, full_select=full_select, ctes=ctes
    )


def validate_binding_fds(
    concept: BuildConcept,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None = None,
) -> list[ValidationTest]:
    results: list[ValidationTest] = []
    for determinants, sources in binding_sources(concept, build_env):
        expected = f"binding_fd_{concept.safe_address}"
        if exec is None:
            results.append(
                ValidationTest(
                    check_type=ExpectationType.ROWCOUNT,
                    expected=expected,
                    result=None,
                    ran=False,
                )
            )
            continue
        keys = [build_env.concepts[a] for a in sorted(determinants)]
        sql = binding_violation_sql(exec, env, concept, keys, sources)
        result = exec.execute_raw_sql(sql)
        columns = list(result.keys())
        rows = result.fetchmany(SAMPLE_LIMIT)
        err = None
        if rows:
            err = ConceptModelValidationError(
                f"Concept {concept.address} is bound beside "
                f"({', '.join(sorted(determinants))}), which each binding claims "
                f"determines it model-wide, but "
                f"{', '.join(ds.name for ds in sources)} pair these values with "
                f"more than one {concept.address}: "
                f"{[dict(zip(columns, row)) for row in rows]}"
            )
        results.append(
            ValidationTest(
                generated_query=sql,
                check_type=ExpectationType.ROWCOUNT,
                expected=expected,
                result=err,
                ran=True,
            )
        )
    return results


def validate_datasources(
    concept: BuildConcept, build_env: BuildEnvironment
) -> list[ValidationTest]:
    if concept.lineage:
        return []
    for datasource in build_env.datasources.values():
        if concept.address in [c.address for c in datasource.concepts]:
            return []
    if concept.derivation != Derivation.ROOT:
        return []
    if concept.name.startswith("__") or (
        concept.namespace and concept.namespace.startswith("__")
    ):
        return []
    return [
        ValidationTest(
            check_type=ExpectationType.LOGICAL,
            expected=None,
            result=ConceptModelValidationError(
                f"Concept {concept.address} is a root concept but has no datasources bound"
            ),
            ran=True,
        )
    ]


def validate_concept(
    concept: BuildConcept,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None = None,
) -> list[ValidationTest]:
    base: list[ValidationTest] = []
    base += validate_datasources(concept, build_env)
    base += validate_multi_datasource_concept(concept, env, build_env, exec)
    base += validate_binding_fds(concept, env, build_env, exec)
    return base
