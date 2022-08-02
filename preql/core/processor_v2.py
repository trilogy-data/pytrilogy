
# for each concept
# see if there is a direct select
# see if there is a join
# add to

def process_query(environment: Environment, statement: Select,
                  hooks: Optional[List[BaseProcessingHook]] = None) -> ProcessedQuery:
    '''Turn the raw query input into an instantiated execution tree.'''
    graph = generate_graph(environment)

    query_graph = build_execution_graph(statement, environment=environment, relation_graph=graph)

    # run lifecycle hooks
    # typically for logging
    hooks = hooks or []
    for hook in hooks:
        hook.query_graph_built(query_graph)
    starts = [
        n for n in query_graph.nodes if query_graph.in_degree(n) == 0
    ]  # type: ignore
    ctes = []
    seen = set()
    for node in starts:
        ctes += walk_query_graph(node, query_graph, statement.grain, seen=seen)
    joins = []
    return ProcessedQuery(
        order_by=statement.order_by,
        grain=statement.grain,
        limit=statement.limit,
        where_clause=statement.where_clause,
        output_columns=statement.output_components,
        ctes=ctes,
        joins=joins,
    )
