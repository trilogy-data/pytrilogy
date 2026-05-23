import tempfile
from pathlib import Path

from click.testing import CliRunner

from tests.scripts._fault_dialect import FailingExecutor
from trilogy.dialect.enums import Dialects
from trilogy.scripts.ingest_helpers.fk_inference import (
    FKCandidate,
    InferredFK,
    TableFKInfo,
    _rollback,
    _stem_related,
    generate_candidates,
    infer_foreign_keys,
    measure_overlap,
    merge_fk_maps,
)
from trilogy.scripts.ingest_helpers.formatting import canonicalize_names
from trilogy.scripts.trilogy import cli


def _info(name, columns, key_columns, relation=None):
    return TableFKInfo(
        name=name,
        sql_relation=relation or f'"{name}"',
        raw_columns=columns,
        raw_to_canonical=canonicalize_names(columns),
        key_raw_columns=key_columns,
    )


def _targets(candidates, from_table, from_column):
    return {(c.to_table, c.to_column) for c in candidates[(from_table, from_column)]}


class TestCandidateGeneration:
    """Stage 1 — fuzzy name matching on canonical (prefix-stripped) names."""

    def test_tpcds_store_returns(self):
        store_returns = _info(
            "store_returns",
            [
                "sr_returned_date_sk",
                "sr_return_time_sk",
                "sr_item_sk",
                "sr_customer_sk",
                "sr_store_sk",
                "sr_ticket_number",
            ],
            ["sr_return_time_sk", "sr_ticket_number"],
        )
        date_dim = _info("date_dim", ["d_date_sk", "d_year"], ["d_date_sk"])
        time_dim = _info("time_dim", ["t_time_sk", "t_hour"], ["t_time_sk"])
        store = _info("store", ["s_store_sk", "s_market_id"], ["s_store_sk"])
        customer = _info(
            "customer", ["c_customer_sk", "c_first_name"], ["c_customer_sk"]
        )
        item = _info("item", ["i_item_sk", "i_brand"], ["i_item_sk"])
        candidates = generate_candidates(
            [store_returns, date_dim, time_dim, store, customer, item]
        )

        # Exact canonical matches.
        assert ("store", "s_store_sk") in _targets(
            candidates, "store_returns", "sr_store_sk"
        )
        assert ("customer", "c_customer_sk") in _targets(
            candidates, "store_returns", "sr_customer_sk"
        )
        assert ("item", "i_item_sk") in _targets(
            candidates, "store_returns", "sr_item_sk"
        )
        # Suffix match: returned_date_sk -> date_sk.
        assert ("date_dim", "d_date_sk") in _targets(
            candidates, "store_returns", "sr_returned_date_sk"
        )
        # Composite-key member that is also an FK: return_time_sk -> time_sk.
        assert ("time_dim", "t_time_sk") in _targets(
            candidates, "store_returns", "sr_return_time_sk"
        )

    def test_match_kinds(self):
        store_returns = _info(
            "store_returns",
            ["sr_store_sk", "sr_returned_date_sk", "sr_ticket_number"],
            ["sr_ticket_number"],
        )
        store = _info("store", ["s_store_sk", "s_market_id"], ["s_store_sk"])
        date_dim = _info("date_dim", ["d_date_sk", "d_year"], ["d_date_sk"])
        candidates = generate_candidates([store_returns, store, date_dim])

        store_edge = candidates[("store_returns", "sr_store_sk")][0]
        assert store_edge.match_kind == "exact"
        date_edge = candidates[("store_returns", "sr_returned_date_sk")][0]
        assert date_edge.match_kind == "suffix"

    def test_tpch_exact_after_prefix_strip(self):
        orders = _info(
            "orders",
            ["o_orderkey", "o_custkey", "o_orderdate"],
            ["o_orderkey"],
        )
        customer = _info(
            "customer", ["c_custkey", "c_name", "c_nationkey"], ["c_custkey"]
        )
        nation = _info(
            "nation", ["n_nationkey", "n_name", "n_regionkey"], ["n_nationkey"]
        )
        region = _info("region", ["r_regionkey", "r_name"], ["r_regionkey"])
        candidates = generate_candidates([orders, customer, nation, region])

        assert ("customer", "c_custkey") in _targets(candidates, "orders", "o_custkey")
        assert ("nation", "n_nationkey") in _targets(
            candidates, "customer", "c_nationkey"
        )
        assert ("region", "r_regionkey") in _targets(
            candidates, "nation", "n_regionkey"
        )

    def test_own_key_is_not_a_candidate(self):
        orders = _info("orders", ["order_id", "total"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        candidates = generate_candidates([orders, customers])
        # order_id is the table's own identity, not an FK.
        assert ("orders", "order_id") not in candidates

    def test_generic_id_suffix_does_not_overmatch(self):
        # order_id must NOT suffix-match a table merely keyed `id`.
        orders = _info("orders", ["order_id", "shipped"], [])
        customers = _info("customers", ["id", "name"], ["id"])
        candidates = generate_candidates([orders, customers])
        assert ("orders", "order_id") not in candidates

    def test_stem_to_table_name_match(self):
        # customer_id -> customers.id via stem<->table-name (plural) match.
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        candidates = generate_candidates([orders, customers])
        assert ("customers", "id") in _targets(candidates, "orders", "customer_id")

    def test_abbreviation_substring_match(self):
        # addr_sk -> customer_address.address_sk via stem substring (addr <- address).
        store_returns = _info(
            "store_returns", ["sr_addr_sk", "sr_ticket_number"], ["sr_ticket_number"]
        )
        customer_address = _info(
            "customer_address",
            ["ca_address_sk", "ca_state"],
            ["ca_address_sk"],
        )
        candidates = generate_candidates([store_returns, customer_address])
        assert ("customer_address", "ca_address_sk") in _targets(
            candidates, "store_returns", "sr_addr_sk"
        )

    def test_compound_stem_abbreviation_match(self):
        # `c_current_addr_sk` -> `customer_address.ca_address_sk`. The from-stem
        # is the compound ``current_addr``; the ``addr`` token must still be
        # recognized as abbreviating ``address``. Also covers role-playing FKs
        # like ``bill_addr_sk`` / ``ship_addr_sk`` on web_sales / catalog_sales.
        customer = _info(
            "customer",
            ["c_customer_sk", "c_current_addr_sk"],
            ["c_customer_sk"],
        )
        customer_address = _info(
            "customer_address",
            ["ca_address_sk", "ca_state"],
            ["ca_address_sk"],
        )
        candidates = generate_candidates([customer, customer_address])
        assert ("customer_address", "ca_address_sk") in _targets(
            candidates, "customer", "c_current_addr_sk"
        )


class TestValueOverlap:
    """Stage 2 — value-overlap verification on synthetic DuckDB tables."""

    def _executor(self):
        exec = Dialects.DUCK_DB.default_executor()
        exec.execute_raw_sql("CREATE TABLE dim(id INTEGER)")
        exec.execute_raw_sql("INSERT INTO dim VALUES (1),(2),(3),(4)")
        return exec

    def test_complete_containment(self):
        exec = self._executor()
        exec.execute_raw_sql("CREATE TABLE fact(dim_id INTEGER)")
        exec.execute_raw_sql("INSERT INTO fact VALUES (1),(2),(1),(3)")
        src = _info("fact", ["dim_id"], [])
        target = _info("dim", ["id"], ["id"])
        candidate = FKCandidate("fact", "dim_id", "dim", "id", "exact")
        assert measure_overlap(exec, src, candidate, target) == 1.0

    def test_no_overlap(self):
        exec = self._executor()
        exec.execute_raw_sql("CREATE TABLE fact(dim_id INTEGER)")
        exec.execute_raw_sql("INSERT INTO fact VALUES (90),(91),(92)")
        src = _info("fact", ["dim_id"], [])
        target = _info("dim", ["id"], ["id"])
        candidate = FKCandidate("fact", "dim_id", "dim", "id", "exact")
        assert measure_overlap(exec, src, candidate, target) == 0.0

    def test_subset_overlap(self):
        exec = self._executor()
        exec.execute_raw_sql("CREATE TABLE fact(dim_id INTEGER)")
        # 3 of 4 distinct values present in dim.
        exec.execute_raw_sql("INSERT INTO fact VALUES (1),(2),(3),(99)")
        src = _info("fact", ["dim_id"], [])
        target = _info("dim", ["id"], ["id"])
        candidate = FKCandidate("fact", "dim_id", "dim", "id", "exact")
        assert measure_overlap(exec, src, candidate, target) == 0.75

    def test_all_null_column_unverifiable(self):
        exec = self._executor()
        exec.execute_raw_sql("CREATE TABLE fact(dim_id INTEGER)")
        exec.execute_raw_sql("INSERT INTO fact VALUES (NULL),(NULL)")
        src = _info("fact", ["dim_id"], [])
        target = _info("dim", ["id"], ["id"])
        candidate = FKCandidate("fact", "dim_id", "dim", "id", "exact")
        assert measure_overlap(exec, src, candidate, target) is None


class TestInferForeignKeys:
    """End-to-end of the inference engine (Stage 1 + Stage 2)."""

    def _two_table_db(self):
        exec = Dialects.DUCK_DB.default_executor()
        exec.execute_raw_sql("CREATE TABLE customers(id INTEGER, name VARCHAR)")
        exec.execute_raw_sql("INSERT INTO customers VALUES (1,'a'),(2,'b'),(3,'c')")
        exec.execute_raw_sql(
            "CREATE TABLE orders(order_id INTEGER, customer_id INTEGER)"
        )
        exec.execute_raw_sql("INSERT INTO orders VALUES (10,1),(11,2),(12,1)")
        return exec

    def test_full_level_accepts_verified_edge(self):
        exec = self._two_table_db()
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        inferred = infer_foreign_keys([orders, customers], exec, "full")
        assert len(inferred) == 1
        edge = inferred[0]
        assert edge.from_table == "orders"
        assert edge.from_column == "customer_id"
        assert edge.target_ref == "customers.id"
        assert edge.overlap == 1.0

    def test_full_level_rejects_unverified_edge(self):
        exec = Dialects.DUCK_DB.default_executor()
        exec.execute_raw_sql("CREATE TABLE customers(id INTEGER, name VARCHAR)")
        exec.execute_raw_sql("INSERT INTO customers VALUES (1,'a'),(2,'b')")
        exec.execute_raw_sql(
            "CREATE TABLE orders(order_id INTEGER, customer_id INTEGER)"
        )
        # customer_id values are not contained in customers.id at all.
        exec.execute_raw_sql("INSERT INTO orders VALUES (10,77),(11,88)")
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        inferred = infer_foreign_keys([orders, customers], exec, "full")
        assert inferred == []

    def test_fast_level_skips_sniffing(self):
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        inferred = infer_foreign_keys([orders, customers], None, "fast")
        assert len(inferred) == 1
        assert inferred[0].overlap is None

    def test_off_level_returns_nothing(self):
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        assert infer_foreign_keys([orders, customers], None, "off") == []


class TestRolePlayingDimensions:
    """Multiple FKs from one table to the same dim get role-aliased instead of
    dropped, so each retains its own import + reference."""

    def test_conflicting_fks_role_aliased_not_dropped(self):
        customer = _info(
            "customer",
            ["c_customer_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk"],
            ["c_customer_sk"],
        )
        date_dim = _info("date_dim", ["d_date_sk", "d_year"], ["d_date_sk"])
        inferred = infer_foreign_keys([customer, date_dim], None, "fast")
        assert len(inferred) == 2
        aliases = sorted(fk.role_alias for fk in inferred)
        assert aliases == ["first_sales_date", "first_shipto_date"]

    def test_role_aliased_target_ref_encodes_alias(self):
        customer = _info(
            "customer",
            ["c_customer_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk"],
            ["c_customer_sk"],
        )
        date_dim = _info("date_dim", ["d_date_sk", "d_year"], ["d_date_sk"])
        inferred = infer_foreign_keys([customer, date_dim], None, "fast")
        refs = sorted(fk.target_ref for fk in inferred)
        assert refs == [
            "date_dim.d_date_sk@first_sales_date",
            "date_dim.d_date_sk@first_shipto_date",
        ]

    def test_singleton_fk_keeps_bare_alias(self):
        # No conflict — backward compatible: no role alias, no `@` in target_ref.
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        inferred = infer_foreign_keys([orders, customers], None, "fast")
        assert len(inferred) == 1
        assert inferred[0].role_alias is None
        assert inferred[0].target_ref == "customers.id"


class TestStemHelpers:
    """Cover the short-stem rejection and token-length guard in _stem_related."""

    def test_short_stem_rejected(self):
        # _MIN_STEM_LEN is 3 — a 2-char stem must not match anything.
        assert _stem_related("ab", "abcdef") is False
        assert _stem_related("address", "ab") is False

    def test_compound_long_with_short_tokens_skipped(self):
        # Compound long stem has 1-char tokens that must be skipped (length
        # guard); the remaining tokens don't match the short stem, so result
        # is False — but the per-token skip branch was exercised.
        assert _stem_related("a_b_address", "phone") is False


class TestRollbackSwallow:
    """``_rollback`` must swallow rollback failures so callers stay clean."""

    def test_rollback_swallows_executor_failure(self):
        class _NoisyExec:
            class _Conn:
                def rollback(self):
                    raise RuntimeError("connection died")

            connection = _Conn()

        # No exception should escape.
        _rollback(_NoisyExec())


class TestMeasureOverlapSniffFailure:
    """Sniff queries that raise return None and roll back via _rollback."""

    def test_measure_overlap_returns_none_on_sql_error(self):
        # FailingExecutor.execute_raw_sql always raises; measure_overlap must
        # catch, warn, attempt rollback, and return None.
        exec = FailingExecutor(error=RuntimeError("sql blew up"))
        src = _info("fact", ["dim_id"], [])
        target = _info("dim", ["id"], ["id"])
        candidate = FKCandidate("fact", "dim_id", "dim", "id", "exact")
        assert measure_overlap(exec, src, candidate, target) is None
        # And the rollback path was hit.
        assert exec.connection.rolled_back is True


class TestTieBreakByReverseCoverage:
    """When two candidates tie on equal forward overlap and confidence, the
    reverse-coverage tie-break (_break_overlap_tie + _reverse_coverage) picks
    the parent whose key values are more densely used by the child."""

    def test_break_overlap_tie_picks_denser_parent(self):
        exec = Dialects.DUCK_DB.default_executor()
        # Two equally-named target dims; both fully contain `fact.id`.
        exec.execute_raw_sql("CREATE TABLE fact(id INTEGER, label VARCHAR)")
        exec.execute_raw_sql("INSERT INTO fact VALUES (1,'a'),(2,'b'),(3,'c')")
        # `dim_dense` exactly mirrors fact.id — reverse coverage 1.0.
        exec.execute_raw_sql("CREATE TABLE dim_dense(id INTEGER)")
        exec.execute_raw_sql("INSERT INTO dim_dense VALUES (1),(2),(3)")
        # `dim_sparse` is a strict superset — reverse coverage 3/6 = 0.5.
        exec.execute_raw_sql("CREATE TABLE dim_sparse(id INTEGER)")
        exec.execute_raw_sql("INSERT INTO dim_sparse VALUES (1),(2),(3),(4),(5),(6)")

        # `fact` has no own key, so its `id` column becomes an FK candidate.
        fact = _info("fact", ["id", "label"], [])
        dim_dense = _info("dim_dense", ["id"], ["id"])
        dim_sparse = _info("dim_sparse", ["id"], ["id"])

        inferred = infer_foreign_keys([fact, dim_dense, dim_sparse], exec, "full")
        assert len(inferred) == 1
        # Dense parent wins.
        assert inferred[0].to_table == "dim_dense"


class TestVerifyColumnContinuesOnNoneOverlap:
    """A sniff that returns None (all-null column) must let the loop advance
    to the next candidate."""

    def test_skip_unverifiable_candidate(self):
        exec = Dialects.DUCK_DB.default_executor()
        # `fact.dim_id` is all NULL → sniff returns None.
        exec.execute_raw_sql("CREATE TABLE fact(dim_id INTEGER)")
        exec.execute_raw_sql("INSERT INTO fact VALUES (NULL),(NULL)")
        exec.execute_raw_sql("CREATE TABLE dim(id INTEGER)")
        exec.execute_raw_sql("INSERT INTO dim VALUES (1),(2)")

        fact = _info("fact", ["dim_id"], [])
        dim = _info("dim", ["id"], ["id"])
        # No accepted edge — but the continue branch was exercised.
        assert infer_foreign_keys([fact, dim], exec, "full") == []


class TestFKCandidateTargetRef:
    def test_target_ref_is_table_dot_column(self):
        assert (
            FKCandidate("orders", "customer_id", "customers", "id", "exact").target_ref
            == "customers.id"
        )


class TestInferredFKTargetRefRoleAlias:
    def test_target_ref_with_role_alias(self):
        fk = InferredFK(
            "customer",
            "c_first_shipto_date_sk",
            "date_dim",
            "d_date_sk",
            "stem",
            None,
            role_alias="first_shipto_date",
        )
        assert fk.target_ref == "date_dim.d_date_sk@first_shipto_date"


class TestMergeFKMaps:
    def test_explicit_overrides_inferred(self):
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        inferred = infer_foreign_keys([orders, customers], None, "fast")
        explicit = {"orders": {"customer_id": "people.id"}}
        merged = merge_fk_maps(inferred, explicit)
        assert merged["orders"]["customer_id"] == "people.id"

    def test_inferred_used_when_no_explicit(self):
        orders = _info("orders", ["order_id", "customer_id"], ["order_id"])
        customers = _info("customers", ["id", "name"], ["id"])
        inferred = infer_foreign_keys([orders, customers], None, "fast")
        merged = merge_fk_maps(inferred, {})
        assert merged["orders"]["customer_id"] == "customers.id"


def _fk_config(tmppath: Path) -> Path:
    setup_sql = tmppath / "setup.sql"
    setup_sql.write_text(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR);\n"
        "CREATE TABLE orders (\n"
        "  order_id INTEGER PRIMARY KEY,\n"
        "  customer_id INTEGER,\n"
        "  total DOUBLE\n"
        ");\n"
        "INSERT INTO customers VALUES (1,'alice'),(2,'bob');\n"
        "INSERT INTO orders VALUES (10,1,99.5),(11,2,42.0),(12,1,10.0);"
    )
    config_file = tmppath / "trilogy.toml"
    config_file.write_text(
        '[engine]\ndialect = "duckdb"\n\n'
        f'[setup]\nsql = ["{setup_sql.as_posix()}"]\n'
    )
    return config_file


def test_ingest_infers_fk_and_cross_table_query_resolves():
    """ingest with no --fks links the tables; a cross-table query then runs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = _fk_config(tmppath)
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "customers,orders",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        orders_preql = (out_dir / "orders.preql").read_text()
        assert "import customers" in orders_preql

        query = tmppath / "query.preql"
        query.write_text(
            "import raw.orders as orders;\n\n"
            "select orders.customers.name, orders.total\n"
            "order by orders.total desc;\n"
        )
        result = runner.invoke(
            cli, ["run", str(query), "duckdb", "--config", str(config_file)]
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0


def test_ingest_no_infer_fks_leaves_tables_disconnected():
    """--fk-infer-level off keeps the historical independent-datasource output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = _fk_config(tmppath)
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "customers,orders",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
                "--fk-infer-level",
                "off",
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert "import customers" not in (out_dir / "orders.preql").read_text()
