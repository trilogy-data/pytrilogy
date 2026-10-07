# Handoff: grain pin follow-ups

Open items left by the grain pin (`docs/grain_pin.md`, `trilogy/core/grain_pin.py`):
a NULL-absorbing expression read by a select is evaluated on the select's row.
Every wrong-rows item below is pinned by a strict xfail naming it.

## Wrong rows (strict xfails)

1. **Null-accepting WHERE over a pinned value on a `union join` axis.**
   `with r as select customer_id, status; select customer_id, status, r.status
   union join r.customer_id = customer_id where status is null;` returns every
   row. The pinned `status` group is built from the unfiltered rows; the filtered
   ROOT joins back to it, and on the union axis `inject_condition_at_node` does
   not force INNER (`pairs_on_outer_relation`), so the join types LEFT and
   `tighten_join_for_filtered_branch` declines a null-accepting condition
   (`_is_filter_population`: partner keys nullable). The null-rejecting spelling
   (`= 'delivered'`) is fixed. `tests/engine/test_rowset_declared_join_pairing.py`.
2. **Pinned WHERE over an aggregate by a no-ELSE CASE pairs a padded NULL with
   the value-NULL group.** `select customer_id, pstatus, ..., sum(amount) by
   pstatus as s where coalesce(sum(amount) by pstatus, 0) = 0` gives cat
   `s = 10` (the NULL-pstatus group's sum). The pinned WHERE expression reads the
   aggregate on cat's padded row and the join pairs `pstatus` null-safely.
   `tests/engine/test_derived_key_domain.py`
   (`test_where_beside_a_region_fed_output_aggregate_filters_the_rows`).
3. **A redefined rowset in one session changes a reader's rows.** After
   `rowset s <- select customer_id as c, ..., status as st; select s.o, s.st`, a
   later nested `rowset t ...; rowset s <- select t.c as c2, ...; select s.o2,
   s.st2` drops the body's pinned row (alone it keeps it). Pins are identical in
   both runs; the reader's unread-region decision differs. Not the session build
   caches (clearing them does not change it): environment state left by the
   first rowset. `tests/engine/test_rowset_unread_region.py`
   (`test_redefined_rowset_reads_the_same_rows`).

Pre-existing, found by the new bound twins (base branch reproduces both):

4. **A bound `is_returned` column reads NULL beside the reason region.**
   `tests/engine/test_unmodelled_regions.py` (`select item_desc, reason_class,
   is_returned`).
5. **The `by *` gate emits a NULL row when the WHERE empties every row**
   (`RIGHT JOIN gate on 1=1`). `tests/engine/test_derived_key_domain.py`.

## Plan size

6. **A pinned row value builds its own copy of the select's rows.**
   `test_forked_full_column_set` (`tests/engine/test_duckdb_partial_key_assembly.py`)
   went 6 -> 10 joins, rows right. The pinned `order_status` group reads both
   region domains plus the order dimension and is INNER-joined back null-safely
   on every key, while the main row stream already holds those rows. The fix is
   to evaluate a pinned BASIC on the FINAL merge's rows (the group-fold machinery,
   `_read_parents_in_place`, only folds into aggregates today).
7. **An aggregate under a pinned reader takes the region's rows.**
   `min(amount) by user_id` is fed the user region once its reader is no longer
   solid (`region_domains`: an aggregate grouped by a carried key is evaluated
   over the region). For `min`/`max`/`sum` the region adds only NULL groups the
   FINAL would pad anyway; only a count (`zero_on_empty`) or an inline argument
   taking a value on padding needs them. Broad: every span aggregate goes through
   `evaluated_over_region`.
8. **q84 +124 chars**, accepted in `tests/modeling/accepted_growth.toml`: the
   WHERE null-rejects the sales line, so no padded row survives, but the build
   pins before the keyspace that would prove it exists. Pinning after the
   keyspace (or reading its `emptied_by`) would remove it.

## Design edges

9. **A stored-only value pinned to another keyspace** raises the generic
   `NoDatasourceException` for its unbound input
   (`test_stored_only_value_cannot_be_pinned_to_another_keyspace`). A message
   naming the keyspace mismatch ("stored only at the order keyspace") would help.
10. **IS [NOT] NULL is not NULL-absorbing**, by decision: it feeds presence probes
    and null-rejection proofs. A pinned expression inlines its named reads, so a
    CASE over `undelivered` sees `delivery_date is null` on the select's row, while
    `undelivered` itself stays NULL there. Revisit if the registry grows.
11. **Address vs canonical audit.** A pinned concept and the column persisting it
    share an address. Two planner checks compared by address and were fixed
    (`predicate_pushdown._parent_holds_the_same_concepts`,
    `group_graph._scan_columns`); others likely remain. The bound twins in
    `tests/helpers/models.py` are the oracle that catches them.
12. **Anchor heuristics are build-time approximations** of what the keyspace
    knows: `_always_beside`, `_held_beside`, `_co_held_only_beside`, `_may_pad`.
    Each avoids a pin that cannot change a value; an over-pin costs plan size,
    an under-pin would cost rows. Moving the decision after `build_keyspace`
    would make it exact.
