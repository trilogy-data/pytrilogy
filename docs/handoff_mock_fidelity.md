# Handoff: mock-data fidelity — what still can't be caught

`docs/mock_data.md` describes what `trilogy/dialect/mock.py` guarantees. This is
the other half: what mock data still gets wrong, ranked by the bug class each
gap hides.

The test to apply to each item is not "does this look like real data" but
**"what wrong planner behaviour currently reads as correct?"**

---

## 1. Numeric values have no realistic magnitude or relationship

Measured on the battery's mock output: `retail_price` ranges 7,375 – 996,803
(real thelook: 12 – 240) and `cost > retail_price` for 45% of products.

Endpoints are now seated in the pool for *declared* ranges, and traits with a
bounded domain (`::percent`, `::latitude`, `::year`) get real magnitudes. What
remains is cosmetic-but-confusing: an undeclared numeric is uniform over a huge
span, and two related numerics (`cost` vs `retail_price`) are independent, so
margins are nonsense.

Correlation can't be inferred in general — it needs a declared constraint, and
the language has no spelling for "cost <= retail_price" on a datasource that
`literal_domains` could read. A log-normal-ish distribution instead of uniform
would exercise top-N, bucketing and percentile paths more honestly, but catches
nothing on its own.

## 2. Many-to-many fan-out is uniform

A junction now carries several rows per member (`grain_indices`), but exactly
the same number for each: with 300 memberships over 100 users, every user is in
precisely 3 groups. Foreign keys got past this — `MockManager.multiplied` deals
the surplus out at random — but a grain component can't be dealt that way and
stay unique, which is the constraint the lap-shift construction satisfies.

So skew handling is still unexercised on the m:m side, and a query whose plan
depends on one member having many more partners than another sees a flat
distribution. Whatever fixes this has to keep two properties the construction
currently proves: the grain tuple never repeats, and each component still
reaches its whole pool inside the row count.

## 3. `complete where` widens keys only

`MockManager.complete_slice` lays the full key domain across the slice, which is
what "the source is missing nothing here" means for a join. A partial *value*
property keeps its truncated pool inside the slice — for `~amount` the claim
would be about which rows have an amount, and the mock has no way to say that
short of nulling, which is `NULLABLE`'s job.

Two more narrowings, both logged rather than silent: a filter column fixed by
the grain or by another table's dependency can't be biased toward the admitted
values (the slice is then wherever it already landed, and may be too short for
the key domain), and only conjunctions of `=`/`in` against literals are read.

## 4. Trait generators cover the stdlib, not user models

`TRAIT_GENERATORS` is keyed by trait name and guarded by the base type it
produces. It covers the stdlib traits with a closed or bounded domain; a user's
own `type sku_family string` still degrades to `mock_string_<random>`, which is
the unique-value-per-row problem the categorical advice exists to avoid.

`register_trait_mock` is the extension point, but it is a Python call — there is
no spelling in the language that attaches a domain to a declared type short of
making it an `enum<string>[...]`, which is often the right answer anyway.

## 5. Latent: pattern sampling caps unbounded repetition at three

`mock_pattern` treats `*` as `{0,3}` and `+` as `{1,3}`. Values always match the
declared pattern, so nothing is *wrong*, but a model whose pattern implies long
strings (`\S{20,}` is fine; `\S+` is not) mocks short ones, and a column whose
downstream use is length-sensitive won't see it.

---

## Closed

Each of these was on this list and is now covered by a test in
`tests/test_mocking.py` that fails if the behaviour is reverted.

- **Nothing is ever NULL.** `NULLABLE` columns now get `NULL_FRACTION` of their
  rows emptied, never on grain components, never at the cost of a distinct
  value, and never inside a `complete where` slice. This is what lets a unit run
  tell a value NULL from an outer-join padding NULL.
- **A datasource's declared `where` was ignored.** Conjunctions of `=` and `in`
  against literals now restrict the mocked pools; anything else is logged.
- **`complete where` (`non_partial_for`) was not modelled.** The filter column is
  biased toward the admitted values until the slice is as tall as the key
  domain, and the full domain of every `~` key is dealt across it; rows outside
  keep the partial prefix. See item 3 for what it still doesn't cover.
- **Regex-validated strings couldn't be mocked at all.** `mock_pattern` samples
  the forms a type declaration uses, so the stdlib's `::url`, `::ipv4_address`,
  `::email_address` and `::hex` are all reachable from the unit tier. Lookaround
  and backreferences still raise.
- **Trait generators were a hardcoded two-branch if-chain.** Now a registry.
- **Fan-out only recognized single-component entity keys.** Owners are keyed by
  the whole grain, so a junction sits above both its entities and a fact binding
  the pair sits above the junction.
- **Composite grains didn't reach a full cross product.** The lcm cap is gone;
  `grain_indices` keeps the tuple unique across the whole product, so the table
  fills to its row target. A dense cube still needs a row target big enough to
  hold it — which is now reachable, see below.
- **Scale was not reachable from the statement.** `mock datasources a, b with
  (scale_factor=500);`, and `trilogy unit --scale`.
- **Datetime keys round-tripped through local time.** `mock_datetimes` does
  naive arithmetic, so the fixture no longer depends on the host's timezone
  database.
- **The agent tier synthesized rollups.** `_materialize_mock_tables` runs
  through `mock_environment` now, so its pre-aggregates are computed from the
  facts they summarize.
- **Two datasources at one address dropped columns.** The table gets the union
  of every binding's columns.
- **Declared ranges never produced their endpoints.**
- **Uniform fan-out.** `MockManager.multiplied` deals the surplus rows out at
  random, so members carry 1–8 facts rather than exactly the mean. (Grain
  components still can't — item 2.)

## Not gaps

- **Row counts too small to be realistic.** Deliberate. The unit tier's job is
  to make the model's claims observable, not to measure performance.
