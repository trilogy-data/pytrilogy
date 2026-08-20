# Handoff: mock-data fidelity — what still can't be caught

`docs/mock_data.md` describes what `trilogy/dialect/mock.py` guarantees. This is
the other half: what mock data still gets wrong, ranked by the bug class each
gap hides.

The test to apply to each item is not "does this look like real data" but
**"what wrong planner behaviour currently reads as correct?"**

---

## 1. `complete where` (`non_partial_for`) is not modelled

`datasource.where` is now honoured, and `partial datasource` needs nothing (the
parser stamps `PARTIAL` onto every column). `non_partial_for` — the slice in
which a partial datasource *is* complete — still isn't, and it is the newest
and least-covered surface of the partial-bridge machinery.

The design constraint that makes this more than a filter, found while scoping
it: honouring `complete where region = 'NA'` means the rows in that slice must
carry the **whole** key domain. With the filter column drawn from its own pool
the slice is a few rows out of hundreds, far too few to cover a key. So the
generator has to *bias* the filter column toward the admitted values — sizing
the slice to fit the key domain — and then lay the full key pool across it,
while rows outside the slice keep the partial prefix. That is a coordinated
two-column assignment, not a pool restriction, which is why it isn't bolted
onto `declared_value_domains`.

## 2. Regex-validated strings can't be mocked at all

`mock_validated` raises `NotImplementedError` when the type carries a pattern.
A model using `string['[A-Z]{3}']` fails `trilogy unit` outright unless it also
carries a trait with a hardcoded generator. The loud failure is the right
default, but it makes the whole tier unavailable to those models.

Shape of the fix: a small pattern sampler covering the anchored character-class
and repetition forms people actually write, falling back to the current raise.

## 3. Trait generators are a hardcoded two-branch if-chain

`mock_datatype` knows `email_address` and `hex`, with a `TODO: get stdlib
inventory some other way?` beside them. Every other trait — stdlib or
user-declared — degrades to `mock_string_<random>`, which for a trait like
`::url` or `::iso_country` is exactly the "unique value per row" problem the
categorical-domain advice exists to avoid.

Shape of the fix: a registry keyed by trait name that the stdlib populates and
user models can extend.

## 4. Fan-out only recognizes single-component entity keys

`datasource_depths` anchors on datasources with a one-component grain, so a
junction/bridge table grained on a pair sits at depth 0 and never fans out.
Many-to-many relationships therefore stay 1:1 in mock data, which is the same
blind spot fan-out was introduced to close for the 1:N case.

## 5. Numeric values have no realistic magnitude or relationship

Measured on the battery's mock output: `retail_price` ranges 7,375 – 996,803
(real thelook: 12 – 240) and `cost > retail_price` for 45% of products.

Endpoints are now seated in the pool for *declared* ranges, which is the part
that hid real bugs. What remains is cosmetic-but-confusing: an undeclared
numeric is uniform over a huge span, and two related numerics (`cost` vs
`retail_price`) are independent, so margins are nonsense. Correlation can't be
inferred in general — it needs a declared constraint, so it belongs with item 1.
A log-normal-ish distribution instead of uniform would exercise top-N,
bucketing and percentile paths more honestly, but catches nothing on its own.

## 6. Composite grains still don't reach a full cross product

A `(id, is_active)` grain fills 100 of the 200 possible combinations — the lcm
rule guarantees uniqueness, not coverage. Low value: the combinations that do
appear are enough to exercise the grain, and it only matters for a model that
expects a dense cube.

## 7. Scale is not reachable from the statement

`mock_environment(scale_factor=...)` exists for Python callers, but `mock
datasources a, b;` has no spelling for it, so `trilogy unit` is fixed at
`DEFAULT_SCALE_FACTOR`. Nothing cardinality-dependent in the planner can be
exercised from the CLI tier.

## 8. Latent: datetime keys round-trip through local time

`mock_datetimes(is_key=True)` builds values as
`datetime.fromtimestamp(base.timestamp() + i)`, which interprets a naive
datetime as local time and converts back. It produces identical values to
`base + timedelta(seconds=i)` on this machine and would only diverge across a
DST transition, so no failure has been observed — but the round-trip buys
nothing and makes the fixture depend on the host's timezone database.

---

## Closed

Each of these was on this list and is now covered by a test in
`tests/test_mocking.py` that fails if the behaviour is reverted.

- **Nothing is ever NULL.** `NULLABLE` columns now get `NULL_FRACTION` of their
  rows emptied, never on grain components and never at the cost of a distinct
  value. This is what lets a unit run tell a value NULL from an outer-join
  padding NULL.
- **A datasource's declared `where` was ignored.** Conjunctions of `=` and `in`
  against literals now restrict the mocked pools; anything else is logged.
- **The agent tier synthesized rollups.** `_materialize_mock_tables` runs
  through `mock_environment` now, so its pre-aggregates are computed from the
  facts they summarize.
- **Two datasources at one address dropped columns.** The table gets the union
  of every binding's columns.
- **Declared ranges never produced their endpoints.**
- **Uniform fan-out.** `MockManager.multiplied` deals the surplus rows out at
  random, so members carry 1–8 facts rather than exactly the mean.

## Not gaps

- **Row counts too small to be realistic.** Deliberate. The unit tier's job is
  to make the model's claims observable, not to measure performance.
