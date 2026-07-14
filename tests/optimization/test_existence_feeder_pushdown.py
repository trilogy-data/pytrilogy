"""A multi-leaf HAVING over non-projected dims/measures builds one membership
semijoin (existence feeder CTE) per leaf. Predicate pushdown must not push
those membership atoms into each other's feeder CTEs: the final AND already
intersects them, so the push is semantically redundant, and each push chains
feeder k over feeders 1..k-1 — O(n^2) SQL and a combinatorial plan for the
executing engine (q11: 23KB SQL, 25GiB OOM)."""

import re

from trilogy import Dialects, Environment

QUERY = """
key cust int;
key yr int;
property <cust, yr>.amt float?;
property cust.fname string?;
property cust.pref string?;

datasource sales (
    c: cust,
    y: yr,
    a: amt,
    f: fname,
    p: pref,
)
grain (cust, yr)
address sales;

with s01 as
where yr = 2001
select cust as cid, sum(amt) as s_rev;

with s02 as
where yr = 2002
select cust as cid, sum(amt) as s_rev, pref as spref;

with w01 as
where yr = 2003
select cust as cid, fname as wfname, sum(amt) as w_rev;

with w02 as
where yr = 2004
select cust as cid, sum(amt) as w_rev;

select
    s01.cid as code,
    w01.wfname as first_name,
    s02.spref as pref_out,
union join s01.cid = s02.cid = w01.cid = w02.cid
having
    s01.cid is not null
    and s02.cid is not null
    and w01.cid is not null
    and w02.cid is not null
    and s01.s_rev > 0
    and w01.w_rev > 0
    and (w02.w_rev - w01.w_rev) / w01.w_rev > (s02.s_rev - s01.s_rev) / s01.s_rev
order by code asc nulls first
limit 100;
"""


def _cte_bodies(sql: str) -> dict[str, str]:
    """CTE name -> body text, excluding the final top-level SELECT."""
    m = re.match(r"\s*WITH\s+(\w+) as \(", sql)
    if not m:
        return {}
    rest = sql[m.end() :]
    names = [m.group(1)]
    bodies = []
    while True:
        nxt = re.search(r"\),\s*\n(\w+) as \(", rest)
        if not nxt:
            break
        bodies.append(rest[: nxt.start()])
        names.append(nxt.group(1))
        rest = rest[nxt.end() :]
    tail = re.search(r"\n\)\s*\nSELECT", rest)
    bodies.append(rest[: tail.start()] if tail else rest)
    return dict(zip(names, bodies))


def test_membership_feeders_do_not_chain():
    env = Environment()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(QUERY)[-1]
    bodies = _cte_bodies(sql)
    # feeders DEFINE membership filter columns (`as "_virt_filter..."`)
    feeders = [n for n, b in bodies.items() if re.search(r'as "_virt_filter', b)]
    assert len(feeders) >= 2, f"expected multiple membership feeders, got {feeders}"
    cross_refs = [
        (name, other)
        for name in feeders
        for other in feeders
        if other != name and re.search(rf"\b{other}\b", bodies[name])
    ]
    assert not cross_refs, (
        f"membership feeder CTEs reference sibling feeders (O(n^2) semijoin "
        f"chaining): {cross_refs}"
    )
