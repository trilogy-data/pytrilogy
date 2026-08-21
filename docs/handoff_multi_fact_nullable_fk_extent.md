# Handoff: a nullable fact FK silently changes the dimension extent of a two-fact select

## OPEN 2026-08-21 — found by the fuzzer's `padding_provenance` family, NOT caused by it

Selecting one dimension property beside an aggregate from each of two facts
returns a different set of dimension members depending on whether one fact's
FK is declared nullable, and the result is asymmetric between the two facts.

Pinned by `tests/engine/test_multi_fact_nullable_fk_extent.py` (`xfail`,
strict). Delete the marker when this is fixed; the strict marker is what makes
a fix announce itself.

## Repro

Four groups. `visits` covers alpha/beta/gamma, `events` covers alpha/beta/delta,
so each fact has exactly one exclusive member.

```
key gid int;
property gid.gname string;
datasource groups (gid: gid, name: gname) grain (gid) query '''...''';

key vid int;
property vid.vamt int;
datasource visits (id: vid, gid: gid, amount: vamt) grain (vid) query '''...''';

key eid int;
property eid.eamt int;
datasource events (id: eid, gid: gid, amount: eamt) grain (eid) query '''...''';

select gname, sum(vamt) as v, sum(eamt) as e order by gname asc;
```

| `visits` FK | result |
| --- | --- |
| `gid` (required) | `alpha, beta` — both exclusive members dropped |
| `?gid` (nullable) | `alpha, beta, gamma` — the VISITS-exclusive member survives |

`delta` (the events-exclusive member) is dropped either way. Swapping the two
aggregates in the select list changes nothing, so this is not select order: it
is the nullable FK on one side.

Single-fact controls establish that neither fact alone reaches the full
dimension: `select gname, sum(vamt)` returns alpha/beta/gamma and
`select gname, sum(eamt)` returns alpha/beta/delta. Extent through a fact is
that fact's members, which is what makes the two-fact case a question about how
the two extents combine rather than about `groups` itself.

## Mechanism

The nullable FK flips the join between the two fact aggregates:

```
visits fk=gid    INNER JOIN "highfalutin"      INNER JOIN "quizzical"  INNER JOIN "wakeful"
visits fk=?gid   LEFT OUTER JOIN "highfalutin" INNER JOIN "quizzical"  LEFT OUTER JOIN "wakeful"
```

`?gid` makes the `visits`->`groups` join outer, which marks the group columns
padding-nullable on the visits side. That nullability reaches the merge between
the two aggregates and preserves the visits side, so visits-exclusive members
ride through while events-exclusive members do not.

## Why it is a defect either way

The required-FK case is a self-consistent INNER: both exclusive members drop.
The nullable-FK case preserves one side only. Whichever extent is intended,
the current pair is not it:

- if the contract is INNER, `?gid` wrongly resurrects `gamma`;
- if the contract is the union of both facts' members, both cases wrongly drop
  members, and `delta` is missing even from the nullable run.

The reachable harm is that a declaration which should be metadata about NULLs
in a column silently changes which rows a report returns.

## Scope

- Reproduces byte-identically on `origin/main` (3c0b43bd3), on this branch, and
  on the branch before it was rebased. It predates the padding-provenance work
  in `join_resolution.py` and is untouched by it.
- Found while building the `padding_provenance` fuzz family: the drafted case
  `padded_and_unpadded_sides` combined `sum(visit_amount)` with
  `sum(event_amount)` and its oracle assumed the union of members. That case was
  replaced with a single-fact one rather than encode an unverified expectation.
  Re-add it to `local_scripts/fuzzer/generate.py` once the contract is settled.

## Open question for the owner

What is the intended extent of a dimension property selected beside aggregates
from two facts? That decision is the fix; the asymmetry above is downstream of
not having one written down.
