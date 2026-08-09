# Q35 handoff: projecting two customer roles from `all_sales` times out

## Status after the discovery-engine rebase

**Not reproduced on the current working tree.** The bounded reproduction reached
execution, generated only 723 characters of SQL, and returned 20 rows within the
20-second limit. Keep this handoff as a regression/performance record, but do not
file it as a confirmed current framework defect without another reproduction.

## Original fresh source

This comes from the enriched TPC-DS run
`evals/tpcds_agent/results/20260808-151955_enriched`, Trilogy 0.3.317. q35
ultimately passed, but one exploratory query consumed the full 600-second tool
timeout. This handoff does not rely on an older bug report.

## Problem query

```preql
import raw.all_sales as al;

select
    al.channel as c,
    al.billing_customer.sk as b,
    al.ship_customer.sk as s
limit 20;
```

The query merely projects channel and two customer-role keys with a limit. In
the recorded run, `trilogy run scratch1.preql` returned:

```text
trilogy error: subprocess timed out after 600s.
```

Equivalent queries against individual store, web, and catalog models completed
in approximately 1–4 seconds. The final q35 answer avoided `all_sales` and
passed.

## Bounded reproduction

Do not invoke the problem query directly without a timeout. Use:

```powershell
.\.venv\Scripts\python.exe \
  evals\tpcds_agent\repro_q35_all_sales_customer_roles_timeout.py \
  --timeout 20
```

The reproduction runs generation and execution in a spawned, killable process.
It reports the last reached stage and treats exceeding the bound as successful
reproduction. Override `--workspace` to use another enriched TPC-DS workspace.
On the post-rebase tree it currently reports `NOT REPRODUCED`.

## Thesis

The original thesis was that resolving both `billing_customer.sk` and
`ship_customer.sk` through the
multi-channel `all_sales` union causes a planner explosion or generates an
execution plan whose work is not bounded by the final `LIMIT 20`. A simple
projection should not scan or combine the customer dimensions in a way that
takes minutes at scale factor 1.

The current result weakens that thesis substantially. Plausible alternatives are
that the rebase fixed the problematic plan, or that the original exact 600-second
event was environmental/transient.

The key split for investigation is whether the child reaches `executing`:

- timeout during `generating`: discovery/planner graph explosion;
- timeout during `executing`: generated SQL shape or missing limit pushdown.

## Investigation checklist

1. Capture the generated SQL when generation completes and inspect each union
   arm's joins and whether the limit exists only at the outermost query.
2. Test `channel` alone, then add one customer role, then both roles.
3. Test each channel under a channel filter to identify the problematic arm.
4. Compare nullable role padding against role dimensions joined into every arm.
5. Add a regression with a small synthetic three-arm union and two optional
   role keys; assert bounded generation and execution.

## Falsification

Reject the framework-performance thesis if the bounded reproduction completes
quickly on current code and the original timeout can be attributed to an
external database lock or sleeping machine. The original trace shows neither:
the tool itself reached its exact 600-second timeout, while neighboring queries
against the same worker database completed normally.
