# thelook partial-bridge agent eval

This deterministic DuckDB suite measures whether an agent recovers after the
customer-product partial-bridge error. Questions 1-5 are the trigger set;
questions 6-10 are safe controls, and 11-12 keep the two domains separate.

The trigger prompts ask for existing semantic fields without output aliases.
That wording is load-bearing today: a direct-field alias is a derived concept,
and the current validator deliberately under-reports derived concept homes.
Without the wording, `state as state` can bypass the error and compile an
unconstrained cross product, turning this into a test of an unrelated alias
gap instead of the user-facing recovery message.

Run the enriched leg with:

```powershell
.\.venv\Scripts\python.exe evals/thelook_agent/run_eval.py --category enriched
```

`run_eval.py` adds `partial_bridge_recovery` to `report.json` and a matching
table to `report.md`. The headline ratio is correct answers after a transcript
encountered the error divided by all transcripts that encountered it.
