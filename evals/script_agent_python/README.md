# Python script datasource agent benchmark

This suite gives a fresh DeepSeek agent one task at a time. The agent must write
a standalone Python program that emits an Arrow IPC stream, model it as a
Trilogy datasource, write a query against it, and run the query successfully.

The 25 cases cover local algorithms, parsing and transformation, public
datasets, and unauthenticated public APIs. Reference programs are executable
true-positive answers; their paired Trilogy files are used directly by the
shared result-set scorer.

Run one smoke case:

```powershell
.venv\Scripts\python.exe evals\script_agent_python\run_eval.py --query-ids 1 --splice-from none
```

Network cases are 19-25. They intentionally use endpoints that require no API
key, but still depend on external availability. Run local-only cases with
`--query-ids 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18`.

