import json, statistics, sys, time
from pathlib import Path
import haybarn

worker = Path(sys.argv[1]).as_posix()
reps = int(sys.argv[2])
sizes = [int(x) for x in sys.argv[3:]]
con = haybarn.connect()
t = time.perf_counter()
con.execute("LOAD vgi")
con.execute(f"ATTACH 'resident' (TYPE vgi, LOCATION 'uv run --no-project --quiet {worker}')")
setup = time.perf_counter() - t
out = []
for rows in sizes:
    case = {"rows": rows, "transport": "resident", "setup_s": setup}
    for name, sql in {
        "full_scan": f"select count(id), sum(value), count(label) from resident.main.src({rows})",
        "one_column": f"select sum(value) from resident.main.src({rows})",
    }.items():
        timings = []
        for _ in range(reps + 1):
            t = time.perf_counter()
            con.execute(sql).fetchall()
            timings.append(time.perf_counter() - t)
        case[name] = {"first_s": timings[0], "warm_median_s": statistics.median(timings[1:]), "warm_min_s": min(timings[1:])}
    print(json.dumps(case), flush=True)
    out.append(case)
