$env:TRILOGY_V4_DISCOVERY = '1'
Set-Location C:\Users\ethan\coding_projects\pytrilogy
& .venv\Scripts\python.exe -m pytest -m "not adventureworks_execution" -q 2>&1 |
    Out-File -Encoding utf8 local_scripts\v4_sweep_0720_s15b.log
