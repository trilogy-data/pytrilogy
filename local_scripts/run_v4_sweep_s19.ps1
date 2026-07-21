$env:TRILOGY_V4_DISCOVERY = "1"
Set-Location "C:\Users\ethan\coding_projects\pytrilogy"
& ".\.venv\Scripts\python.exe" -m pytest -m "not adventureworks_execution" -q 2>&1 |
    Out-File -FilePath "local_scripts\v4_sweep_0721_s19.log" -Encoding utf8
