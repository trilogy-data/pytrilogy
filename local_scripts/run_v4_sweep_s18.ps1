Set-Location C:\Users\ethan\coding_projects\pytrilogy
$env:TRILOGY_V4_DISCOVERY = "1"
& .venv\Scripts\python.exe -m pytest -m "not adventureworks_execution" -q -p no:randomly |
    Out-File -FilePath local_scripts\v4_sweep_0721_s18.log -Encoding utf8
