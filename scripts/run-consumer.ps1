# run-consumer.ps1 — PRIMARY supervised consumer (manual Assign mode, mode=assign, default)
# Identity = group.id + consumerId (group.instance.id is inert under manual Assign).
param(
    [string]$GroupId = "ctc-ingest-group",
    [string]$ConsumerId = "consumer-1",
    [string]$CrashAfterOffset = ""   # optional deterministic crash, format "partition:offset" e.g. "2:57"
)
Set-Location $PSScriptRoot\..

$env:Mode = "assign"
$env:GroupId = $GroupId
$env:ConsumerId = $ConsumerId

if ([string]::IsNullOrWhiteSpace($CrashAfterOffset)) {
    Remove-Item Env:\CrashAfterOffset -ErrorAction SilentlyContinue
} else {
    $env:CrashAfterOffset = $CrashAfterOffset
}

Write-Host "Starting PRIMARY consumer (mode=assign) group.id=$GroupId consumerId=$ConsumerId"
Write-Host "Keypresses while running: 'p' = crash a partition worker, 'c' = crash the consumer thread, 'x' = crash the whole app"
dotnet run --project src/Consumer
