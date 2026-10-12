# produce.ps1 — Publish site event batches to the 'events' topic
# 10 sites -> deterministic site->partition map (p0..p4), 1-10 randomized events per batch (SC-004).
param(
    [int]$Rounds = 3
)
Set-Location $PSScriptRoot\..
dotnet run --project src/Producer -- --rounds $Rounds
