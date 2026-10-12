# down.ps1 — Tear down Kafka and remove volumes
param(
    [switch]$ClearState  # also delete durable per-partition processed-state (./.state)
)
Set-Location $PSScriptRoot\..

Write-Host "Tearing down Kafka..."
docker compose down -v

if ($ClearState) {
    Write-Host "Clearing durable processed-state (./.state)..."
    Remove-Item -Recurse -Force ./.state -ErrorAction SilentlyContinue
}

Write-Host "Done."
