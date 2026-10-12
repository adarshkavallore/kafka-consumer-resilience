# run-contrast.ps1 — CONTRAST consumer (mode=subscribe): native group membership, short session.timeout,
# eviction + peer rebalance on a long inline (no-pause) draw. Demonstrates concerns #4, #6, eviction-half of #3.
param(
    [string]$GroupId = "",          # blank => existing shared contrast group (resumes from committed)
    [switch]$FreshGroup,            # force a brand-new group.id => earliest reset (US6 contrast)
    [string]$ConsumerId = "contrast-1"
)
Set-Location $PSScriptRoot\..

$env:Mode = "subscribe"
if ($FreshGroup -or [string]::IsNullOrWhiteSpace($GroupId)) {
    if ($FreshGroup) {
        $env:GroupId = "ctc-contrast-group-$(Get-Date -Format 'yyyyMMddHHmmss')"
    } else {
        $env:GroupId = "ctc-contrast-group"
    }
} else {
    $env:GroupId = $GroupId
}
$env:ConsumerId = $ConsumerId

Write-Host "Starting CONTRAST consumer (mode=subscribe) group.id=$($env:GroupId) consumerId=$ConsumerId"
Write-Host "Run a 2nd instance concurrently (different -ConsumerId, same -GroupId) to see the peer rebalance on eviction."
dotnet run --project src/Consumer
