# stub-up.ps1 — Set STUB   to AVAILABLE (ends downstream outage)
Write-Host "Setting STUB  to AVAILABLE..."
$response = Invoke-RestMethod -Method Post -Uri "http://localhost:5080/control/up" -ContentType "application/json"
Write-Host "STUB state: available=$($response.available)"
