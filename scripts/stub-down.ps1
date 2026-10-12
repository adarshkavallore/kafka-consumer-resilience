# stub-down.ps1 — Set STUB  to UNAVAILABLE (simulates downstream outage)
Write-Host "Setting STUB stub to UNAVAILABLE..."
$response = Invoke-RestMethod -Method Post -Uri "http://localhost:5080/control/down" -ContentType "application/json"
Write-Host "STUB state: available=$($response.available)"
