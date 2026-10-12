# up.ps1 — Start Kafka and create the events topic
Set-Location $PSScriptRoot\..

Write-Host "Starting Kafka (KRaft mode)..."
docker compose up -d

Write-Host "Waiting for broker to be healthy..."
$retries = 30
for ($i = 0; $i -lt $retries; $i++) {
    $status = docker inspect --format='{{.State.Health.Status}}' kafka-broker 2>$null
    if ($status -eq "healthy") {
        Write-Host "Broker is healthy."
        break
    }
    Write-Host "  Waiting... ($($i+1)/$retries)"
    Start-Sleep -Seconds 3
}

Write-Host "Creating topic 'events' (5 partitions, replication-factor 1)..."
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic events `
    --partitions 5 `
    --replication-factor 1 `
    --if-not-exists

Write-Host "Done. Topic 'events' is ready (5 partitions)."
