
# Kafka Consumer Resilience POC

A .NET 8 proof of concept for exploring resilient Kafka event consumption. It demonstrates supervised, partition-aware processing and recovery from downstream outages, slow processing, crashes, and Kafka redelivery.

The primary consumer uses manual partition assignment, a dedicated worker per partition, and independent offset commits. A second consumer demonstrates the contrasting `Subscribe`-based consumer-group behavior. The solution also includes an event producer and a local HTTP stub for the downstream External API service.

> This is a demonstration project, not a production-ready Kafka platform. Its local Kafka broker and External API service are configured for development and smoke testing.

## What it demonstrates

- Manual assignment and independent processing/committing per partition.
- Pause and resume behavior while a partition is being processed.
- Recovery after simulated External API outages and worker or consumer crashes.
- Resume from committed offsets and handling already-processed records after redelivery.
- A `Subscribe`-based contrast for consumer-group rebalancing and poll/session timeouts.
- Configurable producer batches routed with a stable site-to-partition mapping.

## Solution projects

| Project | Purpose |
| --- | --- |
| `Consumer` | Primary supervised consumer and `Subscribe`-mode contrast. |
| `DynamicConsumer` | Separate consumer prototype with runtime partition discovery and Redis-backed processed state. |
| `Producer` | Publishes sample site events to Kafka. |
| `StubService` | Local HTTP stub that accepts events and can simulate downstream availability. |

## Requirements

- .NET 8 SDK
- Docker Desktop with Docker Compose
- PowerShell
- Redis on `localhost:6379` only if running `DynamicConsumer` (Redis is not included in the supplied Compose file)

## Build

Run these commands from the directory containing this README:

```powershell
dotnet build .\KafkaResilience.sln
```

## Run the primary demonstration

Open separate PowerShell terminals in this directory.

1. Start Kafka and create the five-partition `events` topic:

   ```powershell
   .\scripts\up.ps1
   ```

2. Start the External API stub:

   ```powershell
   dotnet run --project .\src\StubService
   ```

   The service listens at `http://localhost:5080`. Check its health endpoint at `http://localhost:5080/health`.

3. Start the supervised consumer:

   ```powershell
   .\scripts\run-consumer.ps1
   ```

   While it runs, press `p` to crash a partition worker, `c` to crash the consumer thread, or `x` to terminate the app. Restart the app after `x` to demonstrate recovery from persisted state.

4. Publish sample events from another terminal:

   ```powershell
   .\scripts\produce.ps1
   ```

   The script publishes three rounds by default. To set a different number:

   ```powershell
   .\scripts\produce.ps1 -Rounds 5
   ```

## Try the Subscribe contrast and outage scenario

Run the contrast consumer in another terminal:

```powershell
.\scripts\run-contrast.ps1
```

To see consumer-group behavior with a second instance, start another contrast consumer using the same group ID and a different consumer ID:

```powershell
.\scripts\run-contrast.ps1 -GroupId ctc-contrast-group -ConsumerId contrast-2
```

With the External API service running, simulate an outage and recovery:

```powershell
.\scripts\stub-down.ps1
.\scripts\stub-up.ps1
```

The External API stub also exposes `POST /control/down`, `POST /control/up`, and `GET /control/state`. Its availability and duplicate tracking are in memory and reset when the service restarts.

## Stop services and clean state

Stop the consumer and External API service with `Ctrl+C`, then stop Kafka:

```powershell
.\scripts\down.ps1
```

To also remove the primary consumer's local processed-state files:

```powershell
.\scripts\down.ps1 -ClearState
```

## Configuration and further reading

- Kafka, consumer, and External API settings are in each project's `appsettings.json`.
- [Quickstart and scenario runbook](./quickstart.md) describes the resilience scenarios and expected log signals.
- [Partition assignment flow](./consumer-assign-flow.html) shows the primary consumer's assignment flow.

The primary consumer stores processed state locally under `Consumer/.state`. The `DynamicConsumer` is a separate prototype and expects Redis at the connection string configured in `DynamicConsumer/appsettings.json`.
