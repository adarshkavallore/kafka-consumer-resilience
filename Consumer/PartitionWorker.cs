using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;

namespace Consumer;

/// <summary>
/// Per-partition worker (child Task). Reads its input channel, applies the randomized
/// 0.5x-2.5x MaxPollInterval processing delay , calls STUB (retrying on failure), and
/// reports acks on the shared ack channel. NO Kafka client calls anywhere in this file  
/// only STUB HTTP, durable-state reads, and channel I/O.
/// </summary>
public sealed class PartitionWorker
{
    private readonly int _partition;
    private readonly KafkaOptions _opts;
    private readonly StubClient _stubClient;
    private readonly ProcessedStateStore _stateStore;
    private readonly ChannelWriter<Ack> _ackWriter;
    private readonly CrashSwitch _crashSwitch;
    private readonly ILogger<PartitionWorker> _logger;
    private readonly (int Partition, long Offset)? _deterministicCrash;

    public PartitionWorker(
        int partition,
    KafkaOptions opts,
        StubClient stubClient,
        ProcessedStateStore stateStore,
        ChannelWriter<Ack> ackWriter,
        CrashSwitch crashSwitch,
        ILogger<PartitionWorker> logger)
    {
        _partition = partition;
        _opts = opts;
        _stubClient = stubClient;
        _stateStore = stateStore;
        _ackWriter = ackWriter;
        _crashSwitch = crashSwitch;
        _logger = logger;
        _deterministicCrash = ParseCrashAfterOffset(opts.CrashAfterOffset);
    }

    private static (int Partition, long Offset)? ParseCrashAfterOffset(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var parts = raw.Split(':', 2);
        if (parts.Length == 2 && int.TryParse(parts[0], out var p) && long.TryParse(parts[1], out var o))
            return (p, o);
        return null;
    }

    public async Task RunAsync(ChannelReader<RecordBatch> input, CancellationToken ct)
    {
        // read durable state at (re)start to establish the skip baseline for redelivery.
        var lastProcessed = _stateStore.Read(_partition) ?? -1;
        _logger.LogInformation("[STATE] partition={Partition} worker starting, lastProcessedOffset={Offset}",
            _partition, lastProcessed);

        var random = new Random(_partition.GetHashCode() ^ Environment.TickCount);

        await foreach (var rec in input.ReadAllAsync(ct))
        {
            if (_crashSwitch.ConsumePartitionCrash(_partition))
            {
                _logger.LogError("[CRASH] partition={Partition} worker crashing on demand (keypress 'p')", _partition);
                throw new InvalidOperationException($"Injected crash on partition {_partition} (keypress)");
            }

            // skip already-processed offsets on redelivery — no STUB call.
            if (rec.Offset <= lastProcessed)
            {
                _logger.LogInformation("[SKIP] partition={Partition} offset={Offset} (already processed, no STUB call)",
                    _partition, rec.Offset);
                await _ackWriter.WriteAsync(new Ack(_partition, rec.Offset, AckOutcome.SkippedDuplicate), ct);
                continue;
            }

            //  randomized 0.5x-2.5x MaxPollInterval processing delay.
            var factor = _opts.ProcessingDelayMinFactor +
                         random.NextDouble() * (_opts.ProcessingDelayMaxFactor - _opts.ProcessingDelayMinFactor);
            var delayMs = (int)(_opts.MaxPollIntervalMs * factor);
            _logger.LogInformation(
                "[EVENT] partition={Partition} offset={Offset} site={SiteId} processing delay={DelayMs}ms",
                _partition, rec.Offset, rec.SiteId, delayMs);
            await Task.Delay(delayMs, ct);

            // One-shot: a worker restarted after this fires seeks back to the same offset — the
            // gate makes sure it processes normally instead of re-crashing on every restart .
            if (_deterministicCrash is { } dc && dc.Partition == _partition && dc.Offset == rec.Offset
                && _crashSwitch.ShouldInjectDeterministicCrash(dc.Partition, dc.Offset))
            {
                _logger.LogError(
                    "[CRASH] partition={Partition} offset={Offset} deterministic crash (CrashAfterOffset)",
                    _partition, rec.Offset);
                throw new InvalidOperationException($"Injected deterministic crash at partition {_partition} offset {rec.Offset}");
            }

            string? siteId = rec.SiteId;
            object? events = null;
            try
            {
                using var doc = JsonDocument.Parse(rec.Value);
                if (doc.RootElement.TryGetProperty("events", out var evEl))
                    events = evEl.Clone();
            }
            catch (JsonException)
            {
                // Non-JSON payload — forward null events, STUB still records the offset.
            }

            var ingested = false;
            var attempt = 0;
            while (!ingested)
            {
                if (_crashSwitch.ConsumePartitionCrash(_partition))
                {
                    _logger.LogError("[CRASH] partition={Partition} worker crashing on demand (keypress 'p')", _partition);
                    throw new InvalidOperationException($"Injected crash on partition {_partition} (keypress)");
                }

                attempt++;
                var (success, duplicate) = await _stubClient.IngestAsync(
                    _opts.Topic, _partition, rec.Offset, siteId, events, ct);

                if (success)
                {
                    ingested = true;
                    var outcome = duplicate ? AckOutcome.SkippedDuplicate : AckOutcome.Ingested;
                    await _ackWriter.WriteAsync(new Ack(_partition, rec.Offset, outcome), ct);
                }
                else
                {
                    _logger.LogWarning(
                        "[RETRY] partition={Partition} offset={Offset} attempt={Attempt} backoff={BackoffMs}ms",
                        _partition, rec.Offset, attempt, _opts.RetryBackoffMs);
                    await Task.Delay(_opts.RetryBackoffMs, ct);
                }
            }

            lastProcessed = rec.Offset;
        }
    }
}
