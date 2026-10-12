using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DynamicConsumer
{
    public sealed class PartitionConsumer
    {
        /// <summary>Process-lifetime one-shot gate for CrashAfterOffset — a respawned instance for the
        /// SAME partition (after the crash) must NOT re-crash on the same poison offset forever.</summary>
        private static readonly ConcurrentDictionary<(int Partition, long Offset), bool> DeterministicCrashFired = new();

        public int Partition { get; }

        private readonly DynamicKafkaOptions _opts;
        private readonly StubClient _stubClient;
        private readonly RedisProcessedStateStore _stateStore;
        private readonly ILogger _logger;
        private readonly CancellationToken _ct;
        private readonly (int Partition, long Offset)? _deterministicCrash;
        private volatile bool _crashRequested;

        public PartitionConsumer(
            int partition,
            DynamicKafkaOptions opts,
            StubClient stubClient,
            RedisProcessedStateStore stateStore,
            ILogger logger,
            CancellationToken ct)
        {
            Partition = partition;
            _opts = opts;
            _stubClient = stubClient;
            _stateStore = stateStore;
            _logger = logger;
            _ct = ct;
            _deterministicCrash = opts.ParseCrashAfterOffset();
        }

        /// <summary>Operator-triggered ad hoc crash (keypress 'c{N}') — thrown on the loop's next tick.</summary>
        public void RequestCrash() => _crashRequested = true;

        public async Task RunAsync()
        {
            var groupInstanceId = $"dyn-p{Partition}";
            var autoOffsetReset = _opts.AutoOffsetReset.Equals("latest", StringComparison.OrdinalIgnoreCase)
                ? AutoOffsetReset.Latest
                : AutoOffsetReset.Earliest;

            var config = new ConsumerConfig
            {
                BootstrapServers = _opts.BootstrapServers,
                GroupId = _opts.GroupId,
                GroupInstanceId = groupInstanceId, // stable logical identity; INERT under manual Assign (see [ASSIGN] log)
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = autoOffsetReset,
                SessionTimeoutMs = _opts.SessionTimeoutMs,
                HeartbeatIntervalMs = _opts.HeartbeatIntervalMs,
                MaxPollIntervalMs = _opts.MaxPollIntervalMs,
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            var tp = new TopicPartition(_opts.Topic, new Partition(Partition));

            var lastProcessed = -1L;   // durable skip-baseline (data-model.md); advances only on a fresh STUB ack
            var storedOffset = -1L;    // last value handed to StoreOffset — what [COMMIT] reports
            var dirty = false;
            var gracefulShutdown = false;

            try
            {
                // FR-005/006: Assign a SINGLE TopicPartition — Kafka resumes from the group's committed
                // offset for it automatically (or AutoOffsetReset if none), same mechanic as feature 002.
                consumer.Assign(tp);
                _logger.LogInformation(
                    "[ASSIGN] dyn-p{Partition} group.id={GroupId} group.instance.id={GroupInstanceId} partition={Partition} " +
                    "(group.instance.id is inert under manual Assign — resume rests on group.id + committed offset, not broker static membership)",
                    Partition, _opts.GroupId, groupInstanceId, Partition);

                lastProcessed = _stateStore.Read(Partition) ?? -1;
                storedOffset = lastProcessed;
                _logger.LogInformation("[STATE] dyn-p{Partition} partition={Partition} durable lastProcessedOffset={Offset}",
                    Partition, Partition, lastProcessed);
                LogResumeOrReset(consumer, tp);

                Task<(bool Success, bool Duplicate)>? inflightIngest = null;
                ConsumeResult<string, string>? inflightRecord = null;
                var lastCommitAt = DateTime.UtcNow;

                while (!_ct.IsCancellationRequested)
                {
                    if (_crashRequested)
                    {
                        _crashRequested = false;
                        _logger.LogError(
                            "[CRASH] dyn-p{Partition} crashing on demand (keypress) — final committed offset will be {Offset}",
                            Partition, storedOffset + 1);
                        throw new InvalidOperationException($"Injected crash on partition {Partition} (keypress 'c{Partition}')");
                    }

                    if (inflightRecord == null)
                    {
                        ConsumeResult<string, string>? cr = null;
                        try
                        {
                            cr = consumer.Consume(_opts.PollTimeout);
                        }
                        catch (ConsumeException ex) when (!ex.Error.IsFatal)
                        {
                            _logger.LogWarning("[POLL] dyn-p{Partition} consume error: {Reason}", Partition, ex.Error.Reason);
                        }

                        if (cr == null)
                        {
                            // Idle poll — not a hang; a brand-new partition with no events yet looks like this.
                            continue;
                        }

                        _logger.LogInformation("[POLL] dyn-p{Partition} partition={Partition} offset={Offset}",
                            Partition, Partition, cr.Offset.Value);

                        if (cr.Offset.Value <= lastProcessed)
                        {
                            // FR-010: redelivered, already-durable offset — skip, no STUB call, just catch Kafka up.
                            consumer.StoreOffset(new TopicPartitionOffset(tp, cr.Offset.Value + 1));
                            storedOffset = cr.Offset.Value;
                            dirty = true;
                            _logger.LogInformation("[SKIP] partition={Partition} offset={Offset} (already processed, no STUB call)",
                                Partition, cr.Offset.Value);
                            continue;
                        }

                        // One-shot deterministic crash (US5/US7 demo) — fires before the STUB call, mirrors 002.
                        if (_deterministicCrash is { } dc && dc.Partition == Partition && dc.Offset == cr.Offset.Value
                            && DeterministicCrashFired.TryAdd((dc.Partition, dc.Offset), true))
                        {
                            _logger.LogError(
                                "[CRASH] dyn-p{Partition} deterministic crash at partition={Partition} offset={Offset} (CrashAfterOffset)",
                                Partition, Partition, cr.Offset.Value);
                            throw new InvalidOperationException(
                                $"Injected deterministic crash at partition {Partition} offset {cr.Offset.Value}");
                        }

                        var siteId = RecordParsing.TryExtractSiteId(cr.Message.Value);
                        var events = RecordParsing.TryExtractEvents(cr.Message.Value);

                        _logger.LogInformation("[EVENT] partition={Partition} offset={Offset} site={SiteId}",
                            Partition, cr.Offset.Value, siteId);

                        consumer.Pause(new[] { tp });
                        _logger.LogInformation("[PAUSE] dyn-p{Partition} partition={Partition} (STUB ingest in flight)", Partition, Partition);

                        inflightRecord = cr;
                        inflightIngest = IngestWithRetryAsync(cr.Offset.Value, siteId, events);
                    }
                    else
                    {
                        // Paused/in-flight: keep calling Consume() with a short timeout so THIS thread stays
                        // a live member through an arbitrarily long STUB outage — never self-evicts (US3).
                        try
                        {
                            consumer.Consume(_opts.LivenessPollTimeout);
                        }
                        catch (ConsumeException ex) when (!ex.Error.IsFatal)
                        {
                            // swallow — this is the liveness heartbeat poll, not the primary fetch path
                        }
                        _logger.LogInformation("[LIVENESS] dyn-p{Partition} heartbeating (paused)", Partition);

                        if (inflightIngest!.IsCompleted)
                        {
                            var (success, _) = await inflightIngest;
                            if (success)
                            {
                                var offset = inflightRecord.Offset.Value;

                                // Strict write-order: STUB ack -> state.Write -> StoreOffset -> Commit (D5/FR-008).
                                _stateStore.Write(Partition, offset);
                                lastProcessed = offset;
                                _logger.LogInformation("[STATE] partition={Partition} lastProcessedOffset={Offset}", Partition, offset);

                                consumer.StoreOffset(new TopicPartitionOffset(tp, offset + 1));
                                storedOffset = offset;
                                dirty = true;

                                consumer.Resume(new[] { tp });
                                _logger.LogInformation("[RESUME] dyn-p{Partition} partition={Partition}", Partition, Partition);

                                inflightRecord = null;
                                inflightIngest = null;
                            }
                            // else: IngestWithRetryAsync never returns until success or cancellation — a
                            // (false) completion here only happens via cancellation, handled by the outer catch.
                        }
                    }

                    if (dirty && DateTime.UtcNow - lastCommitAt >= TimeSpan.FromMilliseconds(_opts.CommitTickMs))
                    {
                        if (CommitSafely(consumer, groupInstanceId, storedOffset))
                            dirty = false;
                        lastCommitAt = DateTime.UtcNow;
                    }
                }

                gracefulShutdown = true;
            }
            catch (OperationCanceledException) when (_ct.IsCancellationRequested)
            {
                gracefulShutdown = true;
            }
            finally
            {
                if (gracefulShutdown)
                {
                    if (dirty) CommitSafely(consumer, groupInstanceId, storedOffset);
                    _logger.LogInformation("[REMOVE] dyn-p{Partition} final committed offset={Offset}", Partition, storedOffset + 1);
                }

                try
                {
                    consumer.Close();
                }
                catch (Exception)
                {
                    // Already faulted/disconnected — nothing more to do.
                }
            }
        }

        /// <summary>US1/US3 retry loop: calls STUB with backoff until it acks. NEVER touches Kafka —
        /// runs concurrently with the main loop's liveness polling while the partition is paused.</summary>
        private async Task<(bool Success, bool Duplicate)> IngestWithRetryAsync(long offset, string? siteId, object? events)
        {
            var attempt = 0;
            while (true)
            {
                attempt++;
                var (success, duplicate) = await _stubClient.IngestAsync(_opts.Topic, Partition, offset, siteId, events, _ct);
                if (success) return (true, duplicate);

                _logger.LogWarning(
                    "[RETRY] partition={Partition} offset={Offset} attempt={Attempt} backoff={BackoffMs}ms (stub down)",
                    Partition, offset, attempt, _opts.RetryBackoffMs);
                await Task.Delay(_opts.RetryBackoffMs, _ct);
            }
        }

        /// <summary>US2/US6: informational resume decision, read once at startup.</summary>
        private void LogResumeOrReset(IConsumer<string, string> consumer, TopicPartition tp)
        {
            try
            {
                var committed = consumer.Committed(new[] { tp }, TimeSpan.FromSeconds(5));
                var tpo = committed.FirstOrDefault();
                if (tpo != null && tpo.Offset != Offset.Unset)
                {
                    _logger.LogInformation(
                        "[STATE] dyn-p{Partition} partition={Partition} resuming from committed offset {Offset} (group={GroupId})",
                        Partition, Partition, tpo.Offset.Value, _opts.GroupId);
                }
                else
                {
                    _logger.LogInformation(
                        "[STATE] dyn-p{Partition} partition={Partition} no committed offset for group '{GroupId}' — applying reset policy ({Policy})",
                        Partition, Partition, _opts.GroupId, _opts.AutoOffsetReset);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[STATE] dyn-p{Partition} could not check committed offsets: {Msg}", Partition, ex.Message);
            }
        }

        private bool CommitSafely(IConsumer<string, string> consumer, string groupInstanceId, long storedOffset)
        {
            try
            {
                consumer.Commit();
            }
            catch (KafkaException ex)
            {
                _logger.LogWarning("[COMMIT] dyn-p{Partition} commit failed: {Msg}", Partition, ex.Message);
                return false;
            }

            _logger.LogInformation(
                "[COMMIT] dyn-p{Partition} group.id={GroupId} group.instance.id={GroupInstanceId} partition={Partition} offset={Offset}",
                Partition, _opts.GroupId, groupInstanceId, Partition, storedOffset + 1);
            return true;
        }
    }
}
