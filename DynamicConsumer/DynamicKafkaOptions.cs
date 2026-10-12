using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DynamicConsumer
{
    public class DynamicKafkaOptions
    {
        public string BootstrapServers { get; set; } = "localhost:9092";
        public string Topic { get; set; } = "events";

        /// <summary>Kafka-side group id — scopes committed offsets. Shared by ALL partition threads.</summary>
        public string GroupId { get; set; } = "dyn-ctc-ingest-group";

        /// <summary>How often (ms) <see cref="PartitionDiscoveryService"/> polls topic metadata for new partitions (D2).</summary>
        public int DiscoveryIntervalMs { get; set; } = 3000;

        public int MaxPollIntervalMs { get; set; } = 10000;
        public int SessionTimeoutMs { get; set; } = 6000;
        public int HeartbeatIntervalMs { get; set; } = 2000;
        public int RetryBackoffMs { get; set; } = 2000;

        /// <summary>Explicit periodic Commit() cadence on each partition's own thread.</summary>
        public int CommitTickMs { get; set; } = 750;

        /// <summary>Randomized per-record processing delay = MaxPollIntervalMs * [Min..Max) factor (contrast/liveness demos).</summary>
        public double ProcessingDelayMinFactor { get; set; } = 0.5;
        public double ProcessingDelayMaxFactor { get; set; } = 2.5;

        /// <summary>
        /// DEPRECATED/unused as of the Phase 9 Redis swap (2026-09-10 change request) — durable
        /// processed-state now lives in Redis (see <see cref="RedisConnectionString"/>). Left in place
        /// for config back-compat; no code reads this anymore.
        /// </summary>
        public string StateDir { get; set; } = "./.state";

        /// <summary>Redis connection string for the durable processed-state store (D8). One shared,
        /// thread-safe <c>ConnectionMultiplexer</c> is created from this at startup and reused by every
        /// partition's <see cref="RedisProcessedStateStore"/>.</summary>
        public string RedisConnectionString { get; set; } = "localhost:6379";

        public string StubUrl { get; set; } = "http://localhost:5080";

        /// <summary>Mirrors the StubService's simulated ingest latency; informational on the consumer side.</summary>
        public int StubLatencyMs { get; set; } = 1000;

        /// <summary>"assign" (PRIMARY, per-partition discovery) or "subscribe" (CONTRAST).</summary>
        public string Mode { get; set; } = "assign";

        public string AutoOffsetReset { get; set; } = "earliest";

        /// <summary>
        /// Optional deterministic crash trigger, format "partition:offset" (e.g. "2:57") — the named
        /// partition's PartitionConsumer throws right after processing that offset (US5/US7 demo).
        /// </summary>
        public string? CrashAfterOffset { get; set; }

        /// <summary>Poll timeout used by the main (not-paused) Consume() loop.</summary>
        public TimeSpan PollTimeout => TimeSpan.FromMilliseconds(300);

        /// <summary>Short poll timeout used while paused/in-flight, just to keep the thread live ([LIVENESS]).</summary>
        public TimeSpan LivenessPollTimeout => TimeSpan.FromMilliseconds(200);

        /// <summary>Parses <see cref="CrashAfterOffset"/> ("partition:offset") into a tuple, or null if unset/invalid.</summary>
        public (int Partition, long Offset)? ParseCrashAfterOffset()
        {
            var raw = CrashAfterOffset;
            if (string.IsNullOrWhiteSpace(raw)) return null;
            var parts = raw.Split(':', 2);
            if (parts.Length == 2 && int.TryParse(parts[0], out var p) && long.TryParse(parts[1], out var o))
                return (p, o);
            return null;
        }
    }

}
