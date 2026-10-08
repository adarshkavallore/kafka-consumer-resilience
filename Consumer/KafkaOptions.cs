using System.Linq;

namespace Consumer;

public class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "events";
    public int PartitionCount { get; set; } = 5;

    public string GroupId { get; set; } = "ctc-ingest-group";

    public string ConsumerId { get; set; } = "contrast-1";

    public Dictionary<string, int[]> Runners { get; set; } = new();

    public string Mode { get; set; } = "assign";

    public int MaxPollIntervalMs { get; set; } = 10000;
    public int SessionTimeoutMs { get; set; } = 6000;
    public int HeartbeatIntervalMs { get; set; } = 2000;
    public int RetryBackoffMs { get; set; } = 2000;

    public int CommitTickMs { get; set; } = 750;

    public double ProcessingDelayMinFactor { get; set; } = 0.5;
    public double ProcessingDelayMaxFactor { get; set; } = 2.5;

    public string StateDir { get; set; } = "./.state";

    public string CtcUrl { get; set; } = "http://localhost:5080";

    public int CtcLatencyMs { get; set; } = 1000;

    public string? CrashAfterOffset { get; set; }

    public string AutoOffsetReset { get; set; } = "earliest";

    public void ValidateRunners()
    {
        if (Runners == null || Runners.Count == 0)
            throw new InvalidOperationException(
                "[CONFIG] Kafka:Runners map is empty — at least one runner (group.instance.id -> partitions) is required  .");

        var owner = new Dictionary<int, string>();
        var overlaps = new List<string>();

        foreach (var (runnerId, partitions) in Runners)
        {
            if (partitions == null || partitions.Length == 0)
                throw new InvalidOperationException(
                    $"[CONFIG] Kafka:Runners['{runnerId}'] owns no partitions — every runner must own at least one partition  .");

            foreach (var p in partitions)
            {
                if (p < 0 || p >= PartitionCount)
                    throw new InvalidOperationException(
                        $"[CONFIG] Kafka:Runners['{runnerId}'] owns partition {p}, outside the valid range 0..{PartitionCount - 1}  .");

                if (owner.TryGetValue(p, out var existingOwner))
                    overlaps.Add($"partition {p} claimed by both '{existingOwner}' and '{runnerId}'");
                else
                    owner[p] = runnerId;
            }
        }

        if (overlaps.Count > 0)
            throw new InvalidOperationException(
                "[CONFIG] Kafka:Runners map is NOT disjoint   — an overlap lets two runners commit the " +
                $"same partition: {string.Join("; ", overlaps)}.");

        var unowned = Enumerable.Range(0, PartitionCount).Where(p => !owner.ContainsKey(p)).ToList();
        if (unowned.Count > 0)
            throw new InvalidOperationException(
                "[CONFIG] Kafka:Runners map is NOT total   — a gap leaves partition(s) unconsumed: " +
                $"{string.Join(",", unowned)} (topic has {PartitionCount} partitions, 0..{PartitionCount - 1}).");
    }
}
