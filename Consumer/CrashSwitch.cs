using System.Collections.Concurrent;

namespace Consumer;

public sealed class CrashSwitch
{
    private readonly ConcurrentDictionary<int, bool> _partitionCrash = new();
    private readonly ConcurrentDictionary<string, bool> _runnerCrash = new();
    private readonly ConcurrentDictionary<(int Partition, long Offset), bool> _deterministicCrashFired = new();

    public void RequestPartitionCrash(int partition) => _partitionCrash[partition] = true;

    public bool ConsumePartitionCrash(int partition) => _partitionCrash.TryRemove(partition, out _);

    public void RequestRunnerCrash(string groupInstanceId) => _runnerCrash[groupInstanceId] = true;

    public bool ConsumeRunnerCrash(string groupInstanceId) => _runnerCrash.TryRemove(groupInstanceId, out _);

    public bool ShouldInjectDeterministicCrash(int partition, long offset) =>
        _deterministicCrashFired.TryAdd((partition, offset), true);
}
