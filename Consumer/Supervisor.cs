using System.Net.Sockets;

namespace Consumer;

public sealed class Supervisor
{
    private readonly KafkaOptions _opts;
    private readonly StubClient _stubClient;
    private readonly CrashSwitch _crashSwitch;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<Supervisor> _logger;

    public Supervisor(
    KafkaOptions opts,
        StubClient ctcClient,
        CrashSwitch crashSwitch,
        ILoggerFactory loggerFactory)
    {
        _opts = opts;
        _stubClient = ctcClient;
        _crashSwitch = crashSwitch;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<Supervisor>();
    }

    /// <summary>Starts and independently supervises one runner Task per entry in the Runners map.</summary>
    public Task RunAsync(CancellationToken ct)
    {
        var runnerTasks = _opts.Runners
            .Select(kv => Task.Run(() => RunSupervisedRunnerAsync(kv.Key, kv.Value, ct), ct))
            .ToList();

        return Task.WhenAll(runnerTasks);
    }

    private async Task RunSupervisedRunnerAsync(string groupInstanceId, int[] ownedPartitions, CancellationToken ct)
    {
        var partitionList = string.Join(",", ownedPartitions.Select(p => $"p{p}"));

        var stateStore = new ProcessedStateStore(_opts.StateDir, _opts.GroupId, groupInstanceId);

        const int baseRestartDelayMs = 500;
        const int maxRestartDelayMs = 5000;
        var restartDelayMs = baseRestartDelayMs;
        var consecutiveRestarts = 0;

        while (!ct.IsCancellationRequested)
        {
            _logger.LogInformation(
                "[SUPERVISOR {RunnerId}] starting ConsumerRunner — group.id={GroupId} owned_partitions=[{Partitions}]",
                groupInstanceId, _opts.GroupId, partitionList);

            var runner = new ConsumerRunner(groupInstanceId, ownedPartitions, _opts, _stubClient, stateStore, _crashSwitch, _loggerFactory);
            var startedAt = DateTime.UtcNow;

            try
            {
                await runner.RunAsync(ct);
                break; // graceful shutdown  
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[CRASH {RunnerId}] ConsumerRunner faulted: {Msg}", groupInstanceId, ex.Message);

                if (DateTime.UtcNow - startedAt >= TimeSpan.FromSeconds(30))
                {
                    restartDelayMs = baseRestartDelayMs;
                    consecutiveRestarts = 0;
                }
                consecutiveRestarts++;

                _logger.LogWarning(
                    "[SUPERVISOR {RunnerId}] restarting SAME group.instance.id, re-Assign SAME owned_partitions=[{Partitions}] " +
                    "after {DelayMs}ms backoff (consecutive_restarts={ConsecutiveRestarts}) " +
                    "— zero partitions migrate to a peer; every OTHER runner is undisturbed and keeps committing",
                    groupInstanceId, partitionList, restartDelayMs, consecutiveRestarts);

                try { await Task.Delay(restartDelayMs, ct); } catch (OperationCanceledException) { break; }

                restartDelayMs = Math.Min(restartDelayMs * 2, maxRestartDelayMs);
            }
        }
    }
}
