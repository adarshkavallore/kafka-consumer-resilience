using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;
using Confluent.Kafka;

namespace Consumer;

public sealed class ConsumerRunner
{
    private readonly string _groupInstanceId;
    private readonly int[] _ownedPartitions;
    private readonly KafkaOptions _opts;
    private readonly StubClient _stubClient;
    private readonly ProcessedStateStore _stateStore;
    private readonly CrashSwitch _crashSwitch;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ConsumerRunner> _logger;

    private readonly Channel<Ack> _ackChannel = Channel.CreateUnbounded<Ack>(new UnboundedChannelOptions { SingleReader = true });
    private readonly Dictionary<int, Channel<RecordBatch>> _inputChannels = new();
    private readonly Dictionary<int, Task> _workerTasks = new();
    private readonly HashSet<int> _dirty = new();
    private readonly Dictionary<int, long> _frontier = new();
    private IConsumer<string, string>? _consumer;

    private readonly Dictionary<int, int> _workerRestartDelayMs = new();
    private readonly Dictionary<int, DateTime> _workerNextRestartAt = new();
    private readonly Dictionary<int, DateTime> _workerStartedAt = new();
    private const int BaseWorkerRestartDelayMs = 500;
    private const int MaxWorkerRestartDelayMs = 5000;

    public ConsumerRunner(
        string groupInstanceId,
        int[] ownedPartitions,
    KafkaOptions opts,
        StubClient stubClient,
        ProcessedStateStore stateStore,
        CrashSwitch crashSwitch,
        ILoggerFactory loggerFactory)
    {
        _groupInstanceId = groupInstanceId;
        _ownedPartitions = ownedPartitions;
        _opts = opts;
        _stubClient = stubClient;
        _stateStore = stateStore;
        _crashSwitch = crashSwitch;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<ConsumerRunner>();
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var autoOffsetReset = _opts.AutoOffsetReset.Equals("latest", StringComparison.OrdinalIgnoreCase)
            ? AutoOffsetReset.Latest
            : AutoOffsetReset.Earliest;

        var config = new ConsumerConfig
        {
            BootstrapServers = _opts.BootstrapServers,
            GroupId = _opts.GroupId,
            GroupInstanceId = _groupInstanceId, 
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = autoOffsetReset,
            SessionTimeoutMs = _opts.SessionTimeoutMs,
            HeartbeatIntervalMs = _opts.HeartbeatIntervalMs,
            MaxPollIntervalMs = _opts.MaxPollIntervalMs,
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();

        var topicPartitions = _ownedPartitions
            .Select(p => new TopicPartition(_opts.Topic, new Partition(p)))
            .ToList();

        var gracefulShutdown = false; //  set true only if the poll loop exits via cancellation, not via a throw

        try
        {
             
            _consumer.Assign(topicPartitions);
            _logger.LogInformation(
                "[ASSIGN {RunnerId}] group.id={GroupId} group.instance.id={RunnerId} owned_partitions=[{Partitions}] " +
                "(this runner's fixed subset only; resume rests on group.id+committed-offsets, group.instance.id " +
                "is the logical Supervisor/state key and stays inert to Kafka's JoinGroup under manual Assign)",
                _groupInstanceId, _opts.GroupId, _groupInstanceId,
                string.Join(",", _ownedPartitions.Select(p => $"p{p}")));

            InitializeFrontierFromCommitted(topicPartitions);

            foreach (var p in _ownedPartitions)
                StartWorker(p);

            var lastCommitAt = DateTime.UtcNow;
            var pollTimeout = TimeSpan.FromMilliseconds(300);

            while (!ct.IsCancellationRequested)
            {
                if (_crashSwitch.ConsumeRunnerCrash(_groupInstanceId))
                {
                    _logger.LogError("[CRASH {RunnerId}] consumer thread crashing on demand (keypress 'c')", _groupInstanceId);
                    throw new InvalidOperationException($"Injected ConsumerRunner crash (keypress) for runner={_groupInstanceId}");
                }

                RestartFaultedWorkers();

                ConsumeResult<string, string>? cr = null;
                try
                {
                    cr = _consumer.Consume(pollTimeout);
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                { 
                    _logger.LogWarning("[POLL {RunnerId}] consume error: {Reason}", _groupInstanceId, ex.Error.Reason);
                }
                catch (ConsumeException ex)
                { 
                    _logger.LogError(ex, "[POLL {RunnerId}] FATAL consume error, faulting runner: {Reason}", _groupInstanceId, ex.Error.Reason);
                    throw;
                }

                if (cr != null)
                    await Dispatch(cr, ct);

                DrainAcks();

                if (DateTime.UtcNow - lastCommitAt >= TimeSpan.FromMilliseconds(_opts.CommitTickMs))
                {
                    CommitIfDirty();
                    lastCommitAt = DateTime.UtcNow;
                }
            }

            gracefulShutdown = true; // reached only when the while-loop exited via cancellation, not via a throw
        }
        finally
        {
            foreach (var channel in _inputChannels.Values)
                channel.Writer.TryComplete();

            try
            {
                await Task.WhenAll(_workerTasks.Values).WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (Exception)
            {
                 
            }

             
            if (gracefulShutdown)
            {
                try
                {
                    CommitIfDirty();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("[COMMIT {RunnerId}] final commit on graceful shutdown failed: {Msg}", _groupInstanceId, ex.Message);
                }
            }

            try
            {
                _consumer.Close();
            }
            catch (Exception)
            {
                 
            }
            _consumer.Dispose();
        }
    }

    private void InitializeFrontierFromCommitted(List<TopicPartition> topicPartitions)
    {
        try
        {
            var committed = _consumer!.Committed(topicPartitions, TimeSpan.FromSeconds(5));
            foreach (var tpo in committed)
            {
                var p = tpo.Partition.Value;
                if (tpo.Offset != Offset.Unset)
                {
                    _frontier[p] = tpo.Offset.Value - 1;
                    _logger.LogInformation(
                        "[STATE {RunnerId}] partition={Partition} resuming from committed offset {Offset} (group={GroupId})",
                        _groupInstanceId, p, tpo.Offset.Value, _opts.GroupId);
                }
                else
                {
                    _frontier[p] = -1;
                    _logger.LogInformation(
                        "[STATE {RunnerId}] partition={Partition} no committed offset for group '{GroupId}' — applying reset policy ({Policy})",
                        _groupInstanceId, p, _opts.GroupId, _opts.AutoOffsetReset);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[STATE {RunnerId}] could not check committed offsets: {Msg}", _groupInstanceId, ex.Message);
            foreach (var p in _ownedPartitions) _frontier[p] = -1;
        }
    }

    private void StartWorker(int partition)
    {
        var channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _inputChannels[partition] = channel;

        var workerLogger = _loggerFactory.CreateLogger<PartitionWorker>();
        var worker = new PartitionWorker(partition, _opts, _stubClient, _stateStore, _ackChannel.Writer, _crashSwitch, workerLogger);
        _workerTasks[partition] = Task.Run(() => worker.RunAsync(channel.Reader, CancellationToken.None));
        _workerStartedAt[partition] = DateTime.UtcNow;
    }

    
    private void RestartFaultedWorkers()
    {
        foreach (var p in _workerTasks.Keys.ToList())
        {
            var task = _workerTasks[p];
            if (!task.IsFaulted) continue;

            var now = DateTime.UtcNow;

            if (!_workerNextRestartAt.TryGetValue(p, out var notBefore))
            {
                var reason = task.Exception?.GetBaseException().Message ?? "unknown";
                _logger.LogError("[CRASH {RunnerId}] partition={Partition} worker faulted: {Reason}", _groupInstanceId, p, reason);

                var uptime = _workerStartedAt.TryGetValue(p, out var startedAt) ? now - startedAt : TimeSpan.Zero;
                var delay = uptime >= TimeSpan.FromSeconds(30) || !_workerRestartDelayMs.TryGetValue(p, out var prevDelay)
                    ? BaseWorkerRestartDelayMs
                    : Math.Min(prevDelay * 2, MaxWorkerRestartDelayMs);

                _workerRestartDelayMs[p] = delay;
                _workerNextRestartAt[p] = now.AddMilliseconds(delay);
                _logger.LogWarning(
                    "[SUPERVISOR {RunnerId}] partition={Partition} worker restart backing off {DelayMs}ms",
                    _groupInstanceId, p, delay);
                continue;
            }

            if (now < notBefore) continue; // still backing off

            _workerNextRestartAt.Remove(p);

            var seekOffset = _frontier[p] + 1;
            var tp = new TopicPartition(_opts.Topic, new Partition(p));
            try
            {
                _consumer!.Seek(new TopicPartitionOffset(tp, seekOffset));
                _consumer.Resume(new[] { tp });
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[SUPERVISOR {RunnerId}] seek/resume after partition crash failed: {Msg}", _groupInstanceId, ex.Message);
            }

            _logger.LogWarning(
                "[SUPERVISOR {RunnerId}] restarting partition={Partition} worker (same runner) — seek to offset={Offset}",
                _groupInstanceId, p, seekOffset);

            StartWorker(p);
        }
    }

    private async Task Dispatch(ConsumeResult<string, string> cr, CancellationToken ct)
    {
        var p = cr.Partition.Value;
        string? siteId = null;
        try
        {
            using var doc = JsonDocument.Parse(cr.Message.Value);
            if (doc.RootElement.TryGetProperty("siteId", out var s)) siteId = s.GetString();
        }
        catch (JsonException)
        {
        }

        _logger.LogInformation("[POLL {RunnerId}] partition={Partition} offset={Offset}", _groupInstanceId, p, cr.Offset.Value);

        var record = new RecordBatch(p, cr.Offset.Value, siteId, cr.Message.Value);

        await _inputChannels[p].Writer.WriteAsync(record, ct);
        _logger.LogInformation("[DISPATCH {RunnerId}] partition={Partition} offset={Offset} site={SiteId} queued=true",
            _groupInstanceId, p, cr.Offset.Value, siteId);

        _consumer!.Pause(new[] { new TopicPartition(_opts.Topic, new Partition(p)) });
        _logger.LogInformation("[PAUSE {RunnerId}] partition={Partition} (worker busy)", _groupInstanceId, p);
    }

    private void DrainAcks()
    {
        while (_ackChannel.Reader.TryRead(out var ack))
        {
            var p = ack.Partition;
            _logger.LogInformation("[ACK {RunnerId}] partition={Partition} offset={Offset} outcome={Outcome}",
                _groupInstanceId, p, ack.Offset, ack.Outcome);

            
            if (ack.Outcome == AckOutcome.Ingested)
            {
                _stateStore.Write(p, ack.Offset);
                _logger.LogInformation("[STATE {RunnerId}] partition={Partition} lastProcessedOffset={Offset}", _groupInstanceId, p, ack.Offset);
            }

            if (ack.Offset == _frontier[p] + 1)
            {
                _frontier[p] = ack.Offset;
                _consumer!.StoreOffset(new TopicPartitionOffset(_opts.Topic, new Partition(p), ack.Offset + 1));
                _dirty.Add(p);
            }
            else if (ack.Offset > _frontier[p] + 1)
            {
                _logger.LogWarning(
                    "[STATE {RunnerId}] partition={Partition} gap detected: acked offset={Offset} frontier={Frontier} — NOT advancing past gap (FR-027)",
                    _groupInstanceId, p, ack.Offset, _frontier[p]);
            }

            _consumer!.Resume(new[] { new TopicPartition(_opts.Topic, new Partition(p)) });
            _logger.LogInformation("[RESUME {RunnerId}] partition={Partition}", _groupInstanceId, p);
        }
    }

    private void CommitIfDirty()
    {
        if (_dirty.Count == 0) return;

        try
        {
            _consumer!.Commit();
        }
        catch (KafkaException ex)
        {
            _logger.LogWarning("[COMMIT {RunnerId}] commit failed: {Msg}", _groupInstanceId, ex.Message);
            return;
        }

        foreach (var p in _dirty)
            _logger.LogInformation("[COMMIT {RunnerId}] partition={Partition} offset={Offset}", _groupInstanceId, p, _frontier[p] + 1);
        _dirty.Clear();
    }
}
