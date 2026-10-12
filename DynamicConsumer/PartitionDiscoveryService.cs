using Confluent.Kafka;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace DynamicConsumer
{
    public sealed class PartitionDiscoveryService
    {
        private sealed record OwnedPartition(PartitionConsumer Consumer, Task Task, CancellationTokenSource Cts);

        private readonly DynamicKafkaOptions _opts;
        private readonly StubClient _stubClient;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<PartitionDiscoveryService> _logger;
        private readonly IConnectionMultiplexer _redisMux;

        private readonly ConcurrentDictionary<int, OwnedPartition> _owned = new();
        private readonly HashSet<int> _removedByOperator = new();
        private readonly object _removedLock = new();
        private int _lastKnownCount;

        public PartitionDiscoveryService(DynamicKafkaOptions opts, StubClient stubClient, ILoggerFactory loggerFactory, IConnectionMultiplexer redisMux)
        {
            _opts = opts;
            _stubClient = stubClient;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<PartitionDiscoveryService>();
            _redisMux = redisMux;
        }

        public async Task RunAsync(CancellationToken appCt)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _opts.BootstrapServers }).Build();

            while (!appCt.IsCancellationRequested)
            {
                PollOnce(admin, appCt);

                try
                {
                    await Task.Delay(_opts.DiscoveryIntervalMs, appCt);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            // App shutdown: cancel every owned partition's thread and wait for a clean stop; each one
            // logs its own [REMOVE] final-committed-offset line (PartitionConsumer.RunAsync finally block).
            foreach (var owned in _owned.Values)
                owned.Cts.Cancel();
            await Task.WhenAll(_owned.Values.Select(o => SafeAwait(o.Task)));
        }

        private void PollOnce(IAdminClient admin, CancellationToken appCt)
        {
            int count;
            try
            {
                var metadata = admin.GetMetadata(_opts.Topic, TimeSpan.FromSeconds(10));
                var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == _opts.Topic);
                count = topicMeta?.Partitions.Count ?? 0;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[DISCOVERY] metadata poll failed: {Msg}", ex.Message);
                return;
            }

            if (count != _lastKnownCount)
            {
                _logger.LogInformation("[DISCOVERY] topic={Topic} partition count was={Old} now={New}",
                    _opts.Topic, _lastKnownCount, count);
                _lastKnownCount = count;
            }

            for (var p = 0; p < count; p++)
            {
                if (_owned.ContainsKey(p)) continue;
                lock (_removedLock)
                {
                    if (_removedByOperator.Contains(p)) continue;
                }

                SpawnPartition(p, appCt);
            }

            DetectFaultedPartitions();
        }

        private void SpawnPartition(int p, CancellationToken appCt)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(appCt);
            var stateStore = new RedisProcessedStateStore(_redisMux, _opts.GroupId, $"dyn-p{p}");
            var logger = _loggerFactory.CreateLogger<PartitionConsumer>();
            var consumer = new PartitionConsumer(p, _opts, _stubClient, stateStore, logger, cts.Token);

            _logger.LogInformation("[SPAWN] dyn-p{Partition} starting new dedicated thread for partition={Partition} (no app restart)", p, p);

            // LongRunning hints the scheduler to give this its own dedicated thread — each partition's
            // Kafka calls stay confined to that one thread, never sharing with another partition's loop.
            var task = Task.Factory.StartNew(
                () => consumer.RunAsync(),
                cts.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();

            _owned[p] = new OwnedPartition(consumer, task, cts);
        }

        /// <summary>Operator removal (keypress 'r{N}', US5/US9). STICKY: partition p is excluded from
        /// auto-respawn until <see cref="Reonboard"/> is called — the other partitions are untouched.</summary>
        public async Task RemoveAsync(int p)
        {
            if (!_owned.TryRemove(p, out var owned))
            {
                _logger.LogWarning("[REMOVE] dyn-p{Partition} requested but partition {Partition} is not currently owned", p, p);
                return;
            }

            lock (_removedLock)
            {
                _removedByOperator.Add(p);
            }

            _logger.LogWarning("[REMOVE] dyn-p{Partition} operator-requested removal — cancelling its thread only", p);
            owned.Cts.Cancel();
            await SafeAwait(owned.Task);
            owned.Cts.Dispose();
        }

        /// <summary>Operator re-onboard (keypress 'a{N}', ties US5/US9 back to US7): clears the sticky
        /// exclusion so the next discovery tick treats partition p as new-and-unowned again, spawning a
        /// fresh dyn-p{N} that resumes at its last committed offset.</summary>
        public void Reonboard(int p)
        {
            lock (_removedLock)
            {
                _removedByOperator.Remove(p);
            }
            _logger.LogInformation("[DISCOVERY] dyn-p{Partition} re-onboard requested for partition={Partition} — will respawn on next discovery tick", p, p);
        }

        /// <summary>Operator ad hoc crash (keypress 'c{N}') — routed to partition p's own thread only.</summary>
        public void RequestCrash(int p)
        {
            if (_owned.TryGetValue(p, out var owned))
            {
                owned.Consumer.RequestCrash();
            }
            else
            {
                _logger.LogWarning("[CRASH] dyn-p{Partition} requested but partition {Partition} is not currently owned", p, p);
            }
        }

        /// <summary>
        /// A crashed (faulted Task) partition is dropped from the owned map — WITHOUT the sticky
        /// exclusion removal uses — so the very next discovery tick treats it as unowned and
        /// automatically respawns it (US5 "recovery after removal", here triggered by a crash rather
        /// than an operator keypress). Never affects any other partition's Task.
        /// </summary>
        private void DetectFaultedPartitions()
        {
            foreach (var kv in _owned.ToArray())
            {
                if (!kv.Value.Task.IsFaulted) continue;

                var reason = kv.Value.Task.Exception?.GetBaseException().Message ?? "unknown";
                _logger.LogError(
                    "[CRASH] dyn-p{Partition} thread faulted: {Reason} — dropping ownership (will auto-respawn on next discovery tick)",
                    kv.Key, reason);

                _owned.TryRemove(kv.Key, out _);
                kv.Value.Cts.Dispose();
            }
        }

        private static async Task SafeAwait(Task t)
        {
            try
            {
                await t;
            }
            catch
            {
                // Already logged by the partition itself ([CRASH]/[REMOVE]); nothing more to surface here.
            }
        }
    }
}
