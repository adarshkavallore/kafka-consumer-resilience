using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace DynamicConsumer
{
    public sealed class SubscribeContrastRunner
    {
        private readonly DynamicKafkaOptions _opts;
        private readonly StubClient _stubClient;
        private readonly ILogger<SubscribeContrastRunner> _logger;
        private readonly string _instanceLabel;
        public SubscribeContrastRunner(
            DynamicKafkaOptions opts, StubClient stubClient, ILogger<SubscribeContrastRunner> logger, string instanceLabel)
        {
            _opts = opts;
            _stubClient = stubClient;
            _logger = logger;
            _instanceLabel = instanceLabel;
        }

        public async Task RunAsync(CancellationToken ct)
        {
            _logger.LogInformation(
                "[IDENTITY] mode=subscribe group.id={GroupId} instance={Instance} " +
                "(CONTRAST — native group membership IS active here, unlike PRIMARY assign mode)",
                _opts.GroupId, _instanceLabel);
            _logger.LogInformation(
                "[CONFIG] session.timeout.ms={SessionTimeout} heartbeat.interval.ms={Heartbeat} max.poll.interval.ms={MaxPoll}",
                _opts.SessionTimeoutMs, _opts.HeartbeatIntervalMs, _opts.MaxPollIntervalMs);

            var autoOffsetReset = _opts.AutoOffsetReset.Equals("latest", StringComparison.OrdinalIgnoreCase)
                ? AutoOffsetReset.Latest
                : AutoOffsetReset.Earliest;

            var config = new ConsumerConfig
            {
                BootstrapServers = _opts.BootstrapServers,
                GroupId = _opts.GroupId,
                ClientId = $"dynamic-contrast-{_instanceLabel}",
                EnableAutoCommit = false,
                AutoOffsetReset = autoOffsetReset,
                SessionTimeoutMs = _opts.SessionTimeoutMs,
                HeartbeatIntervalMs = _opts.HeartbeatIntervalMs,
                MaxPollIntervalMs = _opts.MaxPollIntervalMs,
            };

            using var consumer = new ConsumerBuilder<string, string>(config)
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogInformation("[REBALANCE] assigned partitions=[{Partitions}]",
                        string.Join(", ", partitions.Select(p => $"{p.Topic}-{p.Partition.Value}")));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogInformation("[REBALANCE] revoked (evicted or leaving) partitions=[{Partitions}]",
                        string.Join(", ", partitions.Select(p => $"{p.Topic}-{p.Partition.Value}")));
                })
                .Build();

            consumer.Subscribe(_opts.Topic);

            var random = new Random();
            var lastLiveness = DateTime.MinValue;
            var offsetChecked = false;

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? cr = null;
                    try
                    {
                        cr = consumer.Consume(TimeSpan.FromMilliseconds(300));
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogWarning("[POLL] consume error: {Reason}", ex.Error.Reason);
                    }

                    if (!offsetChecked)
                    {
                        var assignment = consumer.Assignment;
                        if (assignment.Count > 0)
                        {
                            offsetChecked = true;
                            LogCommittedOrReset(consumer, assignment);
                        }
                    }

                    var now = DateTime.UtcNow;
                    if (now - lastLiveness >= TimeSpan.FromSeconds(2))
                    {
                        _logger.LogInformation(
                            "[LIVENESS] heartbeat.interval.ms={Heartbeat} session.timeout.ms={SessionTimeout} " +
                            "max.poll.interval.ms={MaxPoll} — still a group member (no Pause used here, unlike PRIMARY)",
                            _opts.HeartbeatIntervalMs, _opts.SessionTimeoutMs, _opts.MaxPollIntervalMs);
                        lastLiveness = now;
                    }

                    if (cr == null) continue;

                    var siteId = RecordParsing.TryExtractSiteId(cr.Message.Value);
                    var events = RecordParsing.TryExtractEvents(cr.Message.Value);

                    var factor = _opts.ProcessingDelayMinFactor +
                                 random.NextDouble() * (_opts.ProcessingDelayMaxFactor - _opts.ProcessingDelayMinFactor);
                    var delayMs = (int)(_opts.MaxPollIntervalMs * factor);
                    _logger.LogInformation(
                        "[EVENT] partition={Partition} offset={Offset} site={SiteId} processing INLINE (no pause) " +
                        "delay={DelayMs}ms — may exceed max.poll.interval.ms={MaxPoll} and get evicted",
                        cr.Partition.Value, cr.Offset.Value, siteId, delayMs, _opts.MaxPollIntervalMs);

                    // Deliberately inline, no Pause/Resume — this is what causes broker-side eviction.
                    await Task.Delay(delayMs, ct);

                    var ingested = false;
                    while (!ingested)
                    {
                        var (success, duplicate) = await _stubClient.IngestAsync(
                            _opts.Topic, cr.Partition.Value, cr.Offset.Value, siteId, events, ct);
                        if (success)
                        {
                            ingested = true;
                            _logger.LogInformation("[STUB] partition={Partition} offset={Offset} ingested duplicate={Duplicate}",
                                cr.Partition.Value, cr.Offset.Value, duplicate);
                        }
                        else
                        {
                            _logger.LogWarning("[RETRY] partition={Partition} offset={Offset} backoff={BackoffMs}ms",
                                cr.Partition.Value, cr.Offset.Value, _opts.RetryBackoffMs);
                            await Task.Delay(_opts.RetryBackoffMs, ct);
                        }
                    }

                    try
                    {
                        consumer.Commit(cr);
                        _logger.LogInformation("[COMMIT] partition={Partition} offset={Offset}", cr.Partition.Value, cr.Offset.Value);
                    }
                    catch (KafkaException ex)
                    {
                        // Likely evicted between processing and commit (illegal generation / unknown member) —
                        // exactly the CONTRAST payoff: the broker already reassigned this partition to a peer.
                        _logger.LogWarning("[REBALANCE] commit failed (likely evicted): {Msg}", ex.Message);
                    }
                }
            }
            finally
            {
                _logger.LogInformation("[REBALANCE] closing consumer (group={GroupId})", _opts.GroupId);
                consumer.Close();
            }
        }

        private void LogCommittedOrReset(IConsumer<string, string> consumer, List<TopicPartition> assignment)
        {
            try
            {
                var committed = consumer.Committed(assignment, TimeSpan.FromSeconds(5));
                var withOffset = committed.FirstOrDefault(tp => tp.Offset != Offset.Unset);
                if (withOffset != null)
                    _logger.LogInformation("[STATE] resuming from committed offset {Offset} (group={GroupId})",
                        withOffset.Offset.Value, _opts.GroupId);
                else
                    _logger.LogInformation("[STATE] no committed offset for group '{GroupId}' — applying reset policy ({Policy})",
                        _opts.GroupId, _opts.AutoOffsetReset);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[STATE] could not check committed offsets: {Msg}", ex.Message);
            }
        }
    }
}
