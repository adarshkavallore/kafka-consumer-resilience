using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Serilog;

const string topicName = "events";
const int partitionCount = 5;
const int siteCount = 10;

var logDirectory = Path.Combine(GetProducerProjectDirectory(), "logs");

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File(Path.Combine(logDirectory, "producer-.log"), rollingInterval: RollingInterval.Day)
    .CreateLogger();

try
{
    // Parse args: --rounds N (default 3), --bootstrap-servers, --topic,
    // plus the additive --spread [--count N] mode 
    int rounds = 3;
    string bootstrapServers = "localhost:9092";
    string topic = topicName;
    bool spread = false;
    int spreadCount = 60;
    for (int i = 0; i < args.Length; i++)
    {
        if (args[i] == "--spread") spread = true;
        if (args[i] == "--rounds" && i + 1 < args.Length && int.TryParse(args[i + 1], out int r)) rounds = r;
        if (args[i] == "--bootstrap-servers" && i + 1 < args.Length) bootstrapServers = args[i + 1];
        if (args[i] == "--topic" && i + 1 < args.Length) topic = args[i + 1];
        if (args[i] == "--count" && i + 1 < args.Length && int.TryParse(args[i + 1], out int c)) spreadCount = c;
    }

    if (spread)
    {
        await RunSpreadModeAsync(bootstrapServers, topic, spreadCount);
    }
    else
    {
        // Explicit deterministic site→partition map (2 sites per partition across p0..p4) 
        var siteToPartition = new Dictionary<string, int>();
        for (int s = 0; s < siteCount; s++)
            siteToPartition[$"site{s}"] = s % partitionCount;

        Log.Information("[PRODUCER] site→partition map: {Map}",
            string.Join(", ", siteToPartition.Select(kv => $"{kv.Key}->p{kv.Value}")));
        Log.Information("[PRODUCER] Publishing {Rounds} round(s) across {SiteCount} sites to topic '{Topic}' on {Bootstrap}...",
            rounds, siteCount, topic, bootstrapServers);

        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        using var producer = new ProducerBuilder<string, string>(config).Build();

        var random = new Random();
        for (int round = 1; round <= rounds; round++)
        {
            Log.Information("[PRODUCER] round {Round}/{Total}", round, rounds);

            foreach (var (siteId, partition) in siteToPartition)
            {
                //  randomized 1–10 site events per message.
                int batchSize = random.Next(1, 11);
                var events = Enumerable.Range(0, batchSize)
                    .Select(i => new { id = Guid.NewGuid().ToString("N"), value = $"{siteId}-evt-{i}" })
                    .ToArray();
                var payload = JsonSerializer.Serialize(new { siteId, events });

                var message = new Message<string, string> { Key = siteId, Value = payload };
                var result = await producer.ProduceAsync(new TopicPartition(topic, new Partition(partition)), message);

                Log.Information("[PRODUCER] siteId={SiteId} partition={Partition} offset={Offset} batchSize={BatchSize}",
                    siteId, result.Partition.Value, result.Offset.Value, batchSize);
            }
        }

        producer.Flush(TimeSpan.FromSeconds(5));
        Log.Information("[PRODUCER] Done — {Rounds} round(s) across {SiteCount} sites produced.", rounds, siteCount);
    }
}
finally
{
    Log.CloseAndFlush();
}


static async Task RunSpreadModeAsync(string bootstrapServers, string topic, int count)
{
    using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
    var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(10));
    var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
    if (topicMetadata == null || topicMetadata.Partitions.Count == 0)
    {
        Log.Error("[PRODUCER-SPREAD] Could not read metadata for topic '{Topic}' — is the broker up?", topic);
        return;
    }

    int partitionCount = topicMetadata.Partitions.Count;
    Log.Information(
        "[PRODUCER-SPREAD] topic={Topic} currentPartitionCount={PartitionCount} — producing {Count} keyless event(s) round-robined across partitions 0..{MaxPartition}",
        topic, partitionCount, count, partitionCount - 1);

    var config = new ProducerConfig { BootstrapServers = bootstrapServers };
    using var producer = new ProducerBuilder<string, string>(config).Build();

    for (int i = 0; i < count; i++)
    {
        int partition = i % partitionCount;
        var evt = new { id = Guid.NewGuid().ToString("N"), value = $"spread-evt-{i}" };
        var payload = JsonSerializer.Serialize(new { siteId = "spread", events = new[] { evt } });
        var message = new Message<string, string> { Value = payload };
        var result = await producer.ProduceAsync(new TopicPartition(topic, new Partition(partition)), message);

        Log.Information("[PRODUCER-SPREAD] partition={Partition} offset={Offset} index={Index}",
            result.Partition.Value, result.Offset.Value, i);
    }

    producer.Flush(TimeSpan.FromSeconds(5));
    Log.Information("[PRODUCER-SPREAD] Done — {Count} spread record(s) produced across {PartitionCount} partition(s) (0..{MaxPartition}).",
        count, partitionCount, partitionCount - 1);
}

static string GetProducerProjectDirectory()
{
    var directory = new DirectoryInfo(AppContext.BaseDirectory);

    while (directory != null)
    {
        if (File.Exists(Path.Combine(directory.FullName, "Producer.csproj")))
            return directory.FullName;

        directory = directory.Parent;
    }

    return Directory.GetCurrentDirectory();
}

