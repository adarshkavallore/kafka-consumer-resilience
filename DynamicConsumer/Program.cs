using DynamicConsumer;
using Microsoft.Extensions.Options;
using Serilog;
using StackExchange.Redis;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.File(Path.Combine("logs", "dynamic-consumer-.log"), rollingInterval: RollingInterval.Day)
    .CreateLogger();

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddSerilog(Log.Logger, dispose: true);

// Bind config — env vars override appsettings (required for the mode/contrast/crash scripts).
builder.Configuration.AddEnvironmentVariables();
builder.Services.Configure<DynamicKafkaOptions>(builder.Configuration.GetSection("Kafka"));

// Override individual keys from top-level env vars set by scripts/run-dynamic*.ps1.
builder.Services.PostConfigure<DynamicKafkaOptions>(opts =>
{
    var mode = builder.Configuration["Mode"];
    if (!string.IsNullOrWhiteSpace(mode)) opts.Mode = mode;

    var groupId = builder.Configuration["GroupId"];
    if (!string.IsNullOrWhiteSpace(groupId)) opts.GroupId = groupId;

    var aor = builder.Configuration["AutoOffsetReset"];
    if (!string.IsNullOrWhiteSpace(aor)) opts.AutoOffsetReset = aor;

    var crashAfterOffset = builder.Configuration["CrashAfterOffset"];
    if (!string.IsNullOrWhiteSpace(crashAfterOffset)) opts.CrashAfterOffset = crashAfterOffset;
});

builder.Services.AddHttpClient<StubClient>();

var host = builder.Build();

var opts = host.Services.GetRequiredService<IOptions<DynamicKafkaOptions>>().Value;
var loggerFactory = host.Services.GetRequiredService<ILoggerFactory>();
var logger = loggerFactory.CreateLogger("DynamicConsumer.Program");
var stubClient = host.Services.GetRequiredService<StubClient>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

Task appTask;
PartitionDiscoveryService? discovery = null;

if (opts.Mode.Equals("subscribe", StringComparison.OrdinalIgnoreCase))
{
    // CONTRAST-mode-only identity: distinguishes a 1st vs 2nd instance in the logs (run-dynamic-contrast.ps1 -Second).
    var instanceLabel = builder.Configuration["Instance"] ?? "primary";
    var contrastRunner = new SubscribeContrastRunner(opts, stubClient, loggerFactory.CreateLogger<SubscribeContrastRunner>(), instanceLabel);
    appTask = contrastRunner.RunAsync(cts.Token);
}
else
{
    // PRIMARY mode: one PartitionDiscoveryService, one dedicated PartitionConsumer thread per
    // partition. group.instance.id (dyn-p{N}) stays inert to Kafka's JoinGroup protocol under
    // manual Assign() — resume rests on group.id + committed offsets, not broker static membership.
    logger.LogInformation(
        "[IDENTITY] mode=assign group.id={GroupId} topic={Topic} discovery_interval_ms={DiscoveryIntervalMs} — " +
        "one dedicated thread per partition (dyn-p{{N}}), discovered at runtime, no app restart to onboard new ones",
        opts.GroupId, opts.Topic, opts.DiscoveryIntervalMs);

    // ONE shared, thread-safe multiplexer for durable processed-state (D8) — reused by every
    // partition's RedisProcessedStateStore; safe to share since StackExchange.Redis multiplexers
    // are designed for concurrent use across threads.
    logger.LogInformation("[IDENTITY] redis={RedisConnectionString} — shared connection for durable processed-state", opts.RedisConnectionString);
    var redisMux = ConnectionMultiplexer.Connect(opts.RedisConnectionString);

    discovery = new PartitionDiscoveryService(opts, stubClient, loggerFactory, redisMux);
    appTask = discovery.RunAsync(cts.Token);

    // Keypress loop (typed command + Enter): 'r{N}' removes partition N's thread (sticky — stays
    // removed until 'a{N}' re-onboards it); 'c{N}' crashes partition N's thread on demand (auto-
    // respawns via the next discovery tick, since a crashed partition drops ownership but is NOT
    // sticky-excluded); 'a{N}' re-onboards a previously-removed partition; 'q' quits the app.
    var localDiscovery = discovery;
    var localLogger = logger;
    _ = Task.Run(async () =>
    {
        while (!cts.IsCancellationRequested)
        {
            if (Console.IsInputRedirected)
            {
                await Task.Delay(1000, CancellationToken.None);
                continue;
            }

            string? line;
            try
            {
                line = await Task.Run(Console.ReadLine, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (line == null)
            {
                await Task.Delay(500, CancellationToken.None);
                continue;
            }

            line = line.Trim();
            if (line.Length == 0) continue;

            if (line.Equals("q", StringComparison.OrdinalIgnoreCase))
            {
                localLogger.LogInformation("[REMOVE] keypress 'q' — shutting down the whole app");
                cts.Cancel();
                break;
            }

            var action = char.ToLowerInvariant(line[0]);
            var numberPart = line[1..];
            if (!int.TryParse(numberPart, out var partition))
            {
                localLogger.LogWarning("[CRASH] unrecognized command '{Line}' — use r{{N}}, c{{N}}, a{{N}}, or q", line);
                continue;
            }

            switch (action)
            {
                case 'r':
                    localLogger.LogWarning("[REMOVE] keypress 'r{Partition}' — removing partition {Partition}'s thread", partition, partition);
                    await localDiscovery.RemoveAsync(partition);
                    break;
                case 'c':
                    localLogger.LogWarning("[CRASH] keypress 'c{Partition}' — requesting crash on partition {Partition}", partition, partition);
                    localDiscovery.RequestCrash(partition);
                    break;
                case 'a':
                    localDiscovery.Reonboard(partition);
                    break;
                default:
                    localLogger.LogWarning("[CRASH] unrecognized command '{Line}' — use r{{N}}, c{{N}}, a{{N}}, or q", line);
                    break;
            }
        }
    });
}

try
{
    await appTask;
}
finally
{
    Log.CloseAndFlush();
}
