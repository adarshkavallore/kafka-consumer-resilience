using Consumer;
using Microsoft.Extensions.Options;
using Serilog;
using System.Net.Sockets;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.File(Path.Combine("logs", "consumer-.log"), rollingInterval: RollingInterval.Day)
    .CreateLogger();

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddSerilog(Log.Logger, dispose: true);

builder.Configuration.AddEnvironmentVariables();
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));

builder.Services.PostConfigure<KafkaOptions>(opts =>
{
    var mode = builder.Configuration["Mode"];
    if (!string.IsNullOrWhiteSpace(mode)) opts.Mode = mode;

    var groupId = builder.Configuration["GroupId"];
    if (!string.IsNullOrWhiteSpace(groupId)) opts.GroupId = groupId;

    var consumerId = builder.Configuration["ConsumerId"];
    if (!string.IsNullOrWhiteSpace(consumerId)) opts.ConsumerId = consumerId;

    var aor = builder.Configuration["AutoOffsetReset"];
    if (!string.IsNullOrWhiteSpace(aor)) opts.AutoOffsetReset = aor;

    var crashAfterOffset = builder.Configuration["CrashAfterOffset"];
    if (!string.IsNullOrWhiteSpace(crashAfterOffset)) opts.CrashAfterOffset = crashAfterOffset;
});

builder.Services.AddHttpClient<StubClient>();

var host = builder.Build();

var opts = host.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;
var loggerFactory = host.Services.GetRequiredService<ILoggerFactory>();
var logger = loggerFactory.CreateLogger("Consumer.Program");
var ctcClient = host.Services.GetRequiredService<StubClient>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

Task appTask;
CrashSwitch? crashSwitch = null;

if (opts.Mode.Equals("subscribe", StringComparison.OrdinalIgnoreCase))
{
    logger.LogInformation(
        "[IDENTITY] mode=subscribe group.id={GroupId} consumerId={ConsumerId}",
        opts.GroupId, opts.ConsumerId);

    var contrastRunner = new SubscribeContrastRunner(opts, ctcClient, loggerFactory.CreateLogger<SubscribeContrastRunner>());
    appTask = contrastRunner.RunAsync(cts.Token);
}
else
{
    try
    {
        opts.ValidateRunners();
    }
    catch (InvalidOperationException ex)
    {
        logger.LogError("{Message}", ex.Message);
        Log.CloseAndFlush();
        Environment.Exit(1);
        return;
    }

    crashSwitch = new CrashSwitch();
    var runnerIds = opts.Runners.Keys.ToList();

    logger.LogInformation(
        "[IDENTITY] mode=assign group.id={GroupId} runner_count={RunnerCount} — M parallel ConsumerRunners, " +
        "each Assign()ing ONLY its own fixed subset (FR-033); group.instance.id is inert to Kafka under Assign",
        opts.GroupId, runnerIds.Count);

    foreach (var runnerId in runnerIds)
    {
        var owned = opts.Runners[runnerId];
        logger.LogInformation(
            "[ASSIGN {RunnerId}] group.instance.id={RunnerId} owned_partitions=[{Partitions}] state_namespace={StateDir}/{GroupId}/{RunnerId}",
            runnerId, runnerId, string.Join(",", owned.Select(p => $"p{p}")), opts.StateDir, opts.GroupId, runnerId);
    }

    var supervisor = new Supervisor(opts, ctcClient, crashSwitch, loggerFactory);
    appTask = supervisor.RunAsync(cts.Token);

    var localCrashSwitch = crashSwitch;
    var localOpts = opts;
    var localLogger = logger;
    var localRunnerIds = runnerIds;
    _ = Task.Run(async () =>
    {
        var random = new Random();
        while (!cts.IsCancellationRequested)
        {
            if (Console.IsInputRedirected)
            {
                await Task.Delay(1000, CancellationToken.None);
                continue;
            }

            if (Console.KeyAvailable)
            {
                var key = Console.ReadKey(intercept: true).KeyChar;
                switch (key)
                {
                    case 'p':
                        var partition = random.Next(localOpts.PartitionCount);
                        var owningRunner = localRunnerIds.First(id => localOpts.Runners[id].Contains(partition));
                        localLogger.LogWarning(
                            "[CRASH] keypress 'p' — requesting crash on partition={Partition} (routed to owning runner={RunnerId})",
                            partition, owningRunner);
                        localCrashSwitch.RequestPartitionCrash(partition);
                        break;
                    case 'c':
                        var randomRunner = localRunnerIds[random.Next(localRunnerIds.Count)];
                        localLogger.LogWarning(
                            "[CRASH] keypress 'c' — requesting crash on RANDOM runner={RunnerId} owned_partitions=[{Partitions}]",
                            randomRunner, string.Join(",", localOpts.Runners[randomRunner].Select(p => $"p{p}")));
                        localCrashSwitch.RequestRunnerCrash(randomRunner);
                        break;
                    case 'x':
                        localLogger.LogError("[CRASH] keypress 'x' — crashing whole app now (restart-detected recovery, ALL runners re-created)");
                        Environment.Exit(1);
                        break;
                    default:
                        if (char.IsDigit(key))
                        {
                            var idx = key - '0';
                            if (idx >= 1 && idx <= localRunnerIds.Count)
                            {
                                var chosen = localRunnerIds[idx - 1];
                                localLogger.LogWarning(
                                    "[CRASH] keypress '{Key}' — requesting crash on runner #{Index}={RunnerId} owned_partitions=[{Partitions}]",
                                    key, idx, chosen, string.Join(",", localOpts.Runners[chosen].Select(p => $"p{p}")));
                                localCrashSwitch.RequestRunnerCrash(chosen);
                            }
                        }
                        break;
                }
            }

            await Task.Delay(200);
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
