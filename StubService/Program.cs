using System.Net.Http;
using System.Text.Json;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.File(Path.Combine("logs", "stubservice-.log"), rollingInterval: RollingInterval.Day)
    .CreateLogger();

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddSerilog(Log.Logger, dispose: true);

var app = builder.Build();

// In-memory STUB availability toggle + dedup backstop (topic+partition+offset).
bool _available = true;
var _lock = new object();
var _seen = new HashSet<string>();
var _seenLock = new object();
int _latencyMs = app.Configuration.GetValue<int?>("Ctc:LatencyMs") ?? 1000;

// POST /ingest — a PartitionWorker calls this once per commit unit (offset-atomic: one record's
// site batch = one call), BEFORE the consumer thread commits that offset (contracts/stubservice.md).
app.MapPost("/ingest", async (HttpContext ctx) =>
{
    using var reader = new StreamReader(ctx.Request.Body);
    var body = await reader.ReadToEndAsync();

    string? siteId = null;
    string? topic = null;
    int partition = -1;
    long offset = -1;
    try
    {
        var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;
        if (root.TryGetProperty("siteId", out var s)) siteId = s.GetString();
        if (root.TryGetProperty("topic", out var t)) topic = t.GetString();
        if (root.TryGetProperty("partition", out var p)) partition = p.GetInt32();
        if (root.TryGetProperty("offset", out var o)) offset = o.GetInt64();
    }
    catch (JsonException ex)
    {
        app.Logger.LogWarning("[STUB] /ingest received malformed JSON: {Msg}", ex.Message);
    }

    bool isAvailable;
    lock (_lock) { isAvailable = _available; }

    if (!isAvailable)
    {
        app.Logger.LogWarning("[STUB] /ingest rejected (unavailable) site={SiteId} partition={Partition} offset={Offset}",
            siteId, partition, offset);
        return Results.Json(new { ingested = false, reason = "stub-down" }, statusCode: 503);
    }

    // simulate downstream ingest latency.
    await Task.Delay(_latencyMs);

    // dedup backstop keyed on topic+partition+offset (secondary safeguard only).
    var key = $"{topic}:{partition}:{offset}";
    bool duplicate;
    lock (_seenLock) { duplicate = !_seen.Add(key); }

    app.Logger.LogInformation("[STUB] /ingest site={SiteId} partition={Partition} offset={Offset} duplicate={Duplicate}",
        siteId, partition, offset, duplicate);

    return Results.Ok(new { ingested = true, duplicate, offset });
});

// POST /control/down — operator sets stub unavailable (live, no restart).
app.MapPost("/control/down", () =>
{
    lock (_lock) { _available = false; }
    app.Logger.LogInformation("[STUB] /control/down called — STUB is now UNAVAILABLE");
    return Results.Ok(new { available = false });
});

// POST /control/up — operator sets stub available.
app.MapPost("/control/up", () =>
{
    lock (_lock) { _available = true; }
    app.Logger.LogInformation("[STUB] /control/up called — STUB is now AVAILABLE");
    return Results.Ok(new { available = true });
});

// GET /control/state — observe toggle state.
app.MapGet("/control/state", () =>
{
    bool current;
    lock (_lock) { current = _available; }
    return Results.Ok(new { available = current });
});

// POST /control/crash — optional whole-StubService crash hook .
app.MapPost("/control/crash", () =>
{
    app.Logger.LogWarning("[STUB] /control/crash called — process exiting");
    _ = Task.Run(async () => { await Task.Delay(200); Environment.Exit(1); });
    return Results.Ok(new { crashing = true });
});

// GET /health — always 200 (independent of the available toggle).
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
Log.CloseAndFlush();

