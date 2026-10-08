using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace Consumer;

/// <summary>
/// Batch HTTP wrapper for the STUB stub (contracts/ctcservice.md). Called from PartitionWorker
/// threads only — never touches Kafka. Success = HTTP 2xx with ingested:true (duplicate counts
/// as success, per the dedup backstop contract); anything else is a retryable failure.
/// </summary>
public class StubClient
{
    private readonly HttpClient _http;
    private readonly ILogger<StubClient> _logger;

    public StubClient(HttpClient http, IOptions<KafkaOptions> opts, ILogger<StubClient> logger)
    {
        _logger = logger;
        _http = http;
        _http.BaseAddress = new Uri(opts.Value.CtcUrl);
        _http.Timeout = TimeSpan.FromSeconds(5);
    }

    /// <summary>
    /// Posts one commit-unit batch to /ingest. Returns (Success, Duplicate). Success is true only
    /// when the response reports ingested:true (duplicate:true still counts as success/net-once).
    /// </summary>
    public async Task<(bool Success, bool Duplicate)> IngestAsync(
        string topic, int partition, long offset, string? siteId, object? events, CancellationToken ct)
    {
        var payload = new { siteId, topic, partition, offset, events };
        var json = JsonSerializer.Serialize(payload);
        var content = new StringContent(json, Encoding.UTF8, "application/json");

        try
        {
            var response = await _http.PostAsync("/ingest", content, ct);

            if (response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                using var doc = JsonDocument.Parse(body);
                bool ingested = doc.RootElement.TryGetProperty("ingested", out var ingestedEl) && ingestedEl.GetBoolean();
                bool duplicate = doc.RootElement.TryGetProperty("duplicate", out var dupEl) && dupEl.GetBoolean();

                if (ingested)
                {
                    _logger.LogInformation(
                        "[STUB] partition={Partition} offset={Offset} ingested=true duplicate={Duplicate}",
                        partition, offset, duplicate);
                    return (true, duplicate);
                }

                _logger.LogWarning("[STUB] partition={Partition} offset={Offset} ingested=false (retryable)",
                    partition, offset);
                return (false, false);
            }

            _logger.LogWarning("[STUB] partition={Partition} offset={Offset} HTTP {Status} (retryable)",
                partition, offset, (int)response.StatusCode);
            return (false, false);
        }
        catch (TaskCanceledException) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning("[STUB] partition={Partition} offset={Offset} request timed out (retryable)",
                partition, offset);
            return (false, false);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogWarning("[STUB] partition={Partition} offset={Offset} connection failed: {Msg} (retryable)",
                partition, offset, ex.Message);
            return (false, false);
        }
    }
}
