using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace DynamicConsumer
{
    public sealed record ConsumedRecord(int Partition, long Offset, string? SiteId, string Value);

    /// <summary>Tolerant helpers for parsing a raw Kafka record value into siteId + events payload.</summary>
    public static class RecordParsing
    {
        /// <summary>
        /// Best-effort extraction of <c>siteId</c> from a JSON value shaped like
        /// <c>{ "siteId": "...", "events": ... }</c>. Returns null (never throws) when the value is
        /// not JSON, or is JSON without a "siteId" property — the raw value is still forwarded as-is.
        /// </summary>
        public static string? TryExtractSiteId(string value)
        {
            try
            {
                using var doc = JsonDocument.Parse(value);
                if (doc.RootElement.ValueKind == JsonValueKind.Object &&
                    doc.RootElement.TryGetProperty("siteId", out var s) &&
                    s.ValueKind == JsonValueKind.String)
                {
                    return s.GetString();
                }
            }
            catch (JsonException)
            {
                // Non-JSON payload (e.g. CLI-injected plain text) — no siteId, forward the raw value.
            }

            return null;
        }

        /// <summary>
        /// Best-effort extraction of an "events" property for the STUB payload; null when the value is
        /// not JSON or has no "events" property (STUB still records the offset either way).
        /// </summary>
        public static object? TryExtractEvents(string value)
        {
            try
            {
                using var doc = JsonDocument.Parse(value);
                if (doc.RootElement.ValueKind == JsonValueKind.Object &&
                    doc.RootElement.TryGetProperty("events", out var evEl))
                {
                    return evEl.Clone();
                }
            }
            catch (JsonException)
            {
                // Non-JSON payload — forward null events, STUB still records the offset.
            }

            return null;
        }
    }
}
