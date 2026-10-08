using System.Text.Json;
using System.Text.Json.Serialization;

namespace Consumer;

public sealed class ProcessedStateStore
{
    private readonly string _baseDir;

    public ProcessedStateStore(string stateDir, string groupId, string runnerId)
    {
        _baseDir = Path.Combine(stateDir, groupId, runnerId);
        Directory.CreateDirectory(_baseDir);
    }

    private string PathFor(int partition) => Path.Combine(_baseDir, $"partition-{partition}.json");

    /// <summary>Returns the last durably-processed offset for a partition, or null if none recorded yet.</summary>
    public long? Read(int partition)
    {
        var path = PathFor(partition);
        if (!File.Exists(path)) return null;

        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                var record = JsonSerializer.Deserialize<ProcessedStateRecord>(stream);
                return record?.LastProcessedOffset;
            }
            catch (IOException) when (attempt < maxAttempts)
            {
                Thread.Sleep(20);
            }
            catch (Exception)
            { 
                return null;
            }
        }

        return null;
    }

    /// <summary>Durably records lastProcessedOffset=offset for a partition .</summary>
    public void Write(int partition, long offset)
    {
        var path = PathFor(partition);
        var tmp = path + ".tmp";
        var json = JsonSerializer.Serialize(new ProcessedStateRecord(offset));
        File.WriteAllText(tmp, json);
        File.Move(tmp, path, overwrite: true);
    }

    private sealed record ProcessedStateRecord(
        [property: JsonPropertyName("lastProcessedOffset")] long LastProcessedOffset);
}
