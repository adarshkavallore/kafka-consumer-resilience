namespace Consumer;

public enum AckOutcome
{
    Ingested,
    SkippedDuplicate
}

public sealed record Ack(int Partition, long Offset, AckOutcome Outcome);

public sealed record RecordBatch(int Partition, long Offset, string? SiteId, string Value);
