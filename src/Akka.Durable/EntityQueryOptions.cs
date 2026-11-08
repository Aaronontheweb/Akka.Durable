namespace Akka.Durable;

/// <summary>Settings for snapshot/event queries of an entity type.</summary>
public sealed record EntityQueryOptions(string ReadJournalPluginId)
{
    /// <summary>Maximum time allowed to load the bootstrap snapshot.</summary>
    public TimeSpan SnapshotTimeout { get; init; } = TimeSpan.FromSeconds(30);

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ReadJournalPluginId);
        if (SnapshotTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SnapshotTimeout));
    }
}
