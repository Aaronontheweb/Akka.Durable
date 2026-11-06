using Akka.Durable.Abstractions.Entities;

namespace Akka.Durable.Entities;

/// <summary>
/// Name of the ShardRegion that will be hosting and running this entity.
/// </summary>
public readonly record struct EntityTypeName(string Value);

public readonly record struct DecisionContext(
    EntityId EntityId,
    EntityTypeName EntityType,
    DateTimeOffset UtcNow);

public sealed record EntityDefinition<TState, TCommand, TEvent>(
    EntityTypeName TypeName,
    Func<EntityId, TState> InitialState,
    Func<TState, TCommand, DecisionContext, Decision<TEvent>> Decide,
    Func<TState, TEvent, TState> Evolve,
    EntityOptions? Options);
    
public sealed record EntityOptions
{
    /// <summary>
    /// Default implementation of how we generate persistence ids-per-entity
    /// </summary>
    public static readonly Func<EntityId, EntityTypeName, string> DefaultPersistenceIdFactory =
        (id, name) => $"{name}-{id}"; 
    
    // Optional override. Otherwise derived from TypeName + EntityId.
    public Func<EntityId, EntityTypeName, string> PersistenceIdFactory
    { get; init; } =  DefaultPersistenceIdFactory;
    
    /// <summary>
    /// For asynchronous effects, this is the default task budget.
    /// </summary>
    public TimeSpan EffectTimeout { get; init; } =
        TimeSpan.FromSeconds(30);

    // Snapshot and retention policy
    public int SnapshotEvery { get; init; } = 100;
    public int KeepSnapshots { get; init; } = 2;
    public bool DeleteEventsOnSnapshot { get; init; } = false;

    // Optional Akka.Persistence plugin overrides
    public string? JournalPluginId { get; init; }
    public string? SnapshotPluginId { get; init; }

    public EntityOptions WithPersistenceId(
        Func<EntityId, EntityTypeName, string> factory) =>
        this with { PersistenceIdFactory = factory };
    
    public static readonly EntityOptions Default = new();
}