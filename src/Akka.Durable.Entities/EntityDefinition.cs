using Akka.Durable.Abstractions.Entities;

namespace Akka.Durable.Entities;

/// <summary>
/// Name of the ShardRegion that will be hosting and running this entity.
/// </summary>
public readonly record struct EntityTypeName(string Value);

/// <summary>
/// Inputs to a decision, plus factories for building one.
/// </summary>
/// <remarks>
/// This is an immutable value: the factory methods only construct decision values and
/// perform no IO, so Decide remains a pure function of (state, command, context).
/// The factories exist to bind <typeparamref name="TCommand"/> and <typeparamref name="TEvent"/>
/// without explicit type arguments, so that effects which feed messages back into the
/// entity (reminders, Run completions) are constrained to <typeparamref name="TCommand"/>.
/// </remarks>
public readonly record struct DecisionContext<TCommand, TEvent>(
    EntityId EntityId,
    EntityTypeName EntityType,
    DateTimeOffset UtcNow)
{
    public Handled<TCommand, TEvent> Persist(params TEvent[] events) =>
        Decisions.Persist<TCommand, TEvent>(events);

    public Handled<TCommand, TEvent> None() => Decisions.None<TCommand, TEvent>();

    public Ignored Ignore() => Decisions.Ignore();
    public Unhandled Unhandled() => Decisions.Unhandled();
    public Stashed Stash() => Decisions.Stash();
}

public sealed record EntityDefinition<TState, TCommand, TEvent>(
    EntityTypeName TypeName,
    Func<EntityId, TState> InitialState,
    Func<TState, TCommand, DecisionContext<TCommand, TEvent>, Decision<TCommand, TEvent>> Decide,
    Func<TState, TEvent, TState> Evolve,
    EntityOptions? Options = null);

public sealed record EntityOptions
{
    /// <summary>
    /// Default implementation of how we generate persistence ids-per-entity
    /// </summary>
    public static readonly Func<EntityId, EntityTypeName, string> DefaultPersistenceIdFactory =
        (id, name) => $"{name.Value}-{id.Value}";

    // Optional override. Otherwise derived from TypeName + EntityId.
    public Func<EntityId, EntityTypeName, string> PersistenceIdFactory
    { get; init; } = DefaultPersistenceIdFactory;

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