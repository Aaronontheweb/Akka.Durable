using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace Akka.Durable;

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
/// without explicit type arguments. Scheduled messages are constrained to <typeparamref name="TCommand"/>.
/// </remarks>
public readonly record struct DecisionContext<TState, TCommand, TEvent>(
    EntityId EntityId,
    EntityTypeName EntityType,
    DateTimeOffset UtcNow,
    TState State)
{
    public Handled<TCommand, TEvent> Persist(params TEvent[] events) =>
        Decisions.Persist<TCommand, TEvent>(events);

    public Handled<TCommand, TEvent> None() =>
        Decisions.None<TCommand, TEvent>();

    public Ignored Ignore() => Decisions.Ignore();
    public NotHandled Unhandled() => Decisions.Unhandled();
    public Stashed Stash() => Decisions.Stash();
}

public sealed record EntityDefinition<TState, TCommand, TEvent>(
    EntityTypeName TypeName,
    Func<EntityId, TState> InitialState,
    Func<TState, TCommand, DecisionContext<TState, TCommand, TEvent>, Decision<TCommand, TEvent>> Decide,
    Func<TState, TEvent, TState> Evolve,
    EntityOptions? Options = null)
{
    /// <summary>
    /// The effects that are still pending for a state. Optional.
    /// </summary>
    /// <remarks>
    /// <para>Runs once after recovery, before the entity handles its first command, and its effects go through
    /// the same path as a decision's. A decider can also call it from a retry arm. It decides nothing, so it
    /// may not return events; the entity fails to start if it does.</para>
    /// <para>Everything it returns follows from state alone, so running it twice must be harmless: give the
    /// effects idempotency keys, and prefer re-sending a query over a change. An entity whose effects never
    /// wait for an outcome does not need it.</para>
    /// <para>Replies issued by resumed effects go to dead letters: the asker of the original request belongs
    /// to the previous incarnation.</para>
    /// </remarks>
    public Func<TState, DecisionContext<TState, TCommand, TEvent>, Handled<TCommand, TEvent>>? Resume { get; init; }

    internal ImmutableDictionary<Type, Func<object, TCommand>> Accepted { get; init; } =
        ImmutableDictionary<Type, Func<object, TCommand>>.Empty;

    /// <summary>
    /// Declares how a message of another type becomes one of this entity's commands. Needed for the replies
    /// of entities this one talks to with <c>Send</c>: they arrive as the other entity's reply type.
    /// </summary>
    /// <remarks>
    /// The table lives on the definition rather than on each send so that it survives a restart. The reply
    /// to a command re-sent by <see cref="Resume"/> is wrapped the same way.
    /// </remarks>
    public EntityDefinition<TState, TCommand, TEvent> Accepts<TMessage>(Func<TMessage, TCommand> wrap)
        where TMessage : notnull
    {
        ArgumentNullException.ThrowIfNull(wrap);
        return this with { Accepted = Accepted.SetItem(typeof(TMessage), message => wrap((TMessage)message)) };
    }

    internal bool TryAccept(object message, [MaybeNullWhen(false)] out TCommand command)
    {
        if (Accepted.TryGetValue(message.GetType(), out var exact))
        {
            command = exact(message);
            return true;
        }

        foreach (var (type, wrap) in Accepted)
        {
            if (!type.IsInstanceOfType(message)) continue;
            command = wrap(message);
            return true;
        }

        command = default;
        return false;
    }
}

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
    /// Number of shards the entity type is spread across in sharded mode. Defaults to 100.
    /// </summary>
    /// <remarks>
    /// <para>Entity IDs are hashed onto this many shards, so every node that hosts the entity type must use
    /// the same value. It lives on the entity definition, which every node shares, for that reason.</para>
    /// <para>Changing it after deployment moves entities to different shards: roll it out with a full
    /// cluster restart, not a rolling upgrade.</para>
    /// <para>Ignored when the entity type is hosted locally.</para>
    /// </remarks>
    public int NumberOfShards { get; init; } = 100;

    /// <summary>
    /// Time budget for one <c>Execute</c> handler call, and the default for <c>Run</c> effects.
    /// </summary>
    public TimeSpan EffectTimeout { get; init; } =
        TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long an entity may go without receiving a message before its host passivates it
    /// (stops its actor; the next message reactivates it from the journal). Defaults to 2 minutes,
    /// matching Akka.Cluster.Sharding's <c>passivate-idle-entity-after</c> default.
    /// </summary>
    /// <remarks>
    /// <para>Set to <see cref="TimeSpan.Zero"/> to disable idle passivation; any non-positive value
    /// disables it. This mirrors Akka.Cluster.Sharding, which represents "off" as
    /// <see cref="TimeSpan.Zero"/> and only passivates idle entities when the timeout is positive, so
    /// the value maps one-to-one onto <c>ShardOptions.PassivateIdleEntityAfter</c>.</para>
    /// <para>Applies in both hosting modes. In sharded mode an explicit
    /// <c>ShardOptions.PassivateIdleEntityAfter</c> takes precedence, and sharding never passivates
    /// idle entities while remember-entities is enabled.</para>
    /// </remarks>
    public TimeSpan PassivateIdleAfter { get; init; } = TimeSpan.FromMinutes(2);

    // Snapshot and retention policy
    public int SnapshotEvery { get; init; } = 100;
    public int KeepSnapshots { get; init; } = 2;
    public bool DeleteEventsOnSnapshot { get; init; } = false;

    // Optional Akka.Persistence plugin overrides
    public string? JournalPluginId { get; init; }
    public string? SnapshotPluginId { get; init; }

    /// <summary>
    /// Skip journal entries that are not of the entity's event type during recovery, logging an error for
    /// each, instead of failing recovery. Off by default.
    /// </summary>
    /// <remarks>
    /// <para>By default an unrecognized entry stops recovery, so the entity never runs with silently missing
    /// state. Enabling this keeps an entity with a poisoned journal available at the cost of the skipped
    /// entries' changes.</para>
    /// <para>For targeted repair, bind an <c>IReadEventAdapter</c> to the journal instead: it can drop an entry
    /// or replace it with a valid event before the entity sees it.</para>
    /// <para>Snapshots are never skipped: replay resumes after a snapshot, so skipping one would lose every
    /// event it covers.</para>
    /// </remarks>
    public bool SkipUnrecognizedJournalEntries { get; init; }

    public EntityOptions WithPersistenceId(
        Func<EntityId, EntityTypeName, string> factory) =>
        this with { PersistenceIdFactory = factory };

    public static readonly EntityOptions Default = new();
}
