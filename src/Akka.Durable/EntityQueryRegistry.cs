using System.Collections.Concurrent;
using Akka;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace Akka.Durable;

/// <summary>Holds reader configuration only. Each subscription materializes its own query.</summary>
internal sealed class EntityQueryRegistry
{
    private readonly ConcurrentDictionary<Type, object> _definitions = new();

    public void Register<TState, TCommand, TEvent>(EntityTypeName type,
        IEntityQueryReader<TState, TEvent> reader, ActorMaterializer materializer, EntityQueryOptions options)
    {
        if (!_definitions.TryAdd(typeof(IDurableEntityRef<TCommand, TState, TEvent>),
                new QueryDefinition<TState, TEvent>(type, reader, materializer, options)))
            throw new InvalidOperationException($"Queries for {type.Value} have already been registered.");
    }

    public Source<EntityUpdate<TState, TEvent>, NotUsed> Subscribe<TState, TCommand, TEvent>(EntityId id, Offset? offset)
    {
        if (!_definitions.TryGetValue(typeof(IDurableEntityRef<TCommand, TState, TEvent>), out var registered))
            throw new InvalidOperationException("Configure WithDurableEntityQueries to enable subscriptions for this entity type.");
        var definition = (QueryDefinition<TState, TEvent>)registered;
        return EntitySubscriptionSource<TState, TEvent>.Create(
            definition.Reader, definition.Materializer, definition.Type, id, offset, definition.Options.SnapshotTimeout);
    }

    private sealed record QueryDefinition<TState, TEvent>(EntityTypeName Type,
        IEntityQueryReader<TState, TEvent> Reader, ActorMaterializer Materializer, EntityQueryOptions Options);
}
