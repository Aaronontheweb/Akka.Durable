using Akka.Actor;
using Akka.DependencyInjection;
using Akka.Durable.Abstractions.Entities;
using Akka.Event;
using Akka.Persistence;

namespace Akka.Durable.Entities;

public abstract class EntityActor<TState, TCommand, TEvent> : ReceivePersistentActor
{
    private readonly EntityId _entityId;
    private readonly EntityDefinition<TState, TCommand, TEvent> _entityDefinition;
    private readonly EntityOptions _entityOptions;
    private readonly IDependencyResolver _dependencyResolver;
    private readonly ITimeProvider _timeProvider;
    protected readonly ILoggingAdapter Log;

    protected EntityActor(EntityId id, EntityDefinition<TState, TCommand, TEvent> entityDefinition,
        IDependencyResolver dependencyResolver, ITimeProvider timeProvider, EntityId entityId)
    {
        _entityDefinition = entityDefinition;
        _dependencyResolver = dependencyResolver;
        _timeProvider = timeProvider;
        _entityId = entityId;
        _entityOptions = entityDefinition.Options ?? EntityOptions.Default;

        Log  = Context.GetLogger().WithContext("EntityType", _entityDefinition.TypeName)
            .WithContext("EntityId", _entityId);
        
        PersistenceId = _entityOptions.PersistenceIdFactory(entityId, _entityDefinition.TypeName);
    }

    public sealed override string PersistenceId { get; }
}