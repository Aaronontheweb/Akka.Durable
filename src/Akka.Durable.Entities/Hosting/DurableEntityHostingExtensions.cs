using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.DependencyInjection;
using Akka.Durable.Abstractions.Entities;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Akka.Durable.Entities.Hosting;

/// <summary>
/// <see cref="ActorRegistry"/> key for the shard region or local host of the entity type whose
/// command protocol is <typeparamref name="TCommand"/>.
/// </summary>
/// <remarks>
/// The registry is keyed by command type, so each entity type must have its own command type
/// (typically an entity-specific command union). Registering two entity types with the same command
/// type makes the second registration collide in the ActorRegistry.
/// </remarks>
public sealed class EntityRegion<TCommand>
{
    private EntityRegion() { }
}

/// <summary>
/// <see cref="ActorRegistry"/> key used by state-aware refs (<see cref="IDurableEntityRefs{TCommand,TState}"/>).
/// It points at the same shard region or local host as <see cref="EntityRegion{TCommand}"/>.
/// </summary>
/// <remarks>
/// A <typeparamref name="TState"/> that doesn't match the registered entity definition has no registry
/// entry, so resolving the ref fails with Akka.Hosting's <see cref="MissingActorRegistryEntryException"/>.
/// </remarks>
public sealed class EntityRegion<TCommand, TState>
{
    private EntityRegion() { }
}

public static class DurableEntityHostingExtensions
{
    private const int NumberOfShards = 100;

    /// <summary>
    /// Registers <see cref="IEntityRefs{TCommand}"/> and <see cref="IDurableEntityRefs{TCommand,TState}"/>
    /// for injection. Call once alongside <see cref="WithDurableEntity{TState,TCommand,TEvent}"/>.
    /// </summary>
    public static IServiceCollection AddDurableEntities(this IServiceCollection services)
    {
        services.TryAddSingleton(typeof(IEntityRefs<>), typeof(EntityRefs<>));
        services.TryAddSingleton(typeof(IDurableEntityRefs<,>), typeof(DurableEntityRefs<,>));
        return services;
    }

    /// <summary>
    /// Hosts a durable entity type: in a shard region when <paramref name="shardOptions"/> is given,
    /// otherwise in a local host actor in this process.
    /// </summary>
    public static AkkaConfigurationBuilder WithDurableEntity<TState, TCommand, TEvent>(
        this AkkaConfigurationBuilder builder,
        EntityDefinition<TState, TCommand, TEvent> definition,
        ShardOptions? shardOptions = null)
    {
        Func<string, Props> EntityProps(ActorSystem system, IDependencyResolver resolver)
        {
            var services = resolver.GetService<IServiceProvider>();
            ITimeProvider time = system.Scheduler;
            return entityId => Props.Create<EntityActor<TState, TCommand, TEvent>>(
                new EntityId(entityId), definition, services, time);
        }

        if (shardOptions is not null)
        {
            // Hand-off uses the same stop message as passivation, so persistent entities stop cleanly.
            shardOptions.HandOffStopMessage ??= EntityActor<TState, TCommand, TEvent>.StopEntity.Instance;

            // Entity refs and reminders send ShardingEnvelopes, which sharding routes on its own.
            return builder
                .WithShardRegion<EntityRegion<TCommand>>(
                    definition.TypeName.Value,
                    (system, _, resolver) => EntityProps(system, resolver),
                    HashCodeMessageExtractor.Create(NumberOfShards, _ => null),
                    shardOptions)
                .WithActors(async (_, registry) =>
                    registry.Register<EntityRegion<TCommand, TState>>(
                        await registry.GetAsync<EntityRegion<TCommand>>()));
        }

        return builder.WithActors((system, registry, resolver) =>
        {
            var entityProps = EntityProps(system, resolver);
            var host = system.ActorOf(
                Props.Create(() => new LocalEntityHost(entityProps)),
                LocalEntityHost.ActorName(definition.TypeName));
            registry.Register<EntityRegion<TCommand>>(host);
            registry.Register<EntityRegion<TCommand, TState>>(host);
        });
    }
}

internal sealed class EntityRefs<TCommand>(IRequiredActor<EntityRegion<TCommand>> region) : IEntityRefs<TCommand>
{
    public IEntityRef<TCommand> Get(EntityId id) => new EntityRef<TCommand>(id, region.ActorRef);
}

internal sealed class DurableEntityRefs<TCommand, TState>(IRequiredActor<EntityRegion<TCommand, TState>> region)
    : IDurableEntityRefs<TCommand, TState>
{
    public IDurableEntityRef<TCommand, TState> Get(EntityId id) => new EntityRef<TCommand, TState>(id, region.ActorRef);
}
