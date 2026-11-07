using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.DependencyInjection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Akka.Durable.Abstractions.Entities;
using Akka.Durable.Entities.Serialization;
using Akka.Hosting;
using Akka.Serialization;
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
/// <see cref="ActorRegistry"/> key used by state-aware refs (<see cref="IDurableEntityRefs{TCommand,TState,TEvent}"/>).
/// It points at the same shard region or local host as <see cref="EntityRegion{TCommand}"/>.
/// </summary>
/// <remarks>
/// A <typeparamref name="TState"/> or <typeparamref name="TEvent"/> that doesn't match the registered entity
/// definition has no registry entry, so resolving the ref fails with Akka.Hosting's
/// <see cref="MissingActorRegistryEntryException"/>.
/// </remarks>
public sealed class EntityRegion<TCommand, TState, TEvent>
{
    private EntityRegion() { }
}

public static class DurableEntityHostingExtensions
{
    /// <summary>
    /// Registers <see cref="IEntityRefs{TCommand}"/> and <see cref="IDurableEntityRefs{TCommand,TState,TEvent}"/>
    /// for injection. Call once alongside <see cref="WithDurableEntity{TState,TCommand,TEvent}"/>.
    /// </summary>
    public static IServiceCollection AddDurableEntities(this IServiceCollection services)
    {
        services.TryAddSingleton(typeof(IEntityRefs<>), typeof(EntityRefs<>));
        services.TryAddSingleton(typeof(IDurableEntityRefs<,,>), typeof(DurableEntityRefs<,,>));
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

        var options = definition.Options ?? EntityOptions.Default;
        var stopMessage = EntityActor<TState, TCommand, TEvent>.StopEntity.Instance;
        var passivateIdleAfter = options.PassivateIdleAfter;

        AddFrameworkSerializerOnce(builder);

        // Fail at startup, before any entity runs, if an entity type would fall back to the
        // System.Object serializer: events, snapshots, reminders and remote commands all serialize.
        builder.WithActors((system, _) => EnsureSerializable(system, definition.TypeName,
            typeof(TState), typeof(TCommand), typeof(TEvent)));

        if (shardOptions is not null)
        {
            if (options.NumberOfShards <= 0)
                throw new ArgumentOutOfRangeException(nameof(definition), options.NumberOfShards,
                    $"EntityOptions.NumberOfShards for [{definition.TypeName.Value}] must be positive.");

            // Hand-off uses the same stop message as passivation, so persistent entities stop cleanly.
            shardOptions.HandOffStopMessage ??= stopMessage;

            // An explicit sharding value wins. Zero is sharding's own "off", so non-positive maps to it.
            shardOptions.PassivateIdleEntityAfter ??=
                passivateIdleAfter > TimeSpan.Zero ? passivateIdleAfter : TimeSpan.Zero;

            // Entity refs and reminders send ShardingEnvelopes, which sharding routes on its own.
            return builder
                .WithShardRegion<EntityRegion<TCommand>>(
                    definition.TypeName.Value,
                    (system, _, resolver) => EntityProps(system, resolver),
                    HashCodeMessageExtractor.Create(options.NumberOfShards, _ => null),
                    shardOptions)
                .WithActors(async (_, registry) =>
                    registry.Register<EntityRegion<TCommand, TState, TEvent>>(
                        await registry.GetAsync<EntityRegion<TCommand>>()));
        }

        return builder.WithActors((system, registry, resolver) =>
        {
            var entityProps = EntityProps(system, resolver);
            var host = system.ActorOf(
                Props.Create(() => new LocalEntityHost(entityProps, stopMessage, passivateIdleAfter)),
                LocalEntityHost.ActorName(definition.TypeName));
            registry.Register<EntityRegion<TCommand>>(host);
            registry.Register<EntityRegion<TCommand, TState, TEvent>>(host);
        });
    }

    private static readonly ConditionalWeakTable<AkkaConfigurationBuilder, object> FrameworkSerializerAdded = [];

    /// <summary>Registers Akka.Durable's own serializer once per builder, however many entity types it hosts.</summary>
    private static void AddFrameworkSerializerOnce(AkkaConfigurationBuilder builder)
    {
        if (FrameworkSerializerAdded.TryAdd(builder, FrameworkSerializerAdded))
            builder.WithCustomSerializer(DurableEntitySerializer.CreateSerializerDetails);
    }

    private static void EnsureSerializable(ActorSystem system, EntityTypeName typeName, params Type[] types)
    {
        var serialization = system.Serialization;
        var fallback = TryFindSerializer(serialization, typeof(object));

        var unbound = types
            .Where(type => TryFindSerializer(serialization, type) is not { } serializer
                           || (fallback is not null && serializer.Identifier == fallback.Identifier))
            .Select(type => type.FullName)
            .ToList();

        if (unbound.Count == 0) return;

        throw new InvalidOperationException(
            $"Durable entity [{typeName.Value}] cannot start: no serializer is registered for " +
            $"{string.Join(", ", unbound)}, so they would fall back to " +
            $"{fallback?.GetType().Name ?? "no serializer at all"}. Mark them [AkkaSerializable(Manifest = \"...\")] " +
            "(for a C# 15 union, the union itself as well as each case) under an [AkkaSerializer<TProtocol>] " +
            "serializer, and register it with WithCustomSerializer(YourSerializer.CreateSerializerDetails).");
    }

    private static Serializer? TryFindSerializer(Akka.Serialization.Serialization serialization, Type type)
    {
        try
        {
            return serialization.FindSerializerForType(type);
        }
        catch (SerializationException)
        {
            // No binding and no System.Object fallback configured.
            return null;
        }
    }
}

internal sealed class EntityRefs<TCommand>(IRequiredActor<EntityRegion<TCommand>> region) : IEntityRefs<TCommand>
{
    public IEntityRef<TCommand> Get(EntityId id) => new EntityRef<TCommand>(id, region.ActorRef);
}

internal sealed class DurableEntityRefs<TCommand, TState, TEvent>(IRequiredActor<EntityRegion<TCommand, TState, TEvent>> region)
    : IDurableEntityRefs<TCommand, TState, TEvent>
{
    public IDurableEntityRef<TCommand, TState, TEvent> Get(EntityId id) => new EntityRef<TCommand, TState, TEvent>(id, region.ActorRef);
}
