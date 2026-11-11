using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.DependencyInjection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Akka.Hosting;
using Akka.Reminders;
using Akka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Akka.Durable;

/// <summary>
/// <see cref="ActorRegistry"/> key for the shard region or local host of the entity type whose
/// command protocol is <typeparamref name="TCommand"/>.
/// </summary>
/// <remarks>
/// The registry is keyed by command type, so each entity type must have its own command type
/// (typically an entity-specific command union). <c>WithDurableEntity</c> rejects a second entity type
/// with the same command type.
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
        services.TryAddSingleton(typeof(IWorkflowRefs<,,,>), typeof(WorkflowRefs<,,,>));
        AddEffectRoutes(services);
        return services;
    }

    /// <summary>
    /// Registers <typeparamref name="THandler"/> as the handler of the effect <typeparamref name="TEffect"/>, with an
    /// answer of type <typeparamref name="TOutcome"/>. For an entity, the answer is the entity's command type. For a
    /// workflow, it is the workflow's fact type.
    /// </summary>
    /// <remarks>
    /// <para>The handler is scoped: each attempt gets a fresh DI scope. The registration also records where the effect
    /// type is handled, so an effect recovered after a restart finds its handler.</para>
    /// <para>One handler for each effect type. A second registration for the same effect type throws.</para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">A handler is already registered for <typeparamref name="TEffect"/>.</exception>
    public static IServiceCollection AddEffectHandler<TEffect, TOutcome, THandler>(this IServiceCollection services)
        where TEffect : notnull
        where THandler : class, IEffectHandler<TEffect, TOutcome>
    {
        AddRoute(services, EffectRoute.WithAnswer<TEffect, TOutcome, THandler>());
        services.AddScoped<IEffectHandler<TEffect, TOutcome>, THandler>();
        return services;
    }

    /// <summary>
    /// Registers <typeparamref name="THandler"/> as the handler of the entity effect <typeparamref name="TEffect"/>, with
    /// no answer for <c>Decide</c>. The effect is acknowledged when the handler returns.
    /// </summary>
    /// <remarks>
    /// <para>The handler is scoped: each attempt gets a fresh DI scope. The registration also records where the effect
    /// type is handled, so an effect recovered after a restart finds its handler.</para>
    /// <para>One handler for each effect type. A second registration for the same effect type throws.</para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">A handler is already registered for <typeparamref name="TEffect"/>.</exception>
    public static IServiceCollection AddEffectHandler<TEffect, THandler>(this IServiceCollection services)
        where TEffect : notnull
        where THandler : class, IEffectHandler<TEffect>
    {
        AddRoute(services, EffectRoute.WithoutAnswer<TEffect, THandler>());
        services.AddScoped<IEffectHandler<TEffect>, THandler>();
        return services;
    }

    private static void AddRoute(IServiceCollection services, EffectRoute route)
    {
        ArgumentNullException.ThrowIfNull(services);
        foreach (var registered in services)
            if (registered.ServiceType == typeof(EffectRoute)
                && registered.ImplementationInstance is EffectRoute existing
                && existing.EffectType == route.EffectType)
                throw EffectRoutes.DuplicateRoute(existing, route);

        services.AddSingleton(route);
        AddEffectRoutes(services);
    }

    private static void AddEffectRoutes(IServiceCollection services) =>
        services.TryAddSingleton(provider => new EffectRoutes(provider.GetServices<EffectRoute>()));

    /// <summary>
    /// Hosts a durable entity type: in a shard region when <paramref name="sharding"/> is given,
    /// otherwise in a local host actor in this process.
    /// </summary>
    public static AkkaConfigurationBuilder WithDurableEntity<TState, TCommand, TEvent>(
        this AkkaConfigurationBuilder builder,
        EntityDefinition<TState, TCommand, TEvent> definition,
        ShardedHosting? sharding = null)
    {
        EnsureOwnCommandType(builder, definition.TypeName, typeof(TCommand));

        if (!definition.Used.IsEmpty)
        {
            // Runs when the actor system starts, after every registration on this builder, so the workflow types may
            // be registered before or after the entity type.
            var uses = definition.Used.Values.ToArray();
            builder.WithActors((_, _) => EnsureWorkflowsHosted(builder, definition.TypeName, uses));

            // A pending start is kept in a reminder. Without this, Native AOT cannot build its envelope.
            ReminderMessageTypes.Register<PendingStart>();
        }

        return builder.HostEntity<TState, TCommand, TEvent, EntityRegion<TCommand>>(definition, sharding,
            (registry, region) => registry.Register<EntityRegion<TCommand, TState, TEvent>>(region));
    }

    /// <summary>
    /// Hosts an entity type and registers its shard region or local host in the <see cref="ActorRegistry"/>
    /// under <typeparamref name="TRegion"/>. Refs find the entity type by that key.
    /// </summary>
    /// <param name="alsoRegister">Registers the same actor under more keys.</param>
    internal static AkkaConfigurationBuilder HostEntity<TState, TCommand, TEvent, TRegion>(
        this AkkaConfigurationBuilder builder,
        EntityDefinition<TState, TCommand, TEvent> definition,
        ShardedHosting? sharding,
        Action<IActorRegistry, IActorRef>? alsoRegister = null)
    {
        Func<string, Props> EntityProps(ActorSystem system, IDependencyResolver resolver)
        {
            var services = resolver.GetService<IServiceProvider>();
            ITimeProvider time = system.Scheduler;
            return entityId => Props.Create<EntityActor<TState, TCommand, TEvent>>(
                new EntityId(entityId), definition, services, time);
        }

        var options = definition.Options ?? EntityOptions.Default;
        if (options.EffectTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(definition), options.EffectTimeout,
                $"EntityOptions.EffectTimeout for [{definition.TypeName.Value}] was {options.EffectTimeout}; " +
                "the limit on one piece of work must be positive, or every effect would time out at once.");

        var stopMessage = StopEntity.Instance;
        var passivateIdleAfter = options.PassivateIdleAfter;

        AddFrameworkSerializerOnce(builder);

        // Fail at startup, before any entity runs, if an entity type would fall back to the
        // System.Object serializer: events, snapshots, reminders and remote commands all serialize.
        builder.WithActors((system, _) => EnsureSerializable(system, "Durable entity", definition.TypeName,
            typeof(TState), typeof(TCommand), typeof(TEvent)));

        // The entity's effect types are known from their routes, so they are checked here, before any entity runs.
        builder.WithActors((system, _, resolver) =>
            EnsureEffectRoutes<TCommand>(system, definition.TypeName, resolver.GetService<IServiceProvider>()));

        if (sharding is not null)
        {
            if (options.NumberOfShards <= 0)
                throw new ArgumentOutOfRangeException(nameof(definition), options.NumberOfShards,
                    $"EntityOptions.NumberOfShards for [{definition.TypeName.Value}] must be positive.");

            // Entity refs and reminders send ShardingEnvelopes, which sharding routes on its own.
            builder.WithShardRegion<TRegion>(
                definition.TypeName.Value,
                (system, _, resolver) => EntityProps(system, resolver),
                HashCodeMessageExtractor.Create(options.NumberOfShards, _ => null),
                ToShardOptions(sharding, options, stopMessage));

            return alsoRegister is null
                ? builder
                : builder.WithActors(async (_, registry) => alsoRegister(registry, await registry.GetAsync<TRegion>()));
        }

        return builder.WithActors((system, registry, resolver) =>
        {
            var entityProps = EntityProps(system, resolver);
            var host = system.ActorOf(
                Props.Create(() => new LocalEntityHost(entityProps, stopMessage, passivateIdleAfter)),
                LocalEntityHost.ActorName(definition.TypeName));
            registry.Register<TRegion>(host);
            alsoRegister?.Invoke(registry, host);
        });
    }

    /// <summary>
    /// The sharding settings for one entity type. The application chooses where the entities run and how
    /// sharding stores its state. The stop message and idle passivation are always the library's.
    /// </summary>
    internal static ShardOptions ToShardOptions(ShardedHosting sharding, EntityOptions options, object stopMessage) =>
        new()
        {
            Role = sharding.Role,
            StateStoreMode = sharding.StateStoreMode,
            RememberEntities = sharding.RememberEntities,
            RememberEntitiesStore = sharding.RememberEntitiesStore,

            // Hand-off uses the same stop message as passivation, so persistent entities stop cleanly.
            HandOffStopMessage = stopMessage,

            // Zero is the value sharding reads as off, so a timeout that is not positive maps to it.
            PassivateIdleEntityAfter =
                options.PassivateIdleAfter > TimeSpan.Zero ? options.PassivateIdleAfter : TimeSpan.Zero,
        };

    private static readonly ConditionalWeakTable<AkkaConfigurationBuilder, Dictionary<Type, EntityTypeName>> RegionKeyOwners = [];

    /// <summary>
    /// Claims an <see cref="ActorRegistry"/> key for an entity type. Returns <c>null</c> when the key was
    /// free, or the entity type that already holds it.
    /// </summary>
    internal static EntityTypeName? ClaimRegionKey(AkkaConfigurationBuilder builder, EntityTypeName typeName, Type key)
    {
        var owners = RegionKeyOwners.GetOrCreateValue(builder);
        return owners.TryAdd(key, typeName) ? null : owners[key];
    }

    /// <summary>
    /// Entity refs and <c>Send</c> find an entity type by its command type, so one actor system cannot host
    /// two entity types with the same one. Without this check the second registration fails later, in the
    /// ActorRegistry, with an error that names neither entity type.
    /// </summary>
    private static void EnsureOwnCommandType(AkkaConfigurationBuilder builder, EntityTypeName typeName, Type commandType)
    {
        if (ClaimRegionKey(builder, typeName, commandType) is not { } owner) return;

        throw new InvalidOperationException(owner == typeName
            ? $"Durable entity [{typeName.Value}] is registered twice. Call WithDurableEntity once for each entity type."
            : $"Durable entities [{owner.Value}] and [{typeName.Value}] both use the command type {commandType.FullName}. " +
              "Each entity type needs its own command type, because entity refs and Send find the entity type by it.");
    }

    /// <summary>
    /// Every workflow type an entity <c>Uses</c> must be hosted with <c>WithDurableWorkflow</c> on the same builder.
    /// Otherwise the first start or notify would fail at run time, far from the cause.
    /// </summary>
    private static void EnsureWorkflowsHosted(
        AkkaConfigurationBuilder builder, EntityTypeName entity, IEnumerable<WorkflowUse> uses)
    {
        var owners = RegionKeyOwners.GetOrCreateValue(builder);
        foreach (var use in uses)
        {
            if (!owners.TryGetValue(use.RegionKey, out var hosted))
                throw new InvalidOperationException(
                    $"Durable entity [{entity.Value}] uses workflow type [{use.Workflow.Value}], but that workflow type " +
                    $"is not hosted in this actor system. Host it with WithDurableWorkflow(...), passing the definition " +
                    $"of [{use.Workflow.Value}], or remove the Uses declaration.");

            if (hosted.Value != use.Workflow.Value)
                throw new InvalidOperationException(
                    $"Durable entity [{entity.Value}] uses workflow type [{use.Workflow.Value}], but the workflow type " +
                    $"hosted with the same input, state, fact and outcome types is [{hosted.Value}]. Pass the same " +
                    "definition to Uses and to WithDurableWorkflow(...).");
        }
    }

    private static readonly ConditionalWeakTable<AkkaConfigurationBuilder, object> FrameworkSerializerAdded = [];

    /// <summary>Registers Akka.Durable's own serializer once per builder, however many entity types it hosts.</summary>
    private static void AddFrameworkSerializerOnce(AkkaConfigurationBuilder builder)
    {
        if (FrameworkSerializerAdded.TryAdd(builder, FrameworkSerializerAdded))
            builder.WithCustomSerializer(DurableEntitySerializer.CreateSerializerDetails);
    }

    internal static void EnsureSerializable(ActorSystem system, string what, EntityTypeName typeName, params Type[] types)
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
            $"{what} [{typeName.Value}] cannot start: no serializer is registered for " +
            $"{string.Join(", ", unbound)}, so they would fall back to " +
            $"{fallback?.GetType().Name ?? "no serializer at all"}. Mark them [AkkaSerializable(Manifest = \"...\")] " +
            "(for a C# 15 union, the union itself as well as each case) under an [AkkaSerializer<TProtocol>] " +
            "serializer, and register it with WithCustomSerializer(YourSerializer.CreateSerializerDetails).");
    }

    /// <summary>
    /// Checks the effect types of an entity: the routes whose effect type implements <see cref="IEffect{TCommand}"/>.
    /// Each answers with the entity's command or not at all, and each has a serializer of its own, because it is
    /// journaled with the decision that issues it.
    /// </summary>
    internal static void EnsureEffectRoutes<TCommand>(ActorSystem system, EntityTypeName typeName, IServiceProvider services)
    {
        var routes = services.GetService<EffectRoutes>() ?? EffectRoutes.Empty;
        var serialization = system.Serialization;
        var fallback = TryFindSerializer(serialization, typeof(object));

        foreach (var route in routes.Of<TCommand>())
        {
            if (route.OutcomeType is { } outcome && !typeof(TCommand).IsAssignableFrom(outcome))
                throw new InvalidOperationException(
                    $"Durable entity [{typeName.Value}] cannot start: the effect {route.EffectType.Name} is an " +
                    $"IEffect<{typeof(TCommand).Name}>, but its handler {route.HandlerType.Name} answers with a " +
                    $"{outcome.Name}. The answer goes to the entity's Decide, so it must be a {typeof(TCommand).Name}. " +
                    $"Register it with AddEffectHandler<{route.EffectType.Name}, {typeof(TCommand).Name}, " +
                    $"{route.HandlerType.Name}>(), or with AddEffectHandler<{route.EffectType.Name}, " +
                    $"{route.HandlerType.Name}>() for a handler with no answer.");

            if (TryFindSerializer(serialization, route.EffectType) is { } found
                && (fallback is null || found.Identifier != fallback.Identifier))
                continue;

            var protocol = ProtocolOf(TryFindSerializer(serialization, typeof(TCommand)));
            throw new InvalidOperationException(
                $"Durable entity [{typeName.Value}] cannot start: the effect {route.EffectType.Name}, handled by " +
                $"{route.HandlerType.Name}, is journaled with the decision that issues it, and no serializer is " +
                $"registered for it, so it would fall back to {fallback?.GetType().Name ?? "no serializer at all"}. " +
                $"Mark it [AkkaSerializable(Manifest = \"...\")] under the [AkkaSerializer<TProtocol>] serializer that " +
                $"holds {typeof(TCommand).Name}" +
                (protocol is null ? "" : $" (for example, make it implement {protocol.Name})") +
                ", and register that serializer with WithCustomSerializer.");
        }
    }

    /// <summary>The protocol type of a source-generated serializer: the type argument of its <c>[AkkaSerializer&lt;T&gt;]</c>.</summary>
    private static Type? ProtocolOf(Serializer? serializer)
    {
        if (serializer is null) return null;
        foreach (var attribute in serializer.GetType().GetCustomAttributes(inherit: false))
        {
            var type = attribute.GetType();
            if (type.IsGenericType && type.Name.StartsWith("AkkaSerializerAttribute", StringComparison.Ordinal))
                return type.GetGenericArguments()[0];
        }

        return null;
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
