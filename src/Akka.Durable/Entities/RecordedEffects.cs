using System.Collections.Immutable;
using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

// The library's own journal records for an entity's Execute. They are written in the same atomic write as the events
// of the decision they belong to, so the journal holds them between the author's events. The author's effect is an
// object field, written with the serializer bound to its type.
//
// Everything here is persisted. Change it extend-only: add fields with new indexes, add records with new manifests,
// and never reuse or repurpose one.

/// <summary>
/// An effect was issued by the decision whose events are in the same write. Its id is this record's journal sequence
/// number. It counts as attempt 1.
/// </summary>
/// <param name="Effect">The author's effect.</param>
/// <param name="IssuedAt">When the decision issued it.</param>
[AkkaSerializable(Manifest = "durable-effect-issued-v1")]
internal sealed record EffectIssued(
    [property: AkkaField(0)] object Effect,
    [property: AkkaField(1)] DateTimeOffset IssuedAt) : IDurableEntityProtocol;

/// <summary>
/// The effect <paramref name="Id"/> was answered. Written with the decision on the answer, or alone for a handler with
/// no answer.
/// </summary>
/// <param name="Id">The sequence number of the effect's <see cref="EffectIssued"/>.</param>
[AkkaSerializable(Manifest = "durable-effect-acknowledged-v1")]
internal sealed record EffectAcknowledged([property: AkkaField(0)] long Id) : IDurableEntityProtocol;

/// <summary>The effect <paramref name="Id"/> got no answer. Written with the decision on its give-up command.</summary>
/// <param name="Id">The sequence number of the effect's <see cref="EffectIssued"/>.</param>
/// <param name="Attempts">How many attempts were made.</param>
/// <param name="Reason">Why it gave up: a <see cref="GaveUpReason"/>, stored as its number.</param>
[AkkaSerializable(Manifest = "durable-effect-gave-up-v1")]
internal sealed record EffectGaveUp(
    [property: AkkaField(0)] long Id,
    [property: AkkaField(1)] int Attempts,
    [property: AkkaField(2)] int Reason) : IDurableEntityProtocol
{
    public EffectGaveUp(long id, int attempts, GaveUpReason reason) : this(id, attempts, (int)reason)
    {
    }
}

/// <summary>An effect that was issued and has no acknowledgement or give-up yet.</summary>
/// <param name="Sequence">The sequence number of its <see cref="EffectIssued"/>: the effect's id.</param>
/// <param name="Effect">The author's effect.</param>
/// <param name="IssuedAt">When the decision issued it.</param>
[AkkaSerializable(Manifest = "durable-pending-effect-v1")]
internal sealed record PendingEffect(
    [property: AkkaField(0)] long Sequence,
    [property: AkkaField(1)] object Effect,
    [property: AkkaField(2)] DateTimeOffset IssuedAt) : IDurableEntityProtocol;

/// <summary>
/// An entity's snapshot: its state and the effects pending when it was taken. With events deleted on snapshot, the
/// <see cref="EffectIssued"/> records of pending effects can be gone from the journal; they live on here.
/// </summary>
/// <remarks>A snapshot that is a bare state, written before effects were recorded, is read as nothing pending.</remarks>
/// <param name="State">The author's state.</param>
/// <param name="Pending">The pending effects, in the order they were issued.</param>
[AkkaSerializable(Manifest = "durable-entity-snapshot-v1")]
internal sealed record EntitySnapshot(
    [property: AkkaField(0)] object State,
    [property: AkkaField(1)] ImmutableArray<object> Pending) : IDurableEntityProtocol;

/// <summary>Test-only query: the effects an entity has pending. Answered with <see cref="PendingEffectsReply"/>.</summary>
[AkkaSerializable(Manifest = "durable-get-pending-effects-v1", AllowEmpty = true)]
internal sealed record GetPendingEffects : IDurableEntityProtocol
{
    public static readonly GetPendingEffects Instance = new();
}

/// <param name="Effects">The pending effects, in the order they were issued.</param>
[AkkaSerializable(Manifest = "durable-pending-effects-reply-v1")]
internal sealed record PendingEffectsReply([property: AkkaField(0)] ImmutableArray<object> Effects) : IDurableEntityProtocol;

/// <summary>
/// Where an effect type is handled: added by <c>AddEffectHandler</c>, one for each effect type. It is how a recovered
/// effect, which is only an <see cref="object"/>, finds its handler, and how the host learns the entity's effect types
/// before the first restart.
/// </summary>
/// <param name="EffectType">The effect type.</param>
/// <param name="OutcomeType">The type of the handler's answer, or null for a handler with no answer.</param>
/// <param name="HandlerType">The handler's class, for error messages.</param>
/// <param name="Invoke">
/// Runs the handler from a DI scope. Returns the answer boxed, or null for a handler with no answer.
/// </param>
internal sealed record EffectRoute(
    Type EffectType,
    Type? OutcomeType,
    Type HandlerType,
    Func<IServiceProvider, object, EffectDelivery, CancellationToken, Task<object?>> Invoke)
{
    public static EffectRoute WithAnswer<TEffect, TOutcome, THandler>()
        where TEffect : notnull
        where THandler : class, IEffectHandler<TEffect, TOutcome> =>
        new(typeof(TEffect), typeof(TOutcome), typeof(THandler), async (services, effect, delivery, ct) =>
            await services.GetRequiredService<IEffectHandler<TEffect, TOutcome>>()
                .HandleAsync((TEffect)effect, delivery, ct).ConfigureAwait(false));

    public static EffectRoute WithoutAnswer<TEffect, THandler>()
        where TEffect : notnull
        where THandler : class, IEffectHandler<TEffect> =>
        new(typeof(TEffect), null, typeof(THandler), async (services, effect, delivery, ct) =>
        {
            await services.GetRequiredService<IEffectHandler<TEffect>>()
                .HandleAsync((TEffect)effect, delivery, ct).ConfigureAwait(false);
            return null;
        });

    /// <summary>The registration call, as the author wrote it.</summary>
    public string Registration => OutcomeType is null
        ? $"AddEffectHandler<{EffectType.Name}, {HandlerType.Name}>()"
        : $"AddEffectHandler<{EffectType.Name}, {OutcomeType.Name}, {HandlerType.Name}>()";
}

/// <summary>Every <see cref="EffectRoute"/> registered in the service provider, by effect type. A singleton.</summary>
internal sealed class EffectRoutes
{
    public static readonly EffectRoutes Empty = new([]);

    private readonly ImmutableDictionary<Type, EffectRoute> _byType;

    public EffectRoutes(IEnumerable<EffectRoute> routes)
    {
        var byType = ImmutableDictionary.CreateBuilder<Type, EffectRoute>();
        foreach (var route in routes)
        {
            // AddEffectHandler refuses a second route already. This guards a route added some other way.
            if (byType.TryGetValue(route.EffectType, out var first))
                throw DuplicateRoute(first, route);
            byType[route.EffectType] = route;
        }

        _byType = byType.ToImmutable();
    }

    public bool TryGet(Type effectType, out EffectRoute route) => _byType.TryGetValue(effectType, out route!);

    /// <summary>
    /// The routes of an entity whose command type is <typeparamref name="TCommand"/>: those whose effect type
    /// implements <see cref="IEffect{TCommand}"/>.
    /// </summary>
    public IEnumerable<EffectRoute> Of<TCommand>() =>
        _byType.Values.Where(route => typeof(IEffect<TCommand>).IsAssignableFrom(route.EffectType));

    public static InvalidOperationException DuplicateRoute(EffectRoute first, EffectRoute second) => new(
        $"Two effect handlers are registered for the effect {first.EffectType.Name}: {first.Registration} and " +
        $"{second.Registration}. One effect type has one handler. Remove one of the registrations.");
}

/// <summary>
/// An entity issued an effect whose type has no handler registered with <c>AddEffectHandler</c>. It is refused before
/// the write, and the entity restarts.
/// </summary>
internal sealed class MissingEffectRouteException(Type effectType, Type commandType) : InvalidOperationException(
    $"No effect handler is registered for {effectType.Name}. Register one at startup with " +
    $"services.{WithAnswer(effectType, commandType)} for a handler that answers with a {commandType.Name}, or with " +
    $"services.{WithoutAnswer(effectType)} for a handler with no answer. The effect was not journaled and not run.")
{
    internal string EffectType { get; } = effectType.Name;
    internal string Registration { get; } = WithAnswer(effectType, commandType);

    private static string WithAnswer(Type effect, Type command) =>
        $"AddEffectHandler<{effect.Name}, {command.Name}, YourHandler>()";

    private static string WithoutAnswer(Type effect) => $"AddEffectHandler<{effect.Name}, YourHandler>()";
}

/// <summary>An <c>Execute</c> returned from <c>Resume</c>: refused, because it would record a new effect on every restart.</summary>
internal sealed class ExecuteFromResumeException(EntityTypeName entity, EntityId id, object effect) : InvalidOperationException(
    $"{entity.Value}.Resume returned Execute({effect.GetType().Name}) for entity [{id.Value}]. Resume may not issue an " +
    "Execute, because it runs after every restart and would record a new effect each time. A pending Execute comes " +
    "back by itself: as its answer, or as its give-up with GaveUpReason.Interrupted. To ask again after a restart, " +
    "issue the Execute from the Decide arm for that give-up.");
