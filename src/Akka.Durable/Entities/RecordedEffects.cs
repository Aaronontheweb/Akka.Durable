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
/// <param name="Effect">The author's effect, or the library's own for a call to a workflow.</param>
/// <param name="IssuedAt">When the decision issued it.</param>
/// <param name="Policy">The retry policy of the effect's handler when it was issued. Null: one attempt.</param>
/// <param name="NextAt">
/// When the next attempt is due if attempt 1 gets no answer: its start, plus the time limit, plus the policy's wait.
/// Without a policy: when the attempt counts as interrupted.
/// </param>
[AkkaSerializable(Manifest = "durable-effect-issued-v1")]
internal sealed record EffectIssued(
    [property: AkkaField(0)] object Effect,
    [property: AkkaField(1)] DateTimeOffset IssuedAt,
    [property: AkkaField(2)] StoredPolicy? Policy = null,
    [property: AkkaField(3)] DateTimeOffset NextAt = default) : IDurableEntityProtocol, IDurableJournalRecord;

/// <summary>Attempt <paramref name="Attempt"/> of the effect <paramref name="Id"/> starts. Journaled before it runs.</summary>
/// <param name="Id">The sequence number of the effect's <see cref="EffectIssued"/>.</param>
/// <param name="Attempt">The number of the attempt that starts: 2 or more.</param>
/// <param name="NextAt">When the attempt after it is due if this one gets no answer.</param>
[AkkaSerializable(Manifest = "durable-effect-retried-v1")]
internal sealed record EffectRetried(
    [property: AkkaField(0)] long Id,
    [property: AkkaField(1)] int Attempt,
    [property: AkkaField(2)] DateTimeOffset NextAt) : IDurableEntityProtocol, IDurableJournalRecord;

/// <summary>
/// The effect <paramref name="Id"/> was answered. Written with the decision on the answer, or alone for a handler with
/// no answer.
/// </summary>
/// <param name="Id">The sequence number of the effect's <see cref="EffectIssued"/>.</param>
[AkkaSerializable(Manifest = "durable-effect-acknowledged-v1")]
internal sealed record EffectAcknowledged([property: AkkaField(0)] long Id) : IDurableEntityProtocol, IDurableJournalRecord;

/// <summary>The effect <paramref name="Id"/> got no answer. Written with the decision on its give-up command.</summary>
/// <param name="Id">The sequence number of the effect's <see cref="EffectIssued"/>.</param>
/// <param name="Attempts">How many attempts were made.</param>
/// <param name="Reason">Why it gave up: a <see cref="GaveUpReason"/>, stored as its number.</param>
[AkkaSerializable(Manifest = "durable-effect-gave-up-v1")]
internal sealed record EffectGaveUp(
    [property: AkkaField(0)] long Id,
    [property: AkkaField(1)] int Attempts,
    [property: AkkaField(2)] int Reason) : IDurableEntityProtocol, IDurableJournalRecord
{
    public EffectGaveUp(long id, int attempts, GaveUpReason reason) : this(id, attempts, (int)reason)
    {
    }
}

/// <summary>A <see cref="RetryPolicy"/> as it is journaled.</summary>
/// <param name="First">The wait before attempt 2.</param>
/// <param name="Max">The longest wait.</param>
/// <param name="Attempts">How many attempts in all. 0: until one is acknowledged.</param>
[AkkaSerializable(Manifest = "durable-retry-policy-v1")]
internal sealed record StoredPolicy(
    [property: AkkaField(0)] TimeSpan First,
    [property: AkkaField(1)] TimeSpan Max,
    [property: AkkaField(2)] int Attempts) : IDurableEntityProtocol
{
    public static StoredPolicy? Of(RetryPolicy? policy) =>
        policy is null ? null : new StoredPolicy(policy.First, policy.Max, policy.Attempts ?? 0);

    public RetryPolicy ToPolicy() => new(First, Max, Attempts == 0 ? null : Attempts);
}

/// <summary>An effect that was issued and has no acknowledgement or give-up yet.</summary>
/// <param name="Sequence">The sequence number of its <see cref="EffectIssued"/>: the effect's id.</param>
/// <param name="Effect">The author's effect, or the library's own.</param>
/// <param name="IssuedAt">When the decision issued it.</param>
/// <param name="Policy">The retry policy it was issued with. Null: one attempt.</param>
/// <param name="Attempt">The number of the last attempt that started.</param>
/// <param name="NextAt">When the next attempt is due if the last one gets no answer.</param>
[AkkaSerializable(Manifest = "durable-pending-effect-v1")]
internal sealed record PendingEffect(
    [property: AkkaField(0)] long Sequence,
    [property: AkkaField(1)] object Effect,
    [property: AkkaField(2)] DateTimeOffset IssuedAt,
    [property: AkkaField(3)] StoredPolicy? Policy = null,
    [property: AkkaField(4)] int Attempt = 1,
    [property: AkkaField(5)] DateTimeOffset NextAt = default) : IDurableEntityProtocol;

/// <summary>
/// The payload of the entity's one reminder for its pending effects. It wakes the entity after a crash, a passivation
/// or a shard move, so its pending effects are attempted again or given up.
/// </summary>
/// <param name="At">The time the reminder was stored for.</param>
[AkkaSerializable(Manifest = "durable-effects-wake-v1", AllowEmpty = true)]
internal sealed record EffectsWake([property: AkkaField(0)] DateTimeOffset? At = null) : IDurableEntityProtocol
{
    /// <summary>The reminder's key. Storing it again replaces it, so an entity has at most one.</summary>
    public const string Key = "durable-effects-wake";
}

// The library's own effects: a call to a workflow from a decision, as it is journaled.

/// <summary>Start the workflow <paramref name="Id"/>. Delivered until the workflow answers.</summary>
[AkkaSerializable(Manifest = "durable-start-workflow-v1")]
internal sealed record RecordedStart(
    [property: AkkaField(0)] string Workflow,
    [property: AkkaField(1)] string Id,
    [property: AkkaField(2)] object Input) : IDurableEntityProtocol;

/// <summary>Send a fact to the workflow <paramref name="Id"/>. Attempted once.</summary>
[AkkaSerializable(Manifest = "durable-notify-workflow-v1")]
internal sealed record RecordedNotify(
    [property: AkkaField(0)] string Workflow,
    [property: AkkaField(1)] string Id,
    [property: AkkaField(2)] object Fact) : IDurableEntityProtocol;

/// <summary>Ask the workflow <paramref name="Id"/> to stop. Attempted once.</summary>
[AkkaSerializable(Manifest = "durable-cancel-workflow-v1")]
internal sealed record RecordedCancel(
    [property: AkkaField(0)] string Workflow,
    [property: AkkaField(1)] string Id,
    [property: AkkaField(2)] string Reason,
    [property: AkkaField(3)] string By) : IDurableEntityProtocol;

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
    [property: AkkaField(1)] ImmutableArray<object> Pending) : IDurableEntityProtocol, IDurableJournalRecord;

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
/// <param name="Policy">The retry policy given at registration. Null: one attempt.</param>
internal sealed record EffectRoute(
    Type EffectType,
    Type? OutcomeType,
    Type HandlerType,
    Func<IServiceProvider, object, EffectDelivery, CancellationToken, Task<object?>> Invoke,
    RetryPolicy? Policy = null)
{
    public static EffectRoute WithAnswer<TEffect, TOutcome, THandler>(RetryPolicy? policy)
        where TEffect : notnull
        where THandler : class, IEffectHandler<TEffect, TOutcome> =>
        new(typeof(TEffect), typeof(TOutcome), typeof(THandler), async (services, effect, delivery, ct) =>
            await services.GetRequiredService<IEffectHandler<TEffect, TOutcome>>()
                .HandleAsync((TEffect)effect, delivery, ct).ConfigureAwait(false), policy);

    public static EffectRoute WithoutAnswer<TEffect, THandler>(RetryPolicy? policy)
        where TEffect : notnull
        where THandler : class, IEffectHandler<TEffect> =>
        new(typeof(TEffect), null, typeof(THandler), async (services, effect, delivery, ct) =>
        {
            await services.GetRequiredService<IEffectHandler<TEffect>>()
                .HandleAsync((TEffect)effect, delivery, ct).ConfigureAwait(false);
            return null;
        }, policy);

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
    /// The routes whose handler answers with <paramref name="commandType"/>: the effects of the entity type that has
    /// this command type. The registration names the command type, so the owner is certain.
    /// </summary>
    public IEnumerable<EffectRoute> AnsweringWith(Type commandType) =>
        _byType.Values.Where(route => route.OutcomeType is { } outcome && commandType.IsAssignableFrom(outcome));

    /// <summary>
    /// The routes whose handler has no answer. Only an entity can issue such an effect: a workflow's work always
    /// answers with a fact. The registration does not name the entity type, so every entity type may be the owner.
    /// </summary>
    public IEnumerable<EffectRoute> WithoutAnswer() => _byType.Values.Where(route => route.OutcomeType is null);

    /// <summary>
    /// The routes an entity whose command type is <paramref name="commandType"/> can issue: those that answer with
    /// its command type, then those with no answer. The effects of these routes are journaled.
    /// </summary>
    public IEnumerable<EffectRoute> OfEntity(Type commandType) => AnsweringWith(commandType).Concat(WithoutAnswer());

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

/// <summary>
/// An entity issued an effect whose handler answers with a type that is not the entity's command type. The answer
/// could not reach <c>Decide</c>, so the effect is refused before the write, and the entity restarts.
/// </summary>
internal sealed class EffectAnswerMismatchException(EntityTypeName entity, EffectRoute route, Type commandType)
    : InvalidOperationException(
        $"Entity type [{entity.Value}] issued the effect {route.EffectType.Name}, and its handler " +
        $"{route.HandlerType.Name} answers with a {route.OutcomeType?.Name}. The answer goes to the entity's Decide, " +
        $"so it must be a {commandType.Name}. Register the handler with " +
        $"services.AddEffectHandler<{route.EffectType.Name}, {commandType.Name}, YourHandler>(), or with " +
        $"services.AddEffectHandler<{route.EffectType.Name}, YourHandler>() for a handler with no answer. The effect " +
        "was not journaled and not run.");

/// <summary>
/// An entity type issues an effect, and its definition has no <c>GaveUp</c>: <c>Decide</c> would get nothing
/// when the effect gets no answer. The host refuses to start when it can see this. Otherwise the effect is refused
/// before the write, and the entity restarts.
/// </summary>
internal sealed class MissingGiveUpException(string message) : InvalidOperationException(message)
{
    /// <summary>The line to add, and where.</summary>
    internal static string Fix(EntityTypeName entity, Type commandType) =>
        $"Add the argument \"GaveUp: gaveUp => new YourCommand(gaveUp.Reason)\" to the EntityDefinition of " +
        $"[{entity.Value}], where YourCommand is one of its commands ({commandType.Name}).";

    private const string Why =
        "its definition does not say which command Decide gets when an effect gets no answer";

    /// <summary>The host sees an effect handler that answers with the entity's command type.</summary>
    internal static MissingGiveUpException AtStart(EntityTypeName entity, EffectRoute route, Type commandType) => new(
        $"Durable entity [{entity.Value}] cannot start: it issues the effect {route.EffectType.Name} (handled by " +
        $"{route.HandlerType.Name}), and {Why}. {Fix(entity, commandType)}");

    /// <summary>An entity issued an effect with <c>Execute</c>.</summary>
    internal static MissingGiveUpException AtExecute(EntityTypeName entity, Type effectType, Type commandType) => new(
        $"Entity type [{entity.Value}] issued the effect {effectType.Name}, and {Why}. {Fix(entity, commandType)} " +
        "The effect was not journaled and not run.");
}
