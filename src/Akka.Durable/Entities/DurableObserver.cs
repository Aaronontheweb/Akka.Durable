using Akka.Actor;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

/// <summary>
/// Watches what durable entities do. It only reads: it can not change an entity, and nothing it does is stored.
/// </summary>
/// <remarks>
/// <para>Register it with <see cref="DurableObserverExtensions.AddDurableObserver{TObserver}"/>. Every observer is
/// called for every entity and workflow type in the process, in the order they were registered.</para>
/// <para>A call is made on the entity's own thread, after the journal write it reports has succeeded, and never while
/// an entity recovers. An observer must be quick, must not block, and must not call back into the entity. One observer
/// instance serves many entities at the same time, so it must be safe to call from many threads.</para>
/// <para>An observer that throws is logged at Warning level and skipped for that call. It never affects the entity.</para>
/// </remarks>
public interface IDurableObserver
{
    /// <summary>An event of an entity was written to the journal.</summary>
    /// <param name="saved">What was written, and why.</param>
    void EventSaved(in EventSaved saved) { }

    /// <summary>A recorded effect (<c>Execute</c>) moved to a new stage.</summary>
    /// <param name="effect">The effect, and the stage it reached.</param>
    void EffectMoved(in EffectObserved effect) { }

    /// <summary>A timer of an entity was set, cancelled or fired.</summary>
    /// <param name="timer">The timer, and what happened to it.</param>
    void TimerMoved(in TimerObserved timer) { }
}

/// <summary>
/// What every observation says: which entity it is about and when it was made. Subscribe to this type on the event
/// stream to get all three kinds.
/// </summary>
public interface IDurableObservation
{
    /// <summary>The type of the entity.</summary>
    EntityTypeName EntityType { get; }

    /// <summary>The id of the entity.</summary>
    EntityId EntityId { get; }

    /// <summary>The time, from the entity's clock: the one that gives <c>DecisionContext.UtcNow</c>.</summary>
    DateTimeOffset At { get; }
}

/// <summary>What started the decision that wrote an event.</summary>
public enum CauseKind
{
    /// <summary>A command that was sent to the entity.</summary>
    Command,

    /// <summary>A timer that fired.</summary>
    Timer,

    /// <summary>The answer of a recorded effect, or its give-up.</summary>
    EffectAnswer,

    /// <summary>The library's own work after the entity recovered. No command decided the event, so its <see cref="EventSaved.Command"/> is null. Only workflows have it today.</summary>
    AfterRecovery,
}

/// <summary>What started a decision.</summary>
/// <param name="Kind">The kind of cause.</param>
/// <param name="Id">The timer's key for a timer, the effect's id for an effect's answer, otherwise null.</param>
public readonly record struct EventCause(CauseKind Kind, string? Id);

/// <summary>An event of an entity was written to the journal.</summary>
/// <param name="EntityType">The type of the entity.</param>
/// <param name="EntityId">The id of the entity.</param>
/// <param name="SequenceNr">The journal sequence number of the event.</param>
/// <param name="Event">
/// The event, as the entity's <c>Decide</c> persisted it. For a union event type, the case that was persisted. A
/// workflow's events are the library's own types, and their contents may change.
/// </param>
/// <param name="Command">
/// The command whose decision wrote the event. A union command type is shown as its case. It is null only when
/// <paramref name="Cause"/> is <see cref="CauseKind.AfterRecovery"/>: the library decided that event without a command.
/// </param>
/// <param name="Cause">What started the decision.</param>
/// <param name="At">The time.</param>
/// <param name="Headers">
/// The headers that travelled beside the command that started the decision, or the headers of the effect whose answer
/// it is, or, for a timer, the headers that a propagator added for it. Empty when there were none. Never null.
/// </param>
public readonly record struct EventSaved(
    EntityTypeName EntityType,
    EntityId EntityId,
    long SequenceNr,
    object Event,
    object? Command,
    EventCause Cause,
    DateTimeOffset At,
    IReadOnlyDictionary<string, string> Headers) : IDurableObservation;

/// <summary>The stage of a recorded effect.</summary>
public enum EffectStage
{
    /// <summary>The effect was written to the journal with the decision that issued it.</summary>
    Issued,

    /// <summary>An attempt of the effect started. Each attempt has its own call.</summary>
    AttemptStarted,

    /// <summary>The effect was acknowledged: it is done.</summary>
    Acknowledged,

    /// <summary>The effect gave up: it will not be attempted again.</summary>
    GaveUp,
}

/// <summary>A recorded effect (<c>Execute</c>) moved to a new stage. Effects that are not recorded are not reported.</summary>
/// <param name="EntityType">The type of the entity.</param>
/// <param name="EntityId">The id of the entity.</param>
/// <param name="Id">The effect's public id, the same text a handler gets in <see cref="EffectId"/>.</param>
/// <param name="Effect">The effect, as the entity issued it.</param>
/// <param name="Stage">The stage it reached.</param>
/// <param name="Attempt">The attempt: 1 when issued, the number of the attempt that started, or that settled the effect.</param>
/// <param name="At">The time.</param>
/// <param name="Headers">
/// The headers of the decision that issued the effect, which every attempt runs with. Empty when there were none.
/// Never null.
/// </param>
public readonly record struct EffectObserved(
    EntityTypeName EntityType,
    EntityId EntityId,
    string Id,
    object Effect,
    EffectStage Stage,
    int Attempt,
    DateTimeOffset At,
    IReadOnlyDictionary<string, string> Headers) : IDurableObservation;

/// <summary>What happened to a timer.</summary>
public enum TimerStage
{
    /// <summary>The timer was stored.</summary>
    Set,

    /// <summary>The timer was cancelled.</summary>
    Cancelled,

    /// <summary>The timer fired and was delivered to the entity.</summary>
    Fired,
}

/// <summary>A timer of an entity was set, cancelled or fired.</summary>
/// <param name="EntityType">The type of the entity.</param>
/// <param name="EntityId">The id of the entity.</param>
/// <param name="Key">The timer's key.</param>
/// <param name="Stage">What happened.</param>
/// <param name="Due">When the timer is due, for <see cref="TimerStage.Set"/>. Otherwise null.</param>
/// <param name="At">The time.</param>
/// <param name="Headers">
/// For <see cref="TimerStage.Set"/> and <see cref="TimerStage.Cancelled"/>: the headers of the decision that set or
/// cancelled the timer. For <see cref="TimerStage.Fired"/>: the headers that a propagator added with
/// <see cref="IHeaderPropagator.InjectForTimer"/>, which the decision the timer starts carries. Not those of the
/// command that set the timer. Empty when there are none. Never null.
/// </param>
public readonly record struct TimerObserved(
    EntityTypeName EntityType,
    EntityId EntityId,
    string Key,
    TimerStage Stage,
    DateTimeOffset? Due,
    DateTimeOffset At,
    IReadOnlyDictionary<string, string> Headers) : IDurableObservation;

/// <summary>Registers observers of durable entities.</summary>
public static class DurableObserverExtensions
{
    /// <summary>
    /// Registers <typeparamref name="TObserver"/> as an observer. It is a singleton, and entities and workflows hosted
    /// with this service collection call it. Call it again to add more observers: they are called in the order added.
    /// </summary>
    /// <param name="services">The service collection.</param>
    public static IServiceCollection AddDurableObserver<TObserver>(this IServiceCollection services)
        where TObserver : class, IDurableObserver
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton<IDurableObserver, TObserver>();
        return services;
    }

    /// <summary>
    /// Publishes every observation to the actor system's event stream. Subscribe once to
    /// <see cref="IDurableObservation"/> to receive all three kinds.
    /// </summary>
    /// <remarks>The event stream is local to one node and in memory. Nothing is published unless this is called.</remarks>
    /// <param name="services">The service collection.</param>
    public static IServiceCollection AddDurableEventStreamPublisher(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton<IDurableObserver>(provider => new EventStreamPublisher(provider));
        return services;
    }
}

/// <summary>The built-in observer that publishes each observation to the event stream.</summary>
internal sealed class EventStreamPublisher(IServiceProvider services) : IDurableObserver
{
    // Found at the first observation, when the actor system is certainly running.
    private ActorSystem? _system;

    private ActorSystem System => _system ??= services.GetRequiredService<ActorSystem>();

    public void EventSaved(in EventSaved saved) => System.EventStream.Publish(saved);

    public void EffectMoved(in EffectObserved effect) => System.EventStream.Publish(effect);

    public void TimerMoved(in TimerObserved timer) => System.EventStream.Publish(timer);
}
