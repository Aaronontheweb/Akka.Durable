using Akka.Actor;

namespace Akka.Durable;

/// <summary>Builds one stage of effects that cannot reply to a request.</summary>
/// <remarks>Every Run continuation receives a fresh builder and updated state and time.</remarks>
public sealed class EffectContext<TState, TCommand, TEvent>
{
    private readonly DecisionContext<TState, TCommand, TEvent> _context;
    internal Handled<TCommand, TEvent> Plan { get; }

    internal EffectContext(DecisionContext<TState, TCommand, TEvent> context, Handled<TCommand, TEvent> plan)
    {
        _context = context;
        Plan = plan;
    }

    public TState State => _context.State;
    public EntityId EntityId => _context.EntityId;
    public EntityTypeName EntityType => _context.EntityType;
    public DateTimeOffset UtcNow => _context.UtcNow;

    public EffectContext<TState, TCommand, TEvent> Persist(params TEvent[] events) =>
        new(_context, Plan with { Events = Plan.Events.AddRange(events) });

    public EffectContext<TState, TCommand, TEvent> Tell<TMessage>(ICanTell recipient, TMessage message)
        where TMessage : notnull => new(_context, Plan.Tell(recipient, message));

    public EffectContext<TState, TCommand, TEvent> Tell<TOther>(IEntityRef<TOther> recipient, TOther message) =>
        new(_context, Plan.Tell(recipient, message));

    public EffectContext<TState, TCommand, TEvent> Schedule(string key, TimeSpan delay, TCommand message) =>
        new(_context, Plan.Schedule(key, delay, message));

    public EffectContext<TState, TCommand, TEvent> Schedule(
        string key, TimeSpan initialDelay, TimeSpan interval, TCommand message) =>
        new(_context, Plan.Schedule(key, initialDelay, interval, message));

    public EffectContext<TState, TCommand, TEvent> CancelTimer(string key) =>
        new(_context, Plan.CancelTimer(key));

    public EffectContext<TState, TCommand, TEvent> UnstashAll() =>
        new(_context, Plan.UnstashAll());

    public EffectDecision<TCommand, TEvent> Stop() => new(Plan.Stop());

    public EffectDecision<TCommand, TEvent> Run<TService, TResult>(
        Func<TService, CancellationToken, Task<TResult>> action,
        Func<TResult, EffectContext<TState, TCommand, TEvent>, EffectDecision<TCommand, TEvent>> onSuccess,
        Func<Exception, EffectContext<TState, TCommand, TEvent>, EffectDecision<TCommand, TEvent>> onFailure,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(onSuccess);
        ArgumentNullException.ThrowIfNull(onFailure);
        return new(Plan.Add(RunEffect<TCommand, TEvent>.Create(action,
            (state, now, result) => onSuccess((TResult)result!, Continue(state, now)).Plan,
            (state, now, error) => onFailure(error, Continue(state, now)).Plan, timeout)));
    }

    public EffectDecision<TCommand, TEvent> Run<TService>(
        Func<TService, CancellationToken, Task> action,
        Func<EffectContext<TState, TCommand, TEvent>, EffectDecision<TCommand, TEvent>> onSuccess,
        Func<Exception, EffectContext<TState, TCommand, TEvent>, EffectDecision<TCommand, TEvent>> onFailure,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(action);
        ArgumentNullException.ThrowIfNull(onSuccess);
        return Run<TService, bool>(async (service, ct) =>
        {
            await action(service, ct);
            return true;
        }, (_, next) => onSuccess(next), onFailure, timeout);
    }

    private EffectContext<TState, TCommand, TEvent> Continue(object state, DateTimeOffset now) =>
        new(_context with { State = (TState)state, UtcNow = now }, Decisions.None<TCommand, TEvent>());

    public EffectDecision<TCommand, TEvent> ToEffectDecision() => new(Plan);
    public Decision<TCommand, TEvent> ToDecision() => Plan;

    public static implicit operator EffectDecision<TCommand, TEvent>(EffectContext<TState, TCommand, TEvent> context) =>
        context.ToEffectDecision();

    public static implicit operator Decision<TCommand, TEvent>(EffectContext<TState, TCommand, TEvent> context) =>
        context.ToDecision();
}

/// <summary>A finished non-reply branch. No more effects can be appended after a Run or Stop.</summary>
public sealed class EffectDecision<TCommand, TEvent>
{
    internal EffectDecision(Handled<TCommand, TEvent> plan) => Plan = plan;
    internal Handled<TCommand, TEvent> Plan { get; }

    public Decision<TCommand, TEvent> ToDecision() => Plan;
    public static implicit operator Decision<TCommand, TEvent>(EffectDecision<TCommand, TEvent> decision) =>
        decision.ToDecision();
}
