namespace Akka.Durable;

/// <summary>An immutable request builder bound to one command's reply contract.</summary>
/// <remarks>
/// Reply and Run finish the current branch. Run continuations retain the same reply type.
/// Continuation overloads accepting a new request context expose state after preceding commits
/// and the time the operation completed.
/// </remarks>
public sealed class RequestContext<TState, TCommand, TEvent, TReply> where TReply : notnull
{
    private readonly DecisionContext<TState, TCommand, TEvent> _context;
    private readonly Handled<TCommand, TEvent> _plan;

    internal RequestContext(DecisionContext<TState, TCommand, TEvent> context, Handled<TCommand, TEvent> plan)
    {
        _context = context;
        _plan = plan;
    }

    public TState State => _context.State;
    public EntityId EntityId => _context.EntityId;
    public EntityTypeName EntityType => _context.EntityType;
    public DateTimeOffset UtcNow => _context.UtcNow;

    public RequestContext<TState, TCommand, TEvent, TReply> Persist(params TEvent[] events) =>
        new(_context, _plan with { Events = _plan.Events.AddRange(events) });

    public RequestContext<TState, TCommand, TEvent, TReply> Tell<TMessage>(Akka.Actor.ICanTell recipient, TMessage message)
        where TMessage : notnull => new(_context, _plan.Tell(recipient, message));

    public RequestContext<TState, TCommand, TEvent, TReply> Tell<TOther>(IEntityRef<TOther> recipient, TOther message) =>
        new(_context, _plan.Tell(recipient, message));

    public RequestContext<TState, TCommand, TEvent, TReply> Schedule(string key, TimeSpan delay, TCommand message) =>
        new(_context, _plan.Schedule(key, delay, message));

    public RequestContext<TState, TCommand, TEvent, TReply> Schedule(
        string key, TimeSpan initialDelay, TimeSpan interval, TCommand message) =>
        new(_context, _plan.Schedule(key, initialDelay, interval, message));

    public RequestContext<TState, TCommand, TEvent, TReply> CancelTimer(string key) =>
        new(_context, _plan.CancelTimer(key));

    public RequestContext<TState, TCommand, TEvent, TReply> UnstashAll() =>
        new(_context, _plan.UnstashAll());

    public RequestDecision<TCommand, TEvent, TReply> Reply(TReply response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return new(_plan.Add(new ReplyEffect(response)));
    }

    /// <summary>Replies, then asks the host to passivate the entity.</summary>
    public RequestDecision<TCommand, TEvent, TReply> ReplyAndStop(TReply response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return new(_plan.Add(new ReplyEffect(response)).Add(new StopEffect()));
    }

    public RequestDecision<TCommand, TEvent, TReply> Run<TService, TResult>(
        Func<TService, CancellationToken, Task<TResult>> action,
        Func<TResult, RequestDecision<TCommand, TEvent, TReply>> onSuccess,
        Func<Exception, RequestDecision<TCommand, TEvent, TReply>> onFailure,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(onSuccess);
        ArgumentNullException.ThrowIfNull(onFailure);
        return Run<TService, TResult>(action,
            (result, _) => onSuccess(result), (error, _) => onFailure(error), timeout);
    }

    public RequestDecision<TCommand, TEvent, TReply> Run<TService, TResult>(
        Func<TService, CancellationToken, Task<TResult>> action,
        Func<TResult, RequestContext<TState, TCommand, TEvent, TReply>, RequestDecision<TCommand, TEvent, TReply>> onSuccess,
        Func<Exception, RequestContext<TState, TCommand, TEvent, TReply>, RequestDecision<TCommand, TEvent, TReply>> onFailure,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(onSuccess);
        ArgumentNullException.ThrowIfNull(onFailure);
        return new(_plan.Add(RunEffect<TCommand, TEvent>.Create(action,
            (state, now, result) => onSuccess((TResult)result!, Continue(state, now)).Plan,
            (state, now, error) => onFailure(error, Continue(state, now)).Plan, timeout)));
    }

    public RequestDecision<TCommand, TEvent, TReply> Run<TService>(
        Func<TService, CancellationToken, Task> action,
        Func<RequestDecision<TCommand, TEvent, TReply>> onSuccess,
        Func<Exception, RequestDecision<TCommand, TEvent, TReply>> onFailure,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(action);
        ArgumentNullException.ThrowIfNull(onSuccess);
        return Run<TService, bool>(async (service, ct) =>
        {
            await action(service, ct);
            return true;
        }, _ => onSuccess(), onFailure, timeout);
    }

    public RequestDecision<TCommand, TEvent, TReply> Run<TService>(
        Func<TService, CancellationToken, Task> action,
        Func<RequestContext<TState, TCommand, TEvent, TReply>, RequestDecision<TCommand, TEvent, TReply>> onSuccess,
        Func<Exception, RequestContext<TState, TCommand, TEvent, TReply>, RequestDecision<TCommand, TEvent, TReply>> onFailure,
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

    private RequestContext<TState, TCommand, TEvent, TReply> Continue(object state, DateTimeOffset now) =>
        new(_context with { State = (TState)state, UtcNow = now }, Decisions.None<TCommand, TEvent>());
}

/// <summary>A finished branch retaining its reply contract through asynchronous continuations.</summary>
public sealed class RequestDecision<TCommand, TEvent, TReply> where TReply : notnull
{
    internal RequestDecision(Handled<TCommand, TEvent> plan) => Plan = plan;

    internal Handled<TCommand, TEvent> Plan { get; }

    /// <summary>Erases the reply type at the entity decider's boundary. Also callable from F#.</summary>
    public Decision<TCommand, TEvent> ToDecision() => Plan;

    public static implicit operator Decision<TCommand, TEvent>(RequestDecision<TCommand, TEvent, TReply> decision) =>
        decision.ToDecision();
}
