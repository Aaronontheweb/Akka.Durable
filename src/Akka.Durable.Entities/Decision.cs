using System.Collections.Immutable;
using Akka.Actor;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable.Entities;

// The command-processing outcomes are mutually exclusive.
// Only Handled<TEvent> can contain events and effects.
public sealed record Ignored;
public sealed record Unhandled;
public sealed record Stashed;

// Built-in, in-process effects. None of these are journaled.
// In particular, RunEffect is a deferred delegate, NOT a serialized operation.
public sealed record ReplyEffect(object Response);
public sealed record TellEffect(ICanTell Recipient, object Message);
public sealed record ScheduleEffect(string Key, TimeSpan Delay, object Message);

public sealed record ScheduleRecurringEffect(string Key, TimeSpan InitialDelay, TimeSpan Interval, object Message);
public sealed record CancelTimerEffect(string Key);
public sealed record UnstashAllEffect;
public sealed record StopEffect;

// Executed only after the decision's events have been persisted successfully.
// The interpreter supplies an IServiceProvider scoped to this execution attempt.
// Returning null means there is no completion message to send to the entity.
// Returning a message means the interpreter sends it back through the mailbox.
// If execution fails, the interpreter maps the error through OnFailure when set;
// otherwise it observes/logs the failure. Cancellation during actor shutdown is
// not treated as a domain failure.
public sealed record RunEffect(
    Func<IServiceProvider, CancellationToken, Task<object?>> ExecuteAsync,
    Func<Exception, object>? OnFailure = null);

public union EntityEffect(
    ReplyEffect,
    TellEffect,
    ScheduleEffect,
    ScheduleRecurringEffect,
    CancelTimerEffect,
    UnstashAllEffect,
    StopEffect,
    RunEffect);

public sealed record Handled<TEvent>(
    ImmutableArray<TEvent> Events,
    ImmutableArray<EntityEffect> Effects)
{
    private Handled<TEvent> Add(EntityEffect effect) =>
        this with { Effects = Effects.Add(effect) };

    public Handled<TEvent> Reply<TReply>(TReply response)
        where TReply : notnull =>
        Add(new ReplyEffect(response));

    public Handled<TEvent> Tell<TMessage>(
        ICanTell recipient,
        TMessage message)
        where TMessage : notnull =>
        Add(new TellEffect(recipient, message));

    // Actor-lifetime timers; these are not durable workflow deadlines.
    public Handled<TEvent> Schedule(
        string key,
        TimeSpan delay,
        object message) =>
        Add(new ScheduleEffect(key, delay, message));

    public Handled<TEvent> CancelTimer(string key) =>
        Add(new CancelTimerEffect(key));

    public Handled<TEvent> UnstashAll() =>
        Add(new UnstashAllEffect());

    // Stops the current actor incarnation after preceding effects. It does not
    // delete the entity's journal, snapshots, or logical identity.
    public Handled<TEvent> Stop() =>
        Add(new StopEffect());

    // Best-effort async effect, with no response expected.
    // The service is resolved by the framework, not by the decider.
    public Handled<TEvent> Run<TService>(
        Func<TService, CancellationToken, Task> action,
        Func<Exception, object>? onFailure = null)
        where TService : notnull =>
        Add(new RunEffect(
            async (services, ct) =>
            {
                var service = services.GetRequiredService<TService>();
                await action(service, ct).ConfigureAwait(false);
                return null;
            },
            onFailure));

    // Best-effort async effect whose result becomes a normal entity message.
    // The framework interprets the effect and sends that message through the
    // entity mailbox; the decider processes it like any other command.
    public Handled<TEvent> Run<TService, TResult>(
        Func<TService, CancellationToken, Task<TResult>> action,
        Func<TResult, object> onSuccess,
        Func<Exception, object> onFailure)
        where TService : notnull =>
        Add(new RunEffect(
            async (services, ct) =>
            {
                var service = services.GetRequiredService<TService>();
                var result = await action(service, ct).ConfigureAwait(false);
                return onSuccess(result);
            },
            onFailure));
}

public union Decision<TEvent>(Handled<TEvent>, Ignored, Unhandled, Stashed);

public static class Decisions
{
    public static Handled<TEvent> Persist<TEvent>(params TEvent[] events) =>
        new([.. events], []);

    public static Handled<TEvent> None<TEvent>() => new([], []);

    public static Ignored Ignore() => new();
    public static Unhandled Unhandled() => new();
    public static Stashed Stash() => new();
}