using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable.Tests.Fixtures;

using CounterContext = DecisionContext<CounterState, CounterCommand, CounterEvent>;

// A deliberately small entity whose commands each exercise one runtime behavior. Every type that can
// cross a serialization boundary (journal, snapshots, reminders, remoting, replies) is serializable,
// so hosted specs can run with serialize-messages on. Effects are plain records: they never leave the process.

public interface ICounterProtocol;

[AkkaSerializable(Manifest = "counter-state-v1")]
public sealed record CounterState(
    [property: AkkaField(0)] int Count,
    [property: AkkaField(1)] bool Locked,
    [property: AkkaField(2)] IncrementViaService? Pending = null) : ICounterProtocol;

// Commands
[AkkaSerializable(Manifest = "counter-increment-v1")]
public sealed record Increment([property: AkkaField(0)] int By) : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-get-count-v1", AllowEmpty = true)]
public sealed record GetCount : ICounterProtocol, IReplies<CounterCommand, CountIs>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-noop-v1", AllowEmpty = true)]
public sealed record Noop : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-unsupported-v1", AllowEmpty = true)]
public sealed record Unsupported : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-lock-v1", AllowEmpty = true)]
public sealed record LockCounter : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-unlock-v1", AllowEmpty = true)]
public sealed record UnlockCounter : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-increment-and-record-v1")]
public sealed record IncrementAndRecord([property: AkkaField(0)] int By) : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-increment-then-fail-v1")]
public sealed record IncrementThenFail([property: AkkaField(0)] int By) : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-wait-for-gate-v1", AllowEmpty = true)]
public sealed record WaitForGate : ICounterProtocol, IReplies<CounterCommand, GateOpened>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-wait-for-gate-timeout-v1")]
public sealed record WaitForGateWithTimeout([property: AkkaField(0)] TimeSpan Timeout) : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-remind-later-v1")]
public sealed record RemindLater([property: AkkaField(0)] TimeSpan Delay) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-shutdown-v1", AllowEmpty = true)]
public sealed record Shutdown : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

/// <summary>Doubles <see cref="By"/> through a service. The request is kept in state until the outcome lands.</summary>
[AkkaSerializable(Manifest = "counter-increment-via-service-v1")]
public sealed record IncrementViaService([property: AkkaField(0)] int By, [property: AkkaField(1)] bool Wait)
    : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

/// <summary>Outcome of <see cref="Compute"/>: decided by the entity.</summary>
[AkkaSerializable(Manifest = "counter-computed-v1")]
public sealed record Computed([property: AkkaField(0)] IncrementViaService Request, [property: AkkaField(1)] int Value) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-explode-later-v1", AllowEmpty = true)]
public sealed record ExplodeLater : ICounterProtocol, IReplies<CounterCommand, Accepted>
{
    public CounterCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "counter-no-handler-v1", AllowEmpty = true)]
public sealed record NoHandler([property: AkkaField(0)] string Note = "") : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-command-v1")]
public readonly union CounterCommand(
    Increment, GetCount, Noop, Unsupported, LockCounter, UnlockCounter,
    IncrementAndRecord, IncrementThenFail, WaitForGate, WaitForGateWithTimeout, RemindLater, Shutdown,
    IncrementViaService, Computed, ExplodeLater, NoHandler) : ICounterProtocol;

// Events
[AkkaSerializable(Manifest = "counter-incremented-v1")]
public sealed record Incremented([property: AkkaField(0)] int By) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-locked-v1", AllowEmpty = true)]
public sealed record CounterLocked : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-unlocked-v1", AllowEmpty = true)]
public sealed record CounterUnlocked : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-service-increment-started-v1")]
public sealed record ServiceIncrementStarted([property: AkkaField(0)] IncrementViaService Request) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-service-increment-finished-v1")]
public sealed record ServiceIncrementFinished([property: AkkaField(0)] int Value) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-event-v1")]
public readonly union CounterEvent(
    Incremented, CounterLocked, CounterUnlocked, ServiceIncrementStarted, ServiceIncrementFinished) : ICounterProtocol;

// Replies (application-defined; no entity-wide response union required)
[AkkaSerializable(Manifest = "counter-accepted-v1", AllowEmpty = true)]
public sealed record Accepted : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-count-is-v1")]
public sealed record CountIs([property: AkkaField(0)] int Count) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-gate-opened-v1", AllowEmpty = true)]
public sealed record GateOpened : ICounterProtocol;

// Effects. Plain data handed to the registered IEffectHandler; never serialized.
public sealed record Compute(IncrementViaService Request);
public sealed record Explode;
public sealed record Unregistered(string Note);

[AkkaSerializer<ICounterProtocol>("counter-test", 22599)]
public sealed partial class CounterSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Counter
{
    public static readonly EntityTypeName TypeName = new("counter");

    public static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =
        new(TypeName, _ => new CounterState(0, false), Decide, Evolve) { Resume = Resume };

    public static Decision<CounterCommand, CounterEvent> Decide(
        CounterState state, CounterCommand command, CounterContext ctx) =>
        command switch
        {
            Increment when state.Locked => ctx.Stash(),
            Increment c => ctx.Persist(new Incremented(c.By)).Reply(c, new Accepted()),

            GetCount c => ctx.None().Reply(c, new CountIs(state.Count)),
            Noop => ctx.Ignore(),
            Unsupported => ctx.Unhandled(),

            LockCounter c => ctx.Persist(new CounterLocked()).Reply(c, new Accepted()),
            UnlockCounter c => ctx.Persist(new CounterUnlocked()).UnstashAll().Reply(c, new Accepted()),

            // Best-effort work: the reply follows once it has run, whether or not it succeeded.
            IncrementAndRecord c => ctx.Persist(new Incremented(c.By))
                .Run<ScopedRecorder>((recorder, ct) => recorder.RecordAsync($"run:{c.By}", ct))
                .Reply(c, new Accepted()),

            IncrementThenFail c => ctx.Persist(new Incremented(c.By))
                .Run<ScopedRecorder>((_, _) => throw new InvalidOperationException("boom"))
                .Reply(c, new Accepted()),

            WaitForGate c => ctx.None()
                .Run<Gate>((gate, ct) => gate.WaitAsync(ct))
                .Reply(c, new GateOpened()),

            WaitForGateWithTimeout c => ctx.None()
                .Run<Gate>((gate, ct) => gate.WaitAsync(ct), timeout: c.Timeout)
                .Reply(c, new Accepted()),

            RemindLater r => ctx.None().Schedule("tick", r.Delay, new Increment(1)),

            Shutdown c => ctx.None().Reply(c, new Accepted()).Stop(),

            // One service increment at a time: the state records the request, the handler answers with Computed.
            IncrementViaService when state.Pending is not null => ctx.Stash(),
            IncrementViaService c => ctx.Persist(new ServiceIncrementStarted(c)).Execute(new Compute(c)),
            Computed r when Equals(state.Pending, r.Request) => ctx.Persist(new ServiceIncrementFinished(r.Value))
                .Reply(r.Request, new Accepted())
                .UnstashAll(),
            Computed => ctx.Ignore(), // a late outcome from a previous incarnation

            ExplodeLater c => ctx.None().Execute(new Explode()).Reply(c, new Accepted()),
            NoHandler c => ctx.None().Execute(new Unregistered(c.Note)),
        };

    /// <summary>A recovered entity with a service increment in flight asks the service again.</summary>
    public static Handled<CounterCommand, CounterEvent> Resume(CounterState state, CounterContext ctx) =>
        state.Pending is { } pending ? ctx.None().Execute(new Compute(pending)) : ctx.None();

    public static CounterState Evolve(CounterState state, CounterEvent @event) =>
        @event switch
        {
            Incremented e => state with { Count = state.Count + e.By },
            CounterLocked => state with { Locked = true },
            CounterUnlocked => state with { Locked = false },
            ServiceIncrementStarted e => state with { Pending = e.Request },
            ServiceIncrementFinished e => state with { Count = state.Count + e.Value, Pending = null },
        };
}

/// <summary>
/// Doubles the request, optionally waiting for the test's <see cref="Gate"/> first. Goes through the scoped
/// recorder so tests can see the handler's DI scope being disposed.
/// </summary>
public sealed class ComputeHandler(Gate gate, ScopedRecorder recorder) : IEffectHandler<Compute, CounterCommand>
{
    public async Task<CounterCommand> HandleAsync(Compute effect, CancellationToken cancellationToken)
    {
        await recorder.RecordAsync($"compute:{effect.Request.By}", cancellationToken);
        if (effect.Request.Wait) await gate.WaitAsync(cancellationToken);
        return new Computed(effect.Request, effect.Request.By * 2);
    }
}

public sealed class ExplodeHandler : IEffectHandler<Explode, CounterCommand>
{
    public Task<CounterCommand> HandleAsync(Explode effect, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("boom");
}

public static class CounterHandlers
{
    public static IServiceCollection AddCounterHandlers(this IServiceCollection services) => services
        .AddScoped<IEffectHandler<Compute, CounterCommand>, ComputeHandler>()
        .AddScoped<IEffectHandler<Explode, CounterCommand>, ExplodeHandler>();
}
