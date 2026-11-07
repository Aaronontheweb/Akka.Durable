using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures;

using CounterContext = DecisionContext<CounterCommand, CounterEvent>;

// A deliberately small entity whose commands each exercise one runtime behavior. Every type that can
// cross a serialization boundary (journal, snapshots, reminders, remoting, replies) is serializable,
// so hosted specs can run with serialize-messages on.

public interface ICounterProtocol;

[AkkaSerializable(Manifest = "counter-state-v1")]
public sealed record CounterState(
    [property: AkkaField(0)] int Count,
    [property: AkkaField(1)] bool Locked) : ICounterProtocol;

// Commands
[AkkaSerializable(Manifest = "counter-increment-v1")]
public sealed record Increment([property: AkkaField(0)] int By) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-get-count-v1", AllowEmpty = true)]
public sealed record GetCount : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-noop-v1", AllowEmpty = true)]
public sealed record Noop : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-unsupported-v1", AllowEmpty = true)]
public sealed record Unsupported : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-lock-v1", AllowEmpty = true)]
public sealed record LockCounter : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-unlock-v1", AllowEmpty = true)]
public sealed record UnlockCounter : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-increment-and-record-v1")]
public sealed record IncrementAndRecord([property: AkkaField(0)] int By) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-increment-then-fail-v1")]
public sealed record IncrementThenFail([property: AkkaField(0)] int By) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-fail-with-fallback-v1", AllowEmpty = true)]
public sealed record FailWithFallback : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-record-failure-v1")]
public sealed record RecordFailure([property: AkkaField(0)] string Reason) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-wait-for-gate-v1", AllowEmpty = true)]
public sealed record WaitForGate : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-wait-for-gate-timeout-v1")]
public sealed record WaitForGateWithTimeout([property: AkkaField(0)] TimeSpan Timeout) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-remind-later-v1")]
public sealed record RemindLater([property: AkkaField(0)] TimeSpan Delay) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-shutdown-v1", AllowEmpty = true)]
public sealed record Shutdown : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-command-v1")]
public readonly union CounterCommand(
    Increment, GetCount, Noop, Unsupported, LockCounter, UnlockCounter,
    IncrementAndRecord, IncrementThenFail, FailWithFallback, RecordFailure,
    WaitForGate, WaitForGateWithTimeout, RemindLater, Shutdown) : ICounterProtocol;

// Events
[AkkaSerializable(Manifest = "counter-incremented-v1")]
public sealed record Incremented([property: AkkaField(0)] int By) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-locked-v1", AllowEmpty = true)]
public sealed record CounterLocked : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-unlocked-v1", AllowEmpty = true)]
public sealed record CounterUnlocked : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-event-v1")]
public readonly union CounterEvent(Incremented, CounterLocked, CounterUnlocked) : ICounterProtocol;

// Replies (application-defined; no entity-wide response union required)
[AkkaSerializable(Manifest = "counter-accepted-v1", AllowEmpty = true)]
public sealed record Accepted : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-count-is-v1")]
public sealed record CountIs([property: AkkaField(0)] int Count) : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-gate-opened-v1", AllowEmpty = true)]
public sealed record GateOpened : ICounterProtocol;

[AkkaSerializable(Manifest = "counter-failure-recorded-v1")]
public sealed record FailureRecorded([property: AkkaField(0)] string Reason) : ICounterProtocol;

[AkkaSerializer<ICounterProtocol>("counter-test", 22599)]
public sealed partial class CounterSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Counter
{
    public static readonly EntityTypeName TypeName = new("counter");

    public static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =
        new(TypeName, _ => new CounterState(0, false), Decide, Evolve);

    public static Decision<CounterCommand, CounterEvent> Decide(
        CounterState state, CounterCommand command, CounterContext ctx) =>
        command switch
        {
            Increment when state.Locked => ctx.Stash(),
            Increment c => ctx.Persist(new Incremented(c.By)).Reply(new Accepted()),

            GetCount => ctx.None().Reply(new CountIs(state.Count)),
            Noop => ctx.Ignore(),
            Unsupported => ctx.Unhandled(),

            LockCounter => ctx.Persist(new CounterLocked()).Reply(new Accepted()),
            UnlockCounter => ctx.Persist(new CounterUnlocked()).Reply(new Accepted()).UnstashAll(),

            // Run before Reply: the reply is only sent once the effect (and its scope) completes.
            IncrementAndRecord c => ctx.Persist(new Incremented(c.By))
                .Run<ScopedRecorder>((recorder, ct) => recorder.RecordAsync($"run:{c.By}", ct))
                .Reply(new Accepted()),

            IncrementThenFail c => ctx.Persist(new Incremented(c.By))
                .Run<ScopedRecorder>((_, _) => throw new InvalidOperationException("boom"))
                .Reply(new Accepted()),

            FailWithFallback => ctx.None()
                .Run<ScopedRecorder>(
                    (_, _) => throw new InvalidOperationException("boom"),
                    onFailure: ex => new RecordFailure(ex.Message)),

            RecordFailure f => ctx.None().Reply(new FailureRecorded(f.Reason)),

            WaitForGate => ctx.None()
                .Run<Gate>((gate, ct) => gate.WaitAsync(ct))
                .Reply(new GateOpened()),

            WaitForGateWithTimeout w => ctx.None()
                .Run<Gate>(
                    (gate, ct) => gate.WaitAsync(ct),
                    onFailure: ex => new RecordFailure(ex.GetType().Name),
                    timeout: w.Timeout),

            RemindLater r => ctx.None().Schedule("tick", r.Delay, new Increment(1)),

            Shutdown => ctx.None().Reply(new Accepted()).Stop(),
        };

    public static CounterState Evolve(CounterState state, CounterEvent @event) =>
        @event switch
        {
            Incremented e => state with { Count = state.Count + e.By },
            CounterLocked => state with { Locked = true },
            CounterUnlocked => state with { Locked = false },
        };
}
