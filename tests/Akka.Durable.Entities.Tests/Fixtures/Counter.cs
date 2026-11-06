namespace Akka.Durable.Entities.Tests.Fixtures;

using CounterContext = DecisionContext<CounterCommand, CounterEvent>;

// A deliberately small entity whose commands each exercise one runtime behavior.

public sealed record CounterState(int Count, bool Locked);

// Commands
public sealed record Increment(int By);
public sealed record GetCount;
public sealed record Noop;
public sealed record Unsupported;
public sealed record LockCounter;
public sealed record UnlockCounter;
public sealed record IncrementAndRecord(int By);
public sealed record IncrementThenFail(int By);
public sealed record FailWithFallback;
public sealed record RecordFailure(string Reason);
public sealed record WaitForGate;
public sealed record WaitForGateWithTimeout(TimeSpan Timeout);
public sealed record RemindLater(TimeSpan Delay);
public sealed record Shutdown;

public readonly union CounterCommand(
    Increment, GetCount, Noop, Unsupported, LockCounter, UnlockCounter,
    IncrementAndRecord, IncrementThenFail, FailWithFallback, RecordFailure,
    WaitForGate, WaitForGateWithTimeout, RemindLater, Shutdown);

// Events
public sealed record Incremented(int By);
public sealed record CounterLocked;
public sealed record CounterUnlocked;

public readonly union CounterEvent(Incremented, CounterLocked, CounterUnlocked);

// Replies (application-defined; no entity-wide response union required)
public sealed record Accepted;
public sealed record CountIs(int Count);
public sealed record GateOpened;
public sealed record FailureRecorded(string Reason);

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
