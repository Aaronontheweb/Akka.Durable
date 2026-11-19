# Observing what entities do

Part of the [Akka.Durable guide](../README.md#guide).

An observer sees what entities do. It can count events, time effects or write an audit trail. It reads only. It can not change an entity, and the entity's `Decide`, `Evolve` and effect handlers contain no code for it.

Nothing is stored for an observer and nothing is sent over the wire. Observing adds no journal record, no serialized type and no field to a command.

## What is reported

An observer implements `IDurableObserver`. It has three methods, and each has an empty default. Override the ones you need.

```csharp
public interface IDurableObserver
{
    void EventSaved(in EventSaved saved) { }
    void EffectMoved(in EffectObserved effect) { }
    void TimerMoved(in TimerObserved timer) { }
}
```

**`EventSaved`.** One call for each event that `Decide` persisted, in journal order. It has the entity type and id, the journal sequence number, the event, the command whose decision wrote it, and the time. It also has the cause of the decision (`EventCause`):

| Kind | What started the decision | `Id` |
| --- | --- | --- |
| `Command` | A command sent to the entity. | `null` |
| `Timer` | A timer that fired. | The timer's key. |
| `EffectAnswer` | The answer of a recorded effect, or its give-up. | The effect's id. |
| `AfterRecovery` | The library's own work after the entity recovered. No command decided it, so `Command` is `null`. Only workflows have this today. | `null` |

The library's own records are not events. An effect's issue, retry, acknowledgement and give-up are never reported as events. A union event or command is shown as the case it holds.

**`EffectMoved`.** One call each time a recorded effect (`Execute`) reaches a stage: `Issued`, `AttemptStarted`, `Acknowledged` or `GaveUp`. It has the effect's id, the same text a handler gets in `EffectId`, the effect, the stage and the attempt number. A retry is a new `AttemptStarted` with the next number. `Run`, `Tell` and `Send` are not recorded, so they are not reported.

**`TimerMoved`.** One call when a timer is `Set` (with its due time), `Cancelled` or `Fired`. It has the timer's key. A timer is reported only for the entity's own keys. The library's own reminders, whose keys start with `durable-`, are never reported. Observing writes nothing: a timer is still only a row in the reminder store.

Every observation has the entity type, the entity id and a time, `At`. The time comes from the entity's clock, the one that gives `DecisionContext.UtcNow`. The three types share the marker interface `IDurableObservation`.

## What is guaranteed

- **After the write.** An event is reported after the journal write that holds it has succeeded. An effect is reported as issued when its write has succeeded, and an attempt is reported when it starts.
- **Never on replay.** Nothing is reported while an entity recovers. A restarted entity does not report old events again.
- **On the entity's thread.** The call is made on the entity actor, one entity at a time. The entity waits for it. An observer must be quick. It must not block, and it must not call back into the entity. Hand slow work to another actor or a queue.
- **An observer can not fail the entity.** If an observer throws, the library logs one warning with the entity type and id, and carries on. The other observers are still called.
- **Order.** Several observers are all called, in the order they were registered.
- **No cost without observers.** The library checks for observers first. With none, it builds no observation, no cause and no effect id, and it does not convert a command. That check is the only work left.

One thing is not guaranteed. If the process stops between the journal write and the call, that observation is never made. After a restart the entity does not report it, because it does not report anything on replay. An observer is a live feed. For a complete record, read the journal.

The same instance serves every entity in the process, from many threads. Write an observer that is safe to call from many threads.

## Register an observer

Observers come from dependency injection. One line adds one:

```csharp
builder.Services.AddDurableObserver<MetricsObserver>();
```

An observer is a singleton. It works for sharded entities, for local hosting and for workflows. It needs nothing else in `WithDurableEntity`.

### Publish to the event stream

One line turns on a built-in observer that publishes every observation to the actor system's event stream:

```csharp
builder.Services.AddDurableEventStreamPublisher();
```

Subscribe once to `IDurableObservation` to get all three kinds:

```csharp
Context.System.EventStream.Subscribe(Self, typeof(IDurableObservation));
```

Nothing is published unless this line is there. The event stream is local to one node and lives in memory. An actor on another node does not receive these observations, and a subscriber that is not running misses them.

## Workflows

A workflow runs on the same actor as an entity, so it is reported the same way. Its events and the commands that decided them are the library's own types, and their contents may change between releases. Do not depend on them. The entity type is the workflow type's name and the id is the workflow's id.

The work of a workflow is not a recorded effect, so no `EffectMoved` is made for it. A workflow's own wake reminder is not reported. An entity that starts, notifies or cancels a workflow records those calls as effects, and they are reported like any other.

## Example: telemetry

This observer counts events and times effects with `System.Diagnostics.Metrics`:

```csharp
public sealed class MetricsObserver : IDurableObserver
{
    private static readonly Meter Meter = new("MyApp.Durable");
    private static readonly Counter<long> Events = Meter.CreateCounter<long>("durable.events");
    private static readonly Histogram<double> EffectSeconds = Meter.CreateHistogram<double>("durable.effect.seconds");
    private readonly ConcurrentDictionary<string, DateTimeOffset> _issued = new();

    public void EventSaved(in EventSaved saved) =>
        Events.Add(1, new KeyValuePair<string, object?>("entity", saved.EntityType.Value));

    public void EffectMoved(in EffectObserved effect)
    {
        if (effect.Stage == EffectStage.Issued)
            _issued[effect.Id] = effect.At;
        else if (effect.Stage is EffectStage.Acknowledged or EffectStage.GaveUp && _issued.TryRemove(effect.Id, out var at))
            EffectSeconds.Record((effect.At - at).TotalSeconds,
                new KeyValuePair<string, object?>("entity", effect.EntityType.Value));
    }
}
```

The time of an effect runs from its issue to its settlement, retries included. An effect that is still open when the process stops is not timed.

## Example: an audit trail

This actor writes one row for each event. Business code does not change:

```csharp
public sealed class AuditActor : ReceiveActor
{
    public AuditActor(IAuditStore store)
    {
        Context.System.EventStream.Subscribe(Self, typeof(IDurableObservation));

        Receive<EventSaved>(saved => store.Add(new AuditRow(
            saved.EntityType.Value, saved.EntityId.Value, saved.SequenceNr, saved.Event.GetType().Name,
            saved.Cause.Kind.ToString(), saved.Cause.Id, saved.At)));
    }
}
```

Start it with `AddDurableEventStreamPublisher()` turned on, for example with `akka.WithActors((system, _, resolver) => system.ActorOf(resolver.Props<AuditActor>(), "audit"))`.

This gives you what happened, to which entity, and when. It also gives you whether a command, a timer or the answer of an effect started it.

Effects and timers can be recorded the same way, from `EffectObserved` and `TimerObserved`.

It does not give you who asked. Nothing travels beside a command today, so the observation can not name the caller. That needs a separate change.

The trail is as reliable as the event stream: local to one node, and it misses what happens while the subscriber is down. If the trail must be complete, read the journal.

---

Next: [Status and direction](status.md)
