# Headers beside a command

Part of the [Akka.Durable guide](../README.md#guide).

A header is a small string that travels beside a command. Use headers to say which request a command belongs to: a request id for an audit trail, a trace id for tracing. The library carries headers and stores them with recorded effects. It never reads them. Business code does not handle them: `Decide`, `Evolve`, `DecisionContext` and effect handlers contain no code for them.

Without headers, a decision knows what happened but not on whose behalf. An effect handler that runs later, even after a restart, and sends a command to another entity has nothing to hand on. Headers fill that gap. They go in a slot beside the command, so your command and event types stay as they are.

## What a header is

A header is a key and a value, both strings. The library does not know what they mean. Your application decides.

Keep them small and stable: ids. Do not put secrets, tokens, large values or personal data in them. The library stores headers in the journal with each recorded effect, and a journal is long-lived. Anything in a header stays there as long as the effect does.

## The propagator

Three things are yours to write: where headers come from, where they go back to, and how a timer is marked. One interface covers all three. The third member has an empty default, so most propagators leave it out:

```csharp
public interface IHeaderPropagator
{
    // Called on the caller's thread at every send of a command. Add what should travel beside it.
    void Inject(IDictionary<string, string> headers);

    // Called just before the library runs your code on behalf of a command that carried headers.
    // Put the headers where your code reads them. Dispose undoes it.
    IDisposable Restore(IReadOnlyDictionary<string, string> headers);

    // A timer of the entity fired. Add the headers that the decision it starts should carry.
    void InjectForTimer(EntityTypeName entityType, EntityId entityId, string key, IDictionary<string, string> headers) { }
}
```

Register it with one line:

```csharp
builder.Services.AddDurableHeaderPropagator<RequestIdPropagator>();
```

A propagator is a singleton, and one instance serves many threads. Register several if you need them. All of them are called, in the order they were registered, and their headers are merged. When two add the same key, the one registered later wins. An entry with a null value is ignored.

A propagator that throws never fails a send, a timer or an effect. The library logs a warning, leaves out what that propagator would have added, and carries on.

An `AsyncLocal<string?>` is the usual place to keep a header. `Inject` reads it, and `Restore` sets it. The example at the end of this page does that.

## When headers are injected, carried, stored and restored

**Injected.** Every send of a command from outside the library asks the propagators for headers: `Tell` and `Ask` on an entity ref, and `Start`, `Notify` and `Cancel` on a workflow ref. `Inject` runs on the caller's thread, so a request-scoped `AsyncLocal` is still in place.

If no propagator is registered, or none adds anything, the command is sent exactly as before. The message is the bare command. There is no wrapper, and the only added work is a check of how many propagators there are.

**Carried.** When there are headers, the command travels in one library message beside them, inside the same envelope. The entity unwraps it and decides on the command as usual. The sender is unchanged, so `Ask` replies work as before. Every node understands this message, whether or not it has a propagator.

**Passed on.** A `Send` that a decision issues passes on the headers of the command being handled. There is no ambient state inside an entity, so the library does this itself.

**Stored.** A recorded effect (`Execute`, and the recorded calls to a workflow: `Start`, `Notify` and `Cancel`) keeps the headers of the decision that issued it. They are written in the same atomic write as the decision's events and the effect. A snapshot keeps them too, with the pending effect.

An effect that a decision issues while deciding on the answer or the give-up of another effect inherits that effect's headers. A chain of effects keeps its headers, hop after hop.

**Restored.** Just before an effect handler runs, the library calls `Restore` on each propagator with the effect's stored headers, and disposes the result when the handler has finished. This holds for the first attempt, for each retry, and for an attempt after the entity restarted and recovered the effect from the journal or a snapshot.

`Restore` runs inside the task that runs the handler. An `AsyncLocal` that it sets reaches the handler and everything the handler calls. When the handler sends a command through a plain `IEntityRefs`, that send asks the propagators for headers, and `Inject` finds them again. Handler code does not change.

`Run` effects and a workflow's work are restored the same way, while the entity or the workflow lives.

**Marked at a timer.** A timer is a starting point of its own: nobody asked, so there is no command with headers. When one of the entity's own timers fires, the library calls `InjectForTimer` on each propagator, on the entity's thread, just before the command the timer delivers is decided. The headers it adds are the decision's headers, exactly like headers that arrived with a command. They are reported with the event, stored with the effects the decision issues, passed on by `Send`, restored for handlers and inherited by answers. Use it to mark a chain ("this started at timer X"), so it can be told apart from a send that lost its headers. The library's own reminders, whose keys start with `durable-`, are never reported to it. With no propagator that overrides it, nothing is called and nothing is allocated.

**Observed.** `EventSaved`, `EffectObserved` and `TimerObserved` have a `Headers` member. It is never null. It is empty when there are none. See [Observing what entities do](observing-what-entities-do.md).

## What does not carry headers

- **Timers, by default.** A command that a timer delivers has no headers, even when the command that set the timer had some. An event decided from a timer has none, and neither has an effect that decision issues. Nothing about a reminder or its stored payload changes. A propagator can give the timer's decision headers with `InjectForTimer`. They are never the headers of the command that set the timer. `TimerObserved` has the headers of the decision that set or cancelled the timer, and for a timer that fired, the ones `InjectForTimer` added.
- **Replies, over the wire.** A reply does not carry headers back. The entity remembers its own: the decision on the reply to its own `Send` keeps the headers of the decision that issued the `Send`, while the entity lives. After a restart the reply is lost anyway, so nothing is stored.
- **State reads.** `GetStateAsync`, `WaitForStateAsync` and `WatchStateAsync` carry none.
- **A workflow's work after a restart.** See the next section.

## Workflows

A workflow is an entity underneath, so it carries headers the same way. `Start`, `Notify` and `Cancel` on a workflow ref carry what the propagators inject. The events that decide them are reported with those headers.

While the workflow is live, its work carries the headers of the command being handled. Each piece of work runs with them restored, so a `ctx.Send` from `Work` reaches the entity with them. The outcome of that work is decided with the same headers, so the next state's work carries them too. A fact that arrives while work is in flight is held. It is decided later, with the headers of the command that settles that work.

Nothing is added to the workflow's stored state. A workflow works out its work again from its state after a restart, and that work has no headers. The same holds for work that a timer starts: a workflow's wake is a library reminder, so `InjectForTimer` is never called for it. Headers go as far as the workflow lives, and no further.

An entity that starts, notifies or cancels a workflow records that call as an effect. The call keeps the headers of the decision that issued it, like any other recorded effect, and the workflow it reaches gets them.

## Rolling out

Registering a propagator is what makes a node send the carrying message. Do it in two steps:

1. Deploy a version of Akka.Durable that understands headers to every node of the cluster.
2. Only then register a propagator, on any node.

A node that has an older version can not read the message. Its serializer fails with `Unknown generated serializer manifest [durable-carried-v1]`, so the command does not arrive and an `Ask` to that entity times out.

What is stored is safe in both directions. `EffectIssued` and `PendingEffect` each have one new, last field for the headers. A record written before the field existed is read with no headers. A build from before the field reads a record that has it and skips the field, so rolling a node back keeps stored effects readable. Those effects then run without headers.

Every recorded effect now has the field in its stored bytes, with no value when there are no headers. That adds two bytes to the record.

## Example: a request id from the web request to a second entity

An application wants every event that a request causes to carry the request id, also the events of another entity that an effect reaches later.

The propagator keeps the id in an `AsyncLocal`:

```csharp
public sealed class RequestIdPropagator : IHeaderPropagator
{
    public const string Key = "request-id";
    private static readonly AsyncLocal<string?> Current = new();

    public static string? RequestId
    {
        get => Current.Value;
        set => Current.Value = value;
    }

    public void Inject(IDictionary<string, string> headers)
    {
        if (Current.Value is { } id) headers[Key] = id;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> headers)
    {
        var before = Current.Value;
        Current.Value = headers.GetValueOrDefault(Key);
        return new Undo(() => Current.Value = before);
    }

    private sealed class Undo(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }
}
```

An ASP.NET Core middleware sets it for each request, and one line registers the propagator:

```csharp
builder.Services.AddDurableHeaderPropagator<RequestIdPropagator>();

app.Use(async (context, next) =>
{
    RequestIdPropagator.RequestId = context.TraceIdentifier;
    await next();
});
```

The controller sends a command as it always did. `IssueInvoice` makes the invoice issue a `VerifyRecipient` effect. That effect is stored with the request id. The handler talks to a second entity through a plain ref and has no header code:

```csharp
public sealed class VerifyRecipientHandler(IEntityRefs<RecipientCommand> recipients)
    : IEffectHandler<VerifyRecipient, InvoiceCommand>
{
    public async Task<InvoiceCommand> HandleAsync(
        VerifyRecipient effect, EffectDelivery delivery, CancellationToken ct) =>
        await recipients.Get(new EntityId(effect.Recipient)).Ask(new CheckRecipient(), ct) switch
        {
            Known => new RecipientVerified(),
            Unknown u => new RecipientRejected(u.Reason),
        };
}
```

Before the handler runs, `Restore` puts the request id back. The `Ask` inside it asks `Inject` for headers, and the second entity gets the same id. The same holds after a restart, for a retry, and for the invoice's own decision on the answer.

An observer reads the id. It hands the row to a queue, because an observer must be quick:

```csharp
public sealed class RequestAuditObserver(IAuditQueue queue) : IDurableObserver
{
    public void EventSaved(in EventSaved saved) =>
        queue.Enqueue(new AuditRow(
            saved.Headers.GetValueOrDefault(RequestIdPropagator.Key),
            saved.EntityType.Value, saved.EntityId.Value, saved.SequenceNr, saved.Event.GetType().Name, saved.At));
}

builder.Services.AddDurableObserver<RequestAuditObserver>();
```

Each row has the request id, the entity and the event. A request that causes events in two entities shows the same id on both. An event that a timer decides has no id, unless a propagator marks the timer with `InjectForTimer`.

---

Next: [Status and direction](status.md)
