# Reading state

Part of the [Akka.Durable guide](../README.md#guide).

Inject `IDurableEntityRefs<TCommand, TState, TEvent>` to get refs that can also read the entity's state. The event type is part of the signature too. No method uses it yet; it is there so that the signature does not change if event subscriptions are added later:

```csharp
public sealed class InvoiceQueries(IDurableEntityRefs<InvoiceCommand, InvoiceState, InvoiceEvent> invoices)
{
    public async Task<string> DescribeAsync(string invoiceId, CancellationToken ct)
    {
        var snapshot = await invoices.Get(new EntityId(invoiceId)).GetStateAsync(ct);
        return snapshot.State switch
        {
            Draft => "draft",
            Issued s => $"issued to {s.Recipient}, {s.Total:C} due",
            Paid s => $"paid {s.Total:C}",
        };
    }
}
```

The snapshot carries the state and the sequence number of the last applied event. The read waits behind commands the entity has already received, so it sees their committed events and runs only after their `Run` effects finish. It does not wait for an `Execute` outcome that is still in flight; the state tells you whether one is. To read the state of an entity you hold a plain ref or an ID for, get a state-aware ref by ID: `invoices.Get(invoiceRef.Id)`. `TState` must match the registered definition; otherwise `Get` throws Akka.Hosting's `MissingActorRegistryEntryException`.

## Waiting for a state, and watching it

Do not read the state in a loop to see when it changes. The ref can wait for a state, and it can watch the state.

`WaitForStateAsync(until, cancellationToken)` returns the first snapshot whose state satisfies `until`. The token is required: it is the time limit. When it fires, the wait throws `OperationCanceledException`. When the state already satisfies `until`, the wait returns after one round trip:

```csharp
using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
limit.CancelAfter(TimeSpan.FromSeconds(5));

var paid = await invoices.Get(new EntityId(invoiceId)).WaitForStateAsync(s => s is Paid, limit.Token);
```

The predicate runs in your process, not in the entity. It gets each snapshot that reaches the wait, in order. Keep it short, and do not block in it. If it throws, the wait fails with that exception.

`WatchStateAsync(cancellationToken)` gives the current snapshot first, then a snapshot after each later write. The subscription starts when you start to read and ends when you stop:

```csharp
await foreach (var snapshot in invoices.Get(new EntityId(invoiceId)).WatchStateAsync(ct))
{
    await WriteStatusAsync(snapshot.State, ct);
    if (snapshot.State is Paid) break;
}
```

A slow reader gets the newest snapshot it has not read yet. It can skip states, but it never reads an older state after a newer one. Treat each snapshot as the whole state, not as a change to apply.

What you get:

- On subscribing, the entity's current state and its sequence number. No write can fall between the subscribe and this first snapshot.
- Then one snapshot after each write that holds at least one event, in the order of the writes. A write that holds only the library's own records, such as the acknowledgement of a handler with no answer, sends none: the state did not change.
- Each snapshot is sent after the write's events are committed and its `Run` effects have finished, at the point where a waiting `GetStateAsync` is answered. So each snapshot is one that `GetStateAsync` could return. As for a read, an `Execute` outcome that is still in flight is not waited for. Its outcome is a later decision, with a snapshot of its own.
- `SequenceNr` is the journal's sequence number, the same as for `GetStateAsync`. It grows from one snapshot to the next. It counts the library's records too, so a jump does not mean a missed state.
- When the entity stops, passivates, restarts or moves to another node, the wait or the stream subscribes again by itself. It then goes on from the state the entity has at that moment. A live subscription starts a passivated entity again.

What it is not:

- It is not durable. The subscription is held in memory, on your side and in the entity. Nothing about it is journaled, snapshotted or put in the reminder store, and nothing is sent again. When your process stops, the subscription is gone.
- It does not give every state. The stream skips states for a slow reader, and while the entity is not running, a state that held only in that time is not seen.
- So a reaction that must happen for every change, also after a crash, does not belong here. It belongs in an effect: `Execute` with a retry policy. See [Effects](effects.md).

During a rolling upgrade, a subscription to an entity that runs on a node with an older version of the library does not connect until that node is upgraded. Until then, a wait runs to its token.

---

Next: [Workflows](workflows.md)
