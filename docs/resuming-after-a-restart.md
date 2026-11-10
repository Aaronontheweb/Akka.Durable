# Resuming after a restart

Part of the [Akka.Durable guide](../README.md#guide).

An entity that is waiting for an outcome when its process dies recovers into the state that says so: the invoice comes back as `Verifying`, with no verification in flight. `Resume` is the optional function that lists the effects still pending for a state. It runs once after recovery, before the first command, through the same effect path as a decision.

```csharp
public static Handled<InvoiceCommand, InvoiceEvent> Resume(InvoiceState state, InvoiceContext ctx) =>
    state switch
    {
        Verifying v => ctx.None().Execute(new VerifyRecipient(v.Request.Recipient)),
        _ => ctx.None(),
    };

public static readonly EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition =
    new(new EntityTypeName("invoices"), _ => new Draft(), Decide, Evolve) { Resume = Resume };
```

Two rules make it safe, and the runtime enforces the first: `Resume` may not return events, because nothing was decided, and everything it returns must follow from state alone, so running it twice is harmless. Give effects idempotency keys, and prefer re-sending a query to re-sending a change. The same function serves a retry timer: `(var s, RetryVerification) => Resume(s, ctx)`.

Replies issued by resumed work go to dead letters: the asker belongs to the previous incarnation. Its retry gets the answer from an arm that handles the repeated request, such as `(Issued, IssueInvoice c) => ctx.None().Reply(c, new InvoiceAccepted(ctx.EntityId))`. A repeated command should return the same reply it returned the first time; callers will retry.

Entities whose effects never wait for an outcome do not need `Resume`. Timers re-arm themselves through Akka.Reminders, and `Run` work is best effort by definition. To retry a reminder-driven effect without restarting the entity, re-arm the same key from the outcome: the old delivery's acknowledgement includes its original due time, so it leaves the new reminder intact.

---

Next: [Talking to other entities](talking-to-other-entities.md)
