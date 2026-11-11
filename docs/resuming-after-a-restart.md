# Resuming after a restart

Part of the [Akka.Durable guide](../README.md#guide).

An `Execute` that was in flight when the process stopped comes back by itself. Its effect is journaled with the decision that issued it, so the next incarnation finds it with no answer and decides its give-up with the reason `Interrupted`, before any other command. It is not run again. To ask again after a crash, issue the `Execute` again from the `Decide` arm for that give-up. See [Effects with outcomes](effects-with-outcomes.md#when-the-handler-fails).

`Resume` is the optional function that lists other work still pending for a recovered state: a `Send` or a `Tell` that must go out again. It runs once after recovery, after the interrupted give-ups, through the same effect path as a decision. It may not return events or an `Execute`.

```csharp
public static Handled<CheckoutCommand, CheckoutEvent> Resume(CheckoutState state, CheckoutContext ctx) =>
    state switch
    {
        AwaitingInvoice a => ctx.None().Send<InvoiceCommand>(a.Invoice, new GetInvoiceStatus()),
        _ => ctx.None(),
    };
```

Everything it returns must follow from state alone, so running it twice is harmless: prefer re-sending a query to re-sending a change. Replies issued by resumed work go to dead letters: the asker belongs to the previous incarnation.

A workflow start needs no `Resume`. `Start` keeps a durable reminder until the workflow answers. See [Starting a workflow from an entity](starting-a-workflow-from-an-entity.md).

Timers re-arm themselves through Akka.Reminders, and `Run` work is best effort by definition.

---

Next: [Talking to other entities](talking-to-other-entities.md)
