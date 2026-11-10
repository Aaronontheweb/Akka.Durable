# Talking to other entities

Part of the [Akka.Durable guide](../README.md#guide).

`Send` addresses another durable entity by its command protocol and ID. The target's reply, if any, comes back to the sender as a foreign type, and `Accepts` on the definition says how it becomes one of the sender's commands. Together they let one entity drive another. A workflow takes messages of other types as facts with `Accepts` too, but it has no `Send` yet. See [Workflows](workflows.md).

```csharp
public sealed record InvoiceReplied(IssueReply Reply);      // a CheckoutCommand case

public static readonly EntityDefinition<CheckoutState, CheckoutCommand, CheckoutEvent> Definition =
    new EntityDefinition<CheckoutState, CheckoutCommand, CheckoutEvent>(
            new EntityTypeName("checkouts"), _ => new NotStarted(), Decide, Evolve) { Resume = Resume }
        .Accepts<IssueReply>(reply => new InvoiceReplied(reply));

(NotStarted, StartCheckout c) => ctx.Persist(new Started(c, InvoiceFor(ctx.EntityId)))
    .Send<InvoiceCommand>(InvoiceFor(ctx.EntityId), new IssueInvoice(c.Total, c.Recipient))
    .Schedule("deadline", TimeSpan.FromMinutes(10), new CheckoutTimedOut()),

(AwaitingInvoice a, InvoiceReplied { Reply: InvoiceAccepted }) => ctx.Persist(new Completed())
    .CancelTimer("deadline").Reply(a.Request, new CheckoutCompleted()).Stop(),

(AwaitingInvoice a, InvoiceReplied { Reply: InvoiceRejected r }) => ctx.Persist(new Abandoned(r.Reason))
    .CancelTimer("deadline").Reply(a.Request, new CheckoutFailed(r.Reason)).Stop(),
```

Name the target's command type explicitly in `Send<InvoiceCommand>(...)`: the compiler would otherwise infer the case type, which no entity type is registered under. The target's entity type must be registered with `WithDurableEntity` in this actor system; in a cluster the entity itself can run on any node. The reply is handled on behalf of the command that sent it, so `Reply(a.Request, ...)` reaches the caller that started the checkout. The `Accepts` table lives on the definition rather than on the send so that a reply to a command re-sent by `Resume` is wrapped the same way after a restart.

There is no timeout on a send. The entity's own reminder is its deadline, as above. A message of a type the definition does not accept is unhandled, as before.

---

Next: [Serialization](serialization.md)
