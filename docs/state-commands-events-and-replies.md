# State, commands, events, and replies

Part of the [Akka.Durable guide](../README.md#guide).

An entity is an initial state, a `Decide` function and an `Evolve` function. You don't subclass an actor. Its state, commands, events and replies are serialized (see [Serialization](serialization.md)), so each one carries a stable manifest and field indexes:

```csharp
public interface IInvoiceProtocol;

// State

[AkkaSerializable(Manifest = "invoice-draft-v1", AllowEmpty = true)]
public sealed record Draft : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-verifying-state-v1")]
public sealed record Verifying([property: AkkaField(0)] IssueInvoice Request) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-issued-state-v1")]
public sealed record Issued(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-paid-state-v1")]
public sealed record Paid([property: AkkaField(0)] decimal Total) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-state-v1")]
public readonly union InvoiceState(
    Draft,
    Verifying,
    Issued,
    Paid) : IInvoiceProtocol;

// Commands

[AkkaSerializable(Manifest = "issue-invoice-v1")]
public sealed record IssueInvoice(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient)
    : IInvoiceProtocol, IReplies<InvoiceCommand, IssueReply>
{
    public InvoiceCommand ToCommand() => new(this);
}

// Outcomes of the VerifyRecipient effect, produced by its handler.
[AkkaSerializable(Manifest = "recipient-verified-v1", AllowEmpty = true)]
public sealed record RecipientVerified : IInvoiceProtocol;

[AkkaSerializable(Manifest = "recipient-rejected-v1")]
public sealed record RecipientRejected([property: AkkaField(0)] string Reason) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "record-payment-v1")]
public sealed record RecordPayment([property: AkkaField(0)] decimal Amount)
    : IInvoiceProtocol, IReplies<InvoiceCommand, PaymentReply>
{
    public InvoiceCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "send-reminder-v1", AllowEmpty = true)]
public sealed record SendReminder : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-command-v1")]
public readonly union InvoiceCommand(
    IssueInvoice,
    RecipientVerified,
    RecipientRejected,
    RecordPayment,
    SendReminder) : IInvoiceProtocol;

// Events

[AkkaSerializable(Manifest = "verification-started-v1")]
public sealed record VerificationStarted([property: AkkaField(0)] IssueInvoice Request) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "verification-failed-v1")]
public sealed record VerificationFailed([property: AkkaField(0)] string Reason) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-issued-v1")]
public sealed record InvoiceIssued(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient,
    [property: AkkaField(2)] DateTimeOffset At) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "payment-recorded-v1")]
public sealed record PaymentRecorded([property: AkkaField(0)] decimal Amount) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-event-v1")]
public readonly union InvoiceEvent(
    VerificationStarted,
    VerificationFailed,
    InvoiceIssued,
    PaymentRecorded) : IInvoiceProtocol;

// Replies

[AkkaSerializable(Manifest = "invoice-accepted-v1")]
public sealed record InvoiceAccepted([property: AkkaField(0)] EntityId Id) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "payment-accepted-v1", AllowEmpty = true)]
public sealed record PaymentAccepted : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-rejected-v1")]
public sealed record InvoiceRejected([property: AkkaField(0)] string Reason) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "issue-reply-v1")]
public readonly union IssueReply(
    InvoiceAccepted,
    InvoiceRejected) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "payment-reply-v1")]
public readonly union PaymentReply(
    PaymentAccepted,
    InvoiceRejected) : IInvoiceProtocol;

// Effects. Journaled with the decision that issues them. Each says which command its give-up becomes.

[AkkaSerializable(Manifest = "verify-recipient-v1")]
public sealed record VerifyRecipient([property: AkkaField(0)] string Recipient)
    : IInvoiceProtocol, IEffect<InvoiceCommand>
{
    public InvoiceCommand GaveUp(WorkGaveUp g) => new RecipientRejected($"directory: {g.Reason}");
}

[AkkaSerializer<IInvoiceProtocol>("invoices", 120100)]
public sealed partial class InvoiceSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}
```

- **Events first, then effects.** Effects run only after the events are committed, in the order they're declared.
- **Typed replies.** `Reply(command, reply)` takes the command that declared the reply type, so a wrong reply is a compile error. The command fixes the type and is not sent anywhere: it can be the command being handled, or one kept in state from earlier.
- **`Execute` for outside work.** The decider issues an effect record. It is journaled in the same write as the decision's events. Its handler runs it once, and the answer, or the effect's give-up, comes back as one of the entity's commands. See [Effects](effects.md) and [Effects with outcomes](effects-with-outcomes.md).
- **`Run` for cheap work you can lose.** `Run` is not recorded: no journal row, no answer for `Decide`, and it is lost in a crash. A notification, a cache invalidation. It takes a lambda against one service, runs in a fresh DI scope and is awaited in place, so later effects and later commands wait for it. A failure or timeout is logged and the decision continues. See [Effects](effects.md).
- **`Schedule` uses Akka.Reminders.** The reminder is stored durably and delivered back to the entity at least once, so `Decide` must tolerate duplicates. Its message must be a `TCommand`. Keys that start with `durable-` belong to the library and are refused.
- **`Send` talks to another entity.** Its reply arrives as one of this entity's commands. See [Talking to other entities](talking-to-other-entities.md).
- **Other outcomes.** `ctx.Ignore()`, `ctx.Unhandled()` and `ctx.Stash()` cover commands you aren't handling now. `.UnstashAll()` releases stashed commands after this decision completes. `.Stop()` must be the last effect; it passivates without deleting the entity.

---

Next: [Effects](effects.md)
