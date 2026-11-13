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
    public InvoiceCommand GaveUp(WorkGaveUp g) =>
        new RecipientRejected($"directory: {g.Reason}");
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
- **Other outcomes.** `ctx.Ignore()`, `ctx.Unhandled()` and `ctx.Stash()` cover commands you aren't handling now. See [How to structure a decider](#how-to-structure-a-decider) for which one to use. `.UnstashAll()` releases stashed commands after this decision completes. `.Stop()` must be the last effect; it passivates without deleting the entity.

## How to structure a decider

Switch on the state first, then on the command. Each state then shows all of its behavior in one place. This is the complete invoice of the [README](../README.md#define-its-behavior). Each state has a nested `switch`, and each one ends with the same arm:

```csharp
using InvoiceContext = DecisionContext<InvoiceState, InvoiceCommand, InvoiceEvent>;
using InvoiceDecision = Decision<InvoiceCommand, InvoiceEvent>;

public static class Invoice
{
    public static readonly EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition =
        new(new EntityTypeName("invoices"), _ => new Draft(), Decide, Evolve);

    // The state comes first. Each state lists only its own commands, then one blanket arm.
    public static InvoiceDecision Decide(InvoiceState state, InvoiceCommand command, InvoiceContext ctx) =>
        state switch
        {
            Draft => command switch
            {
                // Needs an outside answer: remember that we asked, then ask.
                IssueInvoice c when c.Total > 0 =>
                    ctx.Persist(new VerificationStarted(c)).Execute(new VerifyRecipient(c.Recipient)),
                _ => NotHere(command, ctx),   // refused or ignored: it does not belong here
            },
            Verifying v => command switch
            {
                // The answer to VerifyRecipient arrives as a command.
                // v.Request is the IssueInvoice we are still answering.
                RecipientVerified =>
                    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
                        .Schedule("reminder", TimeSpan.FromDays(30), new SendReminder())
                        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId)),
                RecipientRejected r =>
                    ctx.Persist(new VerificationFailed(r.Reason))
                        .Reply(v.Request, new InvoiceRejected(r.Reason)),
                _ => NotHere(command, ctx),   // refused or ignored: it does not belong here
            },
            Issued s => command switch
            {
                RecordPayment p when p.Amount == s.Total =>
                    ctx.Persist(new PaymentRecorded(p.Amount))
                        .CancelTimer("reminder")
                        .Reply(p, new PaymentAccepted()),
                // A lost reminder mail is acceptable, so the light, unrecorded Run is enough.
                SendReminder => ctx.None().Run<IInvoiceMailer>((mailer, ct) => mailer.RemindAsync(ct)),
                _ => NotHere(command, ctx),
            },
            Paid => NotHere(command, ctx),
        };

    // The blanket arm of every state. It builds a decision and does not look at the state.
    // It has no `_` arm of its own, so a new command does not compile until it is placed here.
    private static InvoiceDecision NotHere(InvoiceCommand command, InvoiceContext ctx) =>
        command switch
        {
            // A caller sent these on purpose. Refuse them, so the caller sees the refusal.
            IssueInvoice c =>
                ctx.None().Reply(c, new InvoiceRejected("Invalid invoice operation")),
            RecordPayment p =>
                ctx.None().Reply(p, new InvoiceRejected("Invalid invoice operation")),

            // The entity's own effects and reminders can arrive late. Ignore them.
            RecipientVerified or RecipientRejected or SendReminder => ctx.Ignore(),
        };

    public static InvoiceState Evolve(InvoiceState state, InvoiceEvent @event) =>
        (state, @event) switch
        {
            (Draft, VerificationStarted e) => new Verifying(e.Request),
            (Verifying, VerificationFailed) => new Draft(),
            (Verifying, InvoiceIssued e) => new Issued(e.Total, e.Recipient),
            (Issued s, PaymentRecorded) => new Paid(s.Total),
            _ => throw new InvalidOperationException($"Cannot apply {@event} to {state}."),
        };
}
```

Every state follows the same rules:

- **Filter by state first.** Each state has its own block. For a larger entity, a small function named for the state, such as `WhenIssued`, is the same shape.
- **A block lists only the commands that mean something in that state.** Short `when` guards are fine.
- **Each block ends with one blanket arm.** The state then fits on one screen.
- **Helpers build decisions and do not look at the state.** Otherwise the rules of a state live in two places.

A small entity whose whole table fits on one screen can be one flat `(state, command)` switch. Once the table does not fit, or most rows only say "not here", write it state first. Both are fine.

`IssueInvoice` and `RecordPayment` come from callers and declare replies. `RecipientVerified`, `RecipientRejected` and `SendReminder` are the answer and the reminder of the entity's own work. This is what the blanket arm does with a leftover command:

| The command is | Use | The caller sees |
|---|---|---|
| Sent by a caller, and declares a reply | A typed refusal, with `Reply` | The refusal |
| Sent by a caller, with no reply, or never to be sent | `ctx.Unhandled()` | Nothing. It is logged as unhandled when Akka's unhandled logging is on. |
| Early, and valid after a change of state | `ctx.Stash()`, and `.UnstashAll()` on the arms that leave the state | Nothing until the release |
| A late answer or reminder of the entity's own effect | `ctx.Ignore()` | Nothing. A reminder is acknowledged. |

Keep refusal and ignore apart. A caller's command that is ignored gets no reply, and an effect answer you forgot to handle looks like a late duplicate. `NotHere` has no `_` arm for this reason: the compiler makes you sort each new command into one kind.

**The cost.** The compiler still checks that a new command is placed in `NotHere`, and that the outer `switch` has an arm for a new state. It no longer checks that a command has its own arm in each state where it should mean something: there it falls into the blanket arm and is refused or ignored. A decision test covers this. For each state, test each command it must accept, and at least one it must refuse.

A workflow's fact table has its own rule: see [Workflow rules](workflow-rules.md).

Two things are faults at any size. A switch on the command whose helpers each branch on the state again, each with its own silent `_ => ctx.Ignore()`. And one `_ => ctx.Ignore()` that swallows both a caller's command and the answer of an effect.

---

Next: [Effects](effects.md)
