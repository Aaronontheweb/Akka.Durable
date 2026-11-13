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

Write `Decide` so that a reader sees the whole behavior of one state in one place. The [invoice in the README](../README.md#define-its-behavior) is written this way. Four points make the shape:

1. **Filter by state first.** The outer `switch` is on the state. Each state has its own short block: a nested `switch` on the command, or a small function named for the state, such as `WhenIssued`. A reader finds the rules of a state in one place, and a change to a state touches one block.
2. **A state lists only the commands that mean something in that state.** Each is one short arm. A short `when` guard is fine. A long guard is a sign that the state should be two states.
3. **Everything else gets one blanket arm per state.** It is the last arm of that state's block, and it is visible. The block then fits on one screen, and nobody has to write out every pair of state and command to say "not here".
4. **Helpers build decisions. They do not branch on the state again.** A helper such as `Leftover` below takes the command and builds `Persist`, `Execute` or `Reply`. If a helper looks at the state too, the rules of that state live in two places.

Two conditions make the blanket arm safe. Keep both.

**The blanket arm must be loud for a command that someone sent on purpose.** A caller who sent a command must not be left in silence. Choose by what was sent:

- A command that declares a reply (`IReplies`): refuse it with a typed reply, such as `InvoiceRejected`. The caller sees the refusal. This is the normal choice. `ctx.Ignore()` here is a mistake: the entity sends nothing back, so an `Ask` waits until its cancellation token fires.
- A command that no caller should send, or one sent with `Tell` that has no reply: use `ctx.Unhandled()`. The entity sends nothing back, and the command is published to the actor system's event stream as an unhandled message. Akka.NET writes it to the log when its unhandled-message logging is on (`akka.actor.debug.unhandled`). A reminder that is unhandled is not acknowledged, so the reminder scheduler delivers it again by its own retry policy.
- A command that is early, not wrong, such as a second `IssueInvoice` while the first one is being verified: use `ctx.Stash()` in that state, and release it with `.UnstashAll()` on the arms that leave that state. The entity holds the command in memory, with its sender, and decides it again after the release. A stashed command that is never released waits for ever, and it is lost when the entity stops. Use `Stash` only in a state that has such an exit.

**A late answer of the entity's own effect is ignored, not refused.** The answer or the give-up of an effect can arrive after the entity has moved on: a lookup that answers after a cancel, a reminder that fires twice. That is expected, and nobody is waiting for a reply. Use `ctx.Ignore()`. A reminder that is ignored is acknowledged, so it is not delivered again.

Keep these two kinds apart. If both fall into one `_ => ctx.Ignore()`, an effect answer that you forgot to handle looks exactly like a late duplicate, and a caller's mistaken command disappears without a reply. Name the effect answers in the arm that ignores them, and give the caller's commands a reply. A good way is one shared function with no `_` arm, which every state calls from its blanket arm. The compiler then makes you place each new command in one of the two kinds.

**The cost.** With a blanket arm, the compiler no longer reports a pair of state and command that nobody placed. A new command can fall into the blanket arm of a state where it should have an arm, and it will be refused or ignored without a warning. A decision test is the safety net. For each state, test each command it must accept, and at least one command it must refuse.

A workflow's fact table has its own rule: see [Workflow rules](workflow-rules.md).

Here is a small seat reservation with three states. These are its declarations, without the serialization attributes. `Evolve` is not shown:

```csharp
public interface ISeatProtocol;

public sealed record Free : ISeatProtocol;
public sealed record Held(string By) : ISeatProtocol;
public sealed record Booked(string By) : ISeatProtocol;
public readonly union SeatState(Free, Held, Booked) : ISeatProtocol;

public sealed record Hold(string By) : ISeatProtocol, IReplies<SeatCommand, SeatReply>
{
    public SeatCommand ToCommand() => new(this);
}

public sealed record Confirm(string By) : ISeatProtocol, IReplies<SeatCommand, SeatReply>
{
    public SeatCommand ToCommand() => new(this);
}

public sealed record Release(string By) : ISeatProtocol, IReplies<SeatCommand, SeatReply>
{
    public SeatCommand ToCommand() => new(this);
}

// The entity's own reminder.
public sealed record HoldExpired : ISeatProtocol;
public readonly union SeatCommand(Hold, Confirm, Release, HoldExpired) : ISeatProtocol;

public sealed record SeatHeld(string By) : ISeatProtocol;
public sealed record SeatBooked(string By) : ISeatProtocol;
public sealed record SeatReleased : ISeatProtocol;
public readonly union SeatEvent(SeatHeld, SeatBooked, SeatReleased) : ISeatProtocol;

public sealed record SeatAccepted : ISeatProtocol;
public sealed record SeatRefused(string Reason) : ISeatProtocol;
public readonly union SeatReply(SeatAccepted, SeatRefused) : ISeatProtocol;
```

Each state has a block, and each block ends with `Leftover`:

```csharp
using SeatContext = DecisionContext<SeatState, SeatCommand, SeatEvent>;
using SeatDecision = Decision<SeatCommand, SeatEvent>;

public static class Seat
{
    public static SeatDecision Decide(SeatState state, SeatCommand command, SeatContext ctx) =>
        state switch
        {
            Free => WhenFree(command, ctx),
            Held h => WhenHeld(h, command, ctx),
            Booked => Leftover(command, ctx),
        };

    private static SeatDecision WhenFree(SeatCommand command, SeatContext ctx) =>
        command switch
        {
            Hold c =>
                ctx.Persist(new SeatHeld(c.By))
                    .Schedule("hold", TimeSpan.FromMinutes(10), new HoldExpired())
                    .Reply(c, new SeatAccepted()),

            _ => Leftover(command, ctx),
        };

    private static SeatDecision WhenHeld(Held h, SeatCommand command, SeatContext ctx) =>
        command switch
        {
            Confirm c when c.By == h.By =>
                ctx.Persist(new SeatBooked(c.By))
                    .CancelTimer("hold")
                    .Reply(c, new SeatAccepted()),
            Release c when c.By == h.By =>
                ctx.Persist(new SeatReleased())
                    .CancelTimer("hold")
                    .Reply(c, new SeatAccepted()),
            HoldExpired =>
                ctx.Persist(new SeatReleased()),

            _ => Leftover(command, ctx),
        };

    // Builds a decision. It does not look at the state.
    private static SeatDecision Leftover(SeatCommand command, SeatContext ctx) =>
        command switch
        {
            // A caller sent these on purpose. Refuse them with a reply.
            Hold c => ctx.None().Reply(c, new SeatRefused("The seat is not free.")),
            Confirm c => ctx.None().Reply(c, new SeatRefused("The seat is not held by you.")),
            Release c => ctx.None().Reply(c, new SeatRefused("The seat is not held by you.")),

            // The entity's own reminder can arrive late. Ignore it.
            HoldExpired => ctx.Ignore(),
        };
}
```

### What to avoid

Do not write a flat `switch` on the command whose helpers each branch on the state and each carry their own silent default. The rules of one state are then spread over many helpers, and each default can swallow a command without anyone seeing it:

```csharp
// Do not do this.
command switch
{
    Hold c => OnHold(state, c, ctx),       // OnHold checks the state, and ends with `_ => ctx.Ignore()`
    Confirm c => OnConfirm(state, c, ctx), // so does OnConfirm
    Release c => OnRelease(state, c, ctx), // and OnRelease
    HoldExpired => OnExpired(state, ctx),  // and OnExpired
}
```

Do not write one flat `(state, command)` table that lists every pair either. Most rows only say "ignored", and they hide the few rows that matter:

```csharp
// Do not do this.
(Free, Hold c) => /* the row that matters */,
(Free, Confirm c) => ctx.Ignore(),
(Free, Release c) => ctx.Ignore(),
(Free, HoldExpired) => ctx.Ignore(),
(Held h, Confirm c) when c.By == h.By => /* the row that matters */,
(Held, Hold c) => ctx.Ignore(),
// ... and so on for every pair
```

---

Next: [Effects](effects.md)
