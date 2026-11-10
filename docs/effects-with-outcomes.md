# Effects with outcomes

Part of the [Akka.Durable guide](../README.md#guide).

When an external service decides what happens next, the decision is split in two. The first half persists what must be remembered and issues an effect. The second half is an ordinary command, produced by the effect's handler, that carries the result back into `Decide` against the state at that moment.

The [invoice in the README](../README.md#define-its-behavior) does this for recipient verification. These are its two halves:

```csharp
// First half. Remember that we asked, then ask.
(Draft, IssueInvoice c) when c.Total > 0 =>
    ctx.Persist(new VerificationStarted(c)).Execute(new VerifyRecipient(c.Recipient)),

// Second half. The handler's answer arrives as a command and is decided like any other.
(Verifying v, RecipientVerified) =>
    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
        .Schedule("reminder", TimeSpan.FromDays(30), new SendReminder())
        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId)),
(Verifying v, RecipientRejected r) =>
    ctx.Persist(new VerificationFailed(r.Reason)).Reply(v.Request, new InvoiceRejected(r.Reason)),

// An answer that arrives in any other state has nothing to finish.
(_, RecipientVerified or RecipientRejected) => ctx.Ignore(),
```

`VerificationStarted` and the `Verifying` state are the memory, `VerifyRecipient` is the effect, and `RecipientVerified` and `RecipientRejected` are the outcomes. Between the two halves sits the handler, the only code that touches the directory.

## Command outcomes and event outcomes

The second type argument of the handler's registration says what kind of outcome it produces. Exactly one handler is registered per effect type.

**A command: the entity decides.** The result goes through `Decide` like any other command. Use this when the result can be declined, retried, or ignored. `VerifyRecipientHandler` is one:

```csharp
public sealed class VerifyRecipientHandler(IRecipientDirectory directory)
    : IEffectHandler<VerifyRecipient, InvoiceCommand>       // the outcome is a command
{
    public async Task<InvoiceCommand> HandleAsync(VerifyRecipient effect, CancellationToken ct) =>
        await directory.LookupAsync(effect.Recipient, ct) switch
        {
            Known => new RecipientVerified(),
            Unknown u => new RecipientRejected(u.Reason),
        };
}

services.AddScoped<IEffectHandler<VerifyRecipient, InvoiceCommand>, VerifyRecipientHandler>();
```

**An event: the entity records.** The result is a fact about the world, and it is persisted as it is. Suppose a sequence service assigns each issued invoice its number, kept in an optional `Number` on `Issued` and `Paid`:

```csharp
public sealed record AssignNumber;                           // the effect

public sealed class AssignNumberHandler(IInvoiceNumbers numbers)
    : IEffectHandler<AssignNumber, InvoiceEvent>             // the outcome is an event
{
    public async Task<InvoiceEvent> HandleAsync(AssignNumber effect, CancellationToken ct) =>
        new NumberAssigned(await numbers.NextAsync(ct));
}

services.AddScoped<IEffectHandler<AssignNumber, InvoiceEvent>, AssignNumberHandler>();

// Decide issues the effect. There is no second arm: nothing is decided when the number arrives.
(Verifying v, RecipientVerified) =>
    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
        .Execute(new AssignNumber())
        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId)),

// Evolve takes the event in every state it can arrive in. The payment can get there first.
(Issued s, NumberAssigned e) => s with { Number = e.Number },
(Paid s, NumberAssigned e) => s with { Number = e.Number },
```

An event outcome skips `Decide`, so there is no arm in which to ignore it. Use one only when every state it can arrive in can accept the fact. Otherwise return a command and decide.

## The entity is not blocked

`Execute` starts the handler and moves on; the outcome arrives later as a message. Other commands are handled in the meantime, which is why the invoice records `Verifying`. A second `IssueInvoice` that arrives during verification matches no `Draft` arm and falls through to the rejection:

```csharp
(_, IssueInvoice c) => ctx.None().Reply(c, new InvoiceRejected("Invalid invoice operation")),
```

To queue it instead, stash it while verifying and release the stash from both outcome arms:

```csharp
(Verifying, IssueInvoice) => ctx.Stash(),

(Verifying v, RecipientVerified) =>
    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId))
        .UnstashAll(),
(Verifying v, RecipientRejected r) =>
    ctx.Persist(new VerificationFailed(r.Reason))
        .Reply(v.Request, new InvoiceRejected(r.Reason))
        .UnstashAll(),
```

A released command is decided against the new state. After a rejection the invoice is a `Draft` again, so the queued request starts its own verification.

## Replies find their asker

An outcome is handled on behalf of the command that issued the effect. The caller makes one call and awaits one reply; it does not see that the entity answered in two steps:

```csharp
// The caller.
IssueReply reply = await invoices.Get(id).Ask(new IssueInvoice(250m, "billing@example.com"), ct);

// The entity, several messages later. This reply completes the caller's Ask.
(Verifying v, RecipientRejected r) =>
    ctx.Persist(new VerificationFailed(r.Reason)).Reply(v.Request, new InvoiceRejected(r.Reason)),
```

`v.Request` is the `IssueInvoice` kept in state. It fixes the reply type. The reply itself goes to whoever sent the command that issued `VerifyRecipient`.

## When the handler fails

Expected failures are outcomes. A directory that does not know the recipient has given an answer, so the handler returns it and the entity decides what it means:

```csharp
Unknown u => new RecipientRejected(u.Reason),   // a decline is a result, not an exception
```

An exception from `HandleAsync`, or a handler that runs longer than `EntityOptions.EffectTimeout` (30 seconds by default), is logged as a warning and dropped. No outcome arrives and the state is unchanged: the invoice stays `Verifying`. To try again, arm a recurring timer in the first half, re-issue the effect from it, and cancel it from the outcome arms:

```csharp
(Draft, IssueInvoice c) when c.Total > 0 =>
    ctx.Persist(new VerificationStarted(c))
        .Execute(new VerifyRecipient(c.Recipient))
        .Schedule("verify-retry", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1), new RetryVerification()),

// Still waiting: ask again. Resume already lists what is pending for this state.
(Verifying, RetryVerification) => Resume(state, ctx),
(_, RetryVerification) => ctx.Ignore(),

(Verifying v, RecipientRejected r) =>
    ctx.Persist(new VerificationFailed(r.Reason))
        .CancelTimer("verify-retry")
        .Reply(v.Request, new InvoiceRejected(r.Reason)),
```

The `RecipientVerified` arm cancels the timer the same way. A retried effect can produce two answers; the second finds the invoice no longer `Verifying` and is ignored by the `(_, RecipientVerified or RecipientRejected)` arm.

The timeout is set per entity type:

```csharp
Invoice.Definition with { Options = EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(5) } }
```

A missing handler registration is a configuration error, and so is registering both a command handler and an event handler for one effect type. Either one restarts the entity instead of being logged.

Nothing about the effect is journaled. What survives a crash is the state, and [`Resume`](resuming-after-a-restart.md) turns that state back into the pending effect.

---

Next: [Run or Execute](run-or-execute.md)
