# Effects with outcomes

Part of the [Akka.Durable guide](../README.md#guide).

When an external service decides what happens next, the decision is split in two. The first half persists what must be remembered and issues an effect. The second half is an ordinary command, produced by the effect's handler, that carries the result back into `Decide` against the state at that moment.

The [invoice in the README](../README.md#define-its-behavior) does this for recipient verification. These are its two halves:

```csharp
// First half, in the Draft block. Remember that we asked, then ask.
IssueInvoice c when c.Total > 0 =>
    ctx.Persist(new VerificationStarted(c)).Execute(new VerifyRecipient(c.Recipient)),

// Second half, in the Verifying block. The handler's answer arrives as a command
// and is decided like any other.
RecipientVerified =>
    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
        .Schedule("reminder", TimeSpan.FromDays(30), new SendReminder())
        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId)),
RecipientRejected r =>
    ctx.Persist(new VerificationFailed(r.Reason)).Reply(v.Request, new InvoiceRejected(r.Reason)),
```

An answer that arrives in any other state has nothing to finish. The blanket arm of that state ignores it, because nobody waits for a reply to it. This is the second condition of [How to structure a decider](state-commands-events-and-replies.md#how-to-structure-a-decider).

`VerificationStarted` and the `Verifying` state are the memory, `VerifyRecipient` is the effect, and `RecipientVerified` and `RecipientRejected` are the outcomes. Between the two halves sits the handler, the only code that touches the directory.

## The handler returns a command

The second type argument of the handler is the entity's command type. The answer goes through `Decide` like any other command, so `Decide` can decline it, ask again, or ignore it. Register exactly one handler per effect type with `AddEffectHandler`. A second registration fails at startup. An effect with no handler is a deployment mistake: it is refused before anything is written, an error is logged that names the `AddEffectHandler<...>` call to add, and the entity restarts under its supervisor. `VerifyRecipientHandler` is one:

```csharp
// The handler's second type argument says the answer is a command.
public sealed class VerifyRecipientHandler(IRecipientDirectory directory)
    : IEffectHandler<VerifyRecipient, InvoiceCommand>
{
    public async Task<InvoiceCommand> HandleAsync(
        VerifyRecipient effect,
        EffectDelivery delivery,
        CancellationToken ct) =>
        await directory.LookupAsync(effect.Recipient, ct) switch
        {
            Known => new RecipientVerified(),
            Unknown u => new RecipientRejected(u.Reason),
        };
}

services.AddEffectHandler<VerifyRecipient, InvoiceCommand, VerifyRecipientHandler>();
```

`VerifyRecipient` itself is journaled in the same write as `VerificationStarted`, so it has a manifest, and it implements `IEffect<InvoiceCommand>` to say what its give-up means (see [the declarations](state-commands-events-and-replies.md)). `delivery` holds the effect's id, the same on every read and after a restart.

To record a fact from the outside world, have the handler return a command that carries it. The blocks of the states that can accept it persist the event, and the blanket arm of every other state ignores it.

## The entity is not blocked

`Execute` starts the handler and moves on; the outcome arrives later as a message. Other commands are handled in the meantime, which is why the invoice records `Verifying`. A second `IssueInvoice` that arrives during verification has no arm in the `Verifying` block. Its blanket arm refuses it, and the caller gets the rejection:

```csharp
// In Leftover, which the blanket arm of every state calls.
IssueInvoice c => ctx.None().Reply(c, new InvoiceRejected("Invalid invoice operation")),
```

To queue it instead, give the `Verifying` block an arm that stashes it, and release the stash from both outcome arms:

```csharp
// In the Verifying block.
IssueInvoice => ctx.Stash(),

RecipientVerified =>
    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId))
        .UnstashAll(),
RecipientRejected r =>
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

// The entity, several messages later, in the Verifying block. This reply completes the caller's Ask.
RecipientRejected r =>
    ctx.Persist(new VerificationFailed(r.Reason)).Reply(v.Request, new InvoiceRejected(r.Reason)),
```

`v.Request` is the `IssueInvoice` kept in state. It fixes the reply type. The reply itself goes to whoever sent the command that issued `VerifyRecipient`.

## When the handler fails

Expected failures are outcomes. A directory that does not know the recipient has given an answer, so the handler returns it and the entity decides what it means:

```csharp
Unknown u => new RecipientRejected(u.Reason),   // a decline is a result, not an exception
```

When the handler gets no answer, `Decide` gets the effect's give-up command instead. `GaveUp` on the effect builds it from a `WorkGaveUp`, whose `Reason` says why:

- `Failed`: `HandleAsync` threw, or answered with nothing.
- `TimedOut`: it ran longer than `EntityOptions.EffectTimeout` (30 seconds by default; zero or a negative value is refused at startup).
- `Interrupted`: the process stopped, or the entity restarted, after the effect was journaled and before its answer was. The next incarnation decides it before any other command.

Without a retry policy on the handler, the library never runs the effect again. With one, it runs the handler again until an attempt is acknowledged, and `Decide` gets `RetriesExhausted` only when the policy runs out (see [Retry policies](effects.md#retry-policies-the-outbox)). A give-up does not mean the directory was not asked. `Decide` chooses what to do. The invoice turns every give-up into a rejection, so a caller can try again. An entity that wants to ask again issues the effect again from its arm:

```csharp
public sealed record VerificationGaveUp(GaveUpReason Reason) : IInvoiceProtocol;   // a command case

// on VerifyRecipient
public InvoiceCommand GaveUp(WorkGaveUp g) => new VerificationGaveUp(g.Reason);

// In the Verifying block. Ask once more after a crash; reject on any other give-up.
VerificationGaveUp { Reason: GaveUpReason.Interrupted } =>
    ctx.None().Execute(new VerifyRecipient(v.Request.Recipient)),
VerificationGaveUp g =>
    ctx.Persist(new VerificationFailed($"directory: {g.Reason}")).Reply(v.Request, new InvoiceRejected("directory unavailable")),
```

In every other state, `Leftover` ignores `VerificationGaveUp`, like the other late answers.

Each `Execute` is a new effect with a new id. Answers and give-ups are matched to the effect they belong to, so a late answer of an effect that already gave up is dropped.

The timeout is set per entity type:

```csharp
Invoice.Definition with { Options = EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(5) } }
```

---

Next: [Talking to other entities](talking-to-other-entities.md)
