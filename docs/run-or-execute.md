# Run or Execute

Part of the [Akka.Durable guide](../README.md#guide).

Both run service code after the decision's events commit, in a fresh DI scope, under `EntityOptions.EffectTimeout`. They differ in what the entity gets back.

Take the invoice's reminder mail. With `Run` it is one line:

```csharp
(Issued, SendReminder) => ctx.None().Run<IInvoiceMailer>((mailer, ct) => mailer.RemindAsync(ct)),
```

There is no effect record, no handler and no second arm. If the mail server is down, a warning is logged and the invoice never knows.

Now suppose the invoice must act on the result: remind again in 30 days if the mail went out, and stop if the address bounced. That is `Execute`:

```csharp
public sealed record MailReminder(string Recipient);         // the effect

public sealed class MailReminderHandler(IInvoiceMailer mailer)
    : IEffectHandler<MailReminder, InvoiceCommand>
{
    public async Task<InvoiceCommand> HandleAsync(MailReminder effect, CancellationToken ct) =>
        await mailer.DeliverAsync(effect.Recipient, ct) ? new ReminderDelivered() : new ReminderBounced();
}

(Issued s, SendReminder) => ctx.None().Execute(new MailReminder(s.Recipient)),
(Issued, ReminderDelivered) => ctx.None().Schedule("reminder", TimeSpan.FromDays(30), new SendReminder()),
(Issued, ReminderBounced) => ctx.None(),                     // no more reminders
(_, ReminderDelivered or ReminderBounced) => ctx.Ignore(),
```

It costs an effect record, a handler and the outcome arms, and the result is now something `Decide` can see.

| | `Run` | `Execute` |
|---|---|---|
| Shape | A lambda against one service, inline in `Decide` | An effect record, plus an `IEffectHandler` registered in DI |
| Result | None. The entity never hears about it | A command or an event, delivered back to the entity |
| While it runs | Awaited in place: later effects and later commands wait | Not awaited: the entity keeps handling commands |
| On failure or timeout | Logged; the decision continues | Logged; no outcome arrives, and the state still says the work is pending |
| After a crash | Lost | Re-issued by `Resume`, if the state records it |
| In tests | Checked by type (`ThenEffects`) | Compared by value (`ThenExecuted`); the handler is tested on its own |

**While it runs.** `Run` is awaited, so everything after it in the chain waits for it. This reply leaves only after the read model is written, and the caller can read what it wrote:

```csharp
(Issued s, RecordPayment p) when p.Amount == s.Total =>
    ctx.Persist(new PaymentRecorded(p.Amount))
        .Run<IInvoiceReadModel>((model, ct) => model.MarkPaidAsync(ctx.EntityId, ct))
        .Reply(p, new PaymentAccepted()),
```

If the write fails, it is logged and the reply still goes out. An `Execute` in the same place does not hold the reply: it leaves at once, while the handler is still running.

**In tests.** The difference is what a test can see:

```csharp
var command = new IssueInvoice(250m, "billing@example.com");
InvoiceEvent[] issued =
[
    new VerificationStarted(command),
    new InvoiceIssued(250m, "billing@example.com", DateTimeOffset.UnixEpoch),
];

// Run holds a lambda. A test can check that one was issued, and nothing about what it does.
DeciderScenario.For(Invoice.Definition)
    .Given(issued)
    .When(new SendReminder())
    .ThenEffects(typeof(RunEffect));

// Execute is data. The decision is compared by value, and the handler is tested as a plain class.
DeciderScenario.For(Invoice.Definition)
    .Given(issued)
    .When(new SendReminder())
    .ThenExecuted(new MailReminder("billing@example.com"));

var handler = new MailReminderHandler(new FakeInvoiceMailer(delivers: false));
Assert.True(await handler.HandleAsync(new MailReminder("billing@example.com"), ct) is ReminderBounced);
```

**After a crash.** An `Execute` survives a restart only if the state records it. The reminder above does not: nothing is persisted before `MailReminder` is issued, so a crash loses it, as it would a `Run`. To keep it, persist a fact first, as the invoice does with `VerificationStarted`, and list the effect in `Resume`.

Use `Run` when nothing in the entity depends on the result and losing one is tolerable: a notification, a cache invalidation, a metric. Use `Execute` when the result changes what the entity does next, or when the work has to survive a restart. If you find yourself wanting to know whether a `Run` succeeded, that is an `Execute`.

---

Next: [Resuming after a restart](resuming-after-a-restart.md)
