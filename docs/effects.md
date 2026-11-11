# Effects

Part of the [Akka.Durable guide](../README.md#guide).

A decision does outside work through effects. Effects run after the decision's events are journaled, in the order the decision lists them. `Decide` itself stays pure: it only returns the effects as data.

## `Execute`: the normal way

Use `Execute` for outside work: a call to a service, a mail, a message to another system. Take the invoice's reminder mail:

```csharp
// The effect. It is journaled, so it has a manifest, and it says which command its give-up becomes.
[AkkaSerializable(Manifest = "mail-reminder-v1")]
public sealed record MailReminder([property: AkkaField(0)] string Recipient)
    : IInvoiceProtocol, IEffect<InvoiceCommand>
{
    public InvoiceCommand GaveUp(WorkGaveUp g) => new ReminderNotSent(g.Reason);
}

// The handler. The only code that touches the mail server.
public sealed class MailReminderHandler(IInvoiceMailer mailer) : IEffectHandler<MailReminder, InvoiceCommand>
{
    public async Task<InvoiceCommand> HandleAsync(MailReminder effect, EffectDelivery delivery, CancellationToken ct) =>
        await mailer.DeliverAsync(effect.Recipient, ct) ? new ReminderDelivered() : new ReminderBounced();
}

// At startup.
services.AddEffectHandler<MailReminder, InvoiceCommand, MailReminderHandler>();

// In Decide.
(Issued s, SendReminder) => ctx.None().Execute(new MailReminder(s.Recipient)),
(Issued, ReminderDelivered) => ctx.None().Schedule("reminder", TimeSpan.FromDays(30), new SendReminder()),
(Issued, ReminderBounced) => ctx.None(),                     // no more reminders
(Issued, ReminderNotSent) => ctx.None().Schedule("reminder", TimeSpan.FromHours(1), new SendReminder()),
(_, ReminderDelivered or ReminderBounced or ReminderNotSent) => ctx.Ignore(),
```

This is what `Execute` promises:

- **It is recorded.** The effect is journaled in the same write as the decision's events. Both are stored, or neither is.
- **Without a retry policy, it is attempted once.** The handler runs after the write, in a fresh DI scope, within `EntityOptions.EffectTimeout`. The entity does not wait for it: it keeps handling commands. With a policy, see [Retry policies](#retry-policies-the-outbox).
- **`Decide` hears how it ended.** It gets exactly one command: the handler's answer, or the effect's give-up. The give-up comes from `GaveUp` on the effect, with a reason:

  | Reason | When |
  |---|---|
  | `Failed` | The handler threw, or it answered with nothing. |
  | `TimedOut` | The handler ran past `EffectTimeout`. |
  | `Interrupted` | The process stopped, or the entity restarted, after the effect was journaled and before its answer was. |

  The answer or the give-up is journaled in the same write as the decision on it. After a crash, the next incarnation decides the `Interrupted` give-up before any other command.
- **Without a policy, the library never runs it again.** A give-up does not mean the work did not happen: a mail can go out and its answer be lost. `Decide` chooses what to do: ask again, look the result up, or ask a person. To ask again, issue a new `Execute` from the arm for the give-up.

A handler that has nothing to tell `Decide` implements `IEffectHandler<TEffect>`. It is acknowledged when it returns, and `Decide` gets nothing. A failure still reaches `Decide` as the give-up:

```csharp
public sealed class PublishPaymentHandler(IBus bus) : IEffectHandler<PublishPayment>
{
    public Task HandleAsync(PublishPayment effect, EffectDelivery delivery, CancellationToken ct) =>
        bus.PublishAsync(effect, messageId: delivery.Id.Value, ct);
}

services.AddEffectHandler<PublishPayment, PublishPaymentHandler>();
```

`delivery.Id` is the effect's id, for example `invoices/inv-42/1187`. It is the same however often you read it, also after a restart. Pass it as an idempotency key to a service that accepts one. A new `Execute` is a new effect with a new id.

The rules for setting it up are checked early:

- An entity effect implements `IEffect<TCommand>` of the entity. Without it, `Execute` does not compile.
- Each effect type has exactly one handler, registered with `AddEffectHandler`. A second registration throws at startup.
- An entity effect needs a serializer of its own, because it is journaled. The host checks this at startup. See [Serialization](serialization.md).
- An effect type with no handler registered at all is refused at its first `Execute`, before anything is written. The entity logs an error that names the `AddEffectHandler<...>` call to add, and restarts.

[Effects with outcomes](effects-with-outcomes.md) shows the two halves of a decision around an `Execute` in detail.

## Retry policies: the outbox

A handler registered with a retry policy is run again until one attempt is acknowledged, or until the policy runs out. This is at least once. An attempt can do its work and lose its answer, so the handler can run more than once for one effect: it must be safe to repeat. Registering a handler with a policy is your statement that it is.

```csharp
services.AddEffectHandler<PublishPayment, PublishPaymentHandler>(
    Retry.UntilAcknowledged(first: TimeSpan.FromSeconds(1), max: TimeSpan.FromMinutes(5)));

services.AddEffectHandler<ChargeCard, InvoiceCommand, ChargeCardHandler>(
    Retry.Backoff(first: TimeSpan.FromSeconds(2), max: TimeSpan.FromMinutes(1), attempts: 6));
```

| Policy | Wait before attempt n+1 | Ends |
|---|---|---|
| `Retry.Every(every, attempts)` | `every` | after `attempts` |
| `Retry.Backoff(first, max, attempts)` | `min(first × 2^(n−1), max)` | after `attempts` |
| `Retry.UntilAcknowledged(first, max)` | as `Backoff` | never |

- `attempts` counts every attempt, the first included. It must be at least 2.
- Waits must be positive, and `first` must not be greater than `max`. A value that is not valid throws `ArgumentOutOfRangeException` where you register the handler, which is at startup.
- There are no defaults without numbers, no jitter and no filter on exceptions. A failed attempt is an exception, a time-out, or an answer with nothing in it. An answer is never retried, because a decline is an answer: to ask for a retry, throw.
- The policy is written once for each effect type, where the handler is registered. `Execute(effect)` takes no policy, so `Decide` says nothing about delivery. For a different policy in another case, use a second effect type.
- The policy is journaled with each effect, so a pending effect keeps the promise made when it was issued, also after a deployment.

What the library does:

- Each attempt after the first is journaled before it runs, so a restart does not reset the count.
- While the entity runs, the wait between attempts is a timer. Between attempts the effect is not in flight: the entity handles other commands, and it can be passivated.
- One durable reminder for each entity wakes it after a crash, a passivation or a shard move, so the next attempt happens without anyone sending it a message. It is stored before the write that leaves the first effect pending, and again before a write whose effect is due earlier than the stored wake. If it cannot be stored, nothing is written and the command fails. It is removed when nothing is pending.
- When a limited policy runs out, `Decide` gets the effect's give-up with `GaveUpReason.RetriesExhausted`. `WorkGaveUp.Attempts` says how many attempts were made, and `Error` holds the last failure.
- An answer from an earlier attempt that arrives late is dropped. `Decide` gets one answer or one give-up for each effect.
- `delivery.Id` is the same on every attempt. `delivery.Attempt` counts from 1.

There is no order between effects. To do one thing after another, issue the second effect from the answer to the first.

### Do not build it by hand

Before the outbox, an entity that had to deliver a message kept delivery flags in its state, with a retry timer and an acknowledge command:

```csharp
// Do not do this.
(Paid s, PaymentRecorded) => ctx.Persist(new LedgerPostingPending(s.Amount))
    .Schedule("ledger-retry", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1), new RetryLedgerPosting()),
(Paid { LedgerPending: true } s, RetryLedgerPosting) => ctx.None().Run<ILedger>((ledger, ct) => ledger.PostAsync(s.Amount, ct)),
(Paid, LedgerPostingAcknowledged) => ctx.Persist(new LedgerPostingDelivered()).CancelTimer("ledger-retry"),
```

The library already does this. Issue the effect with `Execute`, and register its handler with `Retry.UntilAcknowledged`.

## `Run`: cheap work you can lose

`Run` is not recorded: no journal row, no redelivery, no answer for `Decide`, and it is lost in a crash. If you need delivery guaranteed, write an effect handler and use `Execute`.

`Run` takes a lambda against one service, inline in `Decide`. There is no effect record, no handler and no second arm:

```csharp
(Issued, SendReminder) => ctx.None().Run<IInvoiceMailer>((mailer, ct) => mailer.RemindAsync(ct)),
```

If the mail server is down, a warning is logged and the invoice never knows.

`Run` is awaited in place, so everything after it in the chain waits for it. This reply leaves only after the read model is written, and the caller can read what it wrote:

```csharp
(Issued s, RecordPayment p) when p.Amount == s.Total =>
    ctx.Persist(new PaymentRecorded(p.Amount))
        .Run<IInvoiceReadModel>((model, ct) => model.MarkPaidAsync(ctx.EntityId, ct))
        .Reply(p, new PaymentAccepted()),
```

If the write fails, it is logged and the reply still goes out. An `Execute` in the same place does not hold the reply: the reply leaves at once, while the handler is still running. The reply then means "recorded", not "done".

Use `Run` when nothing in the entity depends on the result and losing one is acceptable: a cache invalidation, a metric. If you want to know whether a `Run` succeeded, it is an `Execute`.

## `Tell` and `Send`

`Tell` sends a message to an actor, and `Send` sends a command to another entity (see [Talking to other entities](talking-to-other-entities.md)). Neither is recorded. A `Send` gets the target's reply through `Accepts`, but a send lost in a crash is not sent again. To tell another entity a fact that must not be lost, write an effect handler that sends it and use `Execute`.

| | Recorded | Attempts | `Decide` gets |
|---|---|---|---|
| `Execute`, handler without a policy | yes, with the decision's events | one | the answer, or a give-up: `Failed`, `TimedOut`, `Interrupted` |
| `Execute`, handler with a policy (the outbox) | yes, with the decision's events | until acknowledged or the policy ends | the answer, or a give-up: `RetriesExhausted` |
| `Start` of a workflow | yes, with the decision's events | until answered | nothing |
| `Run` | no | one, best effort | nothing |
| `Tell` | no | one, best effort | nothing |
| `Send` | no | one, best effort | the reply, through `Accepts` |

## In tests

`Execute` is data, so a test compares it by value. `Run` holds a lambda, so a test can only see that one was issued:

```csharp
// Run: only its type.
DeciderScenario.For(Invoice.Definition)
    .Given(issued)
    .When(new SendReminder())
    .ThenEffects(typeof(RunEffect));

// Execute: the effect itself, and each way it can end.
DeciderScenario.For(Invoice.Definition)
    .Given(issued)
    .When(new SendReminder())
    .ThenExecuted(new MailReminder("billing@example.com"));

DeciderScenario.For(Invoice.Definition)
    .Given(issued)
    .WhenGaveUp(new MailReminder("billing@example.com"), GaveUpReason.Interrupted)
    .ThenEffects(typeof(ScheduleEffect));

// The handler is a plain class. EffectDelivery has a public constructor.
var handler = new MailReminderHandler(new FakeInvoiceMailer(delivers: false));
var delivery = new EffectDelivery(new EffectId("invoices/inv-1/7", 7), Attempt: 1);
Assert.True(await handler.HandleAsync(new MailReminder("billing@example.com"), delivery, ct) is ReminderBounced);
```

---

Next: [Effects with outcomes](effects-with-outcomes.md)
