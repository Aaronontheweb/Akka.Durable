# Starting a workflow from an entity

Part of the [Akka.Durable guide](../README.md#guide).

An entity can start, notify and cancel a [workflow](workflows.md) from a decision. You name the workflow by its definition, so the compiler checks the input and the fact. Each call is journaled in the same write as the decision's events, and sent after the write.

The example on this page is an invoice. When the invoice is issued, it starts a reminder campaign for it. When the invoice is paid, it tells the campaign. When the invoice is written off, it cancels the campaign.

## Declare the workflow with `Uses`

The entity's definition names every workflow type it calls. To notify or cancel it, the declaration also says how the answer and the give-up become the entity's commands:

```csharp
public static readonly EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition =
    new EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent>(
            new EntityTypeName("invoices"), _ => new Draft(), Decide, Evolve)
        .Uses(ReminderCampaign.Definition,
            told: answer => new CampaignTold(answer.Value is Delivered or Held),
            gaveUp: g => new CampaignUnreachable(g.Reason));
```

`told` and `gaveUp` live on the definition, not on the call, so they still work after a restart, when the library has only the journaled call. An entity that only starts the workflow can write `.Uses(ReminderCampaign.Definition)`.

Host the workflow type in the same actor system, and configure Akka.Reminders:

```csharp
akka.WithDurableEntity(Invoice.Definition);
akka.WithDurableWorkflow(ReminderCampaign.Definition);
```

The host checks this when it starts. If a workflow type that an entity uses is not hosted, the host does not start. The error names the entity type, the workflow type and `WithDurableWorkflow`. The order of the two calls does not matter. An entity that uses a workflow also needs Akka.Reminders: the host does not start without it.

A decision that calls a workflow type that `Uses` does not name does not reach the workflow. The call is not journaled, and the entity logs an error. It does not restart. The same happens to a `Notify` or `Cancel` when `Uses` has no `told` and `gaveUp`. `DeciderScenario` fails the test in both cases, so you find the mistake before you deploy.

## Start

`Start` takes the workflow's definition, the workflow's id and its input. Choose the id yourself and derive it from the entity's id. A workflow id is used once.

```csharp
static WorkflowId CampaignOf(EntityId invoice) => new($"reminders-{invoice.Value}");

(Verifying v, RecipientVerified) =>
    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
        .Start(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), new CampaignInput(ctx.EntityId.Value, ctx.UtcNow.AddDays(30)))
        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId)),
```

This one line is all you write. The library delivers the start:

- The start is journaled in the same write as `InvoiceIssued`. Both are stored, or neither is.
- It is sent after the write. If it gets no answer, because the call failed, ran out of time, or the process stopped, it is sent again: after 1 second, then twice as long each time, up to 1 minute, until the workflow answers. After a crash, the entity's wake reminder brings it back to send the start, with nobody sending it a message.
- The workflow answers `Started`, `AlreadyRunning` or `AlreadyFinished`. Any of them ends the delivery. The answer is not delivered to the entity.

Sending a start again is safe. A start is addressed by id: the workflow answers a second start with "already running" or "already finished", and starts nothing.

You do not keep anything in the entity's state for this.

## Notify

`Notify` sends a fact to a running workflow. The compiler checks that the fact is of the workflow's fact type:

```csharp
(Issued s, RecordPayment p) when p.Amount == s.Total =>
    ctx.Persist(new PaymentRecorded(p.Amount))
        .Notify(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), new InvoicePaid())
        .Reply(p, new PaymentAccepted()),
```

`told` on `Uses` receives a `DeliveryResult<TOutcome>`:

| Answer | Meaning |
|---|---|
| `Delivered` | The fact was journaled and decided. |
| `Held` | The fact was journaled. The workflow decides it when its work in flight settles. |
| `WorkflowNotStarted` | No workflow with this id was started. Nothing was journaled. |
| `AlreadyFinished<TOutcome>` | The workflow has finished. Nothing was journaled. It carries the outcome. |

`gaveUp` on `Uses` receives a `WorkGaveUp` whose `Work` is the `NotifyWorkflow`, with `GaveUpReason.Failed`, `GaveUpReason.TimedOut`, or `GaveUpReason.Interrupted` when the process stopped before the answer was journaled. A give-up does not mean the fact was not journaled.

The library sends a fact once. It does not send it again, not after a failure and not after a restart: the workflow would journal the fact twice. If your entity wants to send it again, it decides so, from the command `gaveUp` returns.

## Cancel

`Cancel` asks the workflow to stop. The workflow's `Cancel` function turns the request into a fact, and its table decides what the cancel means. `reason` and `by` are required. Use an id for `by`, not a person's name, because the value goes into the journal and the log:

```csharp
(Issued, WriteOff w) =>
    ctx.Persist(new WrittenOff(w.Reason))
        .Cancel(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), w.Reason, by: "invoices"),
```

`Cancel` is answered like `Notify`, through the same `told` and `gaveUp`, and is sent once, like `Notify`. `WorkGaveUp.Work` is the `CancelWorkflow`.

## What the calls share

- Each call is journaled with the decision, and each attempt has the time limit `EntityOptions.EffectTimeout`.
- The entity does not wait. It handles other commands while a call is in flight. Waiting for a workflow is a state of the entity.
- An entity with a call in flight is not passivated until the call is answered or has run out of time.
- The answer of a notify or cancel is handled on behalf of the command that issued the call, so `Reply` in that decision reaches the original asker. After a restart it is handled on behalf of no one.
- No lifetime is shared. The workflow is its own entity, with its own id, journal and reminder. Stopping or restarting the entity does not touch the workflow, and the other way round.

Not available yet: the workflow does not record which entity started it, and its outcome is not delivered to the entity. To learn the outcome, read the workflow with its ref, or have the workflow send a command to the entity from its `Work`.

## Testing

`DeciderScenario` checks the calls by value. See [Testing](testing.md).

```csharp
DeciderScenario.For(Invoice.Definition)
    .Given(new VerificationStarted(command))
    .When(new RecipientVerified())
    .ThenStarted(ReminderCampaign.Definition, new WorkflowId("reminders-test-entity"),
        new CampaignInput("test-entity", new DateTimeOffset(2026, 1, 31, 0, 0, 0, TimeSpan.Zero)));
```

---

Next: [Serialization](serialization.md)
