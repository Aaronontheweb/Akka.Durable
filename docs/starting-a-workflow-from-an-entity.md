# Starting a workflow from an entity

Part of the [Akka.Durable guide](../README.md#guide).

An entity can start, notify and cancel a [workflow](workflows.md) from a decision. You name the workflow by its definition, so the compiler checks the input and the fact. The library sends the call after the decision's events are journaled.

The example on this page is an invoice. When the invoice is issued, it starts a reminder campaign for it. When the invoice is paid, it tells the campaign. When the invoice is written off, it cancels the campaign.

## Declare the workflow with `Uses`

The entity's definition names every workflow type it calls:

```csharp
public static readonly EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition =
    new EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent>(
            new EntityTypeName("invoices"), _ => new Draft(), Decide, Evolve)
        .Uses(ReminderCampaign.Definition);
```

Host the workflow type in the same actor system:

```csharp
akka.WithDurableEntity(Invoice.Definition);
akka.WithDurableWorkflow(ReminderCampaign.Definition);
```

The host checks this when it starts. If a workflow type that an entity uses is not hosted, the host does not start. The error names the entity type, the workflow type and `WithDurableWorkflow`. The order of the two calls does not matter.

A decision that calls a workflow type that `Uses` does not name does not reach the workflow. The entity logs an error and gives the call up with `GaveUpReason.Failed`. It does not restart. `DeciderScenario` fails the test, so you find the mistake before you deploy.

## Start

`Start` takes the workflow's definition, the workflow's id and its input. Choose the id yourself and derive it from the entity's id. A workflow id is used once.

```csharp
static WorkflowId CampaignOf(EntityId invoice) => new($"reminders-{invoice.Value}");

(Verifying v, RecipientVerified) =>
    ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
        .Start(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), new CampaignInput(ctx.EntityId.Value, ctx.UtcNow.AddDays(30)))
        .Reply(v.Request, new InvoiceAccepted(ctx.EntityId)),
```

This one line is all you write. The library completes the start:

- It stores a durable reminder for the start, then sends the start. If the reminder cannot be stored, the start is not sent, and the entity restarts like for any reminder that cannot be stored.
- When the workflow answers `Started`, `AlreadyRunning` or `AlreadyFinished`, the library removes the reminder.
- If the start gets no answer, because the call failed, ran out of time, or the process stopped, the reminder fires and the library sends the start again. It does this every `EntityOptions.EffectTimeout` (30 seconds by default) until the workflow answers. Each attempt that gets no answer is logged as an error.
- The reminder survives a restart, so a start that was in flight when the process stopped is sent again by the next incarnation of the entity.

Sending a start again is safe. A start is addressed by id: the workflow answers a second start with "already running" or "already finished", and starts nothing.

You do not keep anything in the entity's state for this. The entity's events are journaled as they always are.

There is one gap. The reminder is stored after the events are journaled, not in the same write. If the process stops between the two, the start is lost. This is the same rule as for `Schedule`. If a start must never be lost, keep "the campaign is started" in your state and also issue the start from `Resume` (see below). A repeated start is harmless.

### When the entity wants the answer

The second form of `Start` takes `then` and `gaveUp`. The answer becomes one of the entity's commands:

```csharp
ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
    .Start(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), new CampaignInput(ctx.EntityId.Value, ctx.UtcNow.AddDays(30)),
        then: answer => new CampaignStartAnswered(answer.Value is Started),
        gaveUp: g => new CampaignUnreachable(g.Reason))
```

`then` receives a `StartResult<TOutcome>`: `Started`, `AlreadyRunning` or `AlreadyFinished<TOutcome>`. `gaveUp` receives a `WorkGaveUp` when the start failed or ran out of time. Then the library stops sending the start again: your entity decides. It may issue the start again.

The functions belong to the incarnation that issued the start. After a restart, the library still sends a start that was not answered, but it cannot call `then`. An entity that needs the answer after a restart keeps "waiting for the campaign to start" in its state, and issues the same `Start` from `Resume`:

```csharp
public static Handled<InvoiceCommand, InvoiceEvent> Resume(InvoiceState state, InvoiceContext ctx) =>
    state switch
    {
        StartingCampaign s => ctx.None().Start(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), s.Input,
            then: answer => new CampaignStartAnswered(answer.Value is Started),
            gaveUp: g => new CampaignUnreachable(g.Reason)),
        _ => ctx.None(),
    };
```

## Notify

`Notify` sends a fact to a running workflow. The compiler checks that the fact is of the workflow's fact type. `then` and `gaveUp` are both required:

```csharp
(Issued s, RecordPayment p) when p.Amount == s.Total =>
    ctx.Persist(new PaymentRecorded(p.Amount))
        .Notify(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), new InvoicePaid(),
            then: answer => new CampaignTold(answer.Value is Delivered or Held),
            gaveUp: g => new CampaignUnreachable(g.Reason))
        .Reply(p, new PaymentAccepted()),
```

`then` receives a `DeliveryResult<TOutcome>`:

| Answer | Meaning |
|---|---|
| `Delivered` | The fact was journaled and decided. |
| `Held` | The fact was journaled. The workflow decides it when its work in flight settles. |
| `WorkflowNotStarted` | No workflow with this id was started. Nothing was journaled. |
| `AlreadyFinished<TOutcome>` | The workflow has finished. Nothing was journaled. It carries the outcome. |

`gaveUp` receives a `WorkGaveUp` with `GaveUpReason.Failed` or `GaveUpReason.TimedOut`. A time-out does not mean the fact was not journaled.

The library sends a fact once. It does not send it again, not after a failure and not after a restart: the workflow would journal the fact twice. If your entity wants to send it again, it decides so, for example from the `CampaignUnreachable` command.

## Cancel

`Cancel` asks the workflow to stop. The workflow's `Cancel` function turns the request into a fact, and its table decides what the cancel means. `reason` and `by` are required. Use an id for `by`, not a person's name, because the value goes into the journal and the log:

```csharp
(Issued, WriteOff w) =>
    ctx.Persist(new WrittenOff(w.Reason))
        .Cancel(ReminderCampaign.Definition, CampaignOf(ctx.EntityId), w.Reason, by: "invoices",
            then: answer => new CampaignTold(answer.Value is Delivered or Held),
            gaveUp: g => new CampaignUnreachable(g.Reason)),
```

`Cancel` answers like `Notify`, and is sent once, like `Notify`.

## What the calls share

The three calls use the entity's `Execute` path:

- Each call has the time limit `EntityOptions.EffectTimeout`.
- The entity does not wait. It handles other commands while a call is in flight. Waiting for a workflow is a state of the entity.
- An entity with a call in flight is not passivated until the call is answered or has run out of time.
- The answer is handled on behalf of the command that issued the call, so `Reply` in that decision reaches the original asker.
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
