# Workflows

Part of the [Akka.Durable guide](../README.md#guide).

A workflow is a finite process that survives restarts. It starts once, moves through states that you define, and ends once with an outcome. Use an entity when something owns state for a long time and answers commands, such as an invoice. Use a workflow when something is a process with an end, such as a checkout: reserve stock, charge the card, finish.

A workflow is hosted as a durable entity, so it uses the same journal, the same hosting and the same effect handlers. You do not write `Decide` and `Evolve` for it. You write one table that says what each fact means in each state.

## What you write

You write four types and one `WorkflowDefinition`.

| Type | What it is |
|---|---|
| Input | The record the workflow starts with. It is available in every state. |
| States | Where the workflow is. Usually a union of records. |
| Facts | Everything that can arrive: answers to work, and messages from outside. The journal records facts. Usually a union. |
| Outcome | The ways the workflow can end. Usually a union. |

The definition holds these functions:

| Function | What it does |
|---|---|
| `Start` | Returns the state a new workflow begins in. |
| `Work` | Returns the work a state needs. |
| `Decide` | The fact table. Returns what a fact means in a state. |
| `Kind` | Says whether an outcome is `Succeeded`, `Failed` or `Cancelled`. |
| `Cancel` | Turns a cancel request into one of your facts. |
| `GaveUp` | Turns the notice "this work got no answer" into one of your facts. |

`WorkflowDefinition` also takes a `WorkflowTypeName`, and optional `WorkflowOptions`.

### Example: a checkout

The checkout reserves stock, charges the card, and undoes the order when the charge is declined. Work that gets no answer sends the checkout to a person.

The types below leave out the serializer attributes, which [Serialization](#serialization) describes. The input and the states:

```csharp
public sealed record PlaceOrder(string Order, int Total);

public sealed record Reserving;
public sealed record Charging(string Reservation);
public sealed record Undoing(CheckoutOutcome Then);                 // compensate, then end with Then
public sealed record NeedsAttention(CheckoutState Was, string Why); // waits for a person

public readonly union CheckoutState(Reserving, Charging, Undoing, NeedsAttention);
```

The facts and the outcomes:

```csharp
public sealed record Reserved(string Reservation);
public sealed record OutOfStock;
public sealed record Charged(string Charge);
public sealed record Declined(string Reason);
public sealed record Undone;
public sealed record UndoRefused;
public sealed record WorkStuck(string What, GaveUpReason Reason);
public sealed record TicketOpened;
public sealed record CancelRequested(string Reason);
public sealed record OperatorSaidRetry;
public sealed record OperatorSaidAbandon;

public readonly union CheckoutFact(
    Reserved, OutOfStock, Charged, Declined, Undone, UndoRefused, WorkStuck, TicketOpened,
    CancelRequested, OperatorSaidRetry, OperatorSaidAbandon);

public sealed record OrderPlaced(string Charge);
public sealed record OrderFailed(string Reason);
public sealed record OrderCancelled(string Reason);

public readonly union CheckoutOutcome(OrderPlaced, OrderFailed, OrderCancelled);
```

The effects are plain records. A handler runs each one:

```csharp
public sealed record ReserveStock(string Order);
public sealed record ChargeCard(string Order, int Total);
public sealed record UndoOrder(string Order);
public sealed record OpenSupportTicket(string Checkout, string Why);
```

`Work` returns the work that a state needs. Every effect is built from the state and the context. `ctx.Execute` makes one piece of work. `ctx.Nothing()` is for a state that only waits.

```csharp
public static WorkList<CheckoutFact> Work(CheckoutState state, WorkContext<PlaceOrder, CheckoutFact> ctx) =>
    state switch
    {
        Reserving => ctx.Execute(new ReserveStock(ctx.Input.Order)),
        Charging => ctx.Execute(new ChargeCard(ctx.Input.Order, ctx.Input.Total)),
        Undoing => ctx.Execute(new UndoOrder(ctx.Input.Order)),
        NeedsAttention attention => ctx.Execute(new OpenSupportTicket(ctx.Id.Value, attention.Why)),
    };
```

`Decide` is the fact table. It has one arm for every pair of state and fact. It has no catch-all arm. A pair that should not happen is written out and answered with `ctx.Unexpected()`. See [Workflow rules](workflow-rules.md).

```csharp
public static Transition<CheckoutState, CheckoutOutcome> Decide(
    CheckoutState state, CheckoutFact fact, DecideContext<PlaceOrder, CheckoutState, CheckoutOutcome> ctx) =>
    (state, fact) switch
    {
        (Reserving, Reserved reserved) => ctx.Next(new Charging(reserved.Reservation)),
        (Reserving, OutOfStock) => ctx.Finish(new OrderFailed("out of stock")),

        (Charging, Charged charged) => ctx.Finish(new OrderPlaced(charged.Charge)),
        (Charging, Declined declined) => ctx.Next(new Undoing(new OrderFailed(declined.Reason))),

        (Undoing undoing, Undone) => ctx.Finish(undoing.Then),
        (Undoing, UndoRefused) => ctx.Again(),

        // Work that got no answer: a person decides. The state it came from is kept.
        (Reserving or Charging or Undoing, WorkStuck stuck) =>
            ctx.Next(new NeedsAttention(state, $"{stuck.What} {stuck.Reason}")),
        (NeedsAttention, TicketOpened) => ctx.Stay(),
        (NeedsAttention, WorkStuck) => ctx.Stay(),

        // A person decides what happens to a checkout that needs attention.
        (NeedsAttention attention, OperatorSaidRetry) => ctx.Next(attention.Was),
        (NeedsAttention attention, OperatorSaidAbandon) => ctx.Next(new Undoing(new OrderFailed(attention.Why))),

        // A cancel undoes the order, unless it is already being undone or waits for a person.
        (Reserving or Charging, CancelRequested cancel) => ctx.Next(new Undoing(new OrderCancelled(cancel.Reason))),
        (Undoing or NeedsAttention, CancelRequested) => ctx.Stay(),

        // Pairs that should not happen, written out so that a new state or fact does not compile until it is placed.
        (Reserving, Charged or Declined or Undone or UndoRefused or TicketOpened) => ctx.Unexpected(),
        (Charging, Reserved or OutOfStock or Undone or UndoRefused or TicketOpened) => ctx.Unexpected(),
        (Undoing, Reserved or OutOfStock or Charged or Declined or TicketOpened) => ctx.Unexpected(),
        (NeedsAttention, Reserved or OutOfStock or Charged or Declined or Undone or UndoRefused) => ctx.Unexpected(),
        (Reserving or Charging or Undoing, OperatorSaidRetry or OperatorSaidAbandon) => ctx.Unexpected(),
    };

public static OutcomeKind Kind(CheckoutOutcome outcome) =>
    outcome switch
    {
        OrderPlaced => OutcomeKind.Succeeded,
        OrderFailed => OutcomeKind.Failed,
        OrderCancelled => OutcomeKind.Cancelled,
    };
```

A handler runs one kind of effect and returns one of your facts. It is an `IEffectHandler<TEffect, TFact>`, the same interface that entities use:

```csharp
public sealed class ChargeCardHandler(IPaymentGateway gateway) : IEffectHandler<ChargeCard, CheckoutFact>
{
    public async Task<CheckoutFact> HandleAsync(ChargeCard effect, CancellationToken cancellationToken)
    {
        var result = await gateway.ChargeAsync(effect.Order, effect.Total, cancellationToken);
        return result.Approved ? new Charged(result.ChargeId) : new Declined(result.Reason);
    }
}
```

`IPaymentGateway` is your own service. It is not part of Akka.Durable. A decline is an expected failure, so the handler returns it as the fact `Declined`.

The definition puts it together:

```csharp
public static readonly WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =
    new(new WorkflowTypeName("checkouts"),
        Start: _ => new Reserving(),
        Work,
        Decide,
        Kind,
        Cancel: request => new CancelRequested(request.Reason),
        GaveUp: failure => new WorkStuck(failure.What, failure.Reason));
```

`failure` is a `WorkGaveUp`. `What` is the name of the effect type. `Reason` is a `GaveUpReason`. `Error` holds the failure's message when there is one.

## Transitions

`Decide` returns a transition. Build one with the `ctx` that `Decide` receives.

- `ctx.Next(state)` moves to a state and issues that state's work.
- `ctx.Finish(outcome)` ends the workflow with an outcome.
- `ctx.Stay()` changes nothing. Use it for a duplicate, or for a fact that no longer matters.
- `ctx.Again()` keeps the state and issues its work again. It is the deliberate way to repeat work.
- `ctx.Unexpected()` is for a pair of state and fact that should not happen. The fact is journaled and a warning is logged. The state does not change.

The journal records every fact, including those answered with `Stay` or `Unexpected`.

## Work is issued once

When a workflow enters a state, the library issues that state's work one time. It does not issue it again on its own.

Sometimes work gets no answer. The workflow then receives the give-up fact: the fact that your `GaveUp` function returns. The fact table decides what it means. This happens in four cases:

| Case | `GaveUpReason` |
|---|---|
| The handler throws. | `Failed` |
| No handler is registered for the effect. | `Failed` |
| The handler does not answer within `WorkflowOptions.AttemptLimit` (30 seconds by default). | `TimedOut` |
| The workflow restarted after it entered the state and before the answer arrived. | `Interrupted` |

For `Interrupted`, the work may have run, may have run in part, or may not have run. Nothing runs again after the restart. The workflow receives the give-up fact for the work that had no answer.

No answer does not mean the work did not happen. A charge can go through and its reply be lost. The table chooses what to do. [Workflow rules](workflow-rules.md) lists the four choices.

The checkout sends every give-up fact to `NeedsAttention`. That state opens a support ticket and waits for a person.

To set the attempt limit, pass options to the definition:

```csharp
Checkout.Definition with { Options = new WorkflowOptions { AttemptLimit = TimeSpan.FromSeconds(10) } }
```

## Timers, and several things in one state

A state can wait for a point in time. `ctx.Timer` takes a duration or a date, and the fact to deliver when it is due:

```csharp
public static WorkList<CampaignFact> Work(CampaignState state, WorkContext<Invoice, CampaignFact> ctx) =>
    state switch
    {
        Waiting => ctx.Timer(ctx.Input.DueAt, new DueDateReached()),
        Reminding reminding => ctx.Execute(new SendReminder(ctx.Input.Id, reminding.Number)),
        Pausing => ctx.Timer(ctx.Input.Pause, new PauseOver()),
        Escalating => ctx.All(
            ctx.Execute(new NotifyCollections(ctx.Input.Id)),
            ctx.Timer(ctx.Input.Grace, new GraceOver())),
    };
```

- A duration is counted from the time the workflow entered the state. That time is journaled, so the timer does not move when `Work` is called again.
- `ctx.All` gives a state several things at once: here a piece of work and a time limit for it.
- When the workflow leaves a state, that state's timers are gone. There is nothing to cancel.

Work and a wait are different. An `Execute` is work: it expects an answer within the attempt limit, and it holds facts from outside while it has none. A timer is a wait: it can last months, it never gives up, and it holds nothing. A fact from outside that arrives while a workflow only waits is decided at once. A timer that is due while work is in flight is held, like a fact from outside.

### The reminder

Each workflow has one durable reminder, stored through Akka.Reminders. It is due at the earliest time the workflow needs to do something: the next timer, or the time by which work in flight must have answered. So a workflow hosts only with Akka.Reminders configured.

A workflow that waits only for facts from outside has no reminder. Nothing wakes it until a fact arrives.

The journal write comes first. Then, before any work is issued and before any reply is sent, the reminder is stored for the state the workflow is now in. Storing it again replaces the old one, so the old reminder stays in place until the journal has the new state.

When a process dies after the journal write and before the reminder is stored, recovery fixes it: whatever activates the workflow next makes it store the reminder for the state it recovered.

The reminder also wakes a workflow whose process died in the middle of a call. Nobody has to send it a message. The workflow recovers, the interrupted work gives up, and the table decides.

A reminder can arrive a little early: Akka.Reminders fetches reminders that are due within its `MaxSlippage` (5 seconds by default). A timer due by the time the reminder was stored for is then decided at once. So a timer can fire up to `MaxSlippage` early, never more, and one timer wakes the workflow once.

When the reminder cannot be stored, because the reminder store is down, the fact is already journaled, but nothing after it happens: no work is issued and the caller gets no reply. The workflow restarts and tries again each time it recovers. When the store is back, the reminder is stored, and the work that was never issued gives up as interrupted. The workflow cannot tell work that was not issued from work that was issued and lost, so the table decides, as for any interrupted work. A caller that got no reply can send the fact again.

While the store is down, a workflow that hit this restarts at once, again and again, until the store is back. Each restart replays the journal and logs an error.

## Facts from outside

A fact does not have to be the answer to work. Code outside the workflow can send one:

- `Notify(fact)` sends one of the workflow's facts. It is on the ref that knows the fact type.
- `Cancel(reason)` asks the workflow to stop. Your `Cancel` function turns the request into a fact, and the table decides what it means in the current state. A cancel is not an outcome until the table finishes the workflow.
- `Accepts<TMessage>(wrap)` on the definition says how a message of another type becomes a fact. An existing actor can then send its own message type to the workflow's address. It gets no reply. A message of the fact type itself is also accepted.

```csharp
public static readonly WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> WithOperatorTool =
    Definition.Accepts<OperatorDecision>(decision =>
        decision.Retry ? new OperatorSaidRetry() : new OperatorSaidAbandon());
```

`OperatorDecision` is a record with a `bool Retry`. It belongs to an operator tool and is not one of the checkout's facts.

`Notify` and `Cancel` return a `DeliveryResult` with four possible replies:

| Reply | Meaning |
|---|---|
| `Delivered` | The fact was journaled and decided. |
| `Held` | The fact was journaled while work was in flight. It is decided when that work settles. |
| `WorkflowNotStarted` | The workflow was never started. Nothing was journaled. |
| `AlreadyFinished<TOutcome>` | The workflow has finished and is closed. Nothing was journaled. The reply carries the outcome. |

A fact is journaled before the caller gets its reply. A fact that is held survives a restart.

Work is in flight when a piece of the current state's work has no answer yet. A fact that arrives then is held. When the work settles, the library decides facts in this order:

1. The work's own fact.
2. Each held fact, in the order it arrived.
3. Only then, the work of the state where those decisions ended.

The work of a state that the workflow passes through on the way is not issued. Only the state where the decisions end gets its work issued.

For example, a cancel that arrives while the card is being charged is held. The charge's fact is decided first. The held cancel is decided next. The work of the state that follows is issued last.

## Hosting and calling

Hosting is the same as for entities. See [Hosting](hosting.md). Call `AddDurableEntities` on the service collection, register one handler for each effect type, register the serializer, and host the workflow type with `WithDurableWorkflow`:

```csharp
builder.Services.AddDurableEntities();
builder.Services.AddScoped<IEffectHandler<ReserveStock, CheckoutFact>, ReserveStockHandler>();
builder.Services.AddScoped<IEffectHandler<ChargeCard, CheckoutFact>, ChargeCardHandler>();
builder.Services.AddScoped<IEffectHandler<UndoOrder, CheckoutFact>, UndoOrderHandler>();
builder.Services.AddScoped<IEffectHandler<OpenSupportTicket, CheckoutFact>, OpenSupportTicketHandler>();

builder.Services.AddAkka("shop", (akka, sp) =>
{
    // Configure an Akka.Persistence journal / snapshot store and Akka.Reminders here.

    akka.WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails);
    akka.WithDurableWorkflow(Checkout.Definition);
    // or, in a cluster:
    // akka.WithDurableWorkflow(Checkout.Definition, new ShardedHosting { Role = "shop" });
});
```

Without `ShardedHosting`, the workflow type runs in a local host actor in this process. With it, the workflow type runs in a shard region. `ShardedHosting` has the same settings as for entities.

One actor system hosts at most one workflow type for each pair of input and outcome type. `WithDurableWorkflow` rejects a second one.

To call a workflow, inject a ref factory. There are two:

- `IWorkflowRefs<TInput, TOutcome>` gives refs that can start a workflow, read its status, wait for its outcome and cancel it.
- `IWorkflowRefs<TInput, TFact, TOutcome>` gives the same refs, which can also `Notify` the workflow with a fact.

```csharp
public sealed class CheckoutService(IWorkflowRefs<PlaceOrder, CheckoutFact, CheckoutOutcome> checkouts)
{
    public async Task<CheckoutOutcome> PlaceAsync(string order, int total, CancellationToken ct)
    {
        var checkout = checkouts.Get(new WorkflowId(order));

        var started = await checkout.Start(new PlaceOrder(order, total), ct);
        if (started.Value is AlreadyFinished<CheckoutOutcome> finished)
            return finished.Outcome;

        return await checkout.WaitForOutcome(TimeSpan.FromMilliseconds(250), ct);
    }
}
```

Getting a ref is cheap. It does not start or activate the workflow.

**You choose the workflow id.** Choose it from something durable, such as the order number. A retried `Start` then reaches the same workflow. **An id is used once.** A finished workflow is closed. It cannot start again.

`Start` returns one of three replies:

| Reply | Meaning |
|---|---|
| `Started` | This call began the run. |
| `AlreadyRunning` | A workflow with this id is running. Nothing changed. |
| `AlreadyFinished<TOutcome>` | A workflow with this id has finished. The reply carries its outcome. |

`Status` reads the condition the workflow is in. It answers at once and never waits for the workflow. It returns one of:

- `WorkflowNotStarted`.
- `WorkflowRunning`, with `State` (the name of the state's type, or for a union the name of the case it holds) and `Since` (when the workflow entered it).
- `WorkflowFinished<TOutcome>`, with `Outcome`, `Kind` and `At`.

`WaitForOutcome(pollEvery)` reads the status every `pollEvery` until the workflow has finished, then returns the outcome. The waiting happens in the caller, not in the workflow. Use it for a workflow that takes seconds. It throws `InvalidOperationException` when the workflow has not been started.

`DurableEntityTestKit` has `WorkflowRef` helpers that return the same refs in a test.

## Serialization

Register a serializer for your own four types: input, state, fact and outcome. You do not register one for anything else. The library's own messages already have a serializer, and they carry your values inside them.

`WithDurableWorkflow` checks this at startup. If one of the four types would fall back to the default serializer, the host fails to start and the error names the type. The effect records are not journaled, so they need no serializer.

Mark each type with `[AkkaSerializable(Manifest = "...")]`, and each union as well as its cases. The rules for how to do this, and for how to change the types later, are in [Serialization](serialization.md).

## Not available yet

These are not in Akka.Durable today:

- Retry policies. To repeat work, the table uses `Again`, or moves to a state with the same work.
- `Again` with a delay. `Again` issues the work at once. A table that answers a timer's own fact with `Again` loops, because the timer is still due.
- Calls to entities from `Work`. `Work` has no `Send`.
- Child workflows.
- A scenario test kit for workflows, like `DeciderScenario` for entities. Test a workflow through the hosted `DurableEntityTestKit`. You can test `Decide` and `Work` as plain functions too.

---

Next: [Workflow rules](workflow-rules.md)
