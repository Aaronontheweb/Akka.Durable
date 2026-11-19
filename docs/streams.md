# Streams

Part of the [Akka.Durable guide](../README.md#guide).

The package `Aaron.Akka.Durable.Streams` puts Akka.Streams stages over entity refs and workflow refs. Use it when the work comes from a stream: a queue, a file, a batch. Each element starts a workflow, sends a command, or waits for a state. The namespace is `Akka.Durable.Streams`.

There are five stages:

| Stage | For |
|---|---|
| `DurableFlow.Start` | One workflow per element. Emits once the start is stored. |
| `DurableFlow.Ask` | A command to an entity, with its reply. |
| `DurableFlow.Notify` | A fact for a running workflow, with its `DeliveryResult`. |
| `DurableFlow.Until` | Wait until a state shows up, for an entity or for a workflow. |
| `DurableSource.Watch` | One entity's or one workflow's state as a source. |

## The rules

Every flow follows the same rules.

1. **Request and response.** There is no fire and forget. A stage that sends something also waits for the answer.
2. **Bounded.** `parallelism` and `timeout` are required. At most `parallelism` elements are in flight. Upstream is pulled only when a slot is free. A finished element that waits for its turn to be emitted counts as in flight. The `timeout` of an element starts when its call begins, not while it waits for a slot.
3. **A missed deadline is an answer, not a failure of the stream.** So is an exception for one element. The stream carries on with the next element. A condition or a plan function that throws gives a `Failed` answer for that element.
4. **Every output carries its input.** The next stage can retry the element or route it aside.
5. **What is in flight is cancelled.** The cancellation token of the call is cancelled when the element's deadline passes, and when the stream stops for any reason: downstream cancels, upstream fails, or the materializer shuts down. A state wait goes away at once.
6. **Order.** `Start`, `Ask` and `Notify` emit in the input's order. `Until` emits as each wait ends, so a slow wait does not hold up the others.

Every flow emits a pair: the input and an `Answer<T>`. An `Answer<T>` is one of three things: the value, `TimedOut`, or `Failed` with the exception.

```csharp
public readonly union Answer<T>(T, TimedOut, Failed);
```

A `switch` over it names the three cases. When the value is itself a union, such as `StartResult<TOutcome>` or `DeliveryResult<TOutcome>`, the `switch` names that union first, and then its cases. The `Notify` sample below shows this.

Calls begin in the input's order, on the stage's own thread. So commands for the same entity are sent in the input's order.

## Start

`Start` starts one workflow per element. It emits once the start is stored. It never waits for the workflow to finish.

```
orders --> Start --> (order, answer) --+-- TimedOut or Failed --> park
                                       '-- a stored start ------> commit
```

The example starts one checkout per order message. The queue is a plain source here, and its offset is yours to commit. The workflow id comes from the element, and the workflow's input comes from the element too. The element is passed on to `commit` only when the start is stored. The rest is parked.

```csharp
return orders
    .Via(DurableFlow.Start(
        checkouts,
        (OrderMessage m) => (new WorkflowId(m.Order), new PlaceOrder(m.Order, m.Total)),
        parallelism: 8,
        timeout: TimeSpan.FromSeconds(5)))
    // A stored start is a value: Started, AlreadyRunning or AlreadyFinished.
    // Only a timeout or an exception means we do not know.
    .DivertTo(park, r => r.Answer is TimedOut or Failed)
    .Select(r => r.Input)
    .RunWith(commit, materializer);
```

## Ask

`Ask` sends one command per element to an entity, and emits its reply.

```
changes --> Ask --> (change, reply) --> report lines
```

The example is a bulk price change. It sends to eight entities at a time, and turns each answer into a report line.

```csharp
return changes
    .Via(DurableFlow.Ask(
        products,
        (PriceChange c) => (new EntityId(c.Sku), new ChangePrice(c.Cents)),
        parallelism: 8,
        timeout: TimeSpan.FromSeconds(10)))
    .Select(r => r.Answer switch
    {
        PriceChanged done => $"{r.Input.Sku}: {done.OldCents} to {done.NewCents}",
        TimedOut => $"{r.Input.Sku}: no answer in time. Check it before you send it again.",
        Failed failed => $"{r.Input.Sku}: failed, {failed.Error.Message}",
    })
    .RunForeach(report, materializer);
```

The command's type gives the reply type, so no type arguments are needed. Akka's own ask timeout, `akka.actor.ask-timeout`, is a timeout too: the answer is `TimedOut`. Its default is no limit.

## Notify

`Notify` sends one fact per element to a running workflow. It emits the workflow's `DeliveryResult`.

```
requests --> Notify --> (request, delivery) --> report lines
```

The example passes retry requests from a support tool to checkouts that wait for a person.

```csharp
return requests
    .Via(DurableFlow.Notify(
        checkouts,
        (RetryRequest r) => (new WorkflowId(r.Order), new OperatorSaidRetry()),
        parallelism: 4,
        timeout: TimeSpan.FromSeconds(5)))
    .Select(r => r.Answer switch
    {
        // The answer is itself a union, so a switch names it first.
        DeliveryResult<CheckoutOutcome> d => d switch
        {
            Delivered => $"{r.Input.Order}: taken",
            Held => $"{r.Input.Order}: taken, decided when the work in flight ends",
            WorkflowNotStarted => $"{r.Input.Order}: no such checkout",
            AlreadyFinished<CheckoutOutcome> => $"{r.Input.Order}: already finished",
        },
        TimedOut => $"{r.Input.Order}: no answer in time",
        Failed failed => $"{r.Input.Order}: failed, {failed.Error.Message}",
    })
    .RunForeach(report, materializer);
```

## Until

`Until` waits for each element until a state shows up. There is one overload for an entity and one for a workflow. The wait is a subscription, not a poll. It is the ref's `WaitForStateAsync`, with a deadline and a token that the stage cancels.

```
shipments --> Start --> Until --+-- TimedOut or Failed --> follow-up
                                '-- finished -------------> attach to the parcel
```

The example starts a short label-printing workflow and waits up to 30 seconds for it to finish. A label that is not done by then is not waited for any longer. It goes to a follow-up.

```csharp
return shipments
    .Via(DurableFlow.Start(
        labels,
        (Shipment s) => (new WorkflowId($"label-{s.Number}"), new PrintLabel(s.Number)),
        parallelism: 8,
        timeout: TimeSpan.FromSeconds(5)))
    // A start that got no answer may still be stored, so look for the workflow anyway.
    .Select(r => r.Input)
    .Via(DurableFlow.Until(
        labels,
        (Shipment s) => new WorkflowId($"label-{s.Number}"),
        (s, status) => status is WorkflowFinished<LabelOutcome>,
        parallelism: 8,
        timeout: TimeSpan.FromSeconds(30)))
    // The label is not done in 30 seconds, or the wait failed: leave it to the follow-up.
    .DivertTo(followUp, r => r.Answer is TimedOut or Failed)
    .Select(r => r.Input)
    .RunWith(attachToParcel, materializer);
```

The condition gets the element and each state that reaches the wait. For an entity it is `Func<TIn, TState, bool>`. For a workflow it is `Func<TIn, WorkflowStatus<TState, TOutcome>, bool>`. Keep it short and do not block in it.

A workflow that was never started has the status `WorkflowNotStarted`. A wait for any other condition then runs to its deadline and answers `TimedOut`. Say in the condition that you accept `WorkflowNotStarted` if you want to see it at once.

## Watch

`DurableSource.Watch` is the state of one entity, or one workflow, as a source. It emits the current state first, and then a state after each later write.

```
order --> Watch --> view model --> until delivered
```

The example maps one order's state to a view model, until the order is delivered.

```csharp
return DurableSource.Watch(order)
    .Select(snapshot => snapshot.State switch
    {
        Open => new OrderView("Open", "Waiting for the warehouse"),
        Packed p => new OrderView("Packed", $"Handed to {p.Carrier}"),
        Arrived a => new OrderView("Delivered", $"Tracking {a.Tracking}"),
    })
    // Watching an entity never ends by itself. Stop at the last state you care about.
    .TakeWhile(view => !view.IsDelivered, inclusive: true)
    .RunForeach(publish, materializer);
```

The watch of an entity never completes by itself, because an entity has no last state. Bound it: `TakeWhile`, `TakeWithin` or `IdleTimeout`. The watch of a workflow completes after the snapshot in which the workflow is finished. A workflow that was never started does not finish, so bound that one too.

When the stream stops, for any reason, the subscription ends. Watching an entity starts it if it is passivated, so a source that stays open keeps the entity alive.

A slow reader gets the newest state it has not read. It can skip states, but it never reads an older state after a newer one. That is right for "what is it now". It is wrong for "everything it did". For every change, use an effect. See [Reading state](reading-state.md#waiting-for-a-state-and-watching-it).

## What a timed-out answer means

`TimedOut` means unknown. It does not mean failed. The entity or the workflow may still have handled the element. The stage stopped waiting for the answer, and nothing more.

- **`Start` is safe to repeat** when the id is computed from the element. Send the element through again. If the first start was stored, the answer is `AlreadyRunning` or `AlreadyFinished`. If it was not, the workflow starts now.
- **A command sent with `Ask` must be safe to repeat** if you send it again. Look at the entity's state first if it is not.
- **`Notify` has the same rule as `Ask`.** The workflow may have taken the fact. Sending it again gives the workflow a second fact, and its table decides what that means.
- **`Until` can be repeated.** It only reads.

## The stream is not durable

The workflows resume after a restart. The stream does not. When the process stops, the elements that were in flight are gone from the stream, and the workflows they started carry on.

So the source must replay from an offset of its own. `Start` emitting an element is the hand-off point: the start is stored, and the workflow is durable from there. Commit the offset of an element when `Start` has emitted it with a stored start.

## Workflows that run for a long time

A workflow can wait on a timer for months. Do not hold a stream slot for it. A slot that waits for months stops the stream.

Use `Start` and move on. If you must know when it ends, do not wait in this stream. Let the workflow send its outcome out with an effect. Use `Until` only for a workflow that ends in seconds, with a `timeout` that you are happy to wait.

---

Next: [Testing](testing.md)
