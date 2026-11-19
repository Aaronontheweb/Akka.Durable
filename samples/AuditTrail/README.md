# Audit trail sample

This sample builds an audit trail for two entities and a timer. The entities contain no audit code.

An `Order` entity places an order, asks a `Stock` entity to reserve the item (a recorded effect), and sets a timer. If nobody pays before the timer fires, the order expires itself. The trail says who did each thing, through which door, and what started it.

It uses the observer from [Observing what entities do](../../docs/observing-what-entities-do.md) and the headers from [Headers beside a command](../../docs/headers-beside-a-command.md). The journal and the reminders are in memory, so nothing else needs to run.

## Run it

```text
dotnet run --project samples/AuditTrail --urls http://127.0.0.1:5187
```

In another terminal. The timer is 5 seconds; set `ExpireAfterSeconds` to change it.

```text
curl -X POST -H "X-User: ann" http://127.0.0.1:5187/orders/o-1
curl -X POST -H "X-User: ann" "http://127.0.0.1:5187/orders/o-2?item=gadget"
curl -X POST -H "X-User: bob" http://127.0.0.1:5187/orders/o-2/pay
curl http://127.0.0.1:5187/audit
```

Wait about 5 seconds before the last call, to see the expiry. This is the output:

```text
WHEN         ENTITY         EVENT            WHO      DOOR                     ACTION    STARTED BY
15:45:55.932 order/o-1      OrderPlaced      ann      POST /orders/o-1         ae8beb9b  Command
15:45:55.976 stock/widget   StockReserved    ann      POST /orders/o-1         ae8beb9b  Command
15:45:55.992 order/o-1      StockConfirmed   ann      POST /orders/o-1         ae8beb9b  EffectAnswer order/o-1/2
15:45:56.030 order/o-2      OrderPlaced      ann      POST /orders/o-2         29ba97ef  Command
15:45:56.032 stock/gadget   StockReserved    ann      POST /orders/o-2         29ba97ef  Command
15:45:56.032 order/o-2      StockConfirmed   ann      POST /orders/o-2         29ba97ef  EffectAnswer order/o-2/2
15:45:56.086 order/o-2      OrderPaid        bob      POST /orders/o-2/pay     4a129f87  Command
15:46:00.987 order/o-1      OrderExpired     system   timer expire             dc3805e7  Timer expire
```

Order `o-1` was placed by ann. The stock entity recorded the reservation for the same person with the same action id. Nobody paid, so the timer expired the order, and the system did it. Order `o-2` was paid by bob. Its timer fired too, but a paid order ignores it.

## Where the audit code is

All of it is in the [`Audit/`](Audit) folder:

- `AuditContext.cs` holds who, door and action id in an `AsyncLocal`. A middleware (in `AuditSetup.cs`) opens it for each request from the `X-User` header. A real application takes the person from its sign-in, never from a header that a caller can set.
- `AuditPropagator.cs` is the `IHeaderPropagator`. It writes the context into the headers of each command and reads it back before an effect handler runs. When a timer fires, it says the system did it and names the timer.
- `AuditCollector.cs` is an actor that listens to `IDurableObservation` on the event stream. It writes one row for each `EventSaved` to a small in-memory store. `GET /audit` prints the store.

`Program.cs` has three calls into that folder: `AddAuditTrail()`, `WithAuditCollector()` and `UseAuditTrail()`.

## What the business code does not contain

The [`Shop/`](Shop) folder has the commands, events, deciders and the effect handler. It has no audit field, no parameter for a person and no call to anything in `Audit/`. The effect handler asks the stock entity through a plain `IEntityRefs`. The library restores the context before the handler runs, and the propagator puts it beside the command again.

## Limits

- The event stream is in memory and local to one node. If the process stops between the journal write and the collector, that row is lost. The collector also misses what happens while it is not running. If the trail must be complete, read the journal.
- Headers are stored in the journal with each recorded effect. Do not put secrets or personal data in them. Use ids. This sample carries a short name so the output is easy to read; a real application carries the id of the person.
- A timer does not carry the headers of the command that set it. That is why the propagator marks the timer's decision itself, as the system.
- The event stream publisher does not work with `akka.actor.serialize-messages = on`, because the observations are not serializable. Leave that setting off.
