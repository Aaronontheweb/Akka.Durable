# Workflow rules

Part of the [Akka.Durable guide](../README.md#guide).

Each rule is one sentence. The reason follows it. The page [Workflows](workflows.md) explains the terms.

**Do not write a catch-all arm in the fact table.** The compiler checks that the switch over state and fact covers every pair, and reports a missing one. A catch-all arm switches that check off. Write out the pairs that should not happen and answer them with `ctx.Unexpected()`, as the checkout's `Decide` does. Then a new state or a new fact does not compile until you place it. The Akka.Durable tests treat compiler warnings as errors for this reason.

**Build every effect from the state and the values in `ctx` only.** `Work` is called again whenever the library needs to know what a state's work is. It must give the same answer each time. A clock value or a new GUID makes the effect different on each call. `WorkContext` has no clock for this reason. It has `Id`, `Input` and `EnteredAt`, and `EnteredAt` does not change.

**In `Decide`, take the time from `ctx.Now`.** Do not read the clock yourself. Anything else a decision needs must arrive as a fact. Then the decision is a function of the state, the fact and the context, and a plain call can test it.

**Register a handler for every effect type.** Work with no registered handler gets no answer. The workflow receives the give-up fact with the reason `Failed`, and its `Error` names the `AddEffectHandler<...>` call to add. An error is logged as well, because a missing handler is a deployment mistake.

**Return an expected failure as a fact.** A declined card is not an error. A handler that throws gives the workflow the give-up fact, which says only that there is no answer. A handler that returns `Declined` lets the table decide with full knowledge.

**Put a stable key in an effect whose service supports idempotency keys.** Work is issued once, but no answer does not mean the work did not happen. A key lets you ask again without doing the work twice.

**When a result is unknown, choose one of four moves in the table.** The give-up fact means a result is unknown. The four moves are:

- Ask again with the same key.
- Look the result up.
- Wait for the service to report it.
- Ask a person.

The library does not choose for you, and it does not issue the work again on its own. The checkout asks a person.

**Choose the workflow id from something durable.** Use a domain key, such as the order number. Add an attempt number when the same work can run more than once. A retried `Start` then reaches the same workflow. An id is used once: a finished workflow is closed and cannot start again.

**Give each workflow type its own input type.** Refs find the workflow type by its input, state, fact and outcome types. `WithDurableWorkflow` rejects a second workflow type with the same four. An input type of its own is the simplest way to keep them apart.

**Do not write a catch-all arm when you read a workflow.** `GetStateAsync` gives the state with your state type. A `switch` expression over a union state is checked by the compiler, as the fact table is. A catch-all arm switches that check off, so a new state would not be reported where the workflow is read.

**Change persisted types by extension only.** The journal holds your input, states, facts and outcomes, and it keeps them for as long as the workflow runs. Add fields and cases. Never change or reuse a manifest or a field index. The full rules are in [Serialization](serialization.md).

---

Next: [Testing](testing.md)
