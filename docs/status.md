# Status and direction

Part of the [Akka.Durable guide](../README.md#guide).

Working today:
- Decide, persist, evolve, then effects.
- Recovery, which fails on journal entries it can't interpret. An `Execute` that was in flight gives up as `Interrupted`; `Resume` then lists other work still pending.
- Reminders.
- Local and sharded hosting.
- Per-command typed entity refs and state reads.
- `Execute`, recorded in the same journal write as the decision's events and attempted once. `Decide` gets the answer or a give-up (`Failed`, `TimedOut`, `Interrupted`). Handlers are registered with `AddEffectHandler`, with an answer or without one; the host checks at startup that each entity effect has a serializer and one handler. The snapshot keeps the effects that are pending. See [Effects](effects.md).
- `Send` and `Accepts` between entities; `Run` and `Tell`, light and not recorded.
- An entity that starts, notifies and cancels a workflow from a decision: `Uses`, checked when the host starts; `Start`, which the library completes, also after a restart; `Notify` and `Cancel`, whose answers are commands. See [Starting a workflow from an entity](starting-a-workflow-from-an-entity.md).
- C# compiler contract tests and F# DU command/reply tests.
- Workflows: `WorkflowDefinition` with `WithDurableWorkflow`, work that is issued once, give-up facts, commands sent to entities from `Work`, timers, several things in one state, facts from outside (`Notify`, `Cancel`, `Accepts`), and refs with `Start`, `GetStateAsync` and `WaitForOutcome`. See [Workflows](workflows.md).
- Serialization of union commands, events and state. Round-tripped through the V2 serializers, and through a real journal (Akka.Persistence.Sql on SQLite) for events and snapshots.
- Message serialization checks in the TestKit. `VerifyMessageSerialization` runs a spec with `akka.actor.serialize-messages = on`; it stays off by default. Entities and workflows whose events are C# unions persist, restart and recover with it on.

Not done yet:
- **Snapshot retention tests.** Snapshots are written and recovered, but pruning old snapshots and deleting covered events aren't covered by tests yet.
- **Subscriptions.** State can be read on demand, but there are no state or event subscriptions yet.
- **Entity contracts and proxies.** `Send` resolves the target by command type in the same actor system; addressing an entity type hosted only on other nodes, without its definition, is tracked in [#30](https://github.com/Aaronontheweb/Akka.Durable/issues/30).
- **Redelivery of effects (the outbox).** `Execute` is recorded but attempted once; retry policies on `AddEffectHandler`, redelivery and a wake reminder after a crash are the second half of [#69](https://github.com/Aaronontheweb/Akka.Durable/issues/69).
- **Workflow features.** Retry policies, `Again` with a delay, and a scenario test kit for workflows.
- **Workflows started by a starter, phase 2** ([#63](https://github.com/Aaronontheweb/Akka.Durable/issues/63)). A workflow does not record which entity started it, its outcome is not delivered to that entity, and a workflow cannot start another workflow.
- **Source generation.**
- **Agents.** Much later, once the foundations exist.

The bounded Native AOT smoke checks command conversion, typed replies, and effect construction; it does not certify the actor system or persistence plugins for AOT:

```sh
dotnet publish tests/Akka.Durable.AotSmoke -c Release -r linux-x64 -p:PublishAot=true -o ./artifacts/aot-smoke
./artifacts/aot-smoke/Akka.Durable.AotSmoke
```
