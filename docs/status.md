# Status and direction

Part of the [Akka.Durable guide](../README.md#guide).

Working today:
- Decide, persist, evolve, then effects.
- Recovery, which fails on journal entries it can't interpret, followed by `Resume` for effects still pending.
- Reminders.
- Local and sharded hosting.
- Per-command typed entity refs and state reads.
- `Execute` with handlers whose outcome is a command; `Send` and `Accepts` between entities; best-effort `Run`.
- C# compiler contract tests and F# DU command/reply tests.
- Workflows: `WorkflowDefinition` with `WithDurableWorkflow`, work that is issued once, give-up facts, commands sent to entities from `Work`, timers, several things in one state, facts from outside (`Notify`, `Cancel`, `Accepts`), and refs with `Start`, `Status` and `WaitForOutcome`. See [Workflows](workflows.md).
- Serialization of union commands, events and state. Round-tripped through the V2 serializers, and through a real journal (Akka.Persistence.Sql on SQLite) for events and snapshots.

Not done yet:
- **Snapshot retention tests.** Snapshots are written and recovered, but pruning old snapshots and deleting covered events aren't covered by tests yet.
- **Message serialization checks in the TestKit.** `VerifyMessageSerialization` stays off by default until [akkadotnet/akka.net#8910](https://github.com/akkadotnet/akka.net/issues/8910) is fixed.
- **Subscriptions.** State can be read on demand, but there are no state or event subscriptions yet.
- **Entity contracts and proxies.** `Send` resolves the target by command type in the same actor system; addressing an entity type hosted only on other nodes, without its definition, is tracked in [#30](https://github.com/Aaronontheweb/Akka.Durable/issues/30).
- **Workflow features.** Retry policies, `Again` with a delay, child workflows, and a scenario test kit for workflows.
- **Source generation.**
- **Agents.** Much later, once the foundations exist.

The bounded Native AOT smoke checks command conversion, typed replies, and effect construction; it does not certify the actor system or persistence plugins for AOT:

```sh
dotnet publish tests/Akka.Durable.AotSmoke -c Release -r linux-x64 -p:PublishAot=true -o ./artifacts/aot-smoke
./artifacts/aot-smoke/Akka.Durable.AotSmoke
```
