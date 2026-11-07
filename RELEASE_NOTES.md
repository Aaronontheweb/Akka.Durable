#### 0.1.0-alpha.1 November 6th 2026 ####

First public release of Akka.Durable, an early-stage exploration of durable entities on Akka.NET. This is a proof of concept, not an official Akka.NET project, and breaking changes are expected between releases.

* **Durable entities without actors.** Describe an entity as an initial state plus `Decide` and `Evolve` functions. The runtime handles persistence, recovery, delivery and effects, so you never subclass an actor.
* **Decisions and effects.** `Decide` returns events to persist and effects to run afterwards. Effects run only after the events commit, in declaration order, and the entity handles no other command until they finish. `Persist`, `Reply`, `Run`, `Schedule`, `CancelTimer`, `Ignore`, `Unhandled`, `Stash` and `Stop` cover the outcomes.
* **Recovery that fails loudly.** An entity fails recovery on a journal entry or snapshot it can't interpret instead of continuing with missing state. `EntityOptions.SkipUnrecognizedJournalEntries` opts an entity out, logging an error per skipped entry.
* **Durable reminders.** `Schedule` stores the reminder through Akka.Reminders and delivers it back to the entity at least once. `DurableEntityReminderResolver` points Akka.Reminders at locally hosted entities.
* **Local and sharded hosting.** `WithDurableEntity` runs an entity type in a local host actor, or in an Akka.Cluster.Sharding region when you pass `ShardOptions`. `.Stop()` uses sharding's passivation protocol in both modes, so an entity that stops itself isn't restarted when remember-entities is enabled.
* **Idle passivation.** Entities passivate after `EntityOptions.PassivateIdleAfter` (default two minutes) without messages and reactivate from the journal on the next one. Set it to `TimeSpan.Zero` to disable.
* **Configurable shard count.** `EntityOptions.NumberOfShards` (default 100) lives on the entity definition so every node agrees on it.
* **Typed entity refs.** `IEntityRefs<TCommand>` hands out refs by entity ID without starting the entity. A ref's identity is the entity type plus the ID and survives the entity moving between nodes.
* **State reads.** `IDurableEntityRefs<TCommand, TState, TEvent>` adds `GetStateAsync`, which waits behind commands the entity already received and returns the state with the sequence number of the last applied event.
* **Serialization.** Commands, events, state and replies serialize through Akka.Serialization.V2 with stable manifests and field indexes. `WithDurableEntity` checks at startup that `TState`, `TCommand` and `TEvent` won't fall back to the default JSON serializer, and names the offending types if they would.
* **Akka.Hosting integration.** `AddDurableEntities` registers entity refs with DI; `WithDurableEntity` wires a definition into the actor system.
* **TestKit.** `Aaron.Akka.Durable.TestKit` adds `DeciderScenario` Given/When/Then helpers for pure decider tests, and `DurableEntityTestKit` for hosting entities against an in-memory journal with failure injection, in-memory reminders and DI.
* **Two packages.** `Aaron.Akka.Durable` carries the entity runtime, refs and hosting integration; `Aaron.Akka.Durable.TestKit` carries the test infrastructure. Both live in the `Akka.Durable` namespace.

Not yet implemented: durable workflows, state and event subscriptions, source generation, and snapshot retention tests. `VerifyMessageSerialization` stays off by default in the TestKit until akkadotnet/akka.net#8910 is fixed.
