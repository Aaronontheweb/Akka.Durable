#### 0.1.0-alpha.6 November 12th 2026 ####

Sixth alpha release of Akka.Durable. A caller can now wait for an entity's or a workflow's state, and watch it, without polling. `WaitForOutcome` is built on this and no longer polls. Akka.Durable is still a proof of concept, not an official Akka.NET project.

* **`WaitForStateAsync(until, cancellationToken)`** on `IDurableEntityRef` and `IWorkflowRef`. It returns the first snapshot whose state satisfies `until`. The token is required: it is the time limit, and when it fires the wait throws `OperationCanceledException`. When the state already satisfies `until`, it returns after one round trip. The predicate runs in the caller's process; the entity never sees it.
* **`WatchStateAsync(cancellationToken)`** on the same refs. An `IAsyncEnumerable` of snapshots: the current state first, then one after each later write that holds events. A slow reader gets the newest snapshot it has not read. For a workflow, the stream ends after the finished snapshot.
* **The guarantee.** On subscribing you get the current state, and then one snapshot after each later write that holds events, in order, each with the sequence number `GetStateAsync` would return at that moment. A snapshot is sent only after its events are journaled and its `Run` effects have finished. When the entity stops, passivates, restarts or moves, the subscription registers again by itself and goes on from the state the entity has then, so states in that time can be missed. This is a live, best-effort push held in memory, not durable delivery: nothing is journaled or sent again, and a reaction that must happen belongs in an effect.
* **`WaitForOutcome(cancellationToken)`** no longer polls. It waits on the subscription. `pollEvery` is removed, and the token is required.
* **Removed: `CommittedEvent<TEvent>` and `EntityUpdate<TState, TEvent>`.** Nothing used them. The event type stays on `IDurableEntityRef`.

**What you change from 0.1.0-alpha.5:** `WaitForOutcome(pollEvery, token)` becomes `WaitForOutcome(token)`, and a call without a token now needs one. Remove any use of `CommittedEvent<TEvent>` or `EntityUpdate<TState, TEvent>`. A loop that reads `GetStateAsync` until the state changes can become one `WaitForStateAsync`. Nothing stored changes: a subscription writes nothing to the journal, the snapshot store or the reminder store. During a rolling upgrade, a subscription to an entity on a node that still runs 0.1.0-alpha.5 does not connect until that node is upgraded, so a wait runs to its token. Upgrade every node before you rely on a subscription.

Not yet implemented: delivery of a workflow's outcome to the entity or workflow that started it, child workflows, a scenario test kit for workflows, event subscriptions, entity contracts and proxies, source generation, and snapshot retention tests.

#### 0.1.0-alpha.5 November 11th 2026 ####

Fifth alpha release of Akka.Durable. A workflow's work can now be retried by a policy, as an entity's effects are, and `Again` can wait before it repeats the work. Akka.Durable is still a proof of concept, not an official Akka.NET project.

* **Retry policies on workflow work.** Register the handler with a policy, for example `AddEffectHandler<ChargeCard, CheckoutFact, ChargeCardHandler>(Retry.Backoff(first, max, attempts))`. These are the same `Retry.Every`, `Retry.Backoff` and `Retry.UntilAcknowledged` as for entities, written once for each effect type. `ctx.Execute` takes no policy. A handler registered with a policy must be safe to repeat.
* **What a policy does.** A failed or timed-out attempt is attempted again after the policy's wait. Each attempt after the first is journaled before it runs, and the wait is a date in the workflow's reminder, so a restart keeps both, and a passivated workflow is woken. After a restart, an attempt that had no answer is attempted again. Between two attempts nothing is in flight, so facts from outside, cancels and timers are decided at once. A move to another state ends the retries, and a late answer of an earlier attempt is dropped.
* **`RetriesExhausted` for workflows.** When a limited policy runs out, the definition's `GaveUp` receives `GaveUpReason.RetriesExhausted`, with the number of attempts in `WorkGaveUp.Attempts` and the last failure in `Error`. `Retry.UntilAcknowledged` never gives up.
* **The idempotency key.** `EffectDelivery.Id` is the same on every attempt of one piece of work, and `Attempt` counts them. Give `delivery.Id.Value` to the other system as the idempotency key.
* **`ctx.Again(after)`.** Keeps the state and issues its work again after the delay. The delay survives a restart and wakes a passivated workflow. It shares the workflow's one reminder with the state's timers and the pending retries, and it does not move the timers. `ctx.Again()` is unchanged.
* **Held facts are decided before a finish.** A fact or a cancel that arrives while work is in flight is held, and the sender gets `Held`. When the work's answer leads to `Finish`, `Decide` now gets the held facts first, in the order they arrived, in the same state. A held fact that moves the workflow on wins, and the first finish does not happen. Otherwise the workflow finishes with the answer's outcome. Before, the held facts were dropped without a log entry. A workflow that holds no fact at its finish behaves as before.
* **A late answer of earlier work is dropped.** An answer to work of a state the workflow has left, or of an entry that `Again` replaced, is no longer decided or held as a fact from outside. It is logged at debug level. This was already the rule for work with a retry policy. A fact type that several pieces of work share can no longer be taken as the answer to the current work.
* **`IDurableJournalRecord`.** Every record that the library writes to a journal implements this public marker: the records of an entity's `Execute`, a workflow's events, and the snapshot envelope. A projection skips them with one type test.

**What you change from 0.1.0-alpha.4:** little. Work whose handler has no policy behaves as before: one attempt, then the give-up fact. A `Decide` that relied on a late answer of earlier work arriving as a fact no longer gets it: send that fact from outside with `Notify`, or keep the work in the state until it answers. A workflow that added a rest state before it finishes, only so that held facts would be decided, can remove that state. A projection that reads the journal can replace its own list of the library's record types with a test for `IDurableJournalRecord`. A journal written by this release cannot be read by 0.1.0-alpha.4 once a workflow has retried work, used `Again(after)`, or finished after held facts.

Not yet implemented: delivery of a workflow's outcome to the entity or workflow that started it, child workflows, a scenario test kit for workflows, state and event subscriptions, entity contracts and proxies, source generation, and snapshot retention tests.

#### 0.1.0-alpha.4 November 11th 2026 ####

Fourth alpha release of Akka.Durable. An entity's `Execute` is now recorded with its decision, and a retry policy makes the library redeliver it, which gives entities a built-in outbox. An entity can also start, notify and cancel a workflow from a decision. This release changes parts of the 0.1.0-alpha.3 API. Akka.Durable is still a proof of concept, not an official Akka.NET project.

* **Every `Execute` is recorded.** The effect is journaled in the same atomic write as the events of the decision that issued it. It is attempted once, and `Decide` always gets its answer or a give-up: `Failed`, `TimedOut`, or `Interrupted` after a crash. A handler that throws is no longer logged and dropped.
* **A built-in outbox.** Register a handler with a retry policy, for example `AddEffectHandler<TEffect, THandler>(Retry.UntilAcknowledged(first, max))`, and the library redelivers the effect until the handler acknowledges it, after a crash, a passivation or a shard move too. `Retry.Every` and `Retry.Backoff` stop after a number of attempts and give up with `RetriesExhausted`. Delivery is at least once, so a handler that is registered with a policy must be safe to repeat.
* **`AddEffectHandler` registers a handler and its route.** A recovered effect finds its handler through the route. An entity effect whose handler has no route is refused at its first `Execute`. Two handlers for one effect type fail at startup.
* **`IEffect<TCommand>`.** An entity effect type implements it to turn a give-up into one of the entity's commands, and `Execute` requires it. Entity effect types are journaled, so they need a serializer; the host checks this at startup.
* **`EffectDelivery`.** Every handler receives a stable effect id and the attempt number, so its work can be made safe to repeat. `IEffectHandler<TEffect>` is the form with no answer for `Decide`.
* **An entity starts, notifies and cancels a workflow.** Declare the workflow with `Uses(definition)` on the entity definition, then call `Start`, `Notify` or `Cancel` on a decision. A start is recorded and delivered until the workflow answers. `Notify` and `Cancel` are recorded and attempted once; their answer and their give-up become commands through `Uses(definition, told:, gaveUp:)`. A workflow that is not hosted fails the host at startup.
* **`Resume` is removed.** A recorded effect comes back by itself. "Ask again after a crash" is an arm in `Decide` on the `Interrupted` give-up.
* **Akka.Reminders is required** for an entity that has effect handlers or uses a workflow; the host refuses to start without it. Timer keys that start with `durable-` are reserved for the library.
* **A finished workflow cancels its wake reminder.**
* **`VerifyMessageSerialization` works with C# union events** on Akka.NET 1.6.0-beta7. It stays off by default.
* **TestKit.** `DeciderScenario` adds `WhenGaveUp`, `ThenStarted`, `ThenNotified` and `ThenCancelled`, and loses `Resumed()`. `DurableEntityTestKit` adds `PendingEffects`.
* **`Run`, `Tell` and `Send` are unchanged.** They are light and not recorded. If delivery must be guaranteed, write an effect handler and use `Execute`.
* **The repository builds with the stable .NET 11 SDK.**

Not yet implemented: retry policies on workflow work, `Again` with a delay, delivery of a workflow's outcome to the entity or workflow that started it, child workflows, a scenario test kit for workflows, state and event subscriptions, entity contracts and proxies, source generation, and snapshot retention tests.

Known issues: an entity that fails right after recovery is restarted with no delay, so a failure that repeats on every recovery restarts it in a loop. A reader of the journal now sees the library's own records between an entity's events, and `StateSnapshot.SequenceNr` counts them. A snapshot written by this release cannot be read by 0.1.0-alpha.3.

#### 0.1.0-alpha.3 November 10th 2026 ####

Third alpha release of Akka.Durable. It makes a workflow read typed, moves to Akka.NET 1.6.0-beta7, refuses an effect time limit that is not positive, and logs a missing effect handler as an error. Akka.Durable is still a proof of concept, not an official Akka.NET project.

* **A workflow is read the way an entity is read.** `GetStateAsync()` on a workflow ref returns `StateSnapshot<WorkflowStatus<TState, TOutcome>>`. The status is `WorkflowNotStarted`, `WorkflowRunning<TState>` with the state as you typed it, or `WorkflowFinished<TOutcome>` with the outcome. For a state type that is a union, a `switch` expression over `WorkflowRunning<TState>.State` needs no catch-all arm. `StateName` gives the name of the state for logs. `Status()` is removed.
* **Workflow refs have four type arguments.** They are now `IWorkflowRef<TInput, TState, TFact, TOutcome>` and `IWorkflowRefs<TInput, TState, TFact, TOutcome>`, in the same order as `WorkflowDefinition`. A workflow type is found by all four types, and `Get` throws `InvalidOperationException` when no hosted workflow type has them. The `WorkflowRef` helper in `DurableEntityTestKit` takes the same four.
* **`SequenceNr` on a workflow read.** It is the sequence number of the hosting entity's journal. Compare it with an earlier read to see whether anything changed. It is not a count of state changes: a fact that keeps the state also adds one.
* **An effect time limit of zero or less is refused.** `WithDurableEntity` and `WithDurableWorkflow` throw `ArgumentOutOfRangeException` when `EntityOptions.EffectTimeout` is zero or negative. `Timeout.InfiniteTimeSpan` is negative, so it is refused too. The message names the entity or workflow type and the value.
* **A missing effect handler is an error in the log.** When an entity or a workflow issues an effect with `Execute` and no `IEffectHandler` is registered for it, the library logs an error. The entry names the effect type, the handler interface to register, and the entity or workflow type and ID. It does not contain the effect's contents. A workflow still receives the give-up fact with the reason `Failed`, and `WorkGaveUp.Error` names the interface. An entity still fails and restarts, as before.
* **Akka.NET 1.6.0-beta7.** It contains the fix for the command-order issue listed for 0.1.0-alpha.2 (akkadotnet/akka.net#8942).

Not yet implemented: workflow retry policies, `Again` with a delay, child workflows, a scenario test kit for workflows, state and event subscriptions, entity contracts and proxies, source generation, and snapshot retention tests. `VerifyMessageSerialization` stays off by default in the TestKit (see akkadotnet/akka.net#8910).

Known issue: an entity that fails right after recovery is restarted with no delay, so a failure that repeats on every recovery restarts it in a loop.

#### 0.1.0-alpha.2 November 10th 2026 ####

Second alpha release of Akka.Durable. It adds durable workflows and effects whose result the entity decides on, and it changes parts of the 0.1.0-alpha.1 API. Akka.Durable is still a proof of concept, not an official Akka.NET project.

* **Durable workflows.** A workflow is a finite process that survives restarts. It starts once, moves through states that you define, and ends once with an outcome. A `WorkflowDefinition` holds five functions: `Start`, `Work`, `Decide`, `Cancel` and `GaveUp`.
* **Work is issued once.** When a workflow enters a state, the library issues that state's work one time and does not issue it again on its own. Work that fails, times out or is interrupted by a restart gives the workflow the fact that `GaveUp` returns, and `Decide` chooses what happens next.
* **Workflow hosting and refs.** `WithDurableWorkflow` hosts a workflow type, locally or in a shard region. `IWorkflowRefs<TInput, TFact, TOutcome>` hands out refs with `Start`, `Status`, `WaitForOutcome`, `Notify` and `Cancel`.
* **Facts from outside.** `Notify(fact)` sends a fact to a workflow, and `Cancel(reason, by)` asks it to stop. `Accepts<TMessage>` on the definition turns a message of another type into a fact. A fact that arrives while work is in flight is journaled and held, then decided when that work settles.
* **Workflow timers.** `ctx.Timer` in `Work` delivers a fact after a duration or at a date, and `ctx.All` gives one state several pieces of work and timers. Each workflow has one durable reminder, stored through Akka.Reminders, so a workflow type needs Akka.Reminders configured.
* **Commands from a workflow to an entity.** `ctx.Send` in `Work` sends a command to a durable entity, and the reply becomes one of the workflow's facts. The command is sent once. A send that gets no reply gives the workflow the give-up fact.
* **Effects with outcomes.** `Execute(effect)` issues a plain effect record, and the `IEffectHandler<TEffect, TCommand>` registered for it in DI runs it. The handler returns one of the entity's commands, which goes through `Decide`. The entity handles other commands while the handler runs, and a handler that throws or times out is logged and no command arrives.
* **`Run` is best effort.** `Run` no longer takes `onSuccess` or `onFailure`, and no result returns to the entity. A failure or timeout is logged and the decision continues. Use `Execute` when the entity needs the result.
* **`Resume` after a restart.** `EntityDefinition.Resume` lists the effects that are still pending for a recovered state. It runs once after recovery, before the first command, and it may not return events.
* **Entity to entity.** `Send<TOther>(id, command)` sends a command to another durable entity, and `Accepts<TMessage>` on the entity definition turns the reply into one of the sender's commands. `Send` replaces `Tell` to an entity ref from a decision, because `IEntityRef` no longer implements `ICanTell`.
* **Typed replies.** A command declares its reply type with `IReplies<TCommand, TReply>`. `Ask` infers the reply type from the command, and `Reply(request, reply)` takes the command that declared the reply. A command without the interface can be sent with `Tell` only.
* **`DecisionContext` has three type arguments.** It is now `DecisionContext<TState, TCommand, TEvent>`. `Decide` still receives the state as a parameter.
* **A stop waits for `Execute` work.** An entity that is asked to stop while an `Execute` handler is running finishes that work and decides its outcome before it stops. This applies to idle passivation in local and sharded hosting.
* **`ShardedHosting` replaces `ShardOptions`.** `WithDurableEntity` and `WithDurableWorkflow` take a `ShardedHosting` record with `Role`, `StateStoreMode`, `RememberEntities` and `RememberEntitiesStore`. Akka.Durable sets the stop message and idle passivation itself. Use `EntityOptions.PassivateIdleAfter` for the idle timeout.
* **One command type per entity type.** `WithDurableEntity` rejects a second entity type that uses a command type another entity type already uses. The error names both entity types and the command type.
* **Refused messages are logged.** An entity logs a warning when it receives a message that is not one of its commands and that its definition does not accept. The warning names the entity type, the entity ID and the message type.
* **TestKit.** `DeciderScenario` adds `Resumed()`, `ThenExecuted` and `ThenSent`. `DurableEntityTestKit` adds a `WorkflowRef` helper for hosted workflow tests.
* **Documentation.** The README is now a short overview. The guide is in `docs/`, one topic per page, with pages for workflows and workflow rules.

Not yet implemented: workflow retry policies, `Again` with a delay, child workflows, a scenario test kit for workflows, state and event subscriptions, entity contracts and proxies, source generation, and snapshot retention tests. `VerifyMessageSerialization` stays off by default in the TestKit until akkadotnet/akka.net#8910 is fixed.

Known issue: on Akka.NET 1.6.0-beta6, commands that arrive while a decision's asynchronous effect (`Run`, `Schedule` or `CancelTimer`) is running can be handled out of order. This comes from Akka.Persistence, and a fix is being prepared upstream.

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
