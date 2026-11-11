# Serialization

Part of the [Akka.Durable guide](../README.md#guide).

Events, effects and snapshots go to the journal, scheduled commands go to the reminder store, and in a cluster commands, replies and state reads cross the network. All of them are serialized with [Akka.Serialization.V2](https://getakka.net/articles/serialization/source-generated-serialization.html):

- **Give the union itself a manifest, as well as each case.** With `[AkkaSerializable(Manifest = "...")]` on the union, a union value round-trips as the union. That's what the entity's handlers match on.
- **Register the serializer** next to the entity: `akka.WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails)`.
- **`WithDurableEntity` checks this at startup.** If `TState`, `TCommand` or `TEvent` would fall back to the default JSON serializer, the host fails to start and the error names the types. Akka.Durable registers its own serializer (id 22560) for the framework's messages.
- **Recovery fails on an entry it can't read** rather than continuing with missing state. `EntityOptions.SkipUnrecognizedJournalEntries` lets an entity skip such entries, logging an error for each. An `IReadEventAdapter` on the journal can repair or drop them instead.
- **Effects are journaled.** An effect an entity issues with `Execute` is written in the same write as the decision's events, inside a record of Akka.Durable's own (serializer 22560). The effect itself is written by the serializer bound to its type, so it needs one: mark it `[AkkaSerializable(Manifest = "...")]` under the serializer that holds the entity's commands. `WithDurableEntity` checks every effect type that has a handler registered with `AddEffectHandler` and implements `IEffect<TCommand>` of the entity; if one would fall back to the default serializer, the host fails to start and the error names the effect and its handler. Evolve effect types like events: they are a long-lived contract too.
- **The journal holds Akka.Durable's records between your events.** An entity's journal now has a record for each effect issued, answered or given up, so `StateSnapshot.SequenceNr` counts them too. A snapshot is stored as Akka.Durable's own record, which holds your state and the effects pending at that time. A snapshot that is a bare state, written by an earlier version, is still read.
- **Other effects are not journaled.** `Run` and `Tell` stay in the process. The command a `Send` carries is delivered to the target through its shard region, so it and the reply are serialized like any other command to that entity. A workflow's work is not journaled as an effect: the workflow's state says what its work is. Outcome commands and events are part of the issuing entity's protocol and are serialized like the rest of it.

Stored events and snapshots are a long-lived contract, so evolve these types by extension only:

- **Add, don't change.** New fields must be nullable. New union cases and new messages must be deployed to every reader before any node writes them.
- **Never change or reuse a manifest or a field index**, and never remove a union case that may already be in a journal. Mark it `[Obsolete]`, stop writing it, and keep handling it in `Evolve`.
- **Case order doesn't matter;** cases are matched by manifest. Renaming a C# type is safe as long as its manifest stays the same.
- **Switching a union to a closed record hierarchy, or back,** changes the top-level manifest. Treat it as a migration.

---

Next: [Hosting](hosting.md)
