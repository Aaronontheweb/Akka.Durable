# Sending commands

Part of the [Akka.Durable guide](../README.md#guide).

Inject `IEntityRefs<InvoiceCommand>` as shown in the [README](../README.md#inject-it-send-a-message), then call `Get(id)` to address an invoice. Getting a reference is cheap and does not activate the entity; the first `Ask` or `Tell` does. On activation, a new ID starts from `InitialState`, and an existing entity recovers from its journal and snapshot.

Use the same ID for later commands to the same invoice, and a different ID for another invoice. Calling `Get` again does not reset its state. `Ask` waits for a reply; `Tell` sends without waiting for one. [State-aware references](reading-state.md) support both messaging and state reads.

`Ask` infers its return type from `IReplies<TCommand, TReply>`. Tell-only commands have no interface and cannot be asked. `IEntityRef<TCommand>` does not implement `ICanTell`, so Akka.NET's unchecked Ask extension methods cannot bypass the contract.

`ToCommand()` supplies a direct constructor call for a C# union; F# records can construct their native DU in that method instead. There is no runtime conversion table or per-command registration. F# deciders call `Handled.ToDecision()` explicitly when returning a decision.

A ref's identity is the entity type plus the entity ID. It doesn't change when the entity moves between processes or nodes. Messages always go through the shard region or local host, so a command sent while an entity is stopping is delivered to its next incarnation.

A ref is bound to the node that created it, so it isn't serializable. To refer to an entity inside a message, event or reminder, carry its `EntityId` and resolve it on the receiving side. Each entity type has its own command type, so the receiver knows which factory to use:

```csharp
[AkkaSerializable(Manifest = "audit-invoice-v1")]
public sealed record AuditInvoice([property: AkkaField(0)] EntityId Invoice) : IInvoiceProtocol;

// on the receiving side
var invoice = invoices.Get(audit.Invoice);   // invoices: IEntityRefs<InvoiceCommand>
```

Typed `IEntityRef<T>` fields in messages need generic formatter support in Akka.Serialization.V2, tracked in [akkadotnet/akka.net#8911](https://github.com/akkadotnet/akka.net/issues/8911).

---

Next: [Reading state](reading-state.md)
