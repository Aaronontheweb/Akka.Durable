# Hosting

Part of the [Akka.Durable guide](../README.md#guide).

Register entity refs and effect handlers with DI, then host each entity type either locally or in a shard region:

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDurableEntities();
builder.Services.AddScoped<IInvoiceMailer, SmtpInvoiceMailer>();
builder.Services.AddEffectHandler<VerifyRecipient, InvoiceCommand, VerifyRecipientHandler>();

builder.Services.AddAkka("billing", (akka, sp) =>
{
    // Configure an Akka.Persistence journal / snapshot store and Akka.Reminders here.

    akka.WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails);
    akka.WithDurableEntity(Invoice.Definition);
    // or, in a cluster:
    // akka.WithDurableEntity(Invoice.Definition, new ShardedHosting { Role = "billing" });
});

using var host = builder.Build();
await host.StartAsync();
```

Workflow types are hosted the same way, with `WithDurableWorkflow`. See [Workflows](workflows.md).

This starts the infrastructure for the entity type. Individual invoices are created or recovered on their first message, as shown in [Sending commands](sending-commands.md). In a console application, call `await host.StopAsync()` when finished; a web application uses its normal host lifetime.

Without `ShardedHosting` the entity type runs in a local host actor in this process. `ShardedHosting` holds the sharding settings an application chooses: the role, where sharding stores its state, and remember-entities. It has no stop message and no idle timeout, because Akka.Durable sets both. In both modes, `.Stop()` uses sharding's `Passivate` protocol, so an entity that stops itself isn't restarted when remember-entities is enabled. Don't run the same entity type in local mode on more than one node against the same journal. When using Akka.Reminders with locally hosted entities, point it at them with `WithResolver(sys => new DurableEntityReminderResolver(sys))`.

An entity type that issues `Execute` (it has effect handlers registered with `AddEffectHandler`) or calls workflows (it has `Uses`) needs Akka.Reminders: each entity keeps one reminder, `durable-effects-wake`, that wakes it after a crash or a passivation while effects are pending. The host does not start without Akka.Reminders. It also does not start when an entity type has an effect handler for its command type and no `GaveUp` on its definition. See [Effects](effects.md).

In both modes an entity is passivated after `EntityOptions.PassivateIdleAfter` (default 2 minutes) without messages, and its next message reactivates it from the journal; set it to `TimeSpan.Zero` to disable. It is the only setting for idle passivation in both modes. Sharding does not passivate idle entities while remember-entities is on.

In sharded mode, entity IDs are spread across `EntityOptions.NumberOfShards` shards (default 100). It's on the entity definition so every node uses the same value; changing it after deployment moves entities to different shards and needs a full cluster restart. Local hosting ignores it.

---

Next: [Sending commands](sending-commands.md)
