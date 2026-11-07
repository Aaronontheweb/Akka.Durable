# Akka.Durable

Akka.Durable is an early-stage project exploring durable entities and workflows on Akka.NET. The intended foundation is Akka.Persistence and Cluster Sharding, with a local development and execution story as well as clustered deployments. Agent capabilities are a much later possibility, not part of the initial scope.

The design draws on a functional event-sourcing decider: pure decisions and state transitions, with persistence, recovery, delivery, and other effects handled by a runtime shell. A first vertical slice of durable entities exists; workflows are not implemented yet.

## Getting started

Install the .NET 11 RC SDK pinned in `global.json` (including prerelease SDKs), then run:

```sh
dotnet restore
dotnet build
dotnet test
```

The solution targets `net11.0` and enables preview C# features (C# 15 unions are used throughout). Package versions are managed centrally; Akka.NET packages are pinned to 1.6.0-beta6 (Aaron.Akka.Reminders to 1.6.0-beta5).

| Project | Contents |
|---|---|
| `Akka.Durable.Abstractions` | `EntityId`, `IEntityRef<TCommand>`, `IEntityRefs<TCommand>`, `IDurableEntityRefs<TCommand, TState, TEvent>` |
| `Akka.Durable.Entities` | Entity definitions, decisions and effects, the entity actor, and Akka.Hosting integration |
| `Akka.Durable.TestKit` | Test helpers built on Akka.Hosting.TestKit |
| `Akka.Durable.Workflows` | Empty for now |

## Defining an entity

An entity is an initial state, a `Decide` function and an `Evolve` function. You don't subclass an actor. Its state, commands, events and replies are serialized (see [Serialization](#serialization)), so each one carries a stable manifest and field indexes:

```csharp
public interface IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-draft-v1", AllowEmpty = true)]
public sealed record Draft : IInvoiceProtocol;
[AkkaSerializable(Manifest = "invoice-issued-state-v1")]
public sealed record Issued([property: AkkaField(0)] decimal Total, [property: AkkaField(1)] string Recipient) : IInvoiceProtocol;
[AkkaSerializable(Manifest = "invoice-paid-state-v1")]
public sealed record Paid([property: AkkaField(0)] decimal Total) : IInvoiceProtocol;
[AkkaSerializable(Manifest = "invoice-state-v1")]
public readonly union InvoiceState(Draft, Issued, Paid) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "issue-invoice-v1")]
public sealed record IssueInvoice([property: AkkaField(0)] decimal Total, [property: AkkaField(1)] string Recipient) : IInvoiceProtocol;
[AkkaSerializable(Manifest = "record-payment-v1")]
public sealed record RecordPayment([property: AkkaField(0)] decimal Amount) : IInvoiceProtocol;
[AkkaSerializable(Manifest = "send-reminder-v1", AllowEmpty = true)]
public sealed record SendReminder : IInvoiceProtocol;
[AkkaSerializable(Manifest = "invoice-command-v1")]
public readonly union InvoiceCommand(IssueInvoice, RecordPayment, SendReminder) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-issued-v1")]
public sealed record InvoiceIssued(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient,
    [property: AkkaField(2)] DateTimeOffset At) : IInvoiceProtocol;
[AkkaSerializable(Manifest = "payment-recorded-v1")]
public sealed record PaymentRecorded([property: AkkaField(0)] decimal Amount) : IInvoiceProtocol;
[AkkaSerializable(Manifest = "invoice-event-v1")]
public readonly union InvoiceEvent(InvoiceIssued, PaymentRecorded) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-accepted-v1")]
public sealed record InvoiceAccepted([property: AkkaField(0)] EntityId Id) : IInvoiceProtocol;
[AkkaSerializable(Manifest = "payment-accepted-v1", AllowEmpty = true)]
public sealed record PaymentAccepted : IInvoiceProtocol;
[AkkaSerializable(Manifest = "invoice-rejected-v1")]
public sealed record InvoiceRejected([property: AkkaField(0)] string Reason) : IInvoiceProtocol;

[AkkaSerializer<IInvoiceProtocol>("invoices", 120100)]
public sealed partial class InvoiceSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}
```

`Decide` returns events to persist and effects to run afterwards. `Evolve` applies one event to the state. The invoice state is a union, so each state carries only the data that is valid for it and no sentinel values are needed.

```csharp
using InvoiceContext = DecisionContext<InvoiceCommand, InvoiceEvent>;

public static class Invoice
{
    public static readonly EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition =
        new(new EntityTypeName("invoices"), _ => new Draft(), Decide, Evolve);

    public static Decision<InvoiceCommand, InvoiceEvent> Decide(
        InvoiceState state, InvoiceCommand command, InvoiceContext ctx) =>
        (state, command) switch
        {
            (Draft, IssueInvoice c) when c.Total > 0 =>
                ctx.Persist(new InvoiceIssued(c.Total, c.Recipient, ctx.UtcNow))
                    .Reply(new InvoiceAccepted(ctx.EntityId))
                    .Run<IInvoiceMailer>((mailer, ct) => mailer.SendAsync(c.Recipient, ct))
                    .Schedule("reminder", TimeSpan.FromDays(30), new SendReminder()),

            (Issued s, RecordPayment p) when p.Amount == s.Total =>
                ctx.Persist(new PaymentRecorded(p.Amount))
                    .Reply(new PaymentAccepted())
                    .CancelTimer("reminder"),

            (Issued, SendReminder) => ctx.None().Run<IInvoiceMailer>((mailer, ct) => mailer.RemindAsync(ct)),
            (_, SendReminder) => ctx.Ignore(),

            _ => ctx.None().Reply(new InvoiceRejected("Invalid invoice operation")),
        };

    public static InvoiceState Evolve(InvoiceState state, InvoiceEvent @event) =>
        (state, @event) switch
        {
            (Draft, InvoiceIssued e) => new Issued(e.Total, e.Recipient),
            (Issued s, PaymentRecorded) => new Paid(s.Total),
            _ => throw new InvalidOperationException($"Cannot apply {@event} to {state}."),
        };
}
```

- **Events first, then effects.** Effects run only after the events are committed, in the order they're declared. The entity handles no other command until they finish.
- **`Run` is best effort.** Its service is resolved from a fresh DI scope. It runs in-process and is never replayed during recovery. If it fails without an `onFailure` handler, the actor's supervision handles it.
- **`Schedule` uses Akka.Reminders.** The reminder is stored durably and delivered back to the entity at least once, so `Decide` must tolerate duplicates. Its message must be a `TCommand`.
- **Other outcomes.** `ctx.Ignore()`, `ctx.Unhandled()` and `ctx.Stash()` cover commands you aren't handling now. `.Stop()` passivates the current actor without deleting the entity.

## Serialization

Events and snapshots go to the journal, scheduled commands go to the reminder store, and in a cluster commands, replies and state reads cross the network. All of them are serialized with [Akka.Serialization.V2](https://getakka.net/articles/serialization/source-generated-serialization.html):

- **Give the union itself a manifest, as well as each case.** With `[AkkaSerializable(Manifest = "...")]` on the union, a union value round-trips as the union. That's what the entity's handlers match on.
- **Register the serializer** next to the entity: `akka.WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails)`.
- **`WithDurableEntity` checks this at startup.** If `TState`, `TCommand` or `TEvent` would fall back to the default JSON serializer, the host fails to start and the error names the types. Akka.Durable registers its own serializer (id 22560) for the framework's messages.
- **Recovery fails on an entry it can't read** rather than continuing with missing state. `EntityOptions.SkipUnrecognizedJournalEntries` lets an entity skip such entries, logging an error for each. An `IReadEventAdapter` on the journal can repair or drop them instead.

Stored events and snapshots are a long-lived contract, so evolve these types by extension only:

- **Add, don't change.** New fields must be nullable. New union cases and new messages must be deployed to every reader before any node writes them.
- **Never change or reuse a manifest or a field index**, and never remove a union case that may already be in a journal. Mark it `[Obsolete]`, stop writing it, and keep handling it in `Evolve`.
- **Case order doesn't matter;** cases are matched by manifest. Renaming a C# type is safe as long as its manifest stays the same.
- **Switching a union to a closed record hierarchy, or back,** changes the top-level manifest. Treat it as a migration.

## Hosting

Register entity refs with DI, then host each entity type either locally or in a shard region:

```csharp
builder.Services.AddDurableEntities();
builder.Services.AddScoped<IInvoiceMailer, SmtpInvoiceMailer>();

builder.Services.AddAkka("billing", (akka, sp) =>
{
    // Configure an Akka.Persistence journal / snapshot store and Akka.Reminders here.

    akka.WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails);
    akka.WithDurableEntity(Invoice.Definition);
    // or, in a cluster:
    // akka.WithDurableEntity(Invoice.Definition, new ShardOptions { Role = "billing" });
});
```

Without `ShardOptions` the entity type runs in a local host actor in this process. In both modes, `.Stop()` uses sharding's `Passivate` protocol, so an entity that stops itself isn't restarted when remember-entities is enabled. Don't run the same entity type in local mode on more than one node against the same journal. When using Akka.Reminders with locally hosted entities, point it at them with `WithResolver(sys => new DurableEntityReminderResolver(sys))`.

In both modes an entity is passivated after `EntityOptions.PassivateIdleAfter` (default 2 minutes) without messages, and its next message reactivates it from the journal; set it to `TimeSpan.Zero` to disable. In sharded mode an explicit `ShardOptions.PassivateIdleEntityAfter` wins.

In sharded mode, entity IDs are spread across `EntityOptions.NumberOfShards` shards (default 100). It's on the entity definition so every node uses the same value; changing it after deployment moves entities to different shards and needs a full cluster restart. Local hosting ignores it.

## Sending commands

Inject `IEntityRefs<TCommand>` and get a ref by entity ID. Getting a ref is cheap and doesn't start the entity. The first message does.

```csharp
public sealed class BillingService(IEntityRefs<InvoiceCommand> invoices)
{
    public Task<object> IssueAsync(string invoiceId, decimal total, string recipient, CancellationToken ct) =>
        invoices.Get(new EntityId(invoiceId)).Ask<object>(new IssueInvoice(total, recipient), ct);

    public void RecordPayment(string invoiceId, decimal amount) =>
        invoices.Get(new EntityId(invoiceId)).Tell(new RecordPayment(amount));
}
```

A ref's identity is the entity type plus the entity ID. It doesn't change when the entity moves between processes or nodes. Messages always go through the shard region or local host, so a command sent while an entity is stopping is delivered to its next incarnation.

## Reading state

Inject `IDurableEntityRefs<TCommand, TState, TEvent>` to get refs that can also read the entity's state. The event type is part of the signature so that event subscriptions can be added without changing it:

```csharp
public sealed class InvoiceQueries(IDurableEntityRefs<InvoiceCommand, InvoiceState, InvoiceEvent> invoices)
{
    public async Task<string> DescribeAsync(string invoiceId, CancellationToken ct)
    {
        var snapshot = await invoices.Get(new EntityId(invoiceId)).GetStateAsync(ct);
        return snapshot.State switch
        {
            Draft => "draft",
            Issued s => $"issued to {s.Recipient}, {s.Total:C} due",
            Paid s => $"paid {s.Total:C}",
        };
    }
}
```

The snapshot carries the state and the sequence number of the last applied event. The read waits behind commands the entity has already received, so it sees their committed events and runs only after their effects finish. Refs carried in messages are always the address-only `IEntityRef<TCommand>`. To read state from one, upgrade it by ID: `invoices.Get(invoiceRef.Id)`. `TState` must match the registered definition; otherwise `Get` throws Akka.Hosting's `MissingActorRegistryEntryException`.

## Testing

Use `Akka.Durable.TestKit`. Decisions can be tested as plain functions:

```csharp
[Fact]
public void Paying_in_full_marks_the_invoice_paid() =>
    DeciderScenario.For(Invoice.Definition)
        .Given(new InvoiceIssued(250m, "billing@example.com", DateTimeOffset.UnixEpoch))
        .When(new RecordPayment(250m))
        .ThenPersisted(new PaymentRecorded(250m))
        .ThenReplied(new PaymentAccepted())
        .ThenState(s => Assert.True(s is Paid));
```

For the full runtime, `DurableEntityTestKit` hosts entities with a test journal (supports failure injection), in-memory reminders and DI. The test journal keeps objects in memory and never serializes them; to check that your types round-trip, run a spec against a real journal plugin. Akka.Durable's own tests use Akka.Persistence.Sql with SQLite.

```csharp
public class InvoiceSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddScoped<IInvoiceMailer, FakeInvoiceMailer>();

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder
            .WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails)
            .WithDurableEntity(Invoice.Definition);

    [Fact]
    public async Task Issued_invoice_accepts_payment()
    {
        var invoice = EntityRef<InvoiceCommand>("invoice-1");

        await invoice.Ask<InvoiceAccepted>(new IssueInvoice(250m, "billing@example.com"), AskCancellation());
        await invoice.Ask<PaymentAccepted>(new RecordPayment(250m), AskCancellation());

        var snapshot = await DurableEntityRef<InvoiceCommand, InvoiceState, InvoiceEvent>("invoice-1")
            .GetStateAsync(AskCancellation());
        Assert.True(snapshot.State is Paid);
    }
}
```

## Status and direction

Working today:
- Decide, persist, evolve, then effects.
- Recovery, which fails on journal entries it can't interpret.
- Reminders.
- Local and sharded hosting.
- Typed entity refs and state reads.
- Serialization of union commands, events and state. Round-tripped through the V2 serializers, and through a real journal (Akka.Persistence.Sql on SQLite) for events and snapshots.

Not done yet:
- **Snapshot retention tests.** Snapshots are written and recovered, but pruning old snapshots and deleting covered events aren't covered by tests yet.
- **Message serialization checks in the TestKit.** `VerifyMessageSerialization` stays off by default until [akkadotnet/akka.net#8910](https://github.com/akkadotnet/akka.net/issues/8910) is fixed.
- **Subscriptions.** State can be read on demand, but there are no state or event subscriptions yet.
- **Source generation.**
- **Durable workflows.**
- **Agents.** Much later, once the foundations exist.
