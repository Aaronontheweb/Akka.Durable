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

The solution targets `net11.0` and enables preview C# features (C# 15 unions are used throughout). Package versions are managed centrally; Akka.NET packages are pinned to 1.6.0-beta5.

| Project | Contents |
|---|---|
| `Akka.Durable.Abstractions` | `EntityId`, `IEntityRef<TCommand>`, `IEntityRefs<TCommand>`, `IDurableEntityRefs<TCommand, TState, TEvent>` |
| `Akka.Durable.Entities` | Entity definitions, decisions and effects, the entity actor, and Akka.Hosting integration |
| `Akka.Durable.TestKit` | Test helpers built on Akka.Hosting.TestKit |
| `Akka.Durable.Workflows` | Empty for now |

## Defining an entity

An entity is an initial state, a `Decide` function and an `Evolve` function. You don't subclass an actor.

```csharp
public sealed record Draft;
public sealed record Issued(decimal Total, string Recipient);
public sealed record Paid(decimal Total);
public readonly union InvoiceState(Draft, Issued, Paid);

public sealed record IssueInvoice(decimal Total, string Recipient);
public sealed record RecordPayment(decimal Amount);
public sealed record SendReminder;
public readonly union InvoiceCommand(IssueInvoice, RecordPayment, SendReminder);

public sealed record InvoiceIssued(decimal Total, string Recipient, DateTimeOffset At);
public sealed record PaymentRecorded(decimal Amount);
public readonly union InvoiceEvent(InvoiceIssued, PaymentRecorded);

public sealed record InvoiceAccepted(EntityId Id);
public sealed record PaymentAccepted;
public sealed record InvoiceRejected(string Reason);
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

## Hosting

Register entity refs with DI, then host each entity type either locally or in a shard region:

```csharp
builder.Services.AddDurableEntities();
builder.Services.AddScoped<IInvoiceMailer, SmtpInvoiceMailer>();

builder.Services.AddAkka("billing", (akka, sp) =>
{
    // Configure an Akka.Persistence journal / snapshot store and Akka.Reminders here.

    akka.WithDurableEntity(Invoice.Definition);
    // or, in a cluster:
    // akka.WithDurableEntity(Invoice.Definition, new ShardOptions { Role = "billing" });
});
```

Without `ShardOptions` the entity type runs in a local host actor in this process. In both modes, `.Stop()` uses sharding's `Passivate` protocol, so an entity that stops itself isn't restarted when remember-entities is enabled. Don't run the same entity type in local mode on more than one node against the same journal. When using Akka.Reminders with locally hosted entities, point it at them with `WithResolver(sys => new DurableEntityReminderResolver(sys))`.

In both modes an entity is passivated after `EntityOptions.PassivateIdleAfter` (default 2 minutes) without messages, and its next message reactivates it from the journal; set it to `TimeSpan.Zero` to disable. In sharded mode an explicit `ShardOptions.PassivateIdleEntityAfter` wins.

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

For the full runtime, `DurableEntityTestKit` hosts entities with a test journal (supports failure injection), in-memory reminders and DI:

```csharp
public class InvoiceSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddScoped<IInvoiceMailer, FakeInvoiceMailer>();

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder.WithDurableEntity(Invoice.Definition);

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

Working today, and tested against the in-memory test journal:
- Decide, persist, evolve, then effects.
- Recovery.
- Reminders.
- Local and sharded hosting.
- Typed entity refs and state reads.

Not done yet:
- **Serialization.** Union commands and events have only been tested in memory, not through a real serializer and journal.
- **Snapshot tests.** Snapshotting and retention are implemented but not yet covered by tests.
- **Subscriptions.** State can be read on demand, but there are no state or event subscriptions yet.
- **Source generation.**
- **Durable workflows.**
- **Agents.** Much later, once the foundations exist.
