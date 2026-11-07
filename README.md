# Akka.Durable

> [!NOTE]
> Akka.Durable is a proof of concept, not an official Akka.NET project. It is published as `Aaron.Akka.Durable` because the `Akka.*` NuGet prefix is reserved for official Akka.NET packages. Expect breaking changes between releases.

Akka.Durable gives you event-sourced entities on Akka.NET. Define their behavior with pure `Decide` and `Evolve` functions, then send commands through typed references. The runtime handles persistence, recovery, and routing in local or clustered deployments.

## Inject it. Send a message.

**Entity reference factories are .NET DI services.** Call `AddDurableEntities()` at startup, then inject `IEntityRefs<InvoiceCommand>` into any controller, hosted service, or application service:

```csharp
public sealed class BillingService(IEntityRefs<InvoiceCommand> invoices)
{
    public Task<IssueReply> IssueAsync(EntityId id, CancellationToken ct) =>
        invoices.Get(id).Ask(new IssueInvoice(250m, "billing@example.com"), ct);

    public void Remind(EntityId id) =>
        invoices.Get(id).Tell(new SendReminder());
}
```

That's the caller API: choose an ID, send a command, get its declared reply type. The first message creates or recovers the entity. The same calls work with local hosting and Cluster Sharding. [Register and host the entity](#hosting) once at startup.

## Define its behavior

Here is the `Invoice` behind those calls. `Decide` produces events and effects; `Evolve` computes the state from each persisted event. The `InvoiceCommand`, `IssueInvoice`, and `SendReminder` types are the same ones used by the injected service above.

```csharp
using InvoiceContext = DecisionContext<InvoiceState, InvoiceCommand, InvoiceEvent>;

public static class Invoice
{
    public static readonly EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition =
        new(new EntityTypeName("invoices"), _ => new Draft(), Decide, Evolve);

    public static Decision<InvoiceCommand, InvoiceEvent> Decide(
        InvoiceState state, InvoiceCommand command, InvoiceContext ctx) =>
        (state, command) switch
        {
            (Draft, IssueInvoice c) when c.Total > 0 =>
                ctx.For(c).Persist(new InvoiceIssued(c.Total, c.Recipient, ctx.UtcNow))
                    .Schedule("reminder", TimeSpan.FromDays(30), new SendReminder())
                    .Reply(new InvoiceAccepted(ctx.EntityId)),

            (Issued s, RecordPayment p) when p.Amount == s.Total =>
                ctx.For(p).Persist(new PaymentRecorded(p.Amount))
                    .CancelTimer("reminder")
                    .Reply(new PaymentAccepted()),

            (Issued, SendReminder) => ctx.None().Run<IInvoiceMailer>(
                (mailer, ct) => mailer.RemindAsync(ct),
                onSuccess: next => next,
                // Escalate so a failed reminder remains eligible for redelivery.
                onFailure: (error, _) => throw new InvalidOperationException("Reminder delivery failed", error)),
            (_, SendReminder) => ctx.Ignore(),

            (_, IssueInvoice c) => ctx.For(c).Reply(new InvoiceRejected("Invalid invoice operation")),
            (_, RecordPayment p) => ctx.For(p).Reply(new InvoiceRejected("Invalid invoice operation")),
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

The [state, command, event, and reply declarations](#state-commands-events-and-replies) are below. You write the domain functions; Akka.Durable supplies the actor.

## Getting started

Install the .NET 11 RC SDK pinned in `global.json` (including prerelease SDKs), then run:

```sh
dotnet restore
dotnet build
dotnet test
```

The solution targets `net11.0` and enables preview C# features (C# 15 unions are used throughout). Package versions are managed centrally; Akka.NET packages are pinned to 1.6.0-beta6 (Aaron.Akka.Reminders to 1.6.0-beta5).

| Package | Contents |
|---|---|
| `Aaron.Akka.Durable` | Entity definitions, decisions and effects, entity refs, the entity runtime, and Akka.Hosting integration. Everything is in the `Akka.Durable` namespace. |
| `Aaron.Akka.Durable.TestKit` | Test helpers built on Akka.Hosting.TestKit, in the `Akka.Durable.TestKit` namespace |

## State, commands, events, and replies

An entity is an initial state, a `Decide` function and an `Evolve` function. You don't subclass an actor. Its state, commands, events and replies are serialized (see [Serialization](#serialization)), so each one carries a stable manifest and field indexes:

```csharp
public interface IInvoiceProtocol;

// State

[AkkaSerializable(Manifest = "invoice-draft-v1", AllowEmpty = true)]
public sealed record Draft : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-issued-state-v1")]
public sealed record Issued(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-paid-state-v1")]
public sealed record Paid([property: AkkaField(0)] decimal Total) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-state-v1")]
public readonly union InvoiceState(
    Draft,
    Issued,
    Paid) : IInvoiceProtocol;

// Commands

[AkkaSerializable(Manifest = "issue-invoice-v1")]
public sealed record IssueInvoice(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient)
    : IInvoiceProtocol, IReplies<InvoiceCommand, IssueReply>
{
    public InvoiceCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "record-payment-v1")]
public sealed record RecordPayment([property: AkkaField(0)] decimal Amount)
    : IInvoiceProtocol, IReplies<InvoiceCommand, PaymentReply>
{
    public InvoiceCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "send-reminder-v1", AllowEmpty = true)]
public sealed record SendReminder : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-command-v1")]
public readonly union InvoiceCommand(
    IssueInvoice,
    RecordPayment,
    SendReminder) : IInvoiceProtocol;

// Events

[AkkaSerializable(Manifest = "invoice-issued-v1")]
public sealed record InvoiceIssued(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient,
    [property: AkkaField(2)] DateTimeOffset At) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "payment-recorded-v1")]
public sealed record PaymentRecorded([property: AkkaField(0)] decimal Amount) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-event-v1")]
public readonly union InvoiceEvent(
    InvoiceIssued,
    PaymentRecorded) : IInvoiceProtocol;

// Replies

[AkkaSerializable(Manifest = "invoice-accepted-v1")]
public sealed record InvoiceAccepted([property: AkkaField(0)] EntityId Id) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "payment-accepted-v1", AllowEmpty = true)]
public sealed record PaymentAccepted : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-rejected-v1")]
public sealed record InvoiceRejected([property: AkkaField(0)] string Reason) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "issue-reply-v1")]
public readonly union IssueReply(
    InvoiceAccepted,
    InvoiceRejected) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "payment-reply-v1")]
public readonly union PaymentReply(
    PaymentAccepted,
    InvoiceRejected) : IInvoiceProtocol;

[AkkaSerializer<IInvoiceProtocol>("invoices", 120100)]
public sealed partial class InvoiceSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}
```

- **Events first, then effects.** Effects run only after the events are committed, in the order they're declared. The entity handles no other command until they finish.
- **`Run` branches on the operation's outcome.** Both `onSuccess` and `onFailure` are required. Only the selected continuation executes; its events commit before its effects and reply. Each operation gets a fresh DI scope. Completed invocations dispose their scope before continuing; timed-out invocations that ignore cancellation keep their scope until they actually finish. The entity holds later commands and state reads until the entire graph finishes.
- **`Run` is best effort.** Neither the operation nor its continuations are journaled or replayed during recovery. Service work and scope cleanup can finish independently of actor shutdown. An invocation timeout selects `onFailure`; actor shutdown does not. Continuation errors and journal failures go to supervision rather than selecting the service's failure branch.
- **`Schedule` uses Akka.Reminders.** The reminder is stored durably and delivered back to the entity at least once, so `Decide` must tolerate duplicates. Its message must be a `TCommand`.
- **Typed replies.** `ctx.For(command)` binds the reply contract once. `Reply` and `Run` finish a request branch: no unconditional reply can follow either. Use `ReplyAndRun` to reply before best effort work, or `ReplyAndStop` to reply and then passivate; `.UnstashAll().Reply(...)` releases stashed commands after this decision completes.
- **Other outcomes.** `ctx.Ignore()`, `ctx.Unhandled()` and `ctx.Stash()` cover commands you aren't handling now. `ctx.None().Stop()` passivates without replying or deleting the entity.

## Asynchronous request decisions

If an external service determines whether an invoice was issued, persist the outcome in the selected continuation. The command still declares `IssueReply`, and both branches keep that contract:

```csharp
public static RequestDecision<InvoiceCommand, InvoiceEvent, IssueReply> IssueExternally(
    IssueInvoice command, InvoiceContext ctx)
{
    var request = ctx.For(command);
    return request.Run<IInvoiceService, IssueResult>(
        (service, ct) => service.IssueAsync(command.Total, command.Recipient, ct),
        onSuccess: (result, next) => result switch
        {
            Created => next
                .Persist(new InvoiceIssued(command.Total, command.Recipient, next.UtcNow))
                .Reply(new InvoiceAccepted(next.EntityId)),
            Declined rejected => next.Reply(new InvoiceRejected(rejected.Reason)),
        },
        onFailure: (error, next) => next.Reply(new InvoiceRejected(error.Message)));
}
```

Here `IssueResult` is the service's result union, with `Created` and `Declined` cases. A returned business rejection goes through `onSuccess`, which means the invocation returned normally. Exceptions and timeouts go through `onFailure`. A timeout does not establish whether an external action occurred; domains that need that distinction should include an indeterminate result in their reply contract.

Both branches can call a named pure decider or return another `Run`. Every callback receives `next`: its state includes events committed before the operation, and its time is the operation's completion time. It starts with no events or effects. Build the continuation from `next`; returning a captured, previously committed builder is rejected before its events or effects can run again.

For an operation returning `Task`, use `Run<TService>`. Both handlers remain required:

```csharp
return request.Run<IInvoiceMailer>(
    (mailer, ct) => mailer.SendAsync(command.Recipient, ct),
    onSuccess: next => next.Reply(new InvoiceAccepted(next.EntityId)),
    onFailure: (error, next) => next.Reply(new InvoiceRejected(error.Message)));
```

To acknowledge a request before doing best effort work, use `ReplyAndRun`:

```csharp
return request
    .Persist(new InvoiceIssued(command.Total, command.Recipient, request.UtcNow))
    .ReplyAndRun<IInvoiceMailer>(new InvoiceAccepted(request.EntityId),
        (mailer, ct) => mailer.SendAsync(command.Recipient, ct),
        onSuccess: next => next,
        onFailure: (error, next) => next);
```

The reply follows the event commit and precedes the invocation. These continuations receive an `EffectContext` that can persist events, send messages, schedule reminders, or run further operations, but cannot reply again. The runtime logs invocation failures even when the failure branch does nothing. Later commands still wait until the work finishes.

`ctx.None()` and `ctx.Persist(...)` also produce an `EffectContext` for commands without replies. Its `Run` and `Stop` return a finished `EffectDecision`, so effects cannot be appended after either. `ctx.For(command)` produces a `RequestContext` whose continuations retain that command's reply type.

Returning an empty failure branch accepts the failure and completes the command. For reminder commands that also acknowledges the reminder; the `SendReminder` example above deliberately escalates instead, preserving redelivery on failure. Choose that policy explicitly for each operation.

Events explicitly persisted before `Run` remain committed if the operation fails. This API does not provide a transaction with an external service or automatic retries after a crash.

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
var builder = Host.CreateApplicationBuilder(args);

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

using var host = builder.Build();
await host.StartAsync();
```

This starts the infrastructure for the entity type. Individual invoices are created or recovered on their first message, as shown below. In a console application, call `await host.StopAsync()` when finished; a web application uses its normal host lifetime.

Without `ShardOptions` the entity type runs in a local host actor in this process. In both modes, `.Stop()` uses sharding's `Passivate` protocol, so an entity that stops itself isn't restarted when remember-entities is enabled. Don't run the same entity type in local mode on more than one node against the same journal. When using Akka.Reminders with locally hosted entities, point it at them with `WithResolver(sys => new DurableEntityReminderResolver(sys))`.

In both modes an entity is passivated after `EntityOptions.PassivateIdleAfter` (default 2 minutes) without messages, and its next message reactivates it from the journal; set it to `TimeSpan.Zero` to disable. In sharded mode an explicit `ShardOptions.PassivateIdleEntityAfter` wins.

In sharded mode, entity IDs are spread across `EntityOptions.NumberOfShards` shards (default 100). It's on the entity definition so every node uses the same value; changing it after deployment moves entities to different shards and needs a full cluster restart. Local hosting ignores it.

## Sending commands

Inject `IEntityRefs<InvoiceCommand>` as shown above, then call `Get(id)` to address an invoice. Getting a reference is cheap and does not activate the entity; the first `Ask` or `Tell` does. On activation, a new ID starts from `InitialState`, and an existing entity recovers from its journal and snapshot.

Use the same ID for later commands to the same invoice, and a different ID for another invoice. Calling `Get` again does not reset its state. `Ask` waits for a reply; `Tell` sends without waiting for one. [State-aware references](#reading-state) support both messaging and state reads.

`Ask` infers its return type from `IReplies<TCommand, TReply>`. Tell-only commands have no interface and cannot be asked. `IEntityRef<TCommand>` does not implement `ICanTell`, so Akka.NET's unchecked Ask extension methods cannot bypass the contract. Tell effects accept another typed entity ref and convert its command case before boxing it.

`ToCommand()` supplies a direct constructor call for a C# union; F# records can construct their native DU in that method instead. There is no runtime conversion table or per-command registration. F# deciders call `RequestDecision.ToDecision()` explicitly when returning the branch.

A ref's identity is the entity type plus the entity ID. It doesn't change when the entity moves between processes or nodes. Messages always go through the shard region or local host, so a command sent while an entity is stopping is delivered to its next incarnation.

A ref is bound to the node that created it, so it isn't serializable. To refer to an entity inside a message, event or reminder, carry its `EntityId` and resolve it on the receiving side. Each entity type has its own command type, so the receiver knows which factory to use:

```csharp
[AkkaSerializable(Manifest = "audit-invoice-v1")]
public sealed record AuditInvoice([property: AkkaField(0)] EntityId Invoice) : IInvoiceProtocol;

// on the receiving side
var invoice = invoices.Get(audit.Invoice);   // invoices: IEntityRefs<InvoiceCommand>
```

Typed `IEntityRef<T>` fields in messages need generic formatter support in Akka.Serialization.V2, tracked in [akkadotnet/akka.net#8911](https://github.com/akkadotnet/akka.net/issues/8911).

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

The snapshot carries the state and the sequence number of the last applied event. The read waits behind commands the entity has already received, so it sees their committed events and runs only after their effects finish. To read the state of an entity you hold a plain ref or an ID for, get a state-aware ref by ID: `invoices.Get(invoiceRef.Id)`. `TState` must match the registered definition; otherwise `Get` throws Akka.Hosting's `MissingActorRegistryEntryException`.

## Testing

Use the `Aaron.Akka.Durable.TestKit` package. Decisions can be tested as plain functions:

```csharp
[Fact]
public void Paying_in_full_marks_the_invoice_paid() =>
    DeciderScenario.For(Invoice.Definition)
        .Given(new InvoiceIssued(250m, "billing@example.com", DateTimeOffset.UnixEpoch))
        .When(new RecordPayment(250m))
        .ThenPersisted(new PaymentRecorded(250m))
        .ThenReplied(new RecordPayment(250m), new PaymentAccepted())
        .ThenState(s => Assert.True(s is Paid));
```

`WhenRunSucceeded(result)` and `WhenRunFailed(error)` select a continuation without invoking services, so result-dependent decisions remain pure unit tests:

```csharp
DeciderScenario.For(Invoice.Definition)
    .When(command)
    .WhenRunFailed(new InvalidOperationException("unavailable"))
    .ThenNothingPersisted()
    .ThenReplied(command, new InvoiceRejected("unavailable"));
```

Use a definition that routes `command` through `IssueExternally` for that example. `WhenRunSucceeded()` covers an operation returning plain `Task`.

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

        await invoice.Ask(new IssueInvoice(250m, "billing@example.com"), AskCancellation());
        await invoice.Ask(new RecordPayment(250m), AskCancellation());

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
- Per-command typed entity refs, typed request graphs, and state reads.
- Required success/failure continuations for both `Task` and `Task<T>`, including nested operations and continuation-time state.
- C# compiler contract tests and F# DU command/reply tests.
- Serialization of union commands, events and state. Round-tripped through the V2 serializers, and through a real journal (Akka.Persistence.Sql on SQLite) for events and snapshots.

Not done yet:
- **Snapshot retention tests.** Snapshots are written and recovered, but pruning old snapshots and deleting covered events aren't covered by tests yet.
- **Message serialization checks in the TestKit.** `VerifyMessageSerialization` stays off by default until [akkadotnet/akka.net#8910](https://github.com/akkadotnet/akka.net/issues/8910) is fixed.
- **Subscriptions.** State can be read on demand, but there are no state or event subscriptions yet.
- **Source generation.**
- **Durable workflows.**
- **Agents.** Much later, once the foundations exist.

The bounded Native AOT smoke checks command conversion, typed replies, and request graph construction; it does not certify the actor system or persistence plugins for AOT:

```sh
dotnet publish tests/Akka.Durable.AotSmoke -c Release -r linux-x64 -p:PublishAot=true -o ./artifacts/aot-smoke
./artifacts/aot-smoke/Akka.Durable.AotSmoke
```
