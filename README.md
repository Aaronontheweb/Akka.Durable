# Akka.Durable

> [!NOTE]
> Akka.Durable is a proof of concept, not an official Akka.NET project. It is published as `Aaron.Akka.Durable` because the `Akka.*` NuGet prefix is reserved for official Akka.NET packages. Expect breaking changes between releases.

Akka.Durable gives you event-sourced entities on Akka.NET. Define their behavior with pure `Decide` and `Evolve` functions, keep the impure work in effect handlers, then send commands through typed references. The runtime handles persistence, recovery, and routing in local or clustered deployments.

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

Here is the `Invoice` behind those calls. `Decide` produces events and effects; `Evolve` computes the state from each persisted event. Issuing an invoice needs an answer from a recipient directory, so `Decide` records that verification started and issues a `VerifyRecipient` effect. The directory's answer comes back as a command, `RecipientVerified` or `RecipientRejected`, and a second arm finishes the job.

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
            // Needs an outside answer: remember that we asked, then ask.
            (Draft, IssueInvoice c) when c.Total > 0 =>
                ctx.Persist(new VerificationStarted(c)).Execute(new VerifyRecipient(c.Recipient)),

            // The answer arrives as a command. The request kept in state fixes the reply type.
            (Verifying v, RecipientVerified) =>
                ctx.Persist(new InvoiceIssued(v.Request.Total, v.Request.Recipient, ctx.UtcNow))
                    .Schedule("reminder", TimeSpan.FromDays(30), new SendReminder())
                    .Reply(v.Request, new InvoiceAccepted(ctx.EntityId)),
            (Verifying v, RecipientRejected r) =>
                ctx.Persist(new VerificationFailed(r.Reason)).Reply(v.Request, new InvoiceRejected(r.Reason)),
            (_, RecipientVerified or RecipientRejected) => ctx.Ignore(),

            (Issued s, RecordPayment p) when p.Amount == s.Total =>
                ctx.Persist(new PaymentRecorded(p.Amount))
                    .CancelTimer("reminder")
                    .Reply(p, new PaymentAccepted()),

            // Nothing comes back from a reminder, so best-effort Run is enough.
            (Issued, SendReminder) => ctx.None().Run<IInvoiceMailer>((mailer, ct) => mailer.RemindAsync(ct)),
            (_, SendReminder) => ctx.Ignore(),

            (_, IssueInvoice c) => ctx.None().Reply(c, new InvoiceRejected("Invalid invoice operation")),
            (_, RecordPayment p) => ctx.None().Reply(p, new InvoiceRejected("Invalid invoice operation")),
        };

    public static InvoiceState Evolve(InvoiceState state, InvoiceEvent @event) =>
        (state, @event) switch
        {
            (Draft, VerificationStarted e) => new Verifying(e.Request),
            (Verifying, VerificationFailed) => new Draft(),
            (Verifying, InvoiceIssued e) => new Issued(e.Total, e.Recipient),
            (Issued s, PaymentRecorded) => new Paid(s.Total),
            _ => throw new InvalidOperationException($"Cannot apply {@event} to {state}."),
        };
}
```

`VerifyRecipient` is a plain record, `public sealed record VerifyRecipient(string Recipient);`, and it is never serialized. The only impure code is its handler, which Akka.Durable resolves from a fresh DI scope each time the effect runs. The handler's second type argument says what kind of outcome it produces, here a command that goes back through `Decide`:

```csharp
public sealed class VerifyRecipientHandler(IRecipientDirectory directory)
    : IEffectHandler<VerifyRecipient, InvoiceCommand>
{
    public async Task<InvoiceCommand> HandleAsync(VerifyRecipient effect, CancellationToken ct) =>
        await directory.LookupAsync(effect.Recipient, ct) switch
        {
            Known => new RecipientVerified(),
            Unknown u => new RecipientRejected(u.Reason),
        };
}

// at startup, next to the other services
services.AddScoped<IEffectHandler<VerifyRecipient, InvoiceCommand>, VerifyRecipientHandler>();
```

The [state, command, event, and reply declarations](#state-commands-events-and-replies) are below. You write the domain functions and the handlers; Akka.Durable supplies the actor.

The calling surface is small: `IEntityRef` sends commands, and `IDurableEntityRef` adds state reads:

```csharp
public interface IEntityRef<in TCommand>
{
    EntityId Id { get; }

    void Tell(TCommand command, IActorRef? sender = null);

    Task<TReply> Ask<TReply>(IReplies<TCommand, TReply> command,
        CancellationToken cancellationToken = default) where TReply : notnull;
}

public interface IDurableEntityRef<in TCommand, TState, TEvent> : IEntityRef<TCommand>
{
    Task<StateSnapshot<TState>> GetStateAsync(CancellationToken cancellationToken = default);
}
```

`Ask` infers its reply type from the command. `GetStateAsync` returns the committed state and its journal sequence number. **Both reference factories can be injected:** use `IEntityRefs<InvoiceCommand>` for messaging, or `IDurableEntityRefs<InvoiceCommand, InvoiceState, InvoiceEvent>` for messaging plus state reads. Both are registered by `AddDurableEntities()`. The registry is keyed by command type, so each entity type must have its own command type.

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

[AkkaSerializable(Manifest = "invoice-verifying-state-v1")]
public sealed record Verifying([property: AkkaField(0)] IssueInvoice Request) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-issued-state-v1")]
public sealed record Issued(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-paid-state-v1")]
public sealed record Paid([property: AkkaField(0)] decimal Total) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-state-v1")]
public readonly union InvoiceState(
    Draft,
    Verifying,
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

// Outcomes of the VerifyRecipient effect, produced by its handler.
[AkkaSerializable(Manifest = "recipient-verified-v1", AllowEmpty = true)]
public sealed record RecipientVerified : IInvoiceProtocol;

[AkkaSerializable(Manifest = "recipient-rejected-v1")]
public sealed record RecipientRejected([property: AkkaField(0)] string Reason) : IInvoiceProtocol;

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
    RecipientVerified,
    RecipientRejected,
    RecordPayment,
    SendReminder) : IInvoiceProtocol;

// Events

[AkkaSerializable(Manifest = "verification-started-v1")]
public sealed record VerificationStarted([property: AkkaField(0)] IssueInvoice Request) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "verification-failed-v1")]
public sealed record VerificationFailed([property: AkkaField(0)] string Reason) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-issued-v1")]
public sealed record InvoiceIssued(
    [property: AkkaField(0)] decimal Total,
    [property: AkkaField(1)] string Recipient,
    [property: AkkaField(2)] DateTimeOffset At) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "payment-recorded-v1")]
public sealed record PaymentRecorded([property: AkkaField(0)] decimal Amount) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-event-v1")]
public readonly union InvoiceEvent(
    VerificationStarted,
    VerificationFailed,
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

- **Events first, then effects.** Effects run only after the events are committed, in the order they're declared.
- **Typed replies.** `Reply(command, reply)` takes the command that declared the reply type, so a wrong reply is a compile error. The command fixes the type and is not sent anywhere: it can be the command being handled, or one kept in state from earlier.
- **`Execute` for work whose result matters.** The decider issues a plain effect record; its `IEffectHandler` runs it; the result comes back as one of the entity's commands or events. See [Effects with outcomes](#effects-with-outcomes).
- **`Run` for work whose result doesn't.** A notification, a cache invalidation. It takes a lambda against one service, runs in a fresh DI scope and is awaited in place, so later effects and later commands wait for it. A failure or timeout is logged and the decision continues; nothing records that it ran. See [Run or Execute](#run-or-execute).
- **`Schedule` uses Akka.Reminders.** The reminder is stored durably and delivered back to the entity at least once, so `Decide` must tolerate duplicates. Its message must be a `TCommand`.
- **`Send` talks to another entity.** Its reply arrives as one of this entity's commands. See [Talking to other entities](#talking-to-other-entities).
- **Other outcomes.** `ctx.Ignore()`, `ctx.Unhandled()` and `ctx.Stash()` cover commands you aren't handling now. `.UnstashAll()` releases stashed commands after this decision completes. `.Stop()` must be the last effect; it passivates without deleting the entity.

## Effects with outcomes

When an external service decides what happens next, the decision is split in two. The first half persists what must be remembered and issues an effect. The second half is an ordinary command, produced by the effect's handler, that carries the result back into `Decide` against the state at that moment. The invoice above does this for recipient verification: `VerificationStarted` and `Verifying` are the memory, `VerifyRecipient` is the effect, `RecipientVerified` and `RecipientRejected` are the outcomes.

Two kinds of outcome, chosen by the handler's registration:

- **`IEffectHandler<TEffect, TCommand>`: the entity decides.** The result goes through `Decide` like any other command. Use this when the result can be declined, retried, or ignored, as with the directory's answer.
- **`IEffectHandler<TEffect, TEvent>`: the entity records.** The result is a fact about the world, such as a number a sequence service assigned. It is persisted as it is, through `Evolve`, in whatever state the entity is in. Use it only when every state can accept the fact.

Exactly one handler is registered per effect type. What the runtime guarantees:

- **The entity is not blocked.** `Execute` starts the handler and moves on; the outcome arrives later as a message. Other commands are handled in the meantime, which is why the invoice records `Verifying`: a second `IssueInvoice` meanwhile falls through to the rejection arm. To queue it instead, return `ctx.Stash()` while verifying and add `.UnstashAll()` to the outcome arms.
- **Replies find their asker.** An outcome is handled on behalf of the command that issued the effect. `Reply(v.Request, ...)` in the second half answers whoever sent `IssueInvoice`, even though several messages later.
- **Expected failures are outcomes.** A handler that can be declined returns the decline. A thrown exception or a timeout (`EntityOptions.EffectTimeout`, 30 seconds by default) is logged and dropped: the entity's state is unchanged, and no outcome arrives. A missing or ambiguous handler registration is a configuration error and restarts the entity.
- **Nothing about the effect is journaled.** What survives a crash is the state. That is what `Resume` is for.

## Run or Execute

Both run service code after the decision's events commit, in a fresh DI scope, under `EntityOptions.EffectTimeout`. They differ in what the entity gets back.

| | `Run` | `Execute` |
|---|---|---|
| Shape | A lambda against one service, inline in `Decide` | An effect record, plus an `IEffectHandler` registered in DI |
| Result | None. The entity never hears about it | A command or an event, delivered back to the entity |
| While it runs | Awaited in place: later effects and later commands wait | Not awaited: the entity keeps handling commands |
| On failure or timeout | Logged; the decision continues | Logged; no outcome arrives, and the state still says the work is pending |
| After a crash | Lost | Re-issued by `Resume`, if the state records it |
| In tests | Checked by type (`ThenEffects`) | Compared by value (`ThenExecuted`); the handler is tested on its own |

Use `Run` when nothing in the entity depends on the result and losing one is tolerable: a notification, a cache invalidation, a metric. The invoice's reminder mail is one. Use `Execute` when the result changes what the entity does next, or when the work has to survive a restart. If you find yourself wanting to know whether a `Run` succeeded, that is an `Execute`.

## Resuming after a restart

An entity that is waiting for an outcome when its process dies recovers into the state that says so: the invoice comes back as `Verifying`, with no verification in flight. `Resume` is the optional function that lists the effects still pending for a state. It runs once after recovery, before the first command, through the same effect path as a decision.

```csharp
public static Handled<InvoiceCommand, InvoiceEvent> Resume(InvoiceState state, InvoiceContext ctx) =>
    state switch
    {
        Verifying v => ctx.None().Execute(new VerifyRecipient(v.Request.Recipient)),
        _ => ctx.None(),
    };

public static readonly EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition =
    new(new EntityTypeName("invoices"), _ => new Draft(), Decide, Evolve) { Resume = Resume };
```

Two rules make it safe, and the runtime enforces the first: `Resume` may not return events, because nothing was decided, and everything it returns must follow from state alone, so running it twice is harmless. Give effects idempotency keys, and prefer re-sending a query to re-sending a change. The same function serves a retry timer: `(var s, RetryVerification) => Resume(s, ctx)`.

Replies issued by resumed work go to dead letters: the asker belongs to the previous incarnation. Its retry gets the answer from an arm that handles the repeated request, such as `(Issued, IssueInvoice c) => ctx.None().Reply(c, new InvoiceAccepted(ctx.EntityId))`. A repeated command should return the same reply it returned the first time; callers will retry.

Entities whose effects never wait for an outcome do not need `Resume`. Timers re-arm themselves through Akka.Reminders, and `Run` work is best effort by definition. To retry a reminder-driven effect without restarting the entity, re-arm the same key from the outcome: the old delivery's acknowledgement includes its original due time, so it leaves the new reminder intact.

## Talking to other entities

`Send` addresses another durable entity by its command protocol and ID. The target's reply, if any, comes back to the sender as a foreign type, and `Accepts` on the definition says how it becomes one of the sender's commands. Together they are what a workflow needs to drive other entities.

```csharp
public sealed record InvoiceReplied(IssueReply Reply);      // a CheckoutCommand case

public static readonly EntityDefinition<CheckoutState, CheckoutCommand, CheckoutEvent> Definition =
    new EntityDefinition<CheckoutState, CheckoutCommand, CheckoutEvent>(
            new EntityTypeName("checkouts"), _ => new NotStarted(), Decide, Evolve) { Resume = Resume }
        .Accepts<IssueReply>(reply => new InvoiceReplied(reply));

(NotStarted, StartCheckout c) => ctx.Persist(new Started(c, InvoiceFor(ctx.EntityId)))
    .Send<InvoiceCommand>(InvoiceFor(ctx.EntityId), new IssueInvoice(c.Total, c.Recipient))
    .Schedule("deadline", TimeSpan.FromMinutes(10), new CheckoutTimedOut()),

(AwaitingInvoice a, InvoiceReplied { Reply: InvoiceAccepted }) => ctx.Persist(new Completed())
    .CancelTimer("deadline").Reply(a.Request, new CheckoutCompleted()).Stop(),

(AwaitingInvoice a, InvoiceReplied { Reply: InvoiceRejected r }) => ctx.Persist(new Abandoned(r.Reason))
    .CancelTimer("deadline").Reply(a.Request, new CheckoutFailed(r.Reason)).Stop(),
```

Name the target's command type explicitly in `Send<InvoiceCommand>(...)`: the compiler would otherwise infer the case type, which no entity type is registered under. The target's entity type must be registered with `WithDurableEntity` in this actor system; in a cluster the entity itself can run on any node. The reply is handled on behalf of the command that sent it, so `Reply(a.Request, ...)` reaches the caller that started the checkout. The `Accepts` table lives on the definition rather than on the send so that a reply to a command re-sent by `Resume` is wrapped the same way after a restart.

There is no timeout on a send. The entity's own reminder is its deadline, as above. A message of a type the definition does not accept is unhandled, as before.

## Serialization

Events and snapshots go to the journal, scheduled commands go to the reminder store, and in a cluster commands, replies and state reads cross the network. All of them are serialized with [Akka.Serialization.V2](https://getakka.net/articles/serialization/source-generated-serialization.html):

- **Give the union itself a manifest, as well as each case.** With `[AkkaSerializable(Manifest = "...")]` on the union, a union value round-trips as the union. That's what the entity's handlers match on.
- **Register the serializer** next to the entity: `akka.WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails)`.
- **`WithDurableEntity` checks this at startup.** If `TState`, `TCommand` or `TEvent` would fall back to the default JSON serializer, the host fails to start and the error names the types. Akka.Durable registers its own serializer (id 22560) for the framework's messages.
- **Recovery fails on an entry it can't read** rather than continuing with missing state. `EntityOptions.SkipUnrecognizedJournalEntries` lets an entity skip such entries, logging an error for each. An `IReadEventAdapter` on the journal can repair or drop them instead.
- **Effect records are not journaled.** An `Execute` payload stays in the process that issued it. The command a `Send` carries is delivered to the target through its shard region, so it and the reply are serialized like any other command to that entity. Outcome commands and events are part of the issuing entity's protocol and are serialized like the rest of it.

Stored events and snapshots are a long-lived contract, so evolve these types by extension only:

- **Add, don't change.** New fields must be nullable. New union cases and new messages must be deployed to every reader before any node writes them.
- **Never change or reuse a manifest or a field index**, and never remove a union case that may already be in a journal. Mark it `[Obsolete]`, stop writing it, and keep handling it in `Evolve`.
- **Case order doesn't matter;** cases are matched by manifest. Renaming a C# type is safe as long as its manifest stays the same.
- **Switching a union to a closed record hierarchy, or back,** changes the top-level manifest. Treat it as a migration.

## Hosting

Register entity refs and effect handlers with DI, then host each entity type either locally or in a shard region:

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDurableEntities();
builder.Services.AddScoped<IInvoiceMailer, SmtpInvoiceMailer>();
builder.Services.AddScoped<IEffectHandler<VerifyRecipient, InvoiceCommand>, VerifyRecipientHandler>();

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

The snapshot carries the state and the sequence number of the last applied event. The read waits behind commands the entity has already received, so it sees their committed events and runs only after their `Run` effects finish. It does not wait for an `Execute` outcome that is still in flight; the state tells you whether one is. To read the state of an entity you hold a plain ref or an ID for, get a state-aware ref by ID: `invoices.Get(invoiceRef.Id)`. `TState` must match the registered definition; otherwise `Get` throws Akka.Hosting's `MissingActorRegistryEntryException`.

## Testing

Use the `Aaron.Akka.Durable.TestKit` package. Decisions can be tested as plain functions:

```csharp
[Fact]
public void Paying_in_full_marks_the_invoice_paid() =>
    DeciderScenario.For(Invoice.Definition)
        .Given(
            new VerificationStarted(new IssueInvoice(250m, "billing@example.com")),
            new InvoiceIssued(250m, "billing@example.com", DateTimeOffset.UnixEpoch))
        .When(new RecordPayment(250m))
        .ThenPersisted(new PaymentRecorded(250m))
        .ThenReplied(new RecordPayment(250m), new PaymentAccepted())
        .ThenState(s => Assert.True(s is Paid));
```

Effects are data, so both halves of an `Execute` are pure tests with no fakes. `ThenExecuted` and `ThenSent` compare the effect by value; `Resumed()` runs `Resume` against the recovered state:

```csharp
var command = new IssueInvoice(250m, "billing@example.com");

DeciderScenario.For(Invoice.Definition)
    .When(command)
    .ThenPersisted(new VerificationStarted(command))
    .ThenExecuted(new VerifyRecipient("billing@example.com"));

DeciderScenario.For(Invoice.Definition)
    .Given(new VerificationStarted(command))
    .When(new RecipientRejected("unknown recipient"))
    .ThenPersisted(new VerificationFailed("unknown recipient"))
    .ThenReplied(command, new InvoiceRejected("unknown recipient"))
    .ThenState(s => Assert.True(s is Draft));

DeciderScenario.For(Invoice.Definition)
    .Given(new VerificationStarted(command))
    .Resumed()
    .ThenExecuted(new VerifyRecipient("billing@example.com"));
```

For the full runtime, `DurableEntityTestKit` hosts entities with a test journal (supports failure injection), in-memory reminders and DI. Register effect handlers in `ConfigureTestServices`, with real or fake services behind them. The test journal keeps objects in memory and never serializes them; to check that your types round-trip, run a spec against a real journal plugin. Akka.Durable's own tests use Akka.Persistence.Sql with SQLite.

```csharp
public class InvoiceSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services
            .AddScoped<IInvoiceMailer, FakeInvoiceMailer>()
            .AddScoped<IEffectHandler<VerifyRecipient, InvoiceCommand>, VerifyRecipientHandler>()
            .AddSingleton<IRecipientDirectory, FakeDirectory>();

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
- Recovery, which fails on journal entries it can't interpret, followed by `Resume` for effects still pending.
- Reminders.
- Local and sharded hosting.
- Per-command typed entity refs and state reads.
- `Execute` with handlers whose outcome is a command or an event; `Send` and `Accepts` between entities; best-effort `Run`.
- C# compiler contract tests and F# DU command/reply tests.
- Serialization of union commands, events and state. Round-tripped through the V2 serializers, and through a real journal (Akka.Persistence.Sql on SQLite) for events and snapshots.

Not done yet:
- **Snapshot retention tests.** Snapshots are written and recovered, but pruning old snapshots and deleting covered events aren't covered by tests yet.
- **Message serialization checks in the TestKit.** `VerifyMessageSerialization` stays off by default until [akkadotnet/akka.net#8910](https://github.com/akkadotnet/akka.net/issues/8910) is fixed.
- **Subscriptions.** State can be read on demand, but there are no state or event subscriptions yet.
- **Entity contracts and proxies.** `Send` resolves the target by command type in the same actor system; addressing an entity type hosted only on other nodes, without its definition, is tracked in [#30](https://github.com/Aaronontheweb/Akka.Durable/issues/30).
- **Source generation.**
- **Agents.** Much later, once the foundations exist.

The bounded Native AOT smoke checks command conversion, typed replies, and effect construction; it does not certify the actor system or persistence plugins for AOT:

```sh
dotnet publish tests/Akka.Durable.AotSmoke -c Release -r linux-x64 -p:PublishAot=true -o ./artifacts/aot-smoke
./artifacts/aot-smoke/Akka.Durable.AotSmoke
```
