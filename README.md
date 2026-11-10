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

That's the caller API: choose an ID, send a command, get its declared reply type. The first message creates or recovers the entity. The same calls work with local hosting and Cluster Sharding. [Register and host the entity](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/hosting.md) once at startup.

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

The [state, command, event, and reply declarations](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/state-commands-events-and-replies.md) are in the guide. You write the domain functions and the handlers; Akka.Durable supplies the actor.

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

## Guide

The rest of the documentation is in [`docs/`](https://github.com/Aaronontheweb/Akka.Durable/tree/dev/docs), one topic per page. Read it in this order:

| Page | What it covers |
|---|---|
| [State, commands, events, and replies](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/state-commands-events-and-replies.md) | How to declare an entity's state, commands, events and replies, and what a decision can do. |
| [Effects with outcomes](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/effects-with-outcomes.md) | `Execute` and effect handlers, for work whose result the entity decides on. |
| [Run or Execute](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/run-or-execute.md) | Which of the two to use for service work. |
| [Resuming after a restart](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/resuming-after-a-restart.md) | `Resume`, for effects that were pending when the process stopped. |
| [Talking to other entities](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/talking-to-other-entities.md) | `Send` and `Accepts`, for one entity that drives another. |
| [Serialization](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/serialization.md) | What is serialized, and how to change stored types safely. |
| [Hosting](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/hosting.md) | Local and sharded hosting, passivation and shard count. |
| [Sending commands](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/sending-commands.md) | Entity refs, `Ask` and `Tell`, and entity IDs inside messages. |
| [Reading state](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/reading-state.md) | State-aware refs, and what a state read sees. |
| [Workflows](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/workflows.md) | Finite processes that start once and end with an outcome: states, work, the fact table, and workflow refs. |
| [Workflow rules](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/workflow-rules.md) | Rules for writing a workflow that stays correct across restarts. |
| [Testing](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/testing.md) | Decisions as plain functions, and the hosted TestKit. |
| [Status and direction](https://github.com/Aaronontheweb/Akka.Durable/blob/dev/docs/status.md) | What works today and what is not done yet. |
