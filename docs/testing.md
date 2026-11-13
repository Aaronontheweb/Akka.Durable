# Testing

Part of the [Akka.Durable guide](../README.md#guide).

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

A decider with a blanket arm in each state needs these tests: the compiler does not report a pair of state and command that nobody placed (see [How to structure a decider](state-commands-events-and-replies.md#how-to-structure-a-decider)). For each state, test each command it must accept, and at least one command it must refuse. For a command that a caller sent, test the refusal reply. For a late answer of an effect, test that it is ignored:

```csharp
[Fact]
public void Paying_a_draft_is_refused() =>
    DeciderScenario.For(Invoice.Definition)
        .When(new RecordPayment(250m))
        .ThenNothingPersisted()
        .ThenReplied(new RecordPayment(250m), new InvoiceRejected("Invalid invoice operation"));

[Fact]
public void A_late_verification_answer_is_ignored_once_the_invoice_is_issued() =>
    DeciderScenario.For(Invoice.Definition)
        .Given(
            new VerificationStarted(new IssueInvoice(250m, "billing@example.com")),
            new InvoiceIssued(250m, "billing@example.com", DateTimeOffset.UnixEpoch))
        .When(new RecipientVerified())
        .ThenIgnored();
```

Effects are data, so both halves of an `Execute` are pure tests with no fakes. `ThenExecuted` and `ThenSent` compare the effect by value. `WhenGaveUp` decides the command the effect's own `GaveUp` returns, so each way an effect can end is a pure test too:

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

// The process stopped after VerifyRecipient was journaled and before its answer was.
DeciderScenario.For(Invoice.Definition)
    .Given(new VerificationStarted(command))
    .WhenGaveUp(new VerifyRecipient("billing@example.com"), GaveUpReason.Interrupted)
    .ThenPersisted(new VerificationFailed("directory: Interrupted"));
```

A handler is a plain class. `EffectDelivery` has a public constructor, so a test can call a handler twice with the same delivery and check that the second call changes nothing. That is the test a handler registered with a retry policy needs.

For a handler with a limited policy, test the end of the policy the same way: `.WhenGaveUp(effect, GaveUpReason.RetriesExhausted, attempts: 5)`.

Calls to a workflow are effects too. `ThenStarted`, `ThenNotified` and `ThenCancelled` compare them by value. They name the workflow by its definition, like the decision does:

```csharp
DeciderScenario.For(Invoice.Definition)
    .Given(new VerificationStarted(command))
    .When(new RecipientVerified())
    .ThenStarted(ReminderCampaign.Definition, new WorkflowId("reminders-test-entity"),
        new CampaignInput("test-entity", new DateTimeOffset(2026, 1, 31, 0, 0, 0, TimeSpan.Zero)));

DeciderScenario.For(Invoice.Definition)
    .Given(new VerificationStarted(command), new InvoiceIssued(250m, "billing@example.com", DateTimeOffset.UnixEpoch))
    .When(new RecordPayment(250m))
    .ThenNotified(ReminderCampaign.Definition, new WorkflowId("reminders-test-entity"), new InvoicePaid());

DeciderScenario.For(Invoice.Definition)
    .Given(new VerificationStarted(command), new InvoiceIssued(250m, "billing@example.com", DateTimeOffset.UnixEpoch))
    .When(new WriteOff("customer closed"))
    .ThenCancelled(ReminderCampaign.Definition, new WorkflowId("reminders-test-entity"), "customer closed", "invoices");
```

`When` fails the test when a decision calls a workflow type that the definition does not name with `Uses`, or notifies or cancels a workflow whose `Uses` has no `told` and `gaveUp`. At run time that call would be refused with an error log.

The answer of a call is an ordinary command, so you test what it means with `When`, like any other command.

For the full runtime, `DurableEntityTestKit` hosts entities with a test journal (supports failure injection), in-memory reminders and DI. Register effect handlers in `ConfigureTestServices`, with real or fake services behind them. The test journal keeps objects in memory and never serializes them; to check that your types round-trip, run a spec against a real journal plugin. Akka.Durable's own tests use Akka.Persistence.Sql with SQLite. `DurableEntityTestKit` can also run with `akka.actor.serialize-messages = on`: override `VerifyMessageSerialization` to return `true`, and every message, including union events, is serialized and read back as if it crossed the network. The startup check still finds an effect type with no serializer, because it does not depend on the journal.

`PendingEffects<TCommand>(id)` returns the effects an entity has issued and not yet had answered or given up, in the order they were issued. It is for tests only. Asking starts the entity, and a started entity first gives up any effect it finds pending in its journal, so the list holds only effects of the running incarnation.

```csharp
public class InvoiceSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services
            .AddScoped<IInvoiceMailer, FakeInvoiceMailer>()
            .AddEffectHandler<VerifyRecipient, InvoiceCommand, VerifyRecipientHandler>()
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

---

Next: [Status and direction](status.md)
