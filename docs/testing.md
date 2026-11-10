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

Calls to a workflow are effects too. `ThenStarted`, `ThenNotified` and `ThenCancelled` compare them by value, after `When` and after `Resumed()`. They name the workflow by its definition, like the decision does. `ThenStarted` matches a start with or without `then`:

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

`When` and `Resumed()` fail the test when a decision calls a workflow type that the definition does not name with `Uses`. At run time that call would be refused with an error log.

The answer of a call is an ordinary command, so you test what it means with `When`, like any other command.

For the full runtime, `DurableEntityTestKit` hosts entities with a test journal (supports failure injection), in-memory reminders and DI. Register effect handlers in `ConfigureTestServices`, with real or fake services behind them. The test journal keeps objects in memory and never serializes them; to check that your types round-trip, run a spec against a real journal plugin. Akka.Durable's own tests use Akka.Persistence.Sql with SQLite. `DurableEntityTestKit` can also run with `akka.actor.serialize-messages = on`: override `VerifyMessageSerialization` to return `true`, and every message, including union events, is serialized and read back as if it crossed the network.

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

---

Next: [Status and direction](status.md)
