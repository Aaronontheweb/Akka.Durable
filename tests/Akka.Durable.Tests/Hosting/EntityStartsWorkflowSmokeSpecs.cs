using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// One smoke spec for each call an entity makes to a workflow: the billing entity starts, notifies and cancels its
/// invoice's reminder campaign. Both are hosted locally.
/// </summary>
public sealed class EntityStartsWorkflowSmokeSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    private static readonly DateTimeOffset FarAway = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // Counts the calls of the campaign's Start function. The first one throws while FailFirstStart is set, so the
    // workflow never answers it.
    private int _starts;
    private volatile bool _failFirstStart;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCampaignHandlers();

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder
            .WithCustomSerializer(CampaignSerializer.CreateSerializerDetails)
            .WithCustomSerializer(BillingSerializer.CreateSerializerDetails)
            // The entity is registered before the workflow it uses.
            .WithDurableEntity(Billing.Definition with
            {
                Options = EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(1) },
            })
            .WithDurableWorkflow(Campaign.Definition with
            {
                Start = input =>
                {
                    if (Interlocked.Increment(ref _starts) == 1 && _failFirstStart)
                        throw new InvalidOperationException("The first start is lost.");
                    return Campaign.Definition.Start(input);
                },
            });

    private IDurableEntityRef<BillingCommand, BillingState, BillingEvent> BillRef(string id) =>
        DurableEntityRef<BillingCommand, BillingState, BillingEvent>(id);

    private IWorkflowRef<Invoice, CampaignState, CampaignFact, CampaignOutcome> CampaignOf(string bill) =>
        WorkflowRef<Invoice, CampaignState, CampaignFact, CampaignOutcome>(Billing.CampaignOf(new EntityId(bill)).Value);

    private async Task AwaitAnswers(string bill, params string[] expected) =>
        await AwaitAssertAsync(async () =>
            Assert.Equal(expected, (await BillRef(bill).GetStateAsync(AskCancellation())).State.Answers),
            TimeSpan.FromSeconds(10), cancellationToken: TestCancellation);

    [Fact]
    public async Task A_start_reaches_the_workflow()
    {
        await BillRef("b1").Ask(new IssueBill(FarAway), AskCancellation());

        await AwaitAssertAsync(async () =>
        {
            var status = (await CampaignOf("b1").GetStateAsync(AskCancellation())).State;
            var running = Assert.IsType<WorkflowRunning<CampaignState>>(status.Value);
            Assert.IsType<Waiting>(running.State.Value);
        }, TimeSpan.FromSeconds(10), cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task A_start_that_was_not_answered_is_completed_after_the_entity_restarts()
    {
        _failFirstStart = true;
        await BillRef("b2").Ask(new IssueBill(FarAway), AskCancellation());

        // The first start is lost with the workflow's failure. The entity restarts before it is answered.
        await AwaitConditionAsync(() => Task.FromResult(Volatile.Read(ref _starts) >= 1), cancellationToken: TestCancellation);
        BillRef("b2").Tell(new CrashBilling());

        await AwaitAssertAsync(async () =>
            Assert.IsType<WorkflowRunning<CampaignState>>((await CampaignOf("b2").GetStateAsync(AskCancellation())).State.Value),
            TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        Assert.True(Volatile.Read(ref _starts) >= 2);
    }

    [Fact]
    public async Task Start_notify_and_cancel_answers_arrive_as_commands()
    {
        var bill = BillRef("b3");

        await bill.Ask(new IssueBillWatched(FarAway), AskCancellation());
        await AwaitAnswers("b3", "Started");

        await bill.Ask(new StopCampaign("written off"), AskCancellation());
        await AwaitAnswers("b3", "Started", "Delivered");

        // The cancel finished the campaign, so the payment finds it closed.
        await bill.Ask(new PayBill(), AskCancellation());
        await AwaitAnswers("b3", "Started", "Delivered", "AlreadyFinished`1");
    }
}

/// <summary>An entity that uses a workflow type which is not hosted cannot start.</summary>
public sealed class EntityUsesUnhostedWorkflowSmokeSpecs
{
    [Fact]
    public async Task The_host_fails_to_start_and_names_the_entity_the_workflow_and_WithDurableWorkflow()
    {
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddAkka("unhosted-workflow", builder => builder
                    .WithCustomSerializer(CampaignSerializer.CreateSerializerDetails)
                    .WithCustomSerializer(BillingSerializer.CreateSerializerDetails)
                    .WithDurableEntity(Billing.Definition));
            })
            .Build();

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(TestContext.Current.CancellationToken));

        var message = ex.ToString();
        Assert.Contains("[billing]", message);
        Assert.Contains("[campaigns]", message);
        Assert.Contains("WithDurableWorkflow(", message);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }
}
