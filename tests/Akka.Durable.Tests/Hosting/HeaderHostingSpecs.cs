using System.Collections.Concurrent;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Bumps;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Durable.Tests.Hosting.Ownership;
using Akka.Actor;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>What the handler of a stock reservation could read as its request id.</summary>
public sealed class RequestSightings
{
    public ConcurrentQueue<string?> Seen { get; } = new();
}

/// <summary>
/// An effect handler that talks to another entity through a plain <see cref="IEntityRefs{TCommand}"/>. It has no
/// header code: the headers come from the propagator, which the library restores before the handler runs.
/// </summary>
public sealed class ReserveThroughShipmentHandler(IEntityRefs<ShipmentCommand> shipments, RequestSightings sightings)
    : IEffectHandler<StockReservation, OrderCommand>
{
    public async Task<OrderCommand> HandleAsync(
        StockReservation effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        sightings.Seen.Enqueue(RequestIdPropagator.Current);
        await shipments.Get(new EntityId(effect.Order)).Ask(new CreateShipment(), cancellationToken);
        return new StockReserved();
    }
}

/// <summary>Orders and shipments, hosted in this process, with messages serialized as if they crossed the network.</summary>
public sealed class HeaderHostingSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override bool VerifyMessageSerialization => true;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) => services
        .AddSingleton<Observations>()
        .AddSingleton<RequestSightings>()
        .AddDurableObserver<RecordingObserver>()
        .AddDurableHeaderPropagator<RequestIdPropagator>()
        .AddEffectHandler<StockReservation, OrderCommand, ReserveThroughShipmentHandler>();

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) => builder
        .WithCustomSerializer(OrderSerializer.CreateSerializerDetails)
        .WithCustomSerializer(ShipmentSerializer.CreateSerializerDetails)
        .WithDurableEntity(Orders.Definition)
        .WithDurableEntity(Shipments.Definition);

    private Observations Seen => Host.Services.GetRequiredService<Observations>();

    private RequestIdPropagator Propagator =>
        Host.Services.GetServices<IHeaderPropagator>().OfType<RequestIdPropagator>().Single();

    [Fact]
    public async Task Headers_injected_at_a_send_reach_the_entity_and_ride_through_an_effect_to_a_second_entity()
    {
        var orders = DurableEntityRef<OrderCommand, OrderState, OrderEvent>("o-1");

        // Injected from outside, at an Ask and at a Tell. Then nothing is left in this flow but the stored headers.
        RequestIdPropagator.Current = "req-h";
        await orders.Ask(new OpenOrder(), AskCancellation());
        orders.Tell(new ReserveStock());
        RequestIdPropagator.Current = null;

        await orders.WaitForStateAsync(state => state.Reserved == 1, AskCancellation());

        var events = Seen.Events;
        var ordersEvents = events.Where(e => e.EntityType == Orders.Definition.TypeName).ToArray();
        Assert.Equal(["OrderOpened", "StockRequested", "StockConfirmed"], ordersEvents.Select(e => e.Event.GetType().Name));
        Assert.All(ordersEvents, e => HeaderAssert.Request("req-h", e.Headers)); // the last is decided on the answer

        // The handler had them restored, and its own call to the second entity picked them up again.
        var shipment = Assert.Single(events, e => e.EntityType == Shipments.Definition.TypeName);
        Assert.IsType<ShipmentCreated>(shipment.Event);
        HeaderAssert.Request("req-h", shipment.Headers);
        Assert.Equal(["req-h"], Host.Services.GetRequiredService<RequestSightings>().Seen);
        HeaderAssert.Request("req-h", Assert.Single(Propagator.Restored));
        Assert.All(Seen.Effects, e => HeaderAssert.Request("req-h", e.Headers));
        await AwaitAssertAsync(() => Assert.Equal(0, Propagator.Open), cancellationToken: TestCancellation);
    }
}

/// <summary>
/// The bump workflow sends a command to the railway from its work. Headers go as far as the workflow lives:
/// after a restart, the work it re-derives has none.
/// </summary>
public sealed class HeaderWorkflowSpecs(ITestOutputHelper output) : BumpWorkflowKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        base.ConfigureTestServices(context, services);
        services.AddSingleton<Observations>()
            .AddDurableObserver<RecordingObserver>()
            .AddDurableHeaderPropagator<RequestIdPropagator>();
    }

    private Observations Seen => Host.Services.GetRequiredService<Observations>();

    private EventSaved[] WorkflowEvents => [.. Seen.Events.Where(e => e.EntityType.Value == Bump.Definition.TypeName.Value)];

    [Fact]
    public async Task A_workflow_that_is_started_with_headers_carries_them_into_its_work_and_the_command_that_work_sends()
    {
        RequestIdPropagator.Current = "req-w";
        await W1.Start(new BumpInput(5, Plan.Send), AskCancellation());
        RequestIdPropagator.Current = null;
        await W1.WaitForOutcome(AskCancellation());

        // The start, and the fact that the railway's reply became: each decided with the headers of the start.
        Assert.Equal(["WfStarted", "WfFactRecorded"], WorkflowEvents.Select(e => e.Event.GetType().Name));
        Assert.All(WorkflowEvents, e => HeaderAssert.Request("req-w", e.Headers));

        // The work ran with them restored, and its Send to the railway picked them up again.
        var railway = Assert.Single(Seen.Events, e => e.EntityType == Railway.Definition.TypeName);
        HeaderAssert.Request("req-w", railway.Headers);
    }

    [Fact]
    public async Task Work_that_a_restart_re_derives_has_no_headers()
    {
        RequestIdPropagator.Current = "req-r";
        await StartGated(Plan.Stay); // the work is in flight
        RequestIdPropagator.Current = null;
        try
        {
            await RestartWorkflow();

            // After the restart the interrupted work gives up as a fact. The library decided it, not a command with headers.
            await AwaitAssertAsync(() => Assert.Equal(["WfStarted", "WfFactRecorded"],
                WorkflowEvents.Select(e => e.Event.GetType().Name)), cancellationToken: TestCancellation);
            HeaderAssert.Request("req-r", WorkflowEvents[0].Headers);
            Assert.Empty(WorkflowEvents[1].Headers);
        }
        finally
        {
            Release();
        }
    }
}

/// <summary>
/// The billing entity starts its campaign workflow with a recorded call. The call is delivered later, by the library,
/// so it must keep the headers of the decision that issued it.
/// </summary>
public sealed class HeaderEntityCallsSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        base.ConfigureTestServices(context, services);
        services.AddSingleton<Observations>()
            .AddDurableObserver<RecordingObserver>()
            .AddDurableHeaderPropagator<RequestIdPropagator>();

        // The kit puts its own refs in front of the workflow's, built with no propagators. These are the library's own.
        services.AddSingleton<IWorkflowRefs<Invoice, CampaignState, CampaignFact, CampaignOutcome>>(provider =>
            new WorkflowRefs<Invoice, CampaignState, CampaignFact, CampaignOutcome>(
                provider.GetRequiredService<IRequiredActor<WorkflowRegion<Invoice, CampaignState, CampaignFact, CampaignOutcome>>>(),
                provider.GetRequiredService<ActorSystem>(),
                provider.GetRequiredService<HeaderPropagation>()));
    }

    [Fact]
    public async Task A_recorded_call_to_a_workflow_keeps_the_headers_of_the_decision_that_issued_it()
    {
        RequestIdPropagator.Current = "req-b";
        await Issue("h1");
        RequestIdPropagator.Current = null;

        await AwaitCampaign<WorkflowRunning<CampaignState>>("h1");

        var seen = Host.Services.GetRequiredService<Observations>();
        var started = Assert.Single(seen.Effects, e => e.Stage == EffectStage.Issued);
        Assert.IsType<StartWorkflow>(started.Effect);
        HeaderAssert.Is(RequestIdPropagator.Key, "req-b", started.Headers);

        // The library delivered the call with them restored, so the workflow's start carries them too.
        var campaign = Assert.Single(seen.Events, e => e.EntityType.Value == Campaign.Definition.TypeName.Value);
        Assert.IsType<WfStarted>(campaign.Event);
        HeaderAssert.Is(RequestIdPropagator.Key, "req-b", campaign.Headers);
    }
}

/// <summary>
/// An entity that drives another through <c>Send</c>. The library sends the command itself, so it passes on the headers
/// of the command being handled. The reply carries nothing back over the wire, but the entity remembers its own headers,
/// so the decision on the reply keeps them.
/// </summary>
public sealed class HeaderSendSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override bool VerifyMessageSerialization => true;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
        services.AddCounterHandlers();
        services.AddSingleton<Observations>()
            .AddDurableObserver<RecordingObserver>()
            .AddDurableHeaderPropagator<RequestIdPropagator>();
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder
            .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
            .WithCustomSerializer(TallySerializer.CreateSerializerDetails)
            .WithDurableEntity(Counter.Definition)
            .WithDurableEntity(Tally.Definition);

    [Fact]
    public async Task A_Send_from_a_decision_passes_on_its_headers_and_the_decision_on_its_reply_keeps_them()
    {
        RequestIdPropagator.Current = "req-s";
        var done = await EntityRef<TallyCommand>("t").Ask(new StartTally("c", 3), AskCancellation());

        Assert.Equal(3, done.Count);
        var events = Host.Services.GetRequiredService<Observations>().Events;

        // The counter got the increment with the headers of the command that the tally was handling.
        var counted = Assert.Single(events, e => e.EntityType == Counter.TypeName);
        HeaderAssert.Is(RequestIdPropagator.Key, "req-s", counted.Headers);

        // The tally decided its start with them. Its end is decided on the counter's reply, which carried none over the
        // wire, and keeps the headers of the decision that issued the Send, so the chain goes on.
        var tally = events.Where(e => e.EntityType == Tally.TypeName).ToArray();
        Assert.Equal(["TallyStarted", "TallyFinished"], tally.Select(e => e.Event.GetType().Name));
        HeaderAssert.Is(RequestIdPropagator.Key, "req-s", tally[0].Headers);
        HeaderAssert.Is(RequestIdPropagator.Key, "req-s", tally[1].Headers);
    }
}
