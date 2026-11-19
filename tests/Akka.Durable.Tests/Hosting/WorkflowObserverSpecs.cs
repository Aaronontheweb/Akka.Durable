using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>A workflow runs on the same actor as an entity, so an observer sees it like any entity.</summary>
public sealed class WorkflowObserverSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCheckoutHandlers().AddSingleton<Observations>().AddDurableObserver<RecordingObserver>();

    [Fact]
    public async Task A_workflow_run_is_observed_like_an_entity_with_the_librarys_own_events()
    {
        var checkout = CheckoutRef("observed");
        await checkout.Start(new PlaceOrder("observed", 4200), AskCancellation());
        await checkout.WaitForOutcome(AskCancellation());

        var log = Host.Services.GetRequiredService<Observations>();
        var events = log.Events;

        // Each event of the run, in journal order, for this workflow's id.
        Assert.All(events, e => Assert.Equal(new EntityId("observed"), e.EntityId));
        Assert.All(events, e => Assert.Equal(Checkout.Definition.TypeName.Value, e.EntityType.Value));
        Assert.Equal(events.Select(e => e.SequenceNr).Order(), events.Select(e => e.SequenceNr));
        Assert.Equal(events.Length, events.Select(e => e.SequenceNr).Distinct().Count());

        // The objects are the library's own, and a union is shown as the case it holds.
        Assert.IsType<WfStarted>(events[0].Event);
        Assert.IsType<WfStart>(events[0].Command);

        // A workflow's work is not a recorded effect, and its wake reminder is the library's own.
        Assert.Empty(log.Effects);
        Assert.Empty(log.Timers);
    }
}
