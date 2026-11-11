using Akka.Hosting;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A workflow and an entity in one host, each with effect handlers. The workflow's effects are plain records that
/// never reach a journal, so the host must not ask a serializer for them. Its work runs as before.
/// </summary>
public sealed class RecordedExecuteWorkflowSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        base.ConfigureTestServices(context, services);
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>().AddCounterHandlers();
    }

    protected override void ConfigureMore(AkkaConfigurationBuilder builder) =>
        builder.WithCustomSerializer(CounterSerializer.CreateSerializerDetails).WithDurableEntity(Counter.Definition);

    [Fact]
    public async Task A_host_with_an_entity_and_a_workflow_runs_the_effects_of_both_and_only_the_entitys_are_recorded()
    {
        var checkout = CheckoutRef("both");
        var counter = DurableEntityRef<CounterCommand, CounterState, CounterEvent>("both");

        await checkout.Start(new PlaceOrder("both", 100), AskCancellation());
        await counter.Ask(new Announce("hi"), AskCancellation());

        Assert.Equal(new OrderPlaced("charge-both"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        await AwaitAssertAsync(
            async () => Assert.Empty(await PendingEffects<CounterCommand>("both")), cancellationToken: TestCancellation);
        Assert.Contains(Host.Services.GetRequiredService<EffectRecorder>().Entries, e => e.StartsWith("announce:hi:counter/both/", StringComparison.Ordinal));
        Assert.Equal(2, Services.Issued.Count); // reserve and charge, once each
    }
}
