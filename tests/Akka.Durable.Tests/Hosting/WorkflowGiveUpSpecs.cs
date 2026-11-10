using Akka.Durable.Tests.Fixtures.Checkouts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Work that runs out of time gets no second attempt: the workflow receives the give-up fact.
/// </summary>
public sealed class WorkflowAttemptLimitSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        Checkout.Definition with { Options = new WorkflowOptions { AttemptLimit = TimeSpan.FromMilliseconds(300) } };

    [Fact]
    public async Task Work_that_takes_longer_than_the_attempt_limit_gives_up()
    {
        Services.Charge = (_, ct) => Never(ct);

        await CheckoutRef("slow").Start(new PlaceOrder("slow", 100), AskCancellation());
        var running = await SettledIn<NeedsAttention>("slow");

        Assert.Equal(
            new NeedsAttention(new Charging("res-slow"), "ChargeCard TimedOut"),
            ((CheckoutState)running.State).Value);
        Assert.Single(Services.Issued.OfType<ChargeCard>());
    }
}

/// <summary>
/// Work with no registered handler is work that gets no answer. The workflow does not crash.
/// </summary>
public sealed class WorkflowMissingHandlerSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    // Only the stock service is registered.
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services
            .AddSingleton<CheckoutServices>()
            .AddScoped<IEffectHandler<ReserveStock, CheckoutFact>, ReserveStockHandler>();

    [Fact]
    public async Task Work_with_no_registered_handler_gives_up()
    {
        await CheckoutRef("o1").Start(new PlaceOrder("o1", 100), AskCancellation());

        // The charge has no handler, and neither has the ticket: both give up, and the table decides both.
        var running = await SettledIn<NeedsAttention>("o1");

        Assert.Equal(
            new NeedsAttention(new Charging("res-o1"), "ChargeCard Failed"),
            ((CheckoutState)running.State).Value);
        Assert.Equal([new ReserveStock("o1")], Services.Issued);
    }
}
