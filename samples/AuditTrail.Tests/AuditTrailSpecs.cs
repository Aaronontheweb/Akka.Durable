using Akka.Durable.Samples.AuditTrail.Audit;
using Akka.Durable.Samples.AuditTrail.Shop;
using Akka.Durable.TestKit;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Samples.AuditTrail.Tests;

/// <summary>
/// Runs the scenario of the sample: two orders, one paid and one not. The sample's own services and entities are
/// hosted by the test kit. The test plays the part of the middleware by setting the audit context before each send.
/// </summary>
public sealed class AuditTrailSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) => services
        .AddShop()
        .AddAuditTrail();

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) => builder
        .WithShop(expireAfter: TimeSpan.FromSeconds(2))
        .WithAuditCollector();

    private AuditStore Store => Host.Services.GetRequiredService<AuditStore>();

    private async Task<string> Act(string who, string door, Func<Task<OrderReply>> send)
    {
        var context = AuditContext.Start(who, door);
        AuditContext.Current = context;
        try { await send(); }
        finally { AuditContext.Current = null; }
        return context.ActionId;
    }

    private AuditRow Row(string entity, string @event) =>
        Assert.Single(Store.Rows, row => row.Entity == entity && row.Event == @event);

    [Fact]
    public async Task The_trail_says_who_did_each_thing_and_what_followed_from_it()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(TimerObserved));
        var orders = EntityRefs<OrderCommand>();

        // "ann" places order o-1 and leaves it. "ann" places order o-2, and "bob" pays it.
        var placed1 = await Act("ann", "test place o-1", () => orders.Get(new EntityId("o-1")).Ask(new PlaceOrder("widget"), AskCancellation()));
        var placed2 = await Act("ann", "test place o-2", () => orders.Get(new EntityId("o-2")).Ask(new PlaceOrder("gadget"), AskCancellation()));
        var paid2 = await Act("bob", "test pay o-2", () => orders.Get(new EntityId("o-2")).Ask(new PayOrder(), AskCancellation()));

        // Both timers fire after two seconds. Wait for both, and for the expiry of o-1.
        await AwaitAssertAsync(() => Assert.Equal(2, Store.Rows.Count(r => r.Event == "StockConfirmed")), cancellationToken: TestCancellation);
        for (var i = 0; i < 2; i++)
            await FishForMessageAsync<TimerObserved>(
                timer => timer.Stage == TimerStage.Fired && timer.Key == "expire", TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);
        await AwaitAssertAsync(() => Row("order/o-1", "OrderExpired"), TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);

        // o-1: placed by ann, and the stock entity records the same person and the same action id.
        var order = Row("order/o-1", "OrderPlaced");
        Assert.Equal(("ann", placed1), (order.Who, order.ActionId));
        Assert.Equal("Command", order.Cause);
        var stock = Row("stock/widget", "StockReserved");
        Assert.Equal(("ann", placed1, "test place o-1"), (stock.Who, stock.ActionId, stock.Door));
        var confirmed = Row("order/o-1", "StockConfirmed");
        Assert.Equal(("ann", placed1), (confirmed.Who, confirmed.ActionId));
        Assert.StartsWith("EffectAnswer", confirmed.Cause);

        // o-1 expired, started by its timer: the system did it, and the row names the timer.
        var expired = Row("order/o-1", "OrderExpired");
        Assert.Equal(("system", "timer expire", "Timer expire"), (expired.Who, expired.Door, expired.Cause));
        Assert.NotEqual(placed1, expired.ActionId);

        // o-2: placed by ann, paid by bob, and never expired, although its timer fired as well.
        Assert.Equal(("ann", placed2), (Row("stock/gadget", "StockReserved").Who, Row("stock/gadget", "StockReserved").ActionId));
        Assert.Equal(("bob", paid2), (Row("order/o-2", "OrderPaid").Who, Row("order/o-2", "OrderPaid").ActionId));
        Assert.DoesNotContain(Store.Rows, row => row.Entity == "order/o-2" && row.Event == "OrderExpired");
        Assert.Equal("paid", (await DurableEntityRef<OrderCommand, OrderState, OrderEvent>("o-2").GetStateAsync(AskCancellation())).State.Status);
    }
}
