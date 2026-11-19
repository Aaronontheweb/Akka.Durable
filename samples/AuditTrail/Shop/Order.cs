using Akka.Serialization.V2;

namespace Akka.Durable.Samples.AuditTrail.Shop;

using OrderContext = DecisionContext<OrderState, OrderCommand, OrderEvent>;

// An order. Placing it reserves stock (a recorded effect) and sets a timer. If the order is still unpaid when the
// timer fires, it expires. Nothing here knows about the caller.

/// <summary>Marks every type of the shop that the one serializer handles.</summary>
public interface IShopProtocol;

[AkkaSerializable(Manifest = "shop-order-state-v1")]
public sealed record OrderState([property: AkkaField(0)] string Status) : IShopProtocol;

// Commands
[AkkaSerializable(Manifest = "shop-place-order-v1")]
public sealed record PlaceOrder([property: AkkaField(0)] string Item) : IShopProtocol, IReplies<OrderCommand, OrderReply>
{
    public OrderCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "shop-pay-order-v1", AllowEmpty = true)]
public sealed record PayOrder : IShopProtocol, IReplies<OrderCommand, OrderReply>
{
    public OrderCommand ToCommand() => new(this);
}

/// <summary>The command of the timer.</summary>
[AkkaSerializable(Manifest = "shop-expire-v1", AllowEmpty = true)]
public sealed record Expire : IShopProtocol;

/// <summary>The answer of the stock reservation, or its give-up.</summary>
[AkkaSerializable(Manifest = "shop-stock-result-v1")]
public sealed record StockResult([property: AkkaField(0)] bool Reserved) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-order-command-v1")]
public readonly union OrderCommand(PlaceOrder, PayOrder, Expire, StockResult) : IShopProtocol;

// Events
[AkkaSerializable(Manifest = "shop-order-placed-v1")]
public sealed record OrderPlaced([property: AkkaField(0)] string Item) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-stock-confirmed-v1")]
public sealed record StockConfirmed([property: AkkaField(0)] bool Reserved) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-order-paid-v1", AllowEmpty = true)]
public sealed record OrderPaid : IShopProtocol;

[AkkaSerializable(Manifest = "shop-order-expired-v1", AllowEmpty = true)]
public sealed record OrderExpired : IShopProtocol;

[AkkaSerializable(Manifest = "shop-order-event-v1")]
public readonly union OrderEvent(OrderPlaced, StockConfirmed, OrderPaid, OrderExpired) : IShopProtocol;

// Reply and effect
[AkkaSerializable(Manifest = "shop-order-reply-v1")]
public sealed record OrderReply([property: AkkaField(0)] string Status) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-reserve-stock-v1")]
public sealed record ReserveStock([property: AkkaField(0)] string Order, [property: AkkaField(1)] string Item) : IShopProtocol;

/// <summary>Asks the stock entity to reserve one unit, and answers the order with the result.</summary>
public sealed class ReserveStockHandler(IEntityRefs<StockCommand> stock) : IEffectHandler<ReserveStock, OrderCommand>
{
    public async Task<OrderCommand> HandleAsync(ReserveStock effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        await stock.Get(new EntityId(effect.Item)).Ask(new Reserve(effect.Order), cancellationToken);
        return new StockResult(true);
    }
}

public static class Order
{
    public static EntityDefinition<OrderState, OrderCommand, OrderEvent> Definition(TimeSpan expireAfter) => new(
        new EntityTypeName("order"),
        _ => new OrderState("new"),
        (state, command, ctx) => Decide(state, command, ctx, expireAfter),
        Evolve,
        GaveUp: _ => new StockResult(false));

    private static Decision<OrderCommand, OrderEvent> Decide(
        OrderState state, OrderCommand command, OrderContext ctx, TimeSpan expireAfter) =>
        command switch
        {
            PlaceOrder c when state.Status == "new" => ctx.Persist(new OrderPlaced(c.Item))
                .Execute(new ReserveStock(ctx.EntityId.Value, c.Item))
                .Schedule("expire", expireAfter, new Expire())
                .Reply(c, new OrderReply("placed")),
            PlaceOrder c => ctx.None().Reply(c, new OrderReply(state.Status)),

            PayOrder c when state.Status == "placed" => ctx.Persist(new OrderPaid()).Reply(c, new OrderReply("paid")),
            PayOrder c => ctx.None().Reply(c, new OrderReply(state.Status)),

            // The timer fired. Only an unpaid order expires.
            Expire when state.Status == "placed" => ctx.Persist(new OrderExpired()),
            Expire => ctx.Ignore(),

            StockResult r => ctx.Persist(new StockConfirmed(r.Reserved)),
        };

    private static OrderState Evolve(OrderState state, OrderEvent @event) =>
        @event switch
        {
            OrderPlaced => new OrderState("placed"),
            OrderPaid => new OrderState("paid"),
            OrderExpired => new OrderState("expired"),
            _ => state,
        };
}
