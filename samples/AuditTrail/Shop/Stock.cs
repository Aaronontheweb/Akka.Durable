using Akka.Serialization.V2;

namespace Akka.Durable.Samples.AuditTrail.Shop;

using StockContext = DecisionContext<StockState, StockCommand, StockEvent>;

// A stock item, one entity for each item. It counts the units that were reserved.

[AkkaSerializable(Manifest = "shop-stock-state-v1")]
public sealed record StockState([property: AkkaField(0)] int Reserved) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-reserve-v1")]
public sealed record Reserve([property: AkkaField(0)] string Order) : IShopProtocol, IReplies<StockCommand, StockReply>
{
    public StockCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "shop-stock-command-v1")]
public readonly union StockCommand(Reserve) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-stock-reserved-v1")]
public sealed record StockReserved([property: AkkaField(0)] string Order) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-stock-event-v1")]
public readonly union StockEvent(StockReserved) : IShopProtocol;

[AkkaSerializable(Manifest = "shop-stock-reply-v1")]
public sealed record StockReply([property: AkkaField(0)] int Reserved) : IShopProtocol;

public static class Stock
{
    public static readonly EntityDefinition<StockState, StockCommand, StockEvent> Definition = new(
        new EntityTypeName("stock"),
        _ => new StockState(0),
        Decide,
        (state, _) => new StockState(state.Reserved + 1));

    private static Decision<StockCommand, StockEvent> Decide(StockState state, StockCommand command, StockContext ctx) =>
        command switch
        {
            Reserve c => ctx.Persist(new StockReserved(c.Order)).Reply(c, new StockReply(state.Reserved + 1)),
        };
}
