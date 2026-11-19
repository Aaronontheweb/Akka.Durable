using Akka.Actor;
using Akka.Hosting;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Samples.AuditTrail.Shop;

[AkkaSerializer<IShopProtocol>("shop", 22801)]
public sealed partial class ShopSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class ShopHosting
{
    /// <summary>Registers the effect handler of the shop.</summary>
    public static IServiceCollection AddShop(this IServiceCollection services) =>
        services.AddEffectHandler<ReserveStock, OrderCommand, ReserveStockHandler>();

    /// <summary>Hosts the order and stock entities. An unpaid order expires after <paramref name="expireAfter"/>.</summary>
    public static AkkaConfigurationBuilder WithShop(this AkkaConfigurationBuilder builder, TimeSpan expireAfter) => builder
        .WithCustomSerializer(ShopSerializer.CreateSerializerDetails)
        .WithDurableEntity(Order.Definition(expireAfter))
        .WithDurableEntity(Stock.Definition);
}
