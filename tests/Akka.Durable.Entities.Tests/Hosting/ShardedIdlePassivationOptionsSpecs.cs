using Akka.Cluster.Hosting;
using Akka.Durable.Entities.Hosting;
using Akka.Durable.Entities.Tests.Fixtures;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Entities.Tests.Hosting;

/// <summary>
/// How <see cref="EntityOptions.PassivateIdleAfter"/> maps onto sharding's idle passivation setting.
/// </summary>
public class ShardedIdlePassivationOptionsSpecs
{
    private static ShardOptions Register(
        EntityDefinition<CounterState, CounterCommand, CounterEvent> definition, ShardOptions shardOptions)
    {
        new AkkaConfigurationBuilder(new ServiceCollection(), "test").WithDurableEntity(definition, shardOptions);
        return shardOptions;
    }

    private static EntityDefinition<CounterState, CounterCommand, CounterEvent> WithIdleTimeout(TimeSpan timeout) =>
        Counter.Definition with { Options = EntityOptions.Default with { PassivateIdleAfter = timeout } };

    [Fact]
    public void Unset_shard_option_takes_the_entity_default()
    {
        var options = Register(Counter.Definition, new ShardOptions());

        Assert.Equal(EntityOptions.Default.PassivateIdleAfter, options.PassivateIdleEntityAfter);
    }

    [Fact]
    public void Unset_shard_option_takes_the_entity_option()
    {
        var options = Register(WithIdleTimeout(TimeSpan.FromSeconds(42)), new ShardOptions());

        Assert.Equal(TimeSpan.FromSeconds(42), options.PassivateIdleEntityAfter);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1000)]
    public void Disabled_entity_option_maps_to_shardings_off_value(int milliseconds)
    {
        // Zero renders as "0", which sharding reads as off; a negative value would fail HOCON rendering.
        var options = Register(WithIdleTimeout(TimeSpan.FromMilliseconds(milliseconds)), new ShardOptions());

        Assert.Equal(TimeSpan.Zero, options.PassivateIdleEntityAfter);
    }

    [Fact]
    public void Explicit_shard_option_wins()
    {
        var options = Register(
            WithIdleTimeout(TimeSpan.FromSeconds(42)),
            new ShardOptions { PassivateIdleEntityAfter = TimeSpan.FromMinutes(10) });

        Assert.Equal(TimeSpan.FromMinutes(10), options.PassivateIdleEntityAfter);
    }
}
