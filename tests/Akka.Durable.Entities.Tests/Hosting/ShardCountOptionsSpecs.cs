using Akka.Cluster.Hosting;
using Akka.Durable.Entities.Hosting;
using Akka.Durable.Entities.Tests.Fixtures;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Entities.Tests.Hosting;

/// <summary>
/// Registration-time handling of <see cref="EntityOptions.NumberOfShards"/>; no running host.
/// </summary>
public class ShardCountOptionsSpecs
{
    private static AkkaConfigurationBuilder Builder() => new(new ServiceCollection(), "test");

    private static EntityDefinition<CounterState, CounterCommand, CounterEvent> WithShards(int shards) =>
        Counter.Definition with { Options = EntityOptions.Default with { NumberOfShards = shards } };

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Non_positive_shard_count_is_rejected_when_sharded(int shards)
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => Builder().WithDurableEntity(WithShards(shards), new ShardOptions()));

        Assert.Contains("NumberOfShards", ex.Message);
    }

    [Fact]
    public void Shard_count_is_ignored_when_hosted_locally()
    {
        var builder = Builder();

        Assert.Same(builder, builder.WithDurableEntity(WithShards(0)));
    }
}
