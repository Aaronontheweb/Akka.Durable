using Akka.Cluster.Sharding;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// How <see cref="ShardedHosting"/> and <see cref="EntityOptions"/> become the settings of a shard region.
/// The application chooses where the entities run. The stop message and idle passivation are the library's.
/// </summary>
public class ShardedHostingOptionsSpecs
{
    private static readonly object StopMessage = new();

    private static EntityOptions WithIdleTimeout(TimeSpan timeout) =>
        EntityOptions.Default with { PassivateIdleAfter = timeout };

    [Fact]
    public void The_applications_sharding_settings_are_passed_on()
    {
        var sharding = new ShardedHosting
        {
            Role = "billing",
            StateStoreMode = StateStoreMode.DData,
            RememberEntities = true,
            RememberEntitiesStore = RememberEntitiesStore.Eventsourced,
        };

        var options = DurableEntityHostingExtensions.ToShardOptions(sharding, EntityOptions.Default, StopMessage);

        Assert.Equal("billing", options.Role);
        Assert.Equal(StateStoreMode.DData, options.StateStoreMode);
        Assert.True(options.RememberEntities);
        Assert.Equal(RememberEntitiesStore.Eventsourced, options.RememberEntitiesStore);
    }

    [Fact]
    public void Settings_the_application_left_out_stay_unset()
    {
        var options = DurableEntityHostingExtensions.ToShardOptions(new ShardedHosting(), EntityOptions.Default, StopMessage);

        Assert.Null(options.Role);
        Assert.Null(options.StateStoreMode);
        Assert.Null(options.RememberEntities);
        Assert.Null(options.RememberEntitiesStore);
    }

    [Fact]
    public void The_stop_message_is_always_the_librarys()
    {
        var options = DurableEntityHostingExtensions.ToShardOptions(new ShardedHosting(), EntityOptions.Default, StopMessage);

        Assert.Same(StopMessage, options.HandOffStopMessage);
    }

    [Fact]
    public void Idle_passivation_takes_the_entity_default()
    {
        var options = DurableEntityHostingExtensions.ToShardOptions(new ShardedHosting(), EntityOptions.Default, StopMessage);

        Assert.Equal(EntityOptions.Default.PassivateIdleAfter, options.PassivateIdleEntityAfter);
    }

    [Fact]
    public void Idle_passivation_takes_the_entity_option()
    {
        var options = DurableEntityHostingExtensions.ToShardOptions(
            new ShardedHosting(), WithIdleTimeout(TimeSpan.FromSeconds(42)), StopMessage);

        Assert.Equal(TimeSpan.FromSeconds(42), options.PassivateIdleEntityAfter);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1000)]
    public void A_disabled_entity_option_maps_to_the_value_sharding_reads_as_off(int milliseconds)
    {
        // Zero renders as "0", which sharding reads as off; a negative value would fail HOCON rendering.
        var options = DurableEntityHostingExtensions.ToShardOptions(
            new ShardedHosting(), WithIdleTimeout(TimeSpan.FromMilliseconds(milliseconds)), StopMessage);

        Assert.Equal(TimeSpan.Zero, options.PassivateIdleEntityAfter);
    }

    [Fact]
    public void Each_registration_gets_its_own_shard_options()
    {
        var sharding = new ShardedHosting { Role = "billing" };

        var first = DurableEntityHostingExtensions.ToShardOptions(sharding, EntityOptions.Default, StopMessage);
        var second = DurableEntityHostingExtensions.ToShardOptions(sharding, EntityOptions.Default, new object());

        // One ShardedHosting value can be reused for many entity types: nothing is written back to it.
        Assert.NotSame(first, second);
        Assert.Same(StopMessage, first.HandOffStopMessage);
        Assert.NotSame(StopMessage, second.HandOffStopMessage);
    }
}
