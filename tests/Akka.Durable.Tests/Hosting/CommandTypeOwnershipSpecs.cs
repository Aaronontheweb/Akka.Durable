using Akka.Cluster.Hosting;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Entity refs and Send find an entity type by its command type, so <c>WithDurableEntity</c> rejects a second
/// entity type with the same one. Registration-time checks; no running host.
/// </summary>
public class CommandTypeOwnershipSpecs
{
    private static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> SecondCounter =
        Counter.Definition with { TypeName = new EntityTypeName("counter-two") };

    private static AkkaConfigurationBuilder Builder() => new(new ServiceCollection(), "test");

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Two_entity_types_cannot_share_a_command_type(bool sharded)
    {
        var builder = Builder().WithDurableEntity(Counter.Definition, sharded ? new ShardedHosting() : null);

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.WithDurableEntity(SecondCounter, sharded ? new ShardedHosting() : null));

        Assert.Contains("[counter]", ex.Message);
        Assert.Contains("[counter-two]", ex.Message);
        Assert.Contains(typeof(CounterCommand).FullName!, ex.Message);
    }

    [Fact]
    public void An_entity_type_cannot_be_registered_twice()
    {
        var builder = Builder().WithDurableEntity(Counter.Definition);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.WithDurableEntity(Counter.Definition));

        Assert.Contains("[counter] is registered twice", ex.Message);
    }

    [Fact]
    public void Entity_types_with_their_own_command_types_register_together()
    {
        var builder = Builder();

        Assert.Same(builder, builder.WithDurableEntity(Counter.Definition).WithDurableEntity(Tally.Definition));
    }

    [Fact]
    public void Separate_actor_systems_can_host_the_same_entity_type()
    {
        Builder().WithDurableEntity(Counter.Definition);

        var second = Builder();
        Assert.Same(second, second.WithDurableEntity(Counter.Definition));
    }
}
