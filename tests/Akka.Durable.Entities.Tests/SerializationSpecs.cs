using System.Collections.Immutable;
using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Cluster.Sharding;
using Akka.Durable.Entities.Serialization;
using Akka.Durable.Entities.Tests.Fixtures;
using Akka.Serialization;
using Xunit;

namespace Akka.Durable.Entities.Tests;

/// <summary>
/// Exact round trips through the actor system's serialization, for the entity's own types and for the
/// framework messages that carry them.
/// </summary>
public sealed class SerializationSpecs : IAsyncLifetime
{
    private readonly ActorSystem _system = ActorSystem.Create("serialization",
        BootstrapSetup.Create().And(SerializationSetup.Create(system => ImmutableHashSet.Create(
            CounterSerializer.CreateSerializerDetails(system),
            DurableEntitySerializer.CreateSerializerDetails(system)))));

    private Akka.Serialization.Serialization Serialization => _system.Serialization;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _system.Terminate();

    private object RoundTrip(object message)
    {
        var serializer = Serialization.FindSerializerFor(message);
        var bytes = serializer.ToBinary(message);
        var manifest = Akka.Serialization.Serialization.ManifestFor(serializer, message);
        return Serialization.Deserialize(bytes, serializer.Identifier, manifest);
    }

    public static TheoryData<string, object> UnionValues => new()
    {
        { "command with fields", (CounterCommand)new Increment(2) },
        { "fieldless command", (CounterCommand)new GetCount() },
        { "command with TimeSpan", (CounterCommand)new RemindLater(TimeSpan.FromSeconds(3)) },
        { "event", (CounterEvent)new Incremented(5) },
        { "fieldless event", (CounterEvent)new CounterLocked() },
    };

    [Theory]
    [MemberData(nameof(UnionValues))]
    public void Union_values_round_trip_as_the_union(string description, object value)
    {
        var result = RoundTrip(value);

        Assert.IsType(value.GetType(), result);
        Assert.Equal(value, result);
        Assert.NotNull(description);
    }

    [Fact]
    public void State_round_trips()
    {
        var state = new CounterState(7, true);

        Assert.Equal(state, RoundTrip(state));
    }

    [Fact]
    public void Framework_state_reply_carries_the_state_through_its_own_serializer()
    {
        var reply = (EntityStateReply)RoundTrip(new EntityStateReply(new CounterState(3, false), 12));

        Assert.Equal(new CounterState(3, false), reply.State);
        Assert.Equal(12, reply.SequenceNr);
    }

    [Fact]
    public void Framework_state_query_round_trips()
    {
        Assert.IsType<GetEntityState>(RoundTrip(GetEntityState.Instance));
    }

    [Fact]
    public void Sharding_envelope_carries_a_union_command()
    {
        var envelope = (ShardingEnvelope)RoundTrip(new ShardingEnvelope("a", (CounterCommand)new Increment(4)));

        Assert.Equal("a", envelope.EntityId);
        Assert.Equal((CounterCommand)new Increment(4), envelope.Message);
    }
}
