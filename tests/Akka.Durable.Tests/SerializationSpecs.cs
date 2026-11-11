using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Akka.Reminders;
using Akka.Reminders.Serialization;
using Akka.Serialization;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// Exact round trips through the actor system's serialization, for the entity's own types and for the
/// framework messages that carry them.
/// </summary>
public sealed class SerializationSpecs : IAsyncLifetime
{
    private readonly ActorSystem _system = ActorSystem.Create("serialization",
        BootstrapSetup.Create().And(SerializationSetup.Create(system =>
        [
            CounterSerializer.CreateSerializerDetails(system),
            RailwaySerializer.CreateSerializerDetails(system),
            DurableEntitySerializer.CreateSerializerDetails(system),
            // Registered the same way Akka.Reminders' hosting extensions register it.
            SerializerDetails.Create("reminder-serializer", new ReminderSerializer(system),
                [typeof(IReminderWireMessage)]),
        ])));

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

    private static readonly Dictionary<string, object> UnionValues = new()
    {
        ["command with fields"] = (CounterCommand)new Increment(2),
        ["fieldless command"] = (CounterCommand)new GetCount(),
        ["command with TimeSpan"] = (CounterCommand)new RemindLater(TimeSpan.FromSeconds(3)),
        ["event"] = (CounterEvent)new Incremented(5),
        ["fieldless event"] = (CounterEvent)new CounterLocked(),
        ["typed success reply"] = (AdjustmentReply)new Adjusted(7),
        ["typed failure reply"] = (AdjustmentReply)new AdjustmentRejected("failed"),
    };

    public static TheoryData<string> UnionValueNames => [.. UnionValues.Keys];

    [Theory]
    [MemberData(nameof(UnionValueNames))]
    public void Union_values_round_trip_as_the_union(string name)
    {
        var value = UnionValues[name];

        var result = RoundTrip(value);

        Assert.IsType(value.GetType(), result);
        Assert.Equal(value, result);
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
    public void Reminder_envelope_carries_a_union_command_payload()
    {
        // How a scheduled TCommand travels from reminder storage to the entity.
        var due = new DateTimeOffset(2026, 11, 7, 12, 0, 0, TimeSpan.Zero);
        var sent = new ReminderEnvelope(new ReminderEntity("counter", "a"), new ReminderKey("tick"), due,
            ReminderDeadline.Infinite, (CounterCommand)new Increment(1));

        var received = (ReminderEnvelope)RoundTrip(sent);

        Assert.Equal((CounterCommand)new Increment(1), received.Message);
        Assert.Equal(new ReminderKey("tick"), received.Key);
        Assert.Equal(due, received.DueTimeUtc);
    }

    [Fact]
    public void Reminder_envelope_carries_the_wake_of_an_entity_with_pending_effects()
    {
        // The reminder that wakes an entity after a crash, so its pending effects are delivered or given up.
        var due = new DateTimeOffset(2026, 11, 7, 12, 0, 0, TimeSpan.Zero);
        var sent = new ReminderEnvelope(new ReminderEntity("billing", "b1"), new ReminderKey(EffectsWake.Key), due,
            ReminderDeadline.Infinite, new EffectsWake(due));

        var received = (ReminderEnvelope)RoundTrip(sent);

        Assert.Equal(new EffectsWake(due), received.Message);
        Assert.Equal(new ReminderKey("durable-effects-wake"), received.Key);
    }

    [Fact]
    public void Sharding_envelope_carries_a_union_command()
    {
        var envelope = (ShardingEnvelope)RoundTrip(new ShardingEnvelope("a", (CounterCommand)new Increment(4)));

        Assert.Equal("a", envelope.EntityId);
        Assert.Equal((CounterCommand)new Increment(4), envelope.Message);
    }
}
