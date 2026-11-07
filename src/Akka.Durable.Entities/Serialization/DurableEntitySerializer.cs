using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Entities.Serialization;

/// <summary>
/// Marker for Akka.Durable's own messages that can cross a serialization boundary.
/// </summary>
internal interface IDurableEntityProtocol;

/// <summary>
/// Framework query sent by <see cref="Abstractions.Entities.IDurableEntityRef{TCommand,TState,TEvent}.GetStateAsync"/>.
/// The entity actor answers it with an <see cref="EntityStateReply"/> without running Decide.
/// </summary>
[AkkaSerializable(Manifest = "durable-get-state-v1", AllowEmpty = true)]
internal sealed record GetEntityState : IDurableEntityProtocol
{
    public static readonly GetEntityState Instance = new();
}

/// <summary>
/// Reply to <see cref="GetEntityState"/>. Not generic, so no per-entity registration is needed: the state
/// is an envelope payload, written with whatever serializer is bound to the entity's state type.
/// </summary>
[AkkaSerializable(Manifest = "durable-state-reply-v1")]
internal sealed record EntityStateReply(
    [property: AkkaField(0)] object State,
    [property: AkkaField(1)] long SequenceNr) : IDurableEntityProtocol;

/// <summary>
/// Serializer for Akka.Durable's framework messages. Registered automatically by <c>WithDurableEntity</c>.
/// </summary>
[AkkaSerializer<IDurableEntityProtocol>("akka-durable", SerializerId)]
internal sealed partial class DurableEntitySerializer : AkkaSerializer
{
    public const int SerializerId = 22560;

    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}
