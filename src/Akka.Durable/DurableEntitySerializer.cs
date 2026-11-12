using Akka.Actor;
using Akka.Event;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable;

/// <summary>
/// Marker for Akka.Durable's own messages that can cross a serialization boundary.
/// </summary>
internal interface IDurableEntityProtocol;

/// <summary>
/// Framework query sent by <see cref="IDurableEntityRef{TCommand,TState,TEvent}.GetStateAsync"/>.
/// The entity actor answers it with an <see cref="EntityStateReply"/> without running Decide.
/// </summary>
[AkkaSerializable(Manifest = "durable-get-state-v1", AllowEmpty = true)]
internal sealed record GetEntityState : IDurableEntityProtocol
{
    public static readonly GetEntityState Instance = new();
}

/// <summary>
/// Framework message sent by a state watcher (<see cref="StateWatcher"/>). The entity actor keeps the sender in
/// memory, watches it, and answers at once with an <see cref="EntityStateReply"/> of its current state. After each
/// later write that holds events, it sends the sender a new <see cref="EntityStateReply"/>. Nothing about it is
/// journaled.
/// </summary>
[AkkaSerializable(Manifest = "durable-subscribe-state-v1", AllowEmpty = true)]
internal sealed record SubscribeToState : IDurableEntityProtocol
{
    public static readonly SubscribeToState Instance = new();
}

/// <summary>
/// Reply to <see cref="GetEntityState"/>, and the snapshot an entity sends to the watchers that subscribed with
/// <see cref="SubscribeToState"/>. Not generic, so no per-entity registration is needed: the state is an envelope
/// payload, written with whatever serializer is bound to the entity's state type.
/// </summary>
/// <remarks>
/// A push can reach a watcher that has just stopped. It then goes to dead letters, which is expected, so it is not
/// logged.
/// </remarks>
[AkkaSerializable(Manifest = "durable-state-reply-v1")]
internal sealed record EntityStateReply(
    [property: AkkaField(0)] object State,
    [property: AkkaField(1)] long SequenceNr) : IDurableEntityProtocol, IDeadLetterSuppression;

/// <summary>
/// Serializer for Akka.Durable's framework messages. Registered automatically by <c>WithDurableEntity</c>.
/// </summary>
[AkkaSerializer<IDurableEntityProtocol>("akka-durable", SerializerId)]
internal sealed partial class DurableEntitySerializer : AkkaSerializer
{
    public const int SerializerId = 22560;

    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}
