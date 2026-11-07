using Akka.Durable.Abstractions.Entities;

namespace Akka.Durable.Entities;

/// <summary>
/// Framework query sent by <see cref="IDurableEntityRef{TCommand,TState}.GetStateAsync"/>.
/// The entity actor answers it with a <see cref="StateSnapshot{TState}"/> without running Decide.
/// </summary>
/// <remarks>
/// TODO: needs a wire format once serialization lands, so sharded entities can be queried across nodes.
/// The reply carries TState, which may be a C# 15 union: https://github.com/akkadotnet/akka.net/issues/8906
/// </remarks>
internal sealed class GetEntityState
{
    public static readonly GetEntityState Instance = new();
    private GetEntityState() { }
}
