using Akka.Serialization.V2;

namespace Akka.Durable;

/// <summary>
/// Uniquely identifies an entity
/// </summary>
/// <param name="Value">The string representation of the entity's id</param>
/// <remarks>Modeled as a string because under the covers, this is how Akka.Persistence and Akka.Cluster.Sharding
/// do uniqueness resolution.</remarks>
[AkkaSerializable]
public readonly record struct EntityId([property: AkkaField(0)] string Value);

/// <summary>
/// Marker interface for all entity-facing messages
/// </summary>
public interface IWithEntityId
{
    EntityId EntityId { get; }
}
