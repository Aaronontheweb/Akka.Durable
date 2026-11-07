using Akka.Serialization.V2;

namespace Akka.Durable;

/// <summary>
/// Uniquely identifies a workflow instance
/// </summary>
/// <param name="Value">The string representation of the entity's id</param>
/// <remarks>Modeled as a string because under the covers, this is how Akka.Persistence and Akka.Cluster.Sharding
/// do uniqueness resolution.</remarks>
[AkkaSerializable]
public readonly record struct WorkflowId([property: AkkaField(0)] string Value);

public interface IWithWorkflowId
{
    WorkflowId WorkflowId { get; }
}