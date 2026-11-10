namespace Akka.Durable;

/// <summary>
/// Thrown from an entity's recovery handlers when the journal or snapshot store holds data the entity
/// cannot interpret. Failing recovery is deliberate: skipping the data would silently lose state.
/// </summary>
internal sealed class EntityRecoveryException(string message) : Exception(message);
