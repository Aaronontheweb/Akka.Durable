namespace Akka.Durable;

/// <summary>A command's protocol, reply contract, and conversion into the protocol.</summary>
/// <remarks>
/// Implement on individual command cases. The explicit conversion works with C# unions, F# unions,
/// and class hierarchies without reflection, dynamic code generation, or command registration.
/// </remarks>
public interface IReplies<out TCommand, TReply> where TReply : notnull
{
    TCommand ToCommand();
}
