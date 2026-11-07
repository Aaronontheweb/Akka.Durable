using Akka.Durable;

var command = new Calculate(3);
if (command.ToCommand() is not Calculate { Value: 3 })
    throw new InvalidOperationException("Command conversion failed.");

var context = new DecisionContext<int, Command, Event>(
    new EntityId("smoke"), new EntityTypeName("aot"), DateTimeOffset.UnixEpoch, 0);
var request = context.For(command);
Decision<Command, Event> reply = request.Persist(new Added(3)).Reply(new Answer(3));
if (reply is not Handled<Command, Event> { Events.Length: 1, Effects.Length: 1 })
    throw new InvalidOperationException("Typed reply construction failed.");

Decision<Command, Event> graph = request.Run<Calculator, int>(
    (service, ct) => service.CalculateAsync(command.Value, ct),
    onSuccess: result => request.Persist(new Added(result)).Reply(new Answer(result)),
    onFailure: error => request.Reply(new Rejected(error.Message)));
if (graph is not Handled<Command, Event> { Effects: [RunEffect<Command, Event>] })
    throw new InvalidOperationException("Typed graph construction failed.");

Console.WriteLine("Native AOT smoke passed: command conversion, typed replies, and request graph construction.");

public sealed record Calculate(int Value) : IReplies<Command, Reply>
{
    public Command ToCommand() => new(this);
}
public readonly union Command(Calculate);
public sealed record Added(int Value);
public readonly union Event(Added);
public sealed record Answer(int Value);
public sealed record Rejected(string Reason);
public readonly union Reply(Answer, Rejected);
public sealed class Calculator
{
    public Task<int> CalculateAsync(int value, CancellationToken ct) => Task.FromResult(value);
}
