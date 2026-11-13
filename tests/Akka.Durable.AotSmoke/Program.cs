using Akka.Durable;

var command = new Calculate(3);
if (command.ToCommand() is not Calculate { Value: 3 })
    throw new InvalidOperationException("Command conversion failed.");

var context = new DecisionContext<int, Command, Event>(
    new EntityId("smoke"), new EntityTypeName("aot"), DateTimeOffset.UnixEpoch);

Decision<Command, Event> reply = context.Persist(new Added(3)).Reply(command, new Answer(3));
if (reply is not Handled<Command, Event> { Events.Length: 1, Effects: [ReplyEffect { Response: Answer { Value: 3 } }] })
    throw new InvalidOperationException("Typed reply construction failed.");

Decision<Command, Event> executed = context.None().Execute(new Compute(command, command.Value));
if (executed is not Handled<Command, Event> { Effects: [ExecuteEffect { Effect: Compute { Value: 3 } }] })
    throw new InvalidOperationException("Execute effect construction failed.");

Decision<Command, Event> sent = context.None().Send<Command>(new EntityId("other"), new Calculate(1));
if (sent is not Handled<Command, Event> { Effects: [SendEffect { Command: Command { Value: Calculate { Value: 1 } } }] })
    throw new InvalidOperationException("Send effect construction failed.");

Decision<Command, Event> ran = context.None()
    .Run<Calculator>((service, ct) => service.CalculateAsync(command.Value, ct))
    .Reply(command, new Rejected("later"));
if (ran is not Handled<Command, Event> { Effects: [RunEffect, ReplyEffect] })
    throw new InvalidOperationException("Run effect construction failed.");

Console.WriteLine("Native AOT smoke passed: command conversion, typed replies, and effect construction.");

public sealed record Calculate(int Value) : IReplies<Command, Reply>
{
    public Command ToCommand() => new(this);
}
public sealed record Computed(Calculate Request, int Result);
public readonly union Command(Calculate, Computed);
public sealed record Added(int Value);
public readonly union Event(Added);
public sealed record Answer(int Value);
public sealed record Rejected(string Reason);
public readonly union Reply(Answer, Rejected);
public sealed record Compute(Calculate Request, int Value);
public sealed class Calculator
{
    public Task<int> CalculateAsync(int value, CancellationToken ct) => Task.FromResult(value);
}
