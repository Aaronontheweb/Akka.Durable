using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures;

using TallyContext = DecisionContext<TallyState, TallyCommand, TallyEvent>;

// A small workflow: on StartTally it tells a Counter to increment, waits for the counter's reply, asks the
// counter for its count, and answers its own asker with that count. It talks to the counter through Send and
// receives the counter's replies as its own commands through Accepts. It never references Counter.Definition.

public interface ITallyProtocol;

[AkkaSerializable(Manifest = "tally-state-v1")]
public sealed record TallyState(
    [property: AkkaField(0)] StartTally? Pending,
    [property: AkkaField(1)] int? Result) : ITallyProtocol;

[AkkaSerializable(Manifest = "tally-start-v1")]
public sealed record StartTally([property: AkkaField(0)] string Counter, [property: AkkaField(1)] int By)
    : ITallyProtocol, IReplies<TallyCommand, TallyDone>
{
    public TallyCommand ToCommand() => new(this);
}

/// <summary>The counter accepted the increment. Wraps the counter's <see cref="Accepted"/> reply.</summary>
[AkkaSerializable(Manifest = "tally-counter-accepted-v1", AllowEmpty = true)]
public sealed record CounterAccepted : ITallyProtocol;

/// <summary>The counter reported its count. Wraps the counter's <see cref="CountIs"/> reply.</summary>
[AkkaSerializable(Manifest = "tally-counter-counted-v1")]
public sealed record CounterCounted([property: AkkaField(0)] int Count) : ITallyProtocol;

[AkkaSerializable(Manifest = "tally-command-v1")]
public readonly union TallyCommand(StartTally, CounterAccepted, CounterCounted) : ITallyProtocol;

[AkkaSerializable(Manifest = "tally-started-v1")]
public sealed record TallyStarted([property: AkkaField(0)] StartTally Request) : ITallyProtocol;

[AkkaSerializable(Manifest = "tally-finished-v1")]
public sealed record TallyFinished([property: AkkaField(0)] int Count) : ITallyProtocol;

[AkkaSerializable(Manifest = "tally-event-v1")]
public readonly union TallyEvent(TallyStarted, TallyFinished) : ITallyProtocol;

[AkkaSerializable(Manifest = "tally-done-v1")]
public sealed record TallyDone([property: AkkaField(0)] int Count) : ITallyProtocol;

[AkkaSerializer<ITallyProtocol>("tally-test", 22598)]
public sealed partial class TallySerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Tally
{
    public static readonly EntityTypeName TypeName = new("tally");

    public static readonly EntityDefinition<TallyState, TallyCommand, TallyEvent> Definition =
        new EntityDefinition<TallyState, TallyCommand, TallyEvent>(TypeName, _ => new TallyState(null, null), Decide, Evolve)
        .Accepts<Accepted>(_ => new CounterAccepted())
        .Accepts<CountIs>(count => new CounterCounted(count.Count));

    public static Decision<TallyCommand, TallyEvent> Decide(TallyState state, TallyCommand command, TallyContext ctx) =>
        (state, command) switch
        {
            ({ Pending: null, Result: null }, StartTally s) => ctx.Persist(new TallyStarted(s))
                .Send<CounterCommand>(new EntityId(s.Counter), new Increment(s.By)),

            ({ Pending: { } p }, CounterAccepted) => ctx.None()
                .Send<CounterCommand>(new EntityId(p.Counter), new GetCount()),

            ({ Pending: { } p }, CounterCounted c) => ctx.Persist(new TallyFinished(c.Count))
                .Reply(p, new TallyDone(c.Count))
                .Stop(),

            ({ Result: { } r }, StartTally s) => ctx.None().Reply(s, new TallyDone(r)), // asker retried
            (_, StartTally) => ctx.Stash(),
            _ => ctx.Ignore(),
        };


    public static TallyState Evolve(TallyState state, TallyEvent @event) =>
        @event switch
        {
            TallyStarted e => state with { Pending = e.Request },
            TallyFinished e => state with { Pending = null, Result = e.Count },
        };
}
