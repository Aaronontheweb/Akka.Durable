using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures;

using RailwayContext = DecisionContext<RailwayState, RailwayCommand, RailwayEvent>;

// Two-track entity: a request becomes an effect, the effect's handler answers with one of two outcome
// commands, and Decide picks the success or the rejection track from the outcome.

public interface IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-state-v1")]
public sealed record RailwayState([property: AkkaField(0)] int Count) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-adjust-v1")]
public sealed record Adjust([property: AkkaField(0)] int By) : IRailwayProtocol, IReplies<RailwayCommand, AdjustmentReply>
{
    public RailwayCommand ToCommand() => new(this);
}

/// <summary>Persists <see cref="Before"/> first, then continues as an <see cref="Adjust"/> of <see cref="By"/>.</summary>
[AkkaSerializable(Manifest = "railway-begin-adjust-v1")]
public sealed record BeginAdjust([property: AkkaField(0)] int Before, [property: AkkaField(1)] int By)
    : IRailwayProtocol, IReplies<RailwayCommand, AdjustmentReply>
{
    public RailwayCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "railway-calculated-v1")]
public sealed record Calculated([property: AkkaField(0)] Adjust Request, [property: AkkaField(1)] int Result) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-calculation-failed-v1")]
public sealed record CalculationFailed([property: AkkaField(0)] Adjust Request, [property: AkkaField(1)] string Reason) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-command-v1")]
public readonly union RailwayCommand(Adjust, BeginAdjust, Calculated, CalculationFailed) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-added-v1")]
public sealed record Added([property: AkkaField(0)] int Value) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-event-v1")]
public readonly union RailwayEvent(Added) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-adjusted-v1")]
public sealed record Adjusted([property: AkkaField(0)] int Count) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-rejected-v1")]
public sealed record AdjustmentRejected([property: AkkaField(0)] string Reason) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-reply-v1")]
public readonly union AdjustmentReply(Adjusted, AdjustmentRejected) : IRailwayProtocol;

/// <summary>The effect: what to calculate, and for which request.</summary>
[AkkaSerializable(Manifest = "railway-calculate-v1")]
public sealed record Calculate([property: AkkaField(0)] Adjust Request, [property: AkkaField(1)] int By)
    : IRailwayProtocol;

[AkkaSerializer<IRailwayProtocol>("railway-test", 22600)]
public sealed partial class RailwaySerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Railway
{
    // A give-up rejects the request the calculation was for.
    public static readonly EntityDefinition<RailwayState, RailwayCommand, RailwayEvent> Definition =
        new EntityDefinition<RailwayState, RailwayCommand, RailwayEvent>(
                new EntityTypeName("railway"), _ => new RailwayState(0), Decide,
                (state, e) => e switch { Added added => state with { Count = state.Count + added.Value } })
            .GaveUp(g => new CalculationFailed(((Calculate)g.Work).Request, $"gave up: {g.Reason}"));

    public static Decision<RailwayCommand, RailwayEvent> Decide(RailwayState state, RailwayCommand command, RailwayContext ctx) =>
        command switch
        {
            Adjust c => ctx.None().Execute(new Calculate(c, c.By)),

            BeginAdjust c => ctx.Persist(new Added(c.Before)).Execute(new Calculate(new Adjust(c.By), c.By)),

            Calculated { Result: < 0 } r => ctx.None().Reply(r.Request, new AdjustmentRejected("negative result")),
            Calculated r => ctx.Persist(new Added(r.Result)).Reply(r.Request, new Adjusted(state.Count + r.Result)),

            CalculationFailed f => ctx.None().Reply(f.Request, new AdjustmentRejected(f.Reason)),
        };
}

public sealed class Calculator
{
    public Func<int, CancellationToken, Task<int>> Calculate { get; set; } = (value, _) => Task.FromResult(value);
}

public sealed class ScopedCalculator(Calculator calculator, EffectRecorder recorder) : IAsyncDisposable
{
    public Task<int> CalculateAsync(int value, CancellationToken ct)
    {
        recorder.Add($"calculate:{value}");
        return calculator.Calculate(value, ct);
    }

    public ValueTask DisposeAsync()
    {
        recorder.Add("disposed");
        return ValueTask.CompletedTask;
    }
}

/// <summary>Turns the service's result or failure into the outcome command the entity decides on.</summary>
public sealed class CalculateHandler(ScopedCalculator calculator) : IEffectHandler<Calculate, RailwayCommand>
{
    public async Task<RailwayCommand> HandleAsync(Calculate effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        try
        {
            return new Calculated(effect.Request, await calculator.CalculateAsync(effect.By, cancellationToken));
        }
        catch (Exception error) when (!cancellationToken.IsCancellationRequested)
        {
            return new CalculationFailed(effect.Request, error.Message);
        }
    }
}
