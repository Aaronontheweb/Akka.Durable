using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures;

using RailwayContext = DecisionContext<RailwayState, RailwayCommand, RailwayEvent>;
using RailwayRequest = RequestContext<RailwayState, RailwayCommand, RailwayEvent, AdjustmentReply>;

public interface IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-state-v1")]
public sealed record RailwayState([property: AkkaField(0)] int Count) : IRailwayProtocol;

[AkkaSerializable(Manifest = "railway-adjust-v1")]
public sealed record Adjust([property: AkkaField(0)] int By) : IRailwayProtocol, IReplies<RailwayCommand, AdjustmentReply>
{
    public RailwayCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "railway-begin-adjust-v1")]
public sealed record BeginAdjust([property: AkkaField(0)] int Before, [property: AkkaField(1)] int By)
    : IRailwayProtocol, IReplies<RailwayCommand, AdjustmentReply>
{
    public RailwayCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "railway-double-adjust-v1")]
public sealed record DoubleAdjust([property: AkkaField(0)] int By) : IRailwayProtocol, IReplies<RailwayCommand, AdjustmentReply>
{
    public RailwayCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "railway-task-only-v1", AllowEmpty = true)]
public sealed record TaskOnly : IRailwayProtocol, IReplies<RailwayCommand, AdjustmentReply>
{
    public RailwayCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "railway-throw-continuation-v1", AllowEmpty = true)]
public sealed record ThrowInContinuation : IRailwayProtocol, IReplies<RailwayCommand, AdjustmentReply>
{
    public RailwayCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "railway-command-v1")]
public readonly union RailwayCommand(Adjust, BeginAdjust, DoubleAdjust, TaskOnly, ThrowInContinuation) : IRailwayProtocol;

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

[AkkaSerializer<IRailwayProtocol>("railway-test", 22600)]
public sealed partial class RailwaySerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Railway
{
    public static readonly EntityDefinition<RailwayState, RailwayCommand, RailwayEvent> Definition = new(
        new EntityTypeName("railway"), _ => new RailwayState(0), Decide,
        (state, e) => e switch { Added added => state with { Count = state.Count + added.Value } });

    public static Decision<RailwayCommand, RailwayEvent> Decide(RailwayState state, RailwayCommand command, RailwayContext ctx) =>
        command switch
        {
            Adjust c => Calculate(ctx.For(c), c.By),

            BeginAdjust c => Calculate(ctx.For(c).Persist(new Added(c.Before)), c.By),

            DoubleAdjust c => ctx.For(c).Run<ScopedCalculator, int>(
                (service, ct) => service.CalculateAsync(c.By, ct),
                onSuccess: (result, next)
                    => Calculate(next, result),
                onFailure:
                (error, next)
                    => next.Reply(new AdjustmentRejected(error.Message))),

            TaskOnly c => ctx.For(c).Run<ScopedCalculator>(
                (service, ct) => service.WaitAsync(ct),
                onSuccess: next => next.Reply(new Adjusted(next.State.Count)),
                onFailure: (error, next) => next.Reply(new AdjustmentRejected(error.GetType().Name))),

            ThrowInContinuation c => ctx.For(c).Run<ScopedCalculator, int>(
                (service, ct) => service.CalculateAsync(1, ct),
                onSuccess: (_, _) => throw new InvalidOperationException("continuation failed"),
                onFailure: (error, next) => next.Reply(new AdjustmentRejected("incorrect failure branch"))),
        };

    private static RequestDecision<RailwayCommand, RailwayEvent, AdjustmentReply> Calculate(RailwayRequest request, int by) =>
        request.Run<ScopedCalculator, int>(
            (service, ct) => service.CalculateAsync(by, ct),
            onSuccess: (result, next) => result < 0
                ? next.Reply(new AdjustmentRejected("negative result"))
                : next.Persist(new Added(result)).Reply(new Adjusted(next.State.Count + result)),
            onFailure: (error, next)
                => next.Reply(new AdjustmentRejected(error.Message)));
}

public sealed class Calculator
{
    public Func<int, CancellationToken, Task<int>> Calculate { get; set; } = (value, _) => Task.FromResult(value);
    public Func<CancellationToken, Task> Wait { get; set; } = _ => Task.CompletedTask;
}

public sealed class ScopedCalculator(Calculator calculator, EffectRecorder recorder) : IAsyncDisposable
{
    public Task<int> CalculateAsync(int value, CancellationToken ct)
    {
        recorder.Add($"calculate:{value}");
        return calculator.Calculate(value, ct);
    }

    public Task WaitAsync(CancellationToken ct)
    {
        recorder.Add("wait");
        return calculator.Wait(ct);
    }

    public ValueTask DisposeAsync()
    {
        recorder.Add("disposed");
        return ValueTask.CompletedTask;
    }
}
