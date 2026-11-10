using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures.Bumps;

// A workflow that talks to a durable entity from Work, with no effect handler: it sends the counter an
// Increment, then a GetCount, and ends with the count. Each reply becomes one of its facts.

public interface IBumpProtocol;

// Input
[AkkaSerializable(Manifest = "bump-input-v1")]
public sealed record BumpCounter([property: AkkaField(0)] string Counter, [property: AkkaField(1)] int By) : IBumpProtocol;

// States
[AkkaSerializable(Manifest = "bump-bumping-v1", AllowEmpty = true)]
public sealed record Bumping : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-reading-v1", AllowEmpty = true)]
public sealed record Reading : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-state-v1")]
public readonly union BumpState(Bumping, Reading) : IBumpProtocol;

// Facts
/// <summary>The counter accepted the increment.</summary>
[AkkaSerializable(Manifest = "bump-counter-bumped-v1", AllowEmpty = true)]
public sealed record CounterBumped : IBumpProtocol;

/// <summary>The counter reported its count.</summary>
[AkkaSerializable(Manifest = "bump-count-read-v1")]
public sealed record CountRead([property: AkkaField(0)] int Count) : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-gave-up-v1")]
public sealed record BumpGaveUp(
    [property: AkkaField(0)] string What,
    [property: AkkaField(1)] GaveUpReason Reason,
    [property: AkkaField(2)] string Error) : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-cancelled-v1")]
public sealed record BumpCancelled([property: AkkaField(0)] string Reason) : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-fact-v1")]
public readonly union BumpFact(CounterBumped, CountRead, BumpGaveUp, BumpCancelled) : IBumpProtocol;

// Outcomes
[AkkaSerializable(Manifest = "bump-bumped-v1")]
public sealed record Bumped([property: AkkaField(0)] int Count) : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-failed-v1")]
public sealed record BumpFailed(
    [property: AkkaField(0)] string What,
    [property: AkkaField(1)] GaveUpReason Reason,
    [property: AkkaField(2)] string Error) : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-stopped-v1")]
public sealed record BumpStopped([property: AkkaField(0)] string Reason) : IBumpProtocol;

[AkkaSerializable(Manifest = "bump-outcome-v1")]
public readonly union BumpOutcome(Bumped, BumpFailed, BumpStopped) : IBumpProtocol;

[AkkaSerializer<IBumpProtocol>("bump-test", 22605)]
public sealed partial class BumpSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Bump
{
    public static readonly WorkflowDefinition<BumpCounter, BumpState, BumpFact, BumpOutcome> Definition =
        new(new WorkflowTypeName("bumps"),
            Start: _ => new Bumping(),
            Work,
            Decide,
            Cancel: request => new BumpCancelled(request.Reason),
            GaveUp: gaveUp => new BumpGaveUp(gaveUp.What, gaveUp.Reason, gaveUp.Error ?? ""));

    /// <summary>Each state sends one command to the counter. The type arguments of Send are inferred.</summary>
    public static WorkList<BumpFact> Work(BumpState state, WorkContext<BumpCounter, BumpFact> ctx) =>
        state switch
        {
            Bumping => ctx.Send(new EntityId(ctx.Input.Counter), new Increment(ctx.Input.By), _ => new CounterBumped()),
            Reading => ctx.Send(new EntityId(ctx.Input.Counter), new GetCount(), reply => new CountRead(reply.Count)),
        };

    /// <summary>The fact table. It has no catch-all arm.</summary>
    public static Transition<BumpState, BumpOutcome> Decide(
        BumpState state, BumpFact fact, DecideContext<BumpCounter, BumpState, BumpOutcome> ctx) =>
        (state, fact) switch
        {
            (Bumping, CounterBumped) => ctx.Next(new Reading()),
            (Reading, CountRead read) => ctx.Finish(new Bumped(read.Count)),

            (Bumping or Reading, BumpGaveUp gaveUp) => ctx.Finish(new BumpFailed(gaveUp.What, gaveUp.Reason, gaveUp.Error)),
            (Bumping or Reading, BumpCancelled cancelled) => ctx.Finish(new BumpStopped(cancelled.Reason)),

            (Bumping, CountRead) => ctx.Unexpected(),
            (Reading, CounterBumped) => ctx.Unexpected(),
        };
}
