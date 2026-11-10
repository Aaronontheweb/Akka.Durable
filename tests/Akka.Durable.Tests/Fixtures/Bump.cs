using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures.Bumps;

// A one-state workflow whose Work sends Adjust to the railway entity with the workflow's own id. The railway replies
// after an effect of its own, with Adjusted or AdjustmentRejected. The input's plan picks the work:
// Send: a reply or a give-up ends the workflow. Stay: the table answers everything with Stay, so the journal
// shows what was decided. WithTimer and Both: like Stay, with a 1 ms timer or a second send in the state.
// Twice: the first reply is answered with Again. Unhosted: sends the counter, whose entity type is not hosted.
// ThenThrows and ThenNull: the function that turns the reply into a fact throws, or returns no fact.
public enum Plan { Send, Stay, WithTimer, Both, Twice, Unhosted, ThenThrows, ThenNull }

public interface IBumpProtocol;

[AkkaSerializable(Manifest = "bump-input-v1")]
public sealed record BumpInput([property: AkkaField(0)] int By, [property: AkkaField(1)] Plan Plan) : IBumpProtocol;
[AkkaSerializable(Manifest = "bump-sending-v1", AllowEmpty = true)]
public sealed record Sending : IBumpProtocol;
[AkkaSerializable(Manifest = "bump-adjusted-v1")]
public sealed record BumpAdjusted([property: AkkaField(0)] int Count) : IBumpProtocol;
[AkkaSerializable(Manifest = "bump-rejected-v1")]
public sealed record BumpRejected([property: AkkaField(0)] string Reason) : IBumpProtocol;
[AkkaSerializable(Manifest = "bump-gave-up-v1")]
public sealed record BumpGaveUp([property: AkkaField(0)] string What, [property: AkkaField(1)] GaveUpReason Reason,
    [property: AkkaField(2)] string Error) : IBumpProtocol;
[AkkaSerializable(Manifest = "bump-noted-v1")]
public sealed record BumpNoted([property: AkkaField(0)] string Note) : IBumpProtocol;
[AkkaSerializable(Manifest = "bump-timer-due-v1", AllowEmpty = true)]
public sealed record BumpTimerDue : IBumpProtocol;
[AkkaSerializable(Manifest = "bump-fact-v1")]
public readonly union BumpFact(BumpAdjusted, BumpRejected, BumpGaveUp, BumpNoted, BumpTimerDue) : IBumpProtocol;
// The outcome is the fact that ended the workflow. A Notify or a cancel arrives as BumpNoted.
[AkkaSerializable(Manifest = "bump-ended-v1")]
public sealed record Ended([property: AkkaField(0)] BumpFact Last) : IBumpProtocol;

[AkkaSerializer<IBumpProtocol>("bump-test", 22605)]
public sealed partial class BumpSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Bump
{
    public static readonly WorkflowDefinition<BumpInput, Sending, BumpFact, Ended> Definition =
        new(new WorkflowTypeName("bumps"), Start: _ => new Sending(), Work, Decide,
            Cancel: request => new BumpNoted($"cancel:{request.Reason}"),
            GaveUp: gaveUp => new BumpGaveUp(gaveUp.What, gaveUp.Reason, gaveUp.Error ?? ""));

    /// <summary>No type arguments: Send infers them from the command's IReplies.</summary>
    public static WorkList<BumpFact> Work(Sending state, WorkContext<BumpInput, BumpFact> ctx)
    {
        var (target, by) = (new EntityId(ctx.Id.Value), ctx.Input.By);
        var send = ctx.Send(target, new Adjust(by), FactOf);
        return ctx.Input.Plan switch
        {
            Plan.WithTimer => ctx.All(send, ctx.Timer(TimeSpan.FromMilliseconds(1), new BumpTimerDue())),
            Plan.Both => ctx.All(send, ctx.Send(new EntityId($"{ctx.Id.Value}-b"), new Adjust(by), FactOf)),
            Plan.Unhosted => ctx.Send(target, new Increment(by), _ => new BumpNoted("incremented")),
            Plan.ThenThrows => ctx.Send(target, new Adjust(by), _ => throw new InvalidOperationException("then failed")),
            Plan.ThenNull => ctx.Send(target, new Adjust(by), _ => default),
            _ => send,
        };
    }

    public static BumpFact FactOf(AdjustmentReply reply) =>
        reply switch { Adjusted a => new BumpAdjusted(a.Count), AdjustmentRejected r => new BumpRejected(r.Reason) };

    /// <summary>The fact table. One state, so it switches on the fact. It has no catch-all arm.</summary>
    public static Transition<Sending, Ended> Decide(Sending state, BumpFact fact, DecideContext<BumpInput, Sending, Ended> ctx) =>
        fact switch
        {
            BumpAdjusted adjusted when ctx.Input.Plan == Plan.Twice && adjusted.Count < 2 * ctx.Input.By => ctx.Again(),
            BumpAdjusted or BumpRejected or BumpGaveUp when ctx.Input.Plan is Plan.Stay or Plan.WithTimer or Plan.Both => ctx.Stay(),
            BumpAdjusted or BumpRejected or BumpGaveUp => ctx.Finish(new Ended(fact)),
            BumpNoted or BumpTimerDue => ctx.Stay(),
        };
}
