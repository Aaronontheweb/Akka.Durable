using System.Collections.Immutable;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures;

using BillingContext = DecisionContext<BillingState, BillingCommand, BillingEvent>;

// An invoice's billing account. Issuing the bill starts the invoice's reminder campaign, a payment tells the campaign,
// and a stop cancels it. It records every answer and give-up it receives, so a spec can read them from its state.

public interface IBillingProtocol;

/// <param name="Answers">
/// What the entity was told, in order: the name of each answer's case, or "gave up: " and the reason.
/// </param>
[AkkaSerializable(Manifest = "billing-state-v1")]
public sealed record BillingState(
    [property: AkkaField(0)] bool Issued,
    [property: AkkaField(1)] ImmutableArray<string> Answers) : IBillingProtocol;

// Commands

/// <summary>Issues the bill and starts its campaign. The library completes the start; no answer is delivered.</summary>
[AkkaSerializable(Manifest = "billing-issue-v1")]
public sealed record IssueBill([property: AkkaField(0)] DateTimeOffset DueAt) : IBillingProtocol, IReplies<BillingCommand, BillingAck>
{
    public BillingCommand ToCommand() => new(this);
}

/// <summary>Records a payment and tells the campaign.</summary>
[AkkaSerializable(Manifest = "billing-pay-v1", AllowEmpty = true)]
public sealed record PayBill : IBillingProtocol, IReplies<BillingCommand, BillingAck>
{
    public BillingCommand ToCommand() => new(this);
}

/// <summary>Asks the campaign to stop.</summary>
[AkkaSerializable(Manifest = "billing-stop-campaign-v1")]
public sealed record StopCampaign([property: AkkaField(0)] string Reason) : IBillingProtocol, IReplies<BillingCommand, BillingAck>
{
    public BillingCommand ToCommand() => new(this);
}

/// <summary>Decide throws, so the entity restarts.</summary>
[AkkaSerializable(Manifest = "billing-crash-v1", AllowEmpty = true)]
public sealed record CrashBilling : IBillingProtocol;

/// <summary>A workflow answered a call.</summary>
[AkkaSerializable(Manifest = "billing-campaign-answered-v1")]
public sealed record CampaignAnswered([property: AkkaField(0)] string Answer) : IBillingProtocol;

/// <summary>A call to a workflow got no answer.</summary>
[AkkaSerializable(Manifest = "billing-campaign-unreachable-v1")]
public sealed record CampaignUnreachable([property: AkkaField(0)] GaveUpReason Reason) : IBillingProtocol;

[AkkaSerializable(Manifest = "billing-command-v1")]
public readonly union BillingCommand(
    IssueBill, PayBill, StopCampaign, CrashBilling, CampaignAnswered, CampaignUnreachable) : IBillingProtocol;

// Events
[AkkaSerializable(Manifest = "billing-issued-v1", AllowEmpty = true)]
public sealed record BillIssued : IBillingProtocol;

[AkkaSerializable(Manifest = "billing-paid-v1", AllowEmpty = true)]
public sealed record BillPaid : IBillingProtocol;

[AkkaSerializable(Manifest = "billing-told-v1")]
public sealed record BillingTold([property: AkkaField(0)] string What) : IBillingProtocol;

[AkkaSerializable(Manifest = "billing-event-v1")]
public readonly union BillingEvent(BillIssued, BillPaid, BillingTold) : IBillingProtocol;

// Replies
[AkkaSerializable(Manifest = "billing-ack-v1", AllowEmpty = true)]
public sealed record BillingAck : IBillingProtocol;

[AkkaSerializer<IBillingProtocol>("billing-test", 22608)]
public sealed partial class BillingSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Billing
{
    public static readonly EntityTypeName TypeName = new("billing");

    /// <summary>The id of the campaign of the bill <paramref name="bill"/>.</summary>
    public static WorkflowId CampaignOf(EntityId bill) => new($"campaign-{bill.Value}");

    /// <summary>The campaign's input: due far in the future, so a started campaign waits.</summary>
    public static Invoice InputOf(EntityId bill, DateTimeOffset dueAt) =>
        new(bill.Value, dueAt, TimeSpan.FromDays(7), TimeSpan.FromDays(14), Reminders: 3);

    public static readonly EntityDefinition<BillingState, BillingCommand, BillingEvent> Definition =
        new EntityDefinition<BillingState, BillingCommand, BillingEvent>(
                TypeName, _ => new BillingState(false, []), Decide, Evolve)
            .Uses(Campaign.Definition,
                told: answer => new CampaignAnswered(answer.Value!.GetType().Name),
                gaveUp: failure => new CampaignUnreachable(failure.Reason));

    public static Decision<BillingCommand, BillingEvent> Decide(BillingState state, BillingCommand command, BillingContext ctx) =>
        command switch
        {
            IssueBill c => ctx.Persist(new BillIssued())
                .Start(Campaign.Definition, CampaignOf(ctx.EntityId), InputOf(ctx.EntityId, c.DueAt))
                .Reply(c, new BillingAck()),

            PayBill c => ctx.Persist(new BillPaid())
                .Notify(Campaign.Definition, CampaignOf(ctx.EntityId), new Paid())
                .Reply(c, new BillingAck()),

            StopCampaign c => ctx.None()
                .Cancel(Campaign.Definition, CampaignOf(ctx.EntityId), c.Reason, "billing")
                .Reply(c, new BillingAck()),

            CrashBilling => throw new InvalidOperationException("The billing entity was asked to crash."),

            CampaignAnswered a => ctx.Persist(new BillingTold(a.Answer)),
            CampaignUnreachable u => ctx.Persist(new BillingTold($"gave up: {u.Reason}")),
        };

    public static BillingState Evolve(BillingState state, BillingEvent @event) =>
        @event switch
        {
            BillIssued => state with { Issued = true },
            BillPaid => state,
            BillingTold told => state with { Answers = state.Answers.Add(told.What) },
        };
}
