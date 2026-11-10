using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;

// Its own namespace, like the checkout's: names such as Waiting and Paid are too general to share.
namespace Akka.Durable.Tests.Fixtures.Campaigns;

// A reminder campaign for an unpaid invoice. It waits until the invoice is due, sends reminders with a pause
// between them, and after the last one hands the invoice to collections and waits out a grace period. A payment
// ends it in any state. Each of the three kinds of state is here: a wait, work, and work with a wait.

public interface ICampaignProtocol;

// Input
/// <param name="Id">The invoice.</param>
/// <param name="DueAt">When the invoice is due: the first reminder goes out then.</param>
/// <param name="Pause">How long to wait between reminders.</param>
/// <param name="Grace">
/// How long collections gives the customer before it takes over, and how long a payment the customer says they
/// made has to show up.
/// </param>
/// <param name="Reminders">How many reminders to send before collections.</param>
[AkkaSerializable(Manifest = "campaign-invoice-v1")]
public sealed record Invoice(
    [property: AkkaField(0)] string Id,
    [property: AkkaField(1)] DateTimeOffset DueAt,
    [property: AkkaField(2)] TimeSpan Pause,
    [property: AkkaField(3)] TimeSpan Grace,
    [property: AkkaField(4)] int Reminders) : ICampaignProtocol;

// States
/// <summary>Waits for the due date: a timer given as a date.</summary>
[AkkaSerializable(Manifest = "campaign-waiting-v1", AllowEmpty = true)]
public sealed record Waiting : ICampaignProtocol;

/// <summary>Sends reminder number <see cref="Number"/>: work.</summary>
[AkkaSerializable(Manifest = "campaign-reminding-v1")]
public sealed record Reminding([property: AkkaField(0)] int Number) : ICampaignProtocol;

/// <summary>Waits between reminders: a timer given as a duration.</summary>
[AkkaSerializable(Manifest = "campaign-pausing-v1")]
public sealed record Pausing([property: AkkaField(0)] int Sent) : ICampaignProtocol;

/// <summary>Tells collections and waits out the grace period: work and a timer in one state.</summary>
[AkkaSerializable(Manifest = "campaign-escalating-v1")]
public sealed record Escalating([property: AkkaField(0)] int Sent) : ICampaignProtocol;

/// <summary>
/// The customer says they paid. Checks for the payment until it shows up or <see cref="Invoice.Grace"/> has passed:
/// work with a time limit that polls.
/// </summary>
[AkkaSerializable(Manifest = "campaign-verifying-v1")]
public sealed record Verifying([property: AkkaField(0)] int Sent) : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-state-v1")]
public readonly union CampaignState(Waiting, Reminding, Pausing, Escalating, Verifying) : ICampaignProtocol;

// Facts
[AkkaSerializable(Manifest = "campaign-due-date-reached-v1", AllowEmpty = true)]
public sealed record DueDateReached : ICampaignProtocol;

/// <summary>Someone asked for a reminder to go out now.</summary>
[AkkaSerializable(Manifest = "campaign-send-now-v1", AllowEmpty = true)]
public sealed record SendNow : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-reminder-sent-v1", AllowEmpty = true)]
public sealed record ReminderSent : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-pause-over-v1", AllowEmpty = true)]
public sealed record PauseOver : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-collections-notified-v1", AllowEmpty = true)]
public sealed record CollectionsNotified : ICampaignProtocol;

/// <summary>Collections will not take the invoice yet. The campaign goes back to reminders.</summary>
[AkkaSerializable(Manifest = "campaign-collections-declined-v1", AllowEmpty = true)]
public sealed record CollectionsDeclined : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-grace-over-v1", AllowEmpty = true)]
public sealed record GraceOver : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-paid-v1", AllowEmpty = true)]
public sealed record Paid : ICampaignProtocol;

/// <summary>Someone asked to stop the campaign.</summary>
/// <param name="Reason">Why the campaign should stop.</param>
/// <param name="By">Who or what asked: an id, not a person's name.</param>
[AkkaSerializable(Manifest = "campaign-stop-requested-v1")]
public sealed record StopRequested(
    [property: AkkaField(0)] string Reason,
    [property: AkkaField(1)] string By) : ICampaignProtocol;

/// <summary>The customer says they paid.</summary>
[AkkaSerializable(Manifest = "campaign-payment-claimed-v1", AllowEmpty = true)]
public sealed record PaymentClaimed : ICampaignProtocol;

/// <summary>A check found no payment yet.</summary>
[AkkaSerializable(Manifest = "campaign-not-yet-paid-v1", AllowEmpty = true)]
public sealed record NotYetPaid : ICampaignProtocol;

/// <summary>The payment the customer claimed did not show up in time.</summary>
[AkkaSerializable(Manifest = "campaign-check-overdue-v1", AllowEmpty = true)]
public sealed record CheckOverdue : ICampaignProtocol;

/// <summary>A piece of work got no answer.</summary>
[AkkaSerializable(Manifest = "campaign-work-failed-v1")]
public sealed record WorkFailed(
    [property: AkkaField(0)] string What,
    [property: AkkaField(1)] GaveUpReason Reason) : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-fact-v1")]
public readonly union CampaignFact(
    DueDateReached, SendNow, ReminderSent, PauseOver, CollectionsNotified, CollectionsDeclined, GraceOver,
    Paid, StopRequested, PaymentClaimed, NotYetPaid, CheckOverdue, WorkFailed) : ICampaignProtocol;

// Outcomes
[AkkaSerializable(Manifest = "campaign-paid-in-full-v1", AllowEmpty = true)]
public sealed record PaidInFull : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-sent-to-collections-v1", AllowEmpty = true)]
public sealed record SentToCollections : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-stopped-v1")]
public sealed record CampaignStopped([property: AkkaField(0)] string Reason) : ICampaignProtocol;

[AkkaSerializable(Manifest = "campaign-outcome-v1")]
public readonly union CampaignOutcome(PaidInFull, SentToCollections, CampaignStopped) : ICampaignProtocol;

// Effects. Plain records: they never leave the process.
public sealed record SendReminder(string Invoice, int Number);
public sealed record NotifyCollections(string Invoice);
public sealed record CheckPayment(string Invoice);

[AkkaSerializer<ICampaignProtocol>("campaign-test", 22603)]
public sealed partial class CampaignSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Campaign
{
    public static readonly WorkflowDefinition<Invoice, CampaignState, CampaignFact, CampaignOutcome> Definition =
        new(new WorkflowTypeName("campaigns"),
            Start: _ => new Waiting(),
            Work,
            Decide,
            Kind,
            Cancel: request => new StopRequested(request.Reason, request.By),
            GaveUp: failure => new WorkFailed(failure.What, failure.Reason));

    /// <summary>What each state needs: a wait, work, or both.</summary>
    public static WorkList<CampaignFact> Work(CampaignState state, WorkContext<Invoice, CampaignFact> ctx) =>
        state switch
        {
            Waiting => ctx.Timer(ctx.Input.DueAt, new DueDateReached()),
            Reminding reminding => ctx.Execute(new SendReminder(ctx.Input.Id, reminding.Number)),
            Pausing => ctx.Timer(ctx.Input.Pause, new PauseOver()),
            Escalating => ctx.All(
                ctx.Execute(new NotifyCollections(ctx.Input.Id)),
                ctx.Timer(ctx.Input.Grace, new GraceOver())),
            Verifying => ctx.All(
                ctx.Execute(new CheckPayment(ctx.Input.Id)),
                ctx.Timer(ctx.Input.Grace, new CheckOverdue())),
        };

    /// <summary>The fact table. It has no catch-all arm: every pair of state and fact is written out.</summary>
    public static Transition<CampaignState, CampaignOutcome> Decide(
        CampaignState state, CampaignFact fact, DecideContext<Invoice, CampaignState, CampaignOutcome> ctx) =>
        (state, fact) switch
        {
            (Waiting, DueDateReached or SendNow) => ctx.Next(new Reminding(1)),

            (Reminding reminding, ReminderSent) => reminding.Number >= ctx.Input.Reminders
                ? ctx.Next(new Escalating(reminding.Number))
                : ctx.Next(new Pausing(reminding.Number)),
            // The reminder may not have gone out. Count it as not sent and try again after a pause.
            (Reminding reminding, WorkFailed) => ctx.Next(new Pausing(reminding.Number - 1)),

            (Pausing pausing, PauseOver or SendNow) => ctx.Next(new Reminding(pausing.Sent + 1)),

            // Collections knows. The grace period still runs.
            (Escalating, CollectionsNotified or WorkFailed) => ctx.Stay(),
            (Escalating escalating, CollectionsDeclined) => ctx.Next(new Pausing(escalating.Sent)),
            (Escalating, GraceOver) => ctx.Finish(new SentToCollections()),

            // A claimed payment is checked until it shows up. Again keeps the time the state was entered, so the
            // time limit counts from the claim, not from the latest check.
            (Waiting, PaymentClaimed) => ctx.Next(new Verifying(0)),
            (Pausing pausing, PaymentClaimed) => ctx.Next(new Verifying(pausing.Sent)),
            (Verifying, NotYetPaid or WorkFailed) => ctx.Again(),
            (Verifying verifying, CheckOverdue) => ctx.Next(new Pausing(verifying.Sent)),
            (Verifying, PaymentClaimed) => ctx.Stay(),

            // A payment or a stop ends the campaign in any state.
            (Waiting or Reminding or Pausing or Escalating or Verifying, Paid) => ctx.Finish(new PaidInFull()),
            (Waiting or Reminding or Pausing or Escalating or Verifying, StopRequested stop) =>
                ctx.Finish(new CampaignStopped(stop.Reason)),

            // Pairs that should not happen, written out so that a new state or fact does not compile until it is placed.
            (Waiting, ReminderSent or PauseOver or CollectionsNotified or CollectionsDeclined or GraceOver or WorkFailed) =>
                ctx.Unexpected(),
            (Reminding, DueDateReached or SendNow or PauseOver or CollectionsNotified or CollectionsDeclined or GraceOver) =>
                ctx.Unexpected(),
            (Pausing, DueDateReached or ReminderSent or CollectionsNotified or CollectionsDeclined or GraceOver or WorkFailed) =>
                ctx.Unexpected(),
            (Escalating, DueDateReached or SendNow or ReminderSent or PauseOver) => ctx.Unexpected(),
            (Reminding or Escalating, PaymentClaimed) => ctx.Unexpected(),
            (Waiting or Reminding or Pausing or Escalating, NotYetPaid or CheckOverdue) => ctx.Unexpected(),
            (Verifying, DueDateReached or SendNow or ReminderSent or PauseOver or CollectionsNotified or CollectionsDeclined
                or GraceOver) => ctx.Unexpected(),
        };

    public static OutcomeKind Kind(CampaignOutcome outcome) =>
        outcome switch
        {
            PaidInFull => OutcomeKind.Succeeded,
            SentToCollections => OutcomeKind.Failed,
            CampaignStopped => OutcomeKind.Cancelled,
        };
}

/// <summary>
/// What the campaign's services do in a test, and what they were asked to do. Each handler records the effect
/// it received and then answers the way the test set it up.
/// </summary>
public sealed class CampaignServices
{
    private readonly ConcurrentQueue<object> _issued = new();

    /// <summary>Every effect a handler received, in order.</summary>
    public IReadOnlyList<object> Issued => [.. _issued];

    public Func<SendReminder, CancellationToken, Task<CampaignFact>> Send { get; set; } =
        (_, _) => Task.FromResult<CampaignFact>(new ReminderSent());

    public Func<NotifyCollections, CancellationToken, Task<CampaignFact>> Notify { get; set; } =
        (_, _) => Task.FromResult<CampaignFact>(new CollectionsNotified());

    public Func<CheckPayment, CancellationToken, Task<CampaignFact>> Check { get; set; } =
        (_, _) => Task.FromResult<CampaignFact>(new NotYetPaid());

    internal Task<CampaignFact> Run<TEffect>(
        TEffect effect, Func<TEffect, CancellationToken, Task<CampaignFact>> answer, CancellationToken ct)
        where TEffect : notnull
    {
        _issued.Enqueue(effect);
        return answer(effect, ct);
    }
}

public sealed class SendReminderHandler(CampaignServices services) : IEffectHandler<SendReminder, CampaignFact>
{
    public Task<CampaignFact> HandleAsync(SendReminder effect, CancellationToken cancellationToken) =>
        services.Run(effect, services.Send, cancellationToken);
}

public sealed class NotifyCollectionsHandler(CampaignServices services) : IEffectHandler<NotifyCollections, CampaignFact>
{
    public Task<CampaignFact> HandleAsync(NotifyCollections effect, CancellationToken cancellationToken) =>
        services.Run(effect, services.Notify, cancellationToken);
}

public sealed class CheckPaymentHandler(CampaignServices services) : IEffectHandler<CheckPayment, CampaignFact>
{
    public Task<CampaignFact> HandleAsync(CheckPayment effect, CancellationToken cancellationToken) =>
        services.Run(effect, services.Check, cancellationToken);
}

public static class CampaignHandlers
{
    public static IServiceCollection AddCampaignHandlers(this IServiceCollection services) => services
        .AddSingleton<CampaignServices>()
        .AddScoped<IEffectHandler<SendReminder, CampaignFact>, SendReminderHandler>()
        .AddScoped<IEffectHandler<NotifyCollections, CampaignFact>, NotifyCollectionsHandler>()
        .AddScoped<IEffectHandler<CheckPayment, CampaignFact>, CheckPaymentHandler>();
}
