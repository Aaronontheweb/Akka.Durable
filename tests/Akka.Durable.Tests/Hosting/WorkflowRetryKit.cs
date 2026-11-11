using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The checkout workflow with a retry policy on the handler of its charge, and helpers to drive the retries on
/// virtual time: the wait between two attempts is a date in the workflow's reminder, which a spec reads, and the
/// reminder's delivery is a wake that a spec sends with the date it wants the workflow to count as.
/// </summary>
public abstract class WorkflowRetryKit(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    protected static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    // The clock of the host is real: a date read from a reminder is never earlier than the attempt's failure, and
    // never later than the failure plus the wait and this much.
    protected static readonly TimeSpan Slack = TimeSpan.FromSeconds(5);

    // The reminder is stored for a moment and a sleeping timer can fire a moment early.
    protected static readonly TimeSpan Fuzz = TimeSpan.FromMilliseconds(100);

    /// <summary>The policy of the charge's handler. Null: no policy.</summary>
    protected virtual RetryPolicy? ChargePolicy => Retry.Backoff(Minute, 5 * Minute, 6);

    protected virtual TimeSpan EffectTimeout => TimeSpan.FromMilliseconds(300);

    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        Checkout.Definition with
        {
            Options = EntityOptions.Default with { EffectTimeout = EffectTimeout },
            GaveUp = failure => new WorkStuck($"{failure.What}#{failure.Attempts}[{failure.Error}]", failure.Reason),
        };

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) => services
        .AddSingleton<CheckoutServices>()
        .AddEffectHandler<ReserveStock, CheckoutFact, ReserveStockHandler>()
        .AddEffectHandler<ChargeCard, CheckoutFact, ChargeCardHandler>(ChargePolicy)
        .AddEffectHandler<UndoOrder, CheckoutFact, UndoOrderHandler>()
        .AddEffectHandler<OpenSupportTicket, CheckoutFact, OpenSupportTicketHandler>();

    /// <summary>When each attempt at the charge started, in order.</summary>
    protected ConcurrentQueue<DateTimeOffset> ChargeStarts { get; } = new();

    /// <summary>The delivery of each attempt at the charge, in order.</summary>
    protected IReadOnlyList<EffectDelivery> Charges =>
        [.. Services.Issued.Zip(Services.Deliveries).Where(pair => pair.Item1 is ChargeCard).Select(pair => pair.Item2)];

    /// <summary>The charge fails <paramref name="failures"/> times the way <paramref name="how"/> says, then is approved.</summary>
    protected void ChargeFailsFirst(int failures, string how = "throws") =>
        Services.Charge = (effect, ct) =>
        {
            ChargeStarts.Enqueue(DateTimeOffset.UtcNow);
            if (ChargeStarts.Count > failures) return Task.FromResult<CheckoutFact>(new Charged($"charge-{effect.Order}"));
            return how switch
            {
                "throws" => throw new InvalidOperationException("boom"),
                "faults" => Task.FromException<CheckoutFact>(new InvalidOperationException("boom")),
                "empty" => Task.FromResult<CheckoutFact>(default),
                _ => Never(ct),
            };
        };

    private IActorRef Region =>
        Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>>().ActorRef;

    /// <summary>What the workflow's reminder does when it is delivered at <paramref name="at"/>.</summary>
    protected void Wake(string id, DateTimeOffset at) =>
        Region.Tell(new ShardingEnvelope(id, (WfCommand)new WfWake(at)), ActorRefs.NoSender);

    protected async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string id)
    {
        var listed = await Sys.ReminderClient().CreateClient(Definition.TypeName.Value, id).ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    /// <summary>Waits until the workflow's one reminder is due at a time <paramref name="accept"/> takes.</summary>
    protected async Task<DateTimeOffset> AwaitReminderDue(string id, Func<DateTimeOffset, bool> accept)
    {
        var found = default(DateTimeOffset);
        await AwaitAssertAsync(async () =>
        {
            found = Assert.Single(await RemindersOf(id)).DueTimeUtc;
            Assert.True(accept(found), $"The reminder is due at {found:O}.");
        }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        return found;
    }

    /// <summary>When attempt <paramref name="attempt"/> at the charge started, once it has.</summary>
    protected async Task<DateTimeOffset> StartOf(int attempt)
    {
        await AwaitAssertAsync(() => Assert.True(ChargeStarts.Count >= attempt), TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        return ChargeStarts.ElementAt(attempt - 1);
    }

    protected Task AwaitCharges(int count) => AwaitAssertAsync(
        () => Assert.Equal(count, Charges.Count), TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

    /// <summary>Waits until the reminder is due at least <paramref name="wait"/> after <paramref name="failedAt"/>, and returns it.</summary>
    protected async Task<DateTimeOffset> AwaitRetryDue(string id, DateTimeOffset failedAt, TimeSpan wait)
    {
        var due = await AwaitReminderDue(id, d => d >= failedAt + wait - Fuzz);
        Assert.InRange(due, failedAt + wait - Fuzz, failedAt + wait + Slack);
        return due;
    }

    /// <summary>The facts the workflow decided, in order.</summary>
    protected async Task<IReadOnlyList<object?>> FactsOf(string id)
    {
        var probe = CreateTestProbe();
        var persistenceId = EntityOptions.DefaultPersistenceIdFactory(new EntityId(id), new EntityTypeName(Definition.TypeName.Value));
        Sys.ActorOf(Props.Create(() => new JournalReader(persistenceId, probe.Ref)));
        var events = await probe.ExpectMsgAsync<object[]>(cancellationToken: TestCancellation);
        return [.. events.Select(stored => ((WfEvent)stored).Value).OfType<WfFactRecorded>().Select(f => ((CheckoutFact)f.Fact).Value)];
    }

    /// <summary>Waits until the workflow is in the state that <paramref name="expected"/> is.</summary>
    private protected Task<WfRunning> AwaitState(string id, CheckoutState expected) =>
        AwaitRunning(id, running => Assert.Equal(expected.Value, ((CheckoutState)running.State).Value));
}
