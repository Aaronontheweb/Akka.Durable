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

    /// <summary>
    /// Delivers the workflow's stored reminder as Akka.Reminders would, to a workflow that may be stopped. With
    /// <paramref name="asIfStoredFor"/> the delivery comes before the reminder is due.
    /// </summary>
    protected async Task DeliverReminder(string id, DateTimeOffset? asIfStoredFor = null) =>
        StoredReminders.Deliver(Sys, Assert.Single(await RemindersOf(id)), asIfStoredFor);

    protected async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string id)
    {
        var listed = await Sys.ReminderClient().CreateClient(Definition.TypeName.Value, id).ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    /// <summary>
    /// Waits until the workflow's one reminder is its wake for <paramref name="date"/>: it carries that date exactly,
    /// and it is due no earlier.
    /// </summary>
    protected async Task AwaitWakeFor(string id, DateTimeOffset date)
    {
        await AwaitAssertAsync(async () => Assert.Equal(date, Assert.Single(await RemindersOf(id)).StoredFor()),
            TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        Assert.Single(await RemindersOf(id)).IsWakeFor(date);
    }

    /// <summary>When attempt <paramref name="attempt"/> at the charge started, once it has.</summary>
    protected async Task<DateTimeOffset> StartOf(int attempt)
    {
        await AwaitAssertAsync(() => Assert.True(ChargeStarts.Count >= attempt), TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        return ChargeStarts.ElementAt(attempt - 1);
    }

    protected Task AwaitCharges(int count) => AwaitAssertAsync(
        () => Assert.Equal(count, Charges.Count), TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

    /// <summary>
    /// Waits until attempt <paramref name="attempt"/> at the charge (position 0) is in flight, and returns its start as
    /// the workflow journaled it: before its handler ran.
    /// </summary>
    private protected async Task<DateTimeOffset> JournaledStartOf(string id, int attempt)
    {
        var running = await AwaitRunning(id, r =>
        {
            Assert.IsType<Charging>(((CheckoutState)r.State).Value);
            if (attempt == 1) Assert.Null(WorkAttempts.Find(r.Attempts, 0));
            else Assert.Equal((attempt, (DateTimeOffset?)null), (WorkAttempts.Find(r.Attempts, 0)?.Attempt, WorkAttempts.Find(r.Attempts, 0)?.NextAt));
        });
        return attempt == 1 ? running.IssuedAt : WorkAttempts.Find(running.Attempts, 0)!.StartedAt;
    }

    /// <summary>
    /// Waits until attempt <paramref name="failed"/> at the charge has failed and the workflow waits for the next, and
    /// returns the journaled date of the next attempt. The failure is not journaled on its own, so the date is checked
    /// by order: the failure came after the attempt's journaled start and before the spec saw it, so the date is
    /// <paramref name="wait"/> after a moment between the two. The reminder is the wake for that date exactly.
    /// </summary>
    protected async Task<DateTimeOffset> AwaitRetry(string id, int failed, TimeSpan wait)
    {
        var running = await AwaitRunning(id, r =>
            Assert.Equal((failed, true), (WorkAttempts.Find(r.Attempts, 0)?.Attempt, WorkAttempts.Find(r.Attempts, 0)?.NextAt is not null)));
        var seenBy = DateTimeOffset.UtcNow;
        var attempt = WorkAttempts.Find(running.Attempts, 0)!;
        var next = attempt.NextAt!.Value;
        Assert.True(next >= attempt.StartedAt + wait,
            $"Attempt {failed + 1} is due at {next:O}, less than {wait} after attempt {failed} started at {attempt.StartedAt:O}.");
        Assert.True(next <= seenBy + wait, $"Attempt {failed + 1} is due at {next:O}, more than {wait} after {seenBy:O}.");
        await AwaitWakeFor(id, next);
        return next;
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
