using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Event;
using Akka.Hosting;
using Akka.Reminders;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

using CampaignRefs = IWorkflowRefs<Invoice, CampaignState, CampaignFact, CampaignOutcome>;
using CampaignRef = IWorkflowRef<Invoice, CampaignState, CampaignFact, CampaignOutcome>;

/// <summary>How a call from an entity to the campaign workflow goes wrong.</summary>
public enum Break
{
    /// <summary>The call throws before it reaches the workflow.</summary>
    Throw,

    /// <summary>The call never returns, until its token is cancelled.</summary>
    Hang,

    /// <summary>The workflow gets the call, but the answer is lost.</summary>
    LoseAnswer,
}

/// <summary>One call an entity made to the campaign workflow.</summary>
/// <param name="Kind">"start", "notify" or "cancel".</param>
/// <param name="Number">1 for the first call of this kind, 2 for the second, and so on.</param>
public sealed record CallAttempt(string Kind, int Number, DateTimeOffset At);

/// <summary>
/// Sits between the entity and the campaign workflow: records every call, and can make calls fail, hang, lose
/// their answer or wait for a gate. Calls an entity makes find it in place of the library's workflow refs.
/// </summary>
public sealed class CallControl
{
    private readonly ConcurrentQueue<CallAttempt> _attempts = new();
    private readonly ConcurrentDictionary<string, int> _numbers = new();

    /// <summary>Runs before a call reaches the workflow. Throwing fails the call.</summary>
    public Func<CallAttempt, CancellationToken, Task> Before { get; set; } = (_, _) => Task.CompletedTask;

    /// <summary>Runs after the workflow answered, before the answer returns to the entity.</summary>
    public Func<CallAttempt, CancellationToken, Task> After { get; set; } = (_, _) => Task.CompletedTask;

    public IReadOnlyList<CallAttempt> Attempts => [.. _attempts];

    public int Count(string kind) => _attempts.Count(a => a.Kind == kind);

    /// <summary>Calls of <paramref name="kind"/> numbered up to <paramref name="upTo"/> go wrong in this way.</summary>
    public void Fail(string kind, int upTo, Break how)
    {
        if (how == Break.LoseAnswer)
            After = (a, _) => a.Kind == kind && a.Number <= upTo ? throw new InvalidOperationException("The answer is lost.") : Task.CompletedTask;
        else
            Before = (a, ct) => a.Kind != kind || a.Number > upTo ? Task.CompletedTask
                : how == Break.Throw ? throw new InvalidOperationException("The call is lost.")
                : Task.Delay(Timeout.Infinite, ct);
    }

    /// <summary>Calls of <paramref name="kind"/> wait until the returned source is completed.</summary>
    public TaskCompletionSource Hold(string kind)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Before = (a, ct) => a.Kind == kind ? gate.Task.WaitAsync(ct) : Task.CompletedTask;
        return gate;
    }

    internal async Task<T> Run<T>(string kind, Func<Task<T>> call, CancellationToken ct)
    {
        var attempt = new CallAttempt(kind, _numbers.AddOrUpdate(kind, 1, (_, n) => n + 1), DateTimeOffset.UtcNow);
        _attempts.Enqueue(attempt);
        await Before(attempt, ct);
        var answer = await call();
        await After(attempt, ct);
        return answer;
    }
}

internal sealed class ControlledRefs(CampaignRefs inner, CallControl control) : CampaignRefs
{
    public CampaignRef Get(WorkflowId id) => new ControlledRef(inner.Get(id), control);
}

internal sealed class ControlledRef(CampaignRef inner, CallControl control) : CampaignRef
{
    public WorkflowId Id => inner.Id;

    public Task<StartResult<CampaignOutcome>> Start(Invoice input, CancellationToken ct = default) =>
        control.Run("start", () => inner.Start(input, ct), ct);

    public Task<DeliveryResult<CampaignOutcome>> Notify(CampaignFact fact, CancellationToken ct = default) =>
        control.Run("notify", () => inner.Notify(fact, ct), ct);

    public Task<DeliveryResult<CampaignOutcome>> Cancel(string reason, string by, CancellationToken ct = default) =>
        control.Run("cancel", () => inner.Cancel(reason, by, ct), ct);

    public Task<StateSnapshot<WorkflowStatus<CampaignState, CampaignOutcome>>> GetStateAsync(CancellationToken ct = default) =>
        inner.GetStateAsync(ct);

    public Task<StateSnapshot<WorkflowStatus<CampaignState, CampaignOutcome>>> WaitForStateAsync(
        Func<WorkflowStatus<CampaignState, CampaignOutcome>, bool> until, CancellationToken ct) =>
        inner.WaitForStateAsync(until, ct);

    public IAsyncEnumerable<StateSnapshot<WorkflowStatus<CampaignState, CampaignOutcome>>> WatchStateAsync(
        CancellationToken ct = default) =>
        inner.WatchStateAsync(ct);

    public Task<CampaignOutcome> WaitForOutcome(CancellationToken ct) => inner.WaitForOutcome(ct);
}

/// <summary>
/// Hosts the billing entity and the campaign workflow it uses, with every call from the entity to the workflow
/// going through <see cref="Calls"/>. The workflow is registered after the entity.
/// </summary>
public abstract class EntityCallsKit(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected static readonly DateTimeOffset FarAway = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);
    protected static readonly TimeSpan Interval = TimeSpan.FromSeconds(1);

    /// <summary>Long enough for a repeat that should not happen to show up.</summary>
    protected virtual TimeSpan Quiet => TimeSpan.FromMilliseconds(2500);

    protected virtual bool Sharded => false;

    protected virtual EntityOptions Options => EntityOptions.Default with { EffectTimeout = Interval };

    protected virtual EntityDefinition<BillingState, BillingCommand, BillingEvent> Entity => Billing.Definition;

    /// <summary>How often the workflow's Start function ran: the times a workflow was really started.</summary>
    private int _workflowStarts;

    protected int WorkflowStarts => Volatile.Read(ref _workflowStarts);

    protected CallControl Calls => Host.Services.GetRequiredService<CallControl>();

    private IRequiredActor<WorkflowRegion<Invoice, CampaignState, CampaignFact, CampaignOutcome>> RegionActor() =>
        Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<Invoice, CampaignState, CampaignFact, CampaignOutcome>>>();

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddCampaignHandlers();
        services.AddSingleton<CallControl>();
        services.AddSingleton<CampaignRefs>(provider => new ControlledRefs(
            new WorkflowRefs<Invoice, CampaignState, CampaignFact, CampaignOutcome>(
                provider.GetRequiredService<IRequiredActor<WorkflowRegion<Invoice, CampaignState, CampaignFact, CampaignOutcome>>>(),
                provider.GetRequiredService<ActorSystem>()),
            provider.GetRequiredService<CallControl>()));
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        if (Sharded)
            builder.WithRemoting("localhost", 0).WithClustering().AddStartup((system, _) =>
            {
                var cluster = Akka.Cluster.Cluster.Get(system);
                cluster.Join(cluster.SelfAddress);
            });

        builder
            .WithCustomSerializer(CampaignSerializer.CreateSerializerDetails)
            .WithCustomSerializer(BillingSerializer.CreateSerializerDetails)
            .WithDurableEntity(Entity with { Options = Options }, Sharded ? new ShardedHosting() : null)
            .WithDurableWorkflow(Campaign.Definition with
            {
                Start = input =>
                {
                    Interlocked.Increment(ref _workflowStarts);
                    return Campaign.Definition.Start(input);
                },
            }, Sharded ? new ShardedHosting() : null);
    }

    protected TimeSpan FirstAsk => Sharded ? TimeSpan.FromSeconds(30) : DefaultAskTimeout;

    protected IDurableEntityRef<BillingCommand, BillingState, BillingEvent> Bill(string id) =>
        DurableEntityRef<BillingCommand, BillingState, BillingEvent>(id);

    /// <summary>Issues the bill, which starts its campaign.</summary>
    protected Task Issue(string id) => Bill(id).Ask(new IssueBill(FarAway), AskCancellation(FirstAsk));

    /// <summary>The campaign of a bill, reached without the control: the test acts as an outside caller.</summary>
    protected CampaignRef Campaigns(string bill) =>
        new WorkflowRefs<Invoice, CampaignState, CampaignFact, CampaignOutcome>(RegionActor(), Sys).Get(Billing.CampaignOf(new EntityId(bill)));

    protected async Task<WorkflowStatus<CampaignState, CampaignOutcome>> CampaignOf(string bill) =>
        (await Campaigns(bill).GetStateAsync(AskCancellation(FirstAsk))).State;

    protected async Task AwaitCampaign<T>(string bill, Action<T>? check = null)
    {
        await AwaitAssertAsync(async () =>
        {
            var found = Assert.IsType<T>((await CampaignOf(bill)).Value);
            check?.Invoke(found);
        }, TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);
    }

    protected async Task AwaitAnswers(string bill, params string[] expected) =>
        await AwaitAssertAsync(async () =>
            Assert.Equal(expected, (await Bill(bill).GetStateAsync(AskCancellation())).State.Answers),
            TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);

    /// <summary>For answers that can arrive in either order.</summary>
    protected async Task AwaitAnswersInAnyOrder(string bill, params string[] expected) =>
        await AwaitAssertAsync(async () =>
            Assert.Equal(expected.Order(), (await Bill(bill).GetStateAsync(AskCancellation())).State.Answers.Order()),
            TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);

    protected async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string bill)
    {
        var listed = await Sys.ReminderClient().CreateClient(Billing.TypeName.Value, bill).ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    protected async Task AwaitNoReminders(string bill) =>
        await AwaitAssertAsync(async () => Assert.Empty(await RemindersOf(bill)),
            TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);

    protected async Task AwaitAttempts(string kind, int count) =>
        await AwaitConditionAsync(() => Task.FromResult(Calls.Count(kind) >= count), TimeSpan.FromSeconds(20),
            cancellationToken: TestCancellation);

    /// <summary>Nothing else happens for a while: proves that a call is not repeated.</summary>
    protected async Task LetTimePass() => await ExpectNoMsgAsync(Quiet, TestCancellation);

    protected string EntityPath(string id) => Sharded
        ? $"/system/sharding/{Billing.TypeName.Value}/*/{id}"
        : $"/user/{LocalEntityHost.ActorName(Billing.TypeName)}/{Uri.EscapeDataString(id)}";

    protected Task<IActorRef> EntityActor(string id) =>
        Sys.ActorSelection(EntityPath(id)).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    /// <summary>The errors the system logs from now on.</summary>
    protected ConcurrentQueue<LogEvent> Errors() => LogCollector.Attach(Sys);

    protected static IEnumerable<LogEvent> ErrorsAbout(ConcurrentQueue<LogEvent> log, string text) =>
        log.Where(e => e is Error && LogCollector.Text(e).Contains(text));

    /// <summary>Restarts the entity: its decision throws, and the supervisor starts it again from the journal.</summary>
    protected async Task Crash(string bill)
    {
        Bill(bill).Tell(new CrashBilling());
        await AwaitAssertAsync(async () =>
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
            attempt.CancelAfter(TimeSpan.FromMilliseconds(500));
            await Bill(bill).GetStateAsync(attempt.Token);
        }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
    }
}
