using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Hosting;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Hosts the reminder campaign through the public API and gives specs a view of the entity that hosts it, of
/// its reminder, and a reminder storage that can be made to fail.
/// </summary>
public abstract class CampaignWorkflowKit(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(20);

    /// <summary>Host in a shard region of a single-node cluster; otherwise in a local host actor.</summary>
    protected virtual bool Sharded => false;

    protected virtual WorkflowDefinition<Invoice, CampaignState, CampaignFact, CampaignOutcome> Definition =>
        Campaign.Definition;

    /// <summary>The test host's reminder storage. Storing a reminder fails while <see cref="SwitchableReminderStorage.Failing"/> is set.</summary>
    protected SwitchableReminderStorage ReminderStorage { get; } = new();

    protected override IReminderStorage CreateReminderStorage(ActorSystem system) => ReminderStorage;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCampaignHandlers();

    protected sealed override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        if (Sharded)
            builder.WithRemoting("localhost", 0).WithClustering().AddStartup((system, _) =>
            {
                var cluster = Akka.Cluster.Cluster.Get(system);
                cluster.Join(cluster.SelfAddress);
            });

        builder
            .WithCustomSerializer(CampaignSerializer.CreateSerializerDetails)
            .WithDurableWorkflow(Definition, Sharded ? new ShardedHosting() : null);
    }

    protected CampaignServices Services => Host.Services.GetRequiredService<CampaignServices>();

    protected IWorkflowRef<Invoice, CampaignFact, CampaignOutcome> CampaignRef(string id) =>
        WorkflowRef<Invoice, CampaignFact, CampaignOutcome>(id);

    private IActorRef Region =>
        Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<Invoice, CampaignOutcome>>>().ActorRef;

    /// <summary>The state of the entity that hosts a campaign: the author's state and what the library tracks.</summary>
    private protected async Task<StateSnapshot<WfState>> HostedState(string id) =>
        await new EntityRef<WfCommand, WfState, WfEvent>(new EntityId(id), Region).GetStateAsync(AskCancellation());

    private protected async Task<WfRunning> AwaitRunning(string id, Action<WfRunning> assert, TimeSpan? duration = null)
    {
        WfRunning? found = null;
        await AwaitAssertAsync(async () =>
        {
            found = Assert.IsType<WfRunning>((await HostedState(id)).State.Value);
            assert(found);
        }, duration, cancellationToken: TestCancellation);
        return found!;
    }

    private protected static object StateOf(WfRunning running) => ((CampaignState)running.State).Value!;

    /// <summary>Sends the workflow the command its reminder carries, without the reminder.</summary>
    private protected void Wake(string id) => Region.Tell(new ShardingEnvelope(id, (WfCommand)new WfWake()), ActorRefs.NoSender);

    /// <summary>The reminders stored for a workflow. Reading them does not touch the workflow.</summary>
    protected async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string id)
    {
        var client = Sys.ReminderClient().CreateClient(Definition.TypeName.Value, id);
        var listed = await client.ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    /// <summary>Stops the workflow's actor, so the next message starts one that has to recover.</summary>
    protected async Task StopWorkflow(string id)
    {
        var type = Definition.TypeName.Value;
        var path = Sharded
            ? $"/system/sharding/{type}/*/{id}"
            : $"/user/{LocalEntityHost.ActorName(new EntityTypeName(type))}/{id}";
        var actor = await Sys.ActorSelection(path).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(actor);
        actor.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(actor, cancellationToken: TestCancellation);
    }
}

/// <summary>Reminder storage in memory, whose writes of new reminders fail while <see cref="Failing"/> is set.</summary>
public sealed class SwitchableReminderStorage : IConditionalReminderMutationStorage
{
    private readonly InMemoryReminderStorage _inner = new();
    private volatile bool _failing;

    public bool Failing
    {
        get => _failing;
        set => _failing = value;
    }

    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(ScheduledReminder reminder, CancellationToken ct = default) =>
        _failing
            ? throw new InvalidOperationException("The reminder storage is unavailable.")
            : _inner.ScheduleReminderAsync(reminder, ct);

    public Task<bool> UpsertReminderOccurrencesAsync(IEnumerable<ScheduledReminder> reminders, CancellationToken ct = default) =>
        _inner.UpsertReminderOccurrencesAsync(reminders, ct);

    public Task<bool> CommitReminderMutationsAsync(ReminderMutationBatch mutationBatch, CancellationToken ct = default) =>
        _inner.CommitReminderMutationsAsync(mutationBatch, ct);

    public Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(ReminderEntity entity, ReminderKey key, CancellationToken ct = default) =>
        _inner.CancelReminderAsync(entity, key, ct);

    public Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(
        ReminderEntity entity, int take = 10, int skip = 0, CancellationToken ct = default) =>
        _inner.GetRemindersForEntityAsync(entity, take, skip, ct);

    public Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(ReminderEntity entity, CancellationToken ct = default) =>
        _inner.CancelAllRemindersForEntityAsync(entity, ct);

    public Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken ct = default) =>
        _inner.GetRemindersOverviewAsync(now, ct);

    public Task<PendingRemindersWithSummary> GetNextRemindersAsync(
        DateTimeOffset untilDeadline, DateTimeOffset now, ReminderBatchSize maxCount, CancellationToken ct = default) =>
        _inner.GetNextRemindersAsync(untilDeadline, now, maxCount, ct);

    public Task<bool> MarkRemindersAsCompletedAsync(IEnumerable<CompletedReminder> keys, CancellationToken ct = default) =>
        _inner.MarkRemindersAsCompletedAsync(keys, ct);

    public Task<bool> CleanUpCompletedRemindersAsync(DateTimeOffset olderThan, CancellationToken ct = default) =>
        _inner.CleanUpCompletedRemindersAsync(olderThan, ct);

    public Task<int> ExpireRemindersAsync(DateTimeOffset now, CancellationToken ct = default) =>
        _inner.ExpireRemindersAsync(now, ct);

    public Task<bool> MarkRemindersAsAwaitingAckAsync(IEnumerable<AwaitingAckReminder> reminders, CancellationToken ct = default) =>
        _inner.MarkRemindersAsAwaitingAckAsync(reminders, ct);

    public Task<IReadOnlyList<ScheduledReminder>> GetTimedOutAckRemindersAsync(
        DateTimeOffset now, ReminderBatchSize maxCount, CancellationToken ct = default) =>
        _inner.GetTimedOutAckRemindersAsync(now, maxCount, ct);

    public Task<ScheduledReminder?> GetAwaitingAckReminderAsync(
        ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc, CancellationToken ct = default) =>
        _inner.GetAwaitingAckReminderAsync(entity, key, dueTimeUtc, ct);

    public Task<ReminderOccurrenceStatus?> GetReminderOccurrenceStatusAsync(
        ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc, CancellationToken ct = default) =>
        _inner.GetReminderOccurrenceStatusAsync(entity, key, dueTimeUtc, ct);

    public Task<DateTimeOffset?> GetNextAwaitingAckDeadlineAsync(CancellationToken ct = default) =>
        _inner.GetNextAwaitingAckDeadlineAsync(ct);

    public Task<AckResult> AcknowledgeReminderAsync(
        ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc, DateTimeOffset ackedAt, CancellationToken ct = default) =>
        _inner.AcknowledgeReminderAsync(entity, key, dueTimeUtc, ackedAt, ct);

    public Task<IReadOnlyList<AckResult>> AcknowledgeRemindersAsync(
        IEnumerable<ReminderAcknowledgement> acknowledgements, CancellationToken ct = default) =>
        _inner.AcknowledgeRemindersAsync(acknowledgements, ct);
}
