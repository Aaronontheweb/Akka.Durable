using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
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
        Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<Invoice, CampaignFact, CampaignOutcome>>>().ActorRef;

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
    /// <param name="id">The workflow.</param>
    /// <param name="at">The time the wake says its reminder was stored for. None by default.</param>
    private protected void Wake(string id, DateTimeOffset? at = null) =>
        Region.Tell(new ShardingEnvelope(id, (WfCommand)new WfWake(at)), ActorRefs.NoSender);

    /// <summary>Everything the journal holds for a workflow, in order. Reading it does not touch the workflow.</summary>
    private protected async Task<IReadOnlyList<object>> JournalOf(string id)
    {
        var probe = CreateTestProbe();
        var persistenceId = EntityOptions.DefaultPersistenceIdFactory(new EntityId(id), new EntityTypeName(Definition.TypeName.Value));
        Sys.ActorOf(Props.Create(() => new JournalReader(persistenceId, probe.Ref)));
        var events = await probe.ExpectMsgAsync<object[]>(cancellationToken: TestCancellation);
        return [.. events.Select(stored => ((WfEvent)stored).Value!)];
    }

    /// <summary>The reminders stored for a workflow. Reading them does not touch the workflow.</summary>
    protected async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string id)
    {
        var client = Sys.ReminderClient().CreateClient(Definition.TypeName.Value, id);
        var listed = await client.ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    /// <summary>
    /// Kills the workflow's actor and waits until its host has started a new one, which has to recover.
    /// </summary>
    /// <remarks>
    /// The host learns that the actor stopped a moment after this spec does. A message sent in between goes to
    /// the dead actor and is lost. So this reads the state, with a short time limit, until the new actor answers.
    /// </remarks>
    protected async Task RestartWorkflow(string id)
    {
        await StopWorkflow(id);
        await AwaitStarted(id);
    }

    /// <summary>Reads the state, with a short time limit, until an actor for the workflow answers.</summary>
    protected async Task AwaitStarted(string id) =>
        await AwaitAssertAsync(async () =>
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
            attempt.CancelAfter(TimeSpan.FromMilliseconds(500));
            await new EntityRef<WfCommand, WfState, WfEvent>(new EntityId(id), Region).GetStateAsync(attempt.Token);
        }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

    /// <summary>
    /// Kills the workflow's actor and sends it nothing. A message sent straight after this can be lost: see
    /// <see cref="RestartWorkflow"/>.
    /// </summary>
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

/// <summary>
/// Reminder storage in memory, whose writes of new reminders fail while <see cref="Failing"/> is set. It counts
/// the writes and the deliveries of each entity's reminders.
/// </summary>
public sealed class SwitchableReminderStorage : IConditionalReminderMutationStorage
{
    private readonly InMemoryReminderStorage _inner = new();
    private readonly ConcurrentDictionary<string, int> _writes = new();
    private readonly ConcurrentDictionary<string, int> _deliveries = new();
    private volatile bool _failing;
    private int _failedWrites;

    public bool Failing
    {
        get => _failing;
        set => _failing = value;
    }

    /// <summary>Called with each reminder before it is stored.</summary>
    public Action<ScheduledReminder>? Writing { get; set; }

    /// <summary>The writes refused while <see cref="Failing"/> was set.</summary>
    public int FailedWrites => Volatile.Read(ref _failedWrites);

    /// <summary>The reminders stored for an entity.</summary>
    public int Writes(string entityId) => _writes.GetValueOrDefault(entityId);

    /// <summary>The reminder deliveries to an entity: each one is recorded just before it is sent.</summary>
    public int Deliveries(string entityId) => _deliveries.GetValueOrDefault(entityId);

    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(ScheduledReminder reminder, CancellationToken ct = default)
    {
        if (_failing)
        {
            Interlocked.Increment(ref _failedWrites);
            throw new InvalidOperationException("The reminder storage is unavailable.");
        }

        Writing?.Invoke(reminder);
        _writes.AddOrUpdate(reminder.Entity.EntityId, 1, (_, count) => count + 1);
        return _inner.ScheduleReminderAsync(reminder, ct);
    }

    public Task<bool> UpsertReminderOccurrencesAsync(IEnumerable<ScheduledReminder> reminders, CancellationToken ct = default) =>
        _inner.UpsertReminderOccurrencesAsync(reminders, ct);

    // The scheduler marks a reminder as awaiting its ack in this write, then delivers it.
    public async Task<bool> CommitReminderMutationsAsync(ReminderMutationBatch mutationBatch, CancellationToken ct = default)
    {
        var committed = await _inner.CommitReminderMutationsAsync(mutationBatch, ct);
        if (committed)
            foreach (var delivered in mutationBatch.AwaitingAckReminders)
                _deliveries.AddOrUpdate(delivered.Entity.EntityId, 1, (_, count) => count + 1);
        return committed;
    }

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
