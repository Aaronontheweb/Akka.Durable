using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Reminder storage in memory that records what reaches it for acknowledging and cancelling, so a spec can wait for
/// <summary>An acknowledgement that reached the reminder storage, and how the storage answered it.</summary>
public sealed record StoredAck(string EntityId, ReminderKey Key, DateTimeOffset DueTimeUtc, string Status);

/// <summary>
/// Reminder storage in memory that records every acknowledgement it has processed, so a spec can wait for the
/// acknowledgement of a delivery before it reads the store.
/// </summary>
public sealed class RecordingReminderStorage : IConditionalReminderMutationStorage
{
    private readonly InMemoryReminderStorage _inner = new();

    /// <summary>The acknowledgements the storage has processed, in the order they completed.</summary>
    public ConcurrentQueue<StoredAck> Acks { get; } = new();

    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(ScheduledReminder reminder, CancellationToken ct = default) =>
        _inner.ScheduleReminderAsync(reminder, ct);

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

    public async Task<AckResult> AcknowledgeReminderAsync(
        ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc, DateTimeOffset ackedAt, CancellationToken ct = default)
    {
        var result = await _inner.AcknowledgeReminderAsync(entity, key, dueTimeUtc, ackedAt, ct);
        Acks.Enqueue(new StoredAck(entity.EntityId, key, dueTimeUtc, result.Status.ToString()));
        return result;
    }

    public async Task<IReadOnlyList<AckResult>> AcknowledgeRemindersAsync(
        IEnumerable<ReminderAcknowledgement> acknowledgements, CancellationToken ct = default)
    {
        var results = await _inner.AcknowledgeRemindersAsync(acknowledgements, ct);
        foreach (var result in results)
            Acks.Enqueue(new StoredAck(result.Entity.EntityId, result.Key, result.DueTimeUtc, result.Status.ToString()));
        return results;
    }
}

/// <summary>
/// An entity timer under key K whose command arrives and is answered with a timer under the same key, or another, or
/// none. The real scheduler delivers the first firing, from a short delay; what the entity's Decide saw is recorded.
/// A reminder stored for an hour is delivered with the same hand-built delivery the other reminder specs use.
/// </summary>
public sealed class EntityTimerRescheduleSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private const string K = "K";
    private static readonly TimeSpan FirstDelay = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan Later = TimeSpan.FromHours(1);

    private readonly RecordingReminderStorage _store = new();
    private readonly ConcurrentQueue<string> _seen = new();
    private int _initializations;

    protected override ShardedHosting? Sharding => null;

    protected override IReminderStorage CreateReminderStorage(ActorSystem system) => _store;

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition => Counter.Definition with
    {
        InitialState = id =>
        {
            Interlocked.Increment(ref _initializations);
            return Counter.Definition.InitialState(id);
        },
        Decide = (state, command, ctx) =>
        {
            if (command.Value is not NoHandler { Note: var note }) return Counter.Decide(state, command, ctx);
            _seen.Enqueue(note);
            var parts = note.Split(':');
            var plan = parts.Length > 1 ? parts[1] : "";
            return parts[0] switch
            {
                "arm" => ctx.None().Schedule(K, FirstDelay, new NoHandler($"first:{plan}")),
                "first" => plan switch
                {
                    "again" => ctx.None().Schedule(K, Later, new NoHandler("second:again")),
                    "persist" => ctx.Persist(new Incremented(1)).Schedule(K, Later, new NoHandler("second:persist")),
                    "other" => ctx.None().Schedule("other", Later, new NoHandler("second:other")),
                    "chain" => ctx.None().Schedule(K, FirstDelay, new NoHandler("second:chain")),
                    "none" => ctx.None(),
                    _ => throw new InvalidOperationException(plan),
                },
                _ => ctx.None(),
            };
        },
    };

    private async Task<IReadOnlyList<ScheduledReminder>> Stored(string id)
    {
        var listed = await Sys.ReminderClient().CreateClient(Counter.TypeName.Value, id).ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    private static string NoteOf(ScheduledReminder reminder) =>
        Assert.IsType<NoHandler>(Assert.IsType<CounterCommand>(reminder.Message).Value).Note;

    private Task AwaitSeen(string note) =>
        AwaitAssertAsync(() => Assert.Contains(note, _seen), TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);

    /// <summary>Arms K with the plan and returns the reminder as stored, before the scheduler delivers it.</summary>
    private async Task<ScheduledReminder> Arm(string id, string plan)
    {
        DurableCounterRef(id).Tell(new NoHandler($"arm:{plan}"));
        ScheduledReminder? armed = null;
        await AwaitAssertAsync(async () =>
        {
            armed = Assert.Single(await Stored(id));
            Assert.Equal($"first:{plan}", NoteOf(armed));
        }, cancellationToken: TestCancellation);
        return armed!;
    }

    /// <summary>
    /// Waits until the store has processed the acknowledgement of the first delivery: the one the entity sends after
    /// its decision, with the due time of the reminder as the scheduler delivered it.
    /// </summary>
    private async Task<StoredAck> AwaitFirstAcked(string id, ScheduledReminder first)
    {
        StoredAck? ack = null;
        await AwaitAssertAsync(() =>
            ack = Assert.Single(_store.Acks, a => a.EntityId == id && a.Key == first.Key && a.DueTimeUtc == first.DueTimeUtc),
            TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);
        return ack!;
    }

    private async Task<ScheduledReminder> AssertReplacedAfterFirstAck(string id, string plan, ScheduledReminder first)
    {
        await AwaitSeen($"first:{plan}");
        await AwaitFirstAcked(id, first); // the store has answered it: with NotFound, as the first occurrence was replaced

        var replacement = Assert.Single(await Stored(id));
        Assert.Equal(new ReminderKey(K), replacement.Key);
        Assert.Equal($"second:{plan}", NoteOf(replacement));

        // The entity took its time after the first due time, so the new due time is at least that and the delay.
        Assert.True(replacement.DueTimeUtc >= first.DueTimeUtc + Later,
            $"Due {replacement.DueTimeUtc:O}, the first was due {first.DueTimeUtc:O}.");
        return replacement;
    }

    [Fact]
    public async Task A_timer_answered_with_the_same_key_leaves_the_new_reminder_stored_after_the_ack_and_it_fires()
    {
        var first = await Arm("again", "again");

        var replacement = await AssertReplacedAfterFirstAck("again", "again", first);

        StoredReminders.Deliver(Sys, replacement);
        await AwaitSeen("second:again");
    }

    [Fact]
    public async Task A_timer_answered_with_an_event_and_the_same_key_leaves_the_new_reminder_stored_after_the_ack_and_it_fires()
    {
        var first = await Arm("persist", "persist");

        var replacement = await AssertReplacedAfterFirstAck("persist", "persist", first);

        StoredReminders.Deliver(Sys, replacement);
        await AwaitSeen("second:persist");
        Assert.Equal(1, await CountOf(CounterRef("persist")));
    }

    [Fact]
    public async Task A_timer_answered_with_the_same_key_survives_a_stop_and_start_of_the_entity_between_the_firings()
    {
        var first = await Arm("restart", "persist");
        var replacement = await AssertReplacedAfterFirstAck("restart", "persist", first);
        Assert.Equal(1, Volatile.Read(ref _initializations));

        // Kill the entity, then read its state until a new incarnation answers.
        var path = $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/restart";
        var actor = await Sys.ActorSelection(path).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(actor);
        actor.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(actor, cancellationToken: TestCancellation);
        await AwaitAssertAsync(async () =>
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
            attempt.CancelAfter(TimeSpan.FromMilliseconds(500));
            await DurableCounterRef("restart").GetStateAsync(attempt.Token);
        }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        Assert.Equal(2, Volatile.Read(ref _initializations)); // a new incarnation, recovered from the journal

        var stillStored = Assert.Single(await Stored("restart"));
        Assert.Equal(replacement.DueTimeUtc, stillStored.DueTimeUtc);
        Assert.Equal("second:persist", NoteOf(stillStored));

        StoredReminders.Deliver(Sys, stillStored);
        await AwaitSeen("second:persist");
        Assert.Equal(1, await CountOf(CounterRef("restart"))); // the event of the first decision was recovered
    }

    [Fact]
    public async Task A_timer_answered_with_another_key_is_gone_and_the_other_is_stored_and_fires()
    {
        var first = await Arm("other", "other");
        await AwaitSeen("first:other");
        Assert.Equal("Success", (await AwaitFirstAcked("other", first)).Status); // nothing replaced it: the ack removed K

        var other = Assert.Single(await Stored("other")); // K is gone, and only the other key is left
        Assert.Equal(new ReminderKey("other"), other.Key);
        Assert.Equal("second:other", NoteOf(other));
        Assert.True(other.DueTimeUtc >= first.DueTimeUtc + Later);

        StoredReminders.Deliver(Sys, other);
        await AwaitSeen("second:other");
    }

    [Fact]
    public async Task A_timer_answered_with_nothing_is_gone_from_the_store()
    {
        await Arm("none", "none");

        await AwaitSeen("first:none");
        await AwaitAssertAsync(async () => Assert.Empty(await Stored("none")), TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task A_timer_answered_with_the_same_key_and_a_short_delay_fires_again_through_the_scheduler_and_is_then_gone()
    {
        await Arm("chain", "chain");

        await AwaitSeen("second:chain");
        await AwaitAssertAsync(async () => Assert.Empty(await Stored("chain")), TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);
        Assert.Equal(["arm:chain", "first:chain", "second:chain"], _seen.ToArray());
    }
}
