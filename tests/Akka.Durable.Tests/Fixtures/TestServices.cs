using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Persistence;

namespace Akka.Durable.Tests.Fixtures;

/// <summary>
/// Singleton log shared by scoped services, so tests can observe effect execution and scope disposal.
/// </summary>
public sealed class EffectRecorder
{
    private readonly ConcurrentQueue<string> _entries = new();
    public void Add(string entry) => _entries.Enqueue(entry);
    public IReadOnlyList<string> Entries => [.. _entries];
}

/// <summary>
/// Scoped service: records when it's used and when its DI scope is disposed.
/// </summary>
public sealed class ScopedRecorder(EffectRecorder recorder) : IAsyncDisposable
{
    public Task RecordAsync(string entry, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        recorder.Add(entry);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        recorder.Add("disposed");
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Lets a test hold a Run effect or an effect handler open until it chooses to release it.
/// </summary>
public sealed class Gate
{
    private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task WaitAsync(CancellationToken ct) => _tcs.Task.WaitAsync(ct);
    public void Open() => _tcs.TrySetResult();
}

/// <summary>
/// Stand-in for a shard / local entity host: forwards to one entity child and honours
/// the sharding <see cref="Passivate"/> protocol, reporting what it sees to a probe.
/// </summary>
internal sealed class TestEntityParent : ReceiveActor
{
    private readonly bool _stopOnFailure;

    public TestEntityParent(Props entityProps, IActorRef probe, bool stopOnFailure = false)
    {
        _stopOnFailure = stopOnFailure;
        var child = Context.ActorOf(entityProps, "entity");
        Context.Watch(child);

        Receive<Passivate>(p =>
        {
            probe.Forward(p); // preserve Sender: a real shard identifies the entity by it
            Sender.Tell(p.StopMessage);
        });
        // Terminated is auto-received and dropped by actors that aren't watching the dead
        // actor, so forwarding it to the probe would be silently discarded. Report instead.
        Receive<Terminated>(t => probe.Tell(new EntityStopped(t.ActorRef)));
        ReceiveAny(m => child.Forward(m));
    }

    // Stopping instead of restarting keeps a failure during recovery from looping.
    protected override SupervisorStrategy SupervisorStrategy() =>
        _stopOnFailure ? new OneForOneStrategy(_ => Directive.Stop) : base.SupervisorStrategy();
}

internal sealed record EntityStopped(IActorRef Entity);

/// <summary>
/// Replays whatever is stored under a persistence id and reports it; used to verify
/// which persistence id an entity actually wrote to.
/// </summary>
internal sealed class JournalReader : ReceivePersistentActor
{
    public JournalReader(string persistenceId, IActorRef replyTo)
    {
        PersistenceId = persistenceId;
        var events = new List<object>();

        Recover<RecoveryCompleted>(_ => replyTo.Tell(events.ToArray()));
        Recover<SnapshotOffer>(_ => { });
        Recover<object>(events.Add);
    }

    public override string PersistenceId { get; }
}

/// <summary>
/// Lets Akka.Reminders store reminders for an entity actor that a spec creates directly, with no shard region or
/// local host. Deliveries go to dead letters.
/// </summary>
internal sealed class DirectEntityReminderResolver(ActorSystem system) : Akka.Reminders.Sharding.IShardRegionResolver
{
    public IActorRef? TryResolve(Akka.Reminders.ReminderEntity entity) => system.DeadLetters;

    public void DeliverReminder(Akka.Reminders.ReminderEntity entity, Akka.Reminders.ReminderEnvelope envelope, IActorRef? sender = null) =>
        system.DeadLetters.Tell(envelope, sender ?? ActorRefs.NoSender);
}
