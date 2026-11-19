using System.Collections.Concurrent;

namespace Akka.Durable.Tests.Fixtures;

/// <summary>Everything the recording observer was told, in order. One for each test host.</summary>
public sealed class Observations
{
    private readonly ConcurrentQueue<IDurableObservation> _all = new();

    public IReadOnlyList<IDurableObservation> All => [.. _all];

    public EventSaved[] Events => [.. _all.OfType<EventSaved>()];

    public EffectObserved[] Effects => [.. _all.OfType<EffectObserved>()];

    public TimerObserved[] Timers => [.. _all.OfType<TimerObserved>()];

    internal void Add(IDurableObservation observation) => _all.Enqueue(observation);
}

/// <summary>An observer that writes down every call.</summary>
public sealed class RecordingObserver(Observations log) : IDurableObserver
{
    public void EventSaved(in EventSaved saved) => log.Add(saved);

    public void EffectMoved(in EffectObserved effect) => log.Add(effect);

    public void TimerMoved(in TimerObserved timer) => log.Add(timer);
}

/// <summary>An observer that fails on every call.</summary>
public sealed class ThrowingObserver : IDurableObserver
{
    public void EventSaved(in EventSaved saved) => throw new InvalidOperationException("observer failed");

    public void EffectMoved(in EffectObserved effect) => throw new InvalidOperationException("observer failed");

    public void TimerMoved(in TimerObserved timer) => throw new InvalidOperationException("observer failed");
}
