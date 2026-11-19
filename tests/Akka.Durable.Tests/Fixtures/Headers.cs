using System.Collections.Concurrent;

namespace Akka.Durable.Tests.Fixtures;

/// <summary>
/// Carries a request id the way an application would: in an <see cref="AsyncLocal{T}"/>. <see cref="Inject"/> sends it
/// beside a command, and <see cref="Restore"/> puts it back before an effect handler runs. It also writes down what
/// it was asked to restore, and how many restores are still open.
/// </summary>
public sealed class RequestIdPropagator : IHeaderPropagator
{
    public const string Key = "request-id";

    private static readonly AsyncLocal<string?> Ambient = new();

    private int _open;

    /// <summary>The request id of the current flow of execution. Null: none.</summary>
    public static string? Current
    {
        get => Ambient.Value;
        set => Ambient.Value = value;
    }

    /// <summary>The headers of every call to <see cref="Restore"/>, in order.</summary>
    public ConcurrentQueue<IReadOnlyDictionary<string, string>> Restored { get; } = new();

    /// <summary>The restores that were not undone yet.</summary>
    public int Open => Volatile.Read(ref _open);

    public void Inject(IDictionary<string, string> headers)
    {
        if (Current is { } id) headers[Key] = id;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> headers)
    {
        Restored.Enqueue(headers);
        Interlocked.Increment(ref _open);
        var before = Current;
        Current = headers.TryGetValue(Key, out var id) ? id : null;
        return new Undo(this, before);
    }

    private sealed class Undo(RequestIdPropagator owner, string? before) : IDisposable
    {
        public void Dispose()
        {
            Current = before;
            Interlocked.Decrement(ref owner._open);
        }
    }
}

/// <summary>A propagator that adds half a header and throws, and that throws when it should restore.</summary>
public sealed class ThrowingPropagator : IHeaderPropagator
{
    public void Inject(IDictionary<string, string> headers)
    {
        headers["half"] = "written";
        throw new InvalidOperationException("propagator failed in Inject");
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> headers) =>
        throw new InvalidOperationException("propagator failed in Restore");

    public void InjectForTimer(EntityTypeName entityType, EntityId entityId, string key, IDictionary<string, string> headers)
    {
        headers["half"] = "written";
        throw new InvalidOperationException("propagator failed in InjectForTimer");
    }
}

/// <summary>
/// Marks a chain that a timer started: its <c>InjectForTimer</c> says which timer. It keeps the mark in an
/// <see cref="AsyncLocal{T}"/> like <see cref="RequestIdPropagator"/>, so a handler can read it.
/// </summary>
public sealed class TimerOriginPropagator : IHeaderPropagator
{
    public const string Key = "started-by";

    private static readonly AsyncLocal<string?> Ambient = new();

    public static string? Current => Ambient.Value;

    public void Inject(IDictionary<string, string> headers)
    {
        if (Current is { } origin) headers[Key] = origin;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> headers)
    {
        var before = Ambient.Value;
        Ambient.Value = headers.GetValueOrDefault(Key);
        return new Undo(() => Ambient.Value = before);
    }

    public void InjectForTimer(EntityTypeName entityType, EntityId entityId, string key, IDictionary<string, string> headers) =>
        headers[Key] = $"timer:{entityType.Value}/{entityId.Value}/{key}";

    private sealed class Undo(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }
}
