using System.Collections.Concurrent;
using System.Text;
using Akka.Actor;

namespace Akka.Durable.Samples.AuditTrail.Audit;

/// <summary>One line of the trail.</summary>
public sealed record AuditRow(
    DateTimeOffset At, string Entity, string Event, string Who, string Door, string ActionId, string Cause);

/// <summary>The trail, in memory. A real application would write it to a database.</summary>
public sealed class AuditStore
{
    private readonly ConcurrentQueue<AuditRow> _rows = new();

    public IReadOnlyList<AuditRow> Rows => [.. _rows.OrderBy(row => row.At)];

    public void Add(AuditRow row) => _rows.Enqueue(row);

    /// <summary>The trail as a fixed-width table, newest last.</summary>
    public string Render()
    {
        const string Line = "{0,-12} {1,-14} {2,-16} {3,-8} {4,-24} {5,-9} {6}";
        var text = new StringBuilder().AppendLine(string.Format(Line, "WHEN", "ENTITY", "EVENT", "WHO", "DOOR", "ACTION", "STARTED BY"));
        foreach (var row in Rows)
            text.AppendLine(string.Format(Line, row.At.ToString("HH:mm:ss.fff"), row.Entity, row.Event, row.Who, row.Door, row.ActionId, row.Cause));
        return text.ToString();
    }
}

/// <summary>Listens to what the entities do and writes one row for each saved event.</summary>
public sealed class AuditCollector : ReceiveActor
{
    public AuditCollector(AuditStore store)
    {
        Context.System.EventStream.Subscribe(Self, typeof(IDurableObservation));

        Receive<EventSaved>(saved =>
        {
            var who = AuditPropagator.Read(saved.Headers);
            var cause = saved.Cause.Id is { } id ? $"{saved.Cause.Kind} {id}" : saved.Cause.Kind.ToString();
            store.Add(new AuditRow(
                saved.At, $"{saved.EntityType.Value}/{saved.EntityId.Value}", saved.Event.GetType().Name,
                who?.Who ?? "unknown", who?.Door ?? "unknown", who?.ActionId ?? "unknown", cause));
        });

        // Effects and timers are published too. This trail does not need them.
        ReceiveAny(_ => { });
    }
}
