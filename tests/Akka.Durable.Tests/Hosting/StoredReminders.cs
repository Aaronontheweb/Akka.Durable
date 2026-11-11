using Akka.Actor;
using Akka.Reminders;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The delivery of a stored reminder, without the clock: the way Akka.Reminders hands a due reminder to the
/// library's resolver, which starts the entity if it is not running. A spec that does this sends the workflow nothing
/// of its own: the message is the one the workflow stored.
/// </summary>
internal static class StoredReminders
{
    /// <summary>
    /// The date the workflow stored its reminder for: the one its wake carries. The reminder's <c>DueTimeUtc</c> is
    /// counted from a later reading of the clock, so it is never earlier than this date, and not equal to it.
    /// </summary>
    public static DateTimeOffset StoredFor(this ScheduledReminder stored) =>
        stored.Message is WfCommand { Value: WfWake { At: { } at } }
            ? at
            : throw new InvalidOperationException($"The reminder {stored.Key} does not carry a workflow wake: {stored.Message}.");

    /// <summary>
    /// Asserts that <paramref name="stored"/> is the workflow's wake for <paramref name="date"/>: it carries that date
    /// exactly, and it is due no earlier.
    /// </summary>
    public static void IsWakeFor(this ScheduledReminder stored, DateTimeOffset date)
    {
        Xunit.Assert.Equal(date, stored.StoredFor());
        Xunit.Assert.True(stored.DueTimeUtc >= date, $"The reminder is due at {stored.DueTimeUtc:O}, before {date:O}.");
    }

    /// <summary>
    /// Delivers <paramref name="stored"/>. With <paramref name="asIfStoredFor"/> the delivery is one that comes before the
    /// reminder is due: a wake that says it was stored for that time.
    /// </summary>
    public static void Deliver(ActorSystem system, ScheduledReminder stored, DateTimeOffset? asIfStoredFor = null)
    {
        var message = asIfStoredFor is { } early ? (WfCommand)new WfWake(early) : stored.Message;
        var envelope = new ReminderEnvelope(stored.Entity, stored.Key, asIfStoredFor ?? stored.DueTimeUtc, stored.Deadline, message);
        new DurableEntityReminderResolver(system).DeliverReminder(stored.Entity, envelope);
    }
}
