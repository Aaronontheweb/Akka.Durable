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
