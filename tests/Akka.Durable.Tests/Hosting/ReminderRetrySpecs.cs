using Akka.Cluster.Hosting;
using Akka.Durable.Tests.Fixtures;
using Akka.Event;
using Akka.Reminders;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

public sealed class ReminderRetrySpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private int _initializations;

    protected override ShardOptions? Sharding => null;

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition => Counter.Definition with
    {
        InitialState = id =>
        {
            Interlocked.Increment(ref _initializations);
            return Counter.Definition.InitialState(id);
        },
        Decide = (state, command, ctx) => command switch
        {
            RemindLater c => ctx.None().Schedule("reminder", c.Delay, new Noop()),
            Noop => ctx.None().Run<ScopedRecorder>(
                async (recorder, ct) =>
                {
                    await recorder.RecordAsync("mail", ct);
                    throw new InvalidOperationException("mailer unavailable");
                },
                onSuccess: next => next,
                onFailure: (error, next) => next.Schedule("reminder", TimeSpan.FromHours(1), new Noop())),
            _ => Counter.Decide(state, command, ctx)
        }
    };

    [Fact]
    public async Task Re_arming_the_same_key_survives_the_old_delivery_ack_without_restarting_the_entity()
    {
        var counter = DurableCounterRef("retry");
        await counter.Ask(new Increment(5), AskCancellation());
        Assert.Equal(1, Volatile.Read(ref _initializations));
        var started = DateTimeOffset.UtcNow;

        // The stale-ack warning is emitted after storage has processed the terminal stage's ack.
        await EventFilter.Custom<Warning>(warning =>
            warning.Message.ToString()!.StartsWith("Ack for reminder", StringComparison.Ordinal)
            && warning.Message.ToString()!.Contains("returned NotFound", StringComparison.Ordinal))
            .ExpectOneAsync(() =>
            {
                counter.Tell(new RemindLater(TimeSpan.FromMilliseconds(100)));
                return Task.CompletedTask;
            }, TestCancellation);

        var client = Sys.ReminderClient().CreateClient(Counter.TypeName.Value, "retry");
        var listed = await client.ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        var replacement = Assert.Single(listed.Reminders);
        Assert.Equal(new ReminderKey("reminder"), replacement.Key);
        Assert.IsType<Noop>(Assert.IsType<CounterCommand>(replacement.Message).Value);
        Assert.InRange(replacement.DueTimeUtc, started.AddHours(1), DateTimeOffset.UtcNow.AddHours(1));

        var snapshot = await counter.GetStateAsync(AskCancellation());
        Assert.Equal(new CounterState(5, false), snapshot.State);
        Assert.Equal(1, snapshot.SequenceNr);
        Assert.Equal(1, Volatile.Read(ref _initializations));
        Assert.Equal(["mail", "disposed"], Recorder.Entries);
    }
}
