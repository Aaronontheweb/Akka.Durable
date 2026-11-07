using Akka.Durable.Tests.Fixtures;
using Akka.Durable.TestKit;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// Pure decision tests: Decide / Evolve only, no ActorSystem.
/// </summary>
public class CounterDeciderSpecs
{
    private static DeciderScenario<CounterState, CounterCommand, CounterEvent> Scenario() =>
        DeciderScenario.For(Counter.Definition);

    [Fact]
    public void Increment_persists_event_and_replies()
    {
        Scenario()
            .Given(new Incremented(2))
            .When(new Increment(3))
            .ThenPersisted(new Incremented(3))
            .ThenReplied(new Increment(3), new Accepted())
            .ThenState(s => Assert.Equal(5, s.Count));
    }

    [Fact]
    public void Increment_while_locked_is_stashed()
    {
        Scenario()
            .Given(new CounterLocked())
            .When(new Increment(1))
            .ThenStashed();
    }

    [Fact]
    public void Unlock_releases_stash_after_replying()
    {
        Scenario()
            .Given(new CounterLocked())
            .When(new UnlockCounter())
            .ThenPersisted(new CounterUnlocked())
            .ThenEffects(typeof(UnstashAllEffect), typeof(ReplyEffect));
    }

    [Fact]
    public void Ignore_and_Unhandled_produce_no_events()
    {
        Scenario().When(new Noop()).ThenIgnored();
        Scenario().When(new Unsupported()).ThenUnhandled();
    }

    [Fact]
    public void Effects_keep_declaration_order()
    {
        Scenario()
            .When(new IncrementAndRecord(1))
            .ThenPersisted(new Incremented(1))
            .ThenEffects(typeof(RunEffect<CounterCommand, CounterEvent>))
            .WhenRunSucceeded()
            .ThenEffects(typeof(ReplyEffect));
    }

    [Fact]
    public void Query_style_command_replies_without_events()
    {
        Scenario()
            .Given(new Incremented(4), new Incremented(3))
            .When(new GetCount())
            .ThenNothingPersisted()
            .ThenReplied(new GetCount(), new CountIs(7));
    }

    [Fact]
    public void Scheduled_payload_is_boxed_as_the_command_union()
    {
        // Schedule takes TCommand, so `new Increment(1)` goes through the union conversion
        // at the call site; the payload is a CounterCommand, not a bare Increment.
        var schedule = Assert.IsType<ScheduleEffect>(
            Scenario().When(new RemindLater(TimeSpan.FromMinutes(5))).Effects.Single().Value);

        Assert.Equal("tick", schedule.Key);
        Assert.IsType<CounterCommand>(schedule.Message);
        Assert.True(schedule.Message is CounterCommand { Value: Increment { By: 1 } });
    }

    [Fact]
    public void No_effects_may_follow_Stop()
    {
        var ctx = new DecisionContext<CounterState, CounterCommand, CounterEvent>(
            new EntityId("x"), Counter.TypeName, DateTimeOffset.UnixEpoch, new CounterState(0, false));

        Assert.Throws<InvalidOperationException>(() => Decisions.None<CounterCommand, CounterEvent>().Stop().Schedule("tick", TimeSpan.FromSeconds(1), new Increment(1)));
    }
}
