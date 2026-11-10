using Akka.Durable.Tests.Fixtures;
using Akka.Durable.TestKit;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// Pure decision tests: Decide / Evolve / Resume only, no ActorSystem.
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
            .ThenEffects(typeof(RunEffect), typeof(ReplyEffect));
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
    public void Service_increment_records_the_request_and_issues_the_effect()
    {
        var request = new IncrementViaService(2, Wait: false);

        Scenario()
            .When(request)
            .ThenPersisted(new ServiceIncrementStarted(request))
            .ThenExecuted(new Compute(request))
            .ThenState(s => Assert.Equal(request, s.Pending));
    }

    [Fact]
    public void Outcome_of_the_pending_request_is_applied_and_answers_it()
    {
        var request = new IncrementViaService(2, Wait: false);

        Scenario()
            .Given(new ServiceIncrementStarted(request))
            .When(new Computed(request, 4))
            .ThenPersisted(new ServiceIncrementFinished(4))
            .ThenReplied(request, new Accepted())
            .ThenState(s =>
            {
                Assert.Equal(4, s.Count);
                Assert.Null(s.Pending);
            });
    }

    [Fact]
    public void Outcome_of_a_request_that_is_no_longer_pending_is_ignored()
    {
        Scenario()
            .When(new Computed(new IncrementViaService(2, Wait: false), 4))
            .ThenIgnored();
    }

    [Fact]
    public void Resume_re_issues_the_effect_for_a_pending_request()
    {
        var request = new IncrementViaService(2, Wait: false);

        Scenario()
            .Given(new ServiceIncrementStarted(request))
            .Resumed()
            .ThenNothingPersisted()
            .ThenExecuted(new Compute(request));
    }

    [Fact]
    public void Resume_with_nothing_pending_has_no_effects()
    {
        Scenario()
            .Given(new Incremented(1))
            .Resumed()
            .ThenNothingPersisted()
            .ThenEffects();
    }

    [Fact]
    public void A_definition_without_Resume_resumes_nothing()
    {
        DeciderScenario.For(Counter.Definition with { Resume = null })
            .Given(new ServiceIncrementStarted(new IncrementViaService(2, Wait: false)))
            .Resumed()
            .ThenEffects();
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
        Assert.Throws<InvalidOperationException>(() =>
            Decisions.None<CounterCommand, CounterEvent>().Stop().Schedule("tick", TimeSpan.FromSeconds(1), new Increment(1)));
    }

    [Fact]
    public void Execute_and_Send_effects_compare_by_their_data()
    {
        var handled = Decisions.None<CounterCommand, CounterEvent>()
            .Execute(new Compute(new IncrementViaService(3, Wait: false)))
            .Send<CounterCommand>(new EntityId("other"), new Increment(1));

        Assert.Equal(new ExecuteEffect(new Compute(new IncrementViaService(3, Wait: false))), handled.Effects[0].Value);
        Assert.Equal(new SendEffect(typeof(CounterCommand), new EntityId("other"), (CounterCommand)new Increment(1)), handled.Effects[1].Value);
    }
}
