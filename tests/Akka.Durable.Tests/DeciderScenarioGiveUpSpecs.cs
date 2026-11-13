using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Xunit;
using Xunit.Sdk;

namespace Akka.Durable.Tests;

/// <summary>
/// <c>DeciderScenario</c> fails a test for what the entity would refuse at run time: an effect issued by, or given up
/// in, a definition that has no <c>GaveUp</c>. No ActorSystem.
/// </summary>
public sealed class DeciderScenarioGiveUpSpecs
{
    private const string ArgumentToAdd =
        "but the definition of entity type [counter] does not say which command Decide gets when an effect gets no " +
        "answer. Add the argument \"GaveUp: gaveUp => new YourCommand(gaveUp.Reason)\" to the EntityDefinition, where " +
        "YourCommand is one of its commands (CounterCommand).";

    private static readonly IncrementViaService Request = new(3, Wait: false);

    private static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> NoGaveUp =
        Counter.Definition with { GaveUp = null };

    [Fact]
    public void WhenGaveUp_fails_the_test_for_a_definition_with_no_GaveUp_and_names_the_argument_to_add()
    {
        var scenario = DeciderScenario.For(NoGaveUp).Given(new ServiceIncrementStarted(Request));

        var failure = Assert.Throws<XunitException>(() => scenario.WhenGaveUp(new Compute(Request), GaveUpReason.Failed));

        Assert.Equal($"The effect Compute gave up, {ArgumentToAdd}", failure.Message);
    }

    [Fact]
    public void When_fails_the_test_for_a_decision_that_issues_an_effect_in_a_definition_with_no_GaveUp()
    {
        var scenario = DeciderScenario.For(NoGaveUp);

        var failure = Assert.Throws<XunitException>(() => scenario.When(Request));

        Assert.Equal($"The decision issues the effect Compute, {ArgumentToAdd}", failure.Message);
    }

    [Fact]
    public void Neither_fails_for_a_definition_that_has_its_GaveUp()
    {
        DeciderScenario.For(Counter.Definition)
            .When(Request)
            .ThenPersisted(new ServiceIncrementStarted(Request))
            .ThenExecuted(new Compute(Request));

        DeciderScenario.For(Counter.Definition)
            .Given(new ServiceIncrementStarted(Request))
            .WhenGaveUp(new Compute(Request), GaveUpReason.TimedOut, attempts: 2)
            .ThenPersisted(new GaveUpNoted("compute:TimedOut", EndsServiceIncrement: true))
            .ThenState(state => Assert.Null(state.Pending));
    }

    [Fact]
    public void When_does_not_fail_for_a_decision_with_no_Execute_in_a_definition_with_no_GaveUp()
    {
        var scenario = DeciderScenario.For(NoGaveUp);

        scenario.When(new Increment(2)).ThenPersisted(new Incremented(2));
        scenario.When(new GetCount()).ThenNothingPersisted().ThenReplied(new GetCount(), new CountIs(0));

        // Run is best-effort work whose result the entity does not need: it has nothing to give up.
        scenario.When(new IncrementAndRecord(1)).ThenPersisted(new Incremented(1));
    }
}
