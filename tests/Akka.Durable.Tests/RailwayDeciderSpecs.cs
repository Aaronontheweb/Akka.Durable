using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// The two tracks of a request are two outcome commands. Both halves are pure: the request's decision issues
/// an effect, and the outcome's decision persists or rejects. No handler runs here.
/// </summary>
public sealed class RailwayDeciderSpecs
{
    [Fact]
    public void Request_issues_the_calculation_as_data()
    {
        var command = new Adjust(3);

        DeciderScenario.For(Railway.Definition)
            .When(command)
            .ThenNothingPersisted()
            .ThenExecuted(new Calculate(command, 3));
    }

    [Fact]
    public void Success_outcome_persists_and_replies_with_the_new_count()
    {
        var command = new Adjust(3);

        DeciderScenario.For(Railway.Definition)
            .When(new Calculated(command, 6))
            .ThenPersisted(new Added(6))
            .ThenReplied(command, new Adjusted(6))
            .ThenState(state => Assert.Equal(6, state.Count));
    }

    [Fact]
    public void Business_rejection_persists_nothing()
    {
        var command = new Adjust(3);

        DeciderScenario.For(Railway.Definition)
            .When(new Calculated(command, -1))
            .ThenNothingPersisted()
            .ThenReplied(command, new AdjustmentRejected("negative result"));
    }

    [Fact]
    public void Failure_outcome_replies_with_the_reason_and_persists_nothing()
    {
        var command = new Adjust(3);

        DeciderScenario.For(Railway.Definition)
            .When(new CalculationFailed(command, "failed"))
            .ThenNothingPersisted()
            .ThenReplied(command, new AdjustmentRejected("failed"));
    }

    [Fact]
    public void Events_persisted_with_the_request_are_visible_when_the_outcome_is_decided()
    {
        DeciderScenario.For(Railway.Definition)
            .When(new BeginAdjust(10, 3))
            .ThenPersisted(new Added(10))
            .ThenExecuted(new Calculate(new Adjust(3), 3));

        DeciderScenario.For(Railway.Definition)
            .Given(new Added(10))
            .When(new Calculated(new Adjust(3), 3))
            .ThenPersisted(new Added(3))
            .ThenReplied(new Adjust(3), new Adjusted(13))
            .ThenState(state => Assert.Equal(13, state.Count));
    }

    [Fact]
    public void Outcome_decisions_see_the_decision_time_not_the_request_time()
    {
        var finished = DateTimeOffset.UnixEpoch.AddHours(1);
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) => command switch
            {
                Calculated r => ctx.None().Reply(r.Request, new AdjustmentRejected(ctx.UtcNow.ToString("O"))),
                _ => ctx.Ignore(),
            }
        };
        var command = new Adjust(3);

        DeciderScenario.For(definition)
            .At(finished)
            .When(new Calculated(command, 3))
            .ThenReplied(command, new AdjustmentRejected(finished.ToString("O")));
    }
}
