using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Xunit;

namespace Akka.Durable.Tests;

public sealed class RailwayDeciderSpecs
{
    [Fact]
    public void Result_can_be_piped_into_a_pure_continuation_decider()
    {
        var command = new Adjust(3);
        DeciderScenario.For(Railway.Definition)
            .When(command)
            .ThenNothingPersisted()
            .WhenRunSucceeded(6)
            .ThenPersisted(new Added(6))
            .ThenReplied(command, new Adjusted(6))
            .ThenState(state => Assert.Equal(6, state.Count));
    }

    [Fact]
    public void A_failure_branch_cannot_emit_the_success_events()
    {
        var command = new Adjust(3);
        DeciderScenario.For(Railway.Definition)
            .When(command)
            .WhenRunFailed(new InvalidOperationException("failed"))
            .ThenNothingPersisted()
            .ThenReplied(command, new AdjustmentRejected("failed"));
    }

    [Fact]
    public void Continuation_receives_the_state_produced_by_preceding_events()
    {
        var command = new BeginAdjust(10, 3);
        DeciderScenario.For(Railway.Definition)
            .When(command)
            .ThenPersisted(new Added(10))
            .WhenRunSucceeded(3)
            .ThenPersisted(new Added(3))
            .ThenReplied(command, new Adjusted(13))
            .ThenState(state => Assert.Equal(13, state.Count));
    }

    [Fact]
    public void Nested_operations_can_be_decided_without_invoking_services()
    {
        var command = new DoubleAdjust(2);
        DeciderScenario.For(Railway.Definition)
            .When(command)
            .WhenRunSucceeded(4)
            .ThenNothingPersisted()
            .WhenRunSucceeded(8)
            .ThenPersisted(new Added(8))
            .ThenReplied(command, new Adjusted(8));
    }

    [Fact]
    public void Continuation_context_uses_the_completion_time()
    {
        var finished = DateTimeOffset.UnixEpoch.AddHours(1);
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) => command switch
            {
                Adjust c => ctx.For(c).Run<ScopedCalculator, int>(
                    (service, ct) => service.CalculateAsync(c.By, ct),
                    onSuccess: (_, next) => next.Reply(new AdjustmentRejected(next.UtcNow.ToString("O"))),
                    onFailure: (error, next) => next.Reply(new AdjustmentRejected(error.Message))),
                _ => ctx.Ignore(),
            }
        };
        var command = new Adjust(3);

        DeciderScenario.For(definition)
            .At(DateTimeOffset.UnixEpoch)
            .When(command)
            .WhenRunSucceeded(3, finished)
            .ThenReplied(command, new AdjustmentRejected(finished.ToString("O")));
    }

    [Fact]
    public void Null_result_is_a_success_value_for_a_nullable_result_type()
    {
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) => command switch
            {
                Adjust c => ctx.For(c).Run<ScopedCalculator, string?>(
                    (_, _) => Task.FromResult<string?>(null),
                    onSuccess: (result, next) => next.Reply(new AdjustmentRejected(result ?? "null result")),
                    onFailure: (error, next) => next.Reply(new AdjustmentRejected("incorrect failure branch"))),
                _ => ctx.Ignore(),
            }
        };
        var command = new Adjust(3);

        DeciderScenario.For(definition)
            .When(command)
            .WhenRunSucceeded<string?>(null)
            .ThenReplied(command, new AdjustmentRejected("null result"));
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Pure_harness_rejects_captured_ancestors_even_after_a_fresh_intermediate_stage(bool fails)
    {
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) =>
            {
                var captured = ctx.For((Adjust)command.Value).Persist(new Added(10));
                return captured.Run<ScopedCalculator, int>((service, ct) => service.CalculateAsync(1, ct),
                    onSuccess: (_, next) => next.Run<ScopedCalculator, int>(
                        (service, ct) => service.CalculateAsync(2, ct),
                        onSuccess: (_, _) => captured.Reply(new Adjusted(999)),
                        onFailure: (_, _) => captured.Reply(new AdjustmentRejected("incorrect reply"))),
                    onFailure: (error, next) => next.Reply(new AdjustmentRejected(error.Message)));
            }
        };
        var outcome = DeciderScenario.For(definition).When(new Adjust(1)).WhenRunSucceeded(1);

        var error = Assert.Throws<InvalidOperationException>(() =>
        {
            if (fails)
                outcome.WhenRunFailed(new InvalidOperationException("service failed"));
            else
                outcome.WhenRunSucceeded(2);
        });
        Assert.Contains("reused an earlier decision stage", error.Message);
    }

    [Fact]
    public void Stage_tracking_does_not_change_decision_value_equality()
    {
        var first = Decisions.None<RailwayCommand, RailwayEvent>();
        var second = Decisions.None<RailwayCommand, RailwayEvent>();
        Assert.Equal(first, second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
    }

}
