using Akka.Durable.Tests.Fixtures.Campaigns;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>What a state's work is made of: work, timers, and both in one list.</summary>
public sealed class WorkSpecs
{
    private static readonly DateTimeOffset Entered = new(2026, 11, 9, 12, 0, 0, TimeSpan.Zero);

    private static readonly Invoice Input = new("i1", Entered, TimeSpan.FromDays(7), TimeSpan.FromDays(14), 3);

    private static WorkContext<Invoice, CampaignFact> Context(DateTimeOffset enteredAt) =>
        new(new WorkflowId("i1"), Input, enteredAt);

    // Two arms of different kinds and no list arm: the switch has no type of its own and takes the return type.
    private static WorkList<CampaignFact> WorkOf(bool open, WorkContext<Invoice, CampaignFact> ctx) =>
        open switch
        {
            false => ctx.Execute(new SendReminder("i1", 1)),
            true => ctx.Timer(TimeSpan.FromDays(7), new PauseOver()),
        };

    [Fact]
    public void Work_and_a_timer_each_convert_to_a_list_of_one()
    {
        var ctx = Context(Entered);

        Assert.Equal(new WorkList<CampaignFact>([ctx.Execute(new SendReminder("i1", 1))]), WorkOf(false, ctx));
        Assert.Equal(new WorkList<CampaignFact>([ctx.Timer(TimeSpan.FromDays(7), new PauseOver())]), WorkOf(true, ctx));
    }

    [Fact]
    public void A_duration_is_counted_from_when_the_state_was_entered()
    {
        var timer = Context(Entered).Timer(TimeSpan.FromDays(7), new PauseOver());

        Assert.Equal(Entered.AddDays(7), timer.Due);
        Assert.Equal(TimeSpan.FromDays(7), timer.After);
        Assert.Null(timer.At);
    }

    [Fact]
    public void A_date_is_due_at_that_date()
    {
        var timer = Context(Entered).Timer(Entered.AddMonths(3), new DueDateReached());

        Assert.Equal(Entered.AddMonths(3), timer.Due);
        Assert.Equal(Entered.AddMonths(3), timer.At);
        Assert.Null(timer.After);
    }

    [Fact]
    public void Timers_are_equal_when_their_rule_and_fact_are_equal()
    {
        var early = Context(Entered).Timer(TimeSpan.FromDays(7), new PauseOver());
        var late = Context(Entered.AddDays(1)).Timer(TimeSpan.FromDays(7), new PauseOver());

        Assert.Equal(early, late);
        Assert.NotEqual(early, Context(Entered).Timer(TimeSpan.FromDays(8), new PauseOver()));
        Assert.NotEqual(early, Context(Entered).Timer(TimeSpan.FromDays(7), new GraceOver()));
        Assert.NotEqual(early, Context(Entered).Timer(Entered.AddDays(7), new PauseOver()));
    }

    [Fact]
    public void Work_is_never_equal_to_a_timer()
    {
        var ctx = Context(Entered);

        Assert.NotEqual<Work<CampaignFact>>(ctx.Execute(new SendReminder("i1", 1)), ctx.Timer(TimeSpan.FromDays(7), new PauseOver()));
        Assert.Equal<Work<CampaignFact>>(ctx.Execute(new SendReminder("i1", 1)), ctx.Execute(new SendReminder("i1", 1)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void A_timer_needs_a_positive_duration(int seconds)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => Context(Entered).Timer(TimeSpan.FromSeconds(seconds), new PauseOver()));
    }

    [Fact]
    public void All_keeps_the_order_it_was_given()
    {
        var ctx = Context(Entered);
        var work = ctx.Execute(new NotifyCollections("i1"));
        var timer = ctx.Timer(TimeSpan.FromDays(14), new GraceOver());

        Assert.Equal<Work<CampaignFact>>([work, timer], ctx.All(work, timer).Items);
    }
}
