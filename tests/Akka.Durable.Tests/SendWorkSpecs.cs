using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Bumps;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>A send to an entity from Work is plain data: a test asserts on it without an actor system.</summary>
public sealed class SendWorkSpecs
{
    private static readonly WorkContext<BumpCounter, BumpFact> Context =
        new(new WorkflowId("b1"), new BumpCounter("c1", 2), new DateTimeOffset(2026, 11, 9, 12, 0, 0, TimeSpan.Zero));

    [Fact]
    public void The_work_of_a_state_is_the_send_it_needs()
    {
        var work = Bump.Work(new Bumping(), Context);

        var send = Assert.IsType<ExecuteWork<BumpFact>>(Assert.Single(work.Items));
        Assert.Equal(new SendEffect(typeof(CounterCommand), new EntityId("c1"), new Increment(2)), send.Effect);
        Assert.Equal(new WorkList<BumpFact>([Context.Send(new EntityId("c1"), new Increment(2), _ => new CounterBumped())]), work);
        Assert.Equal(
            new WorkList<BumpFact>([Context.Send(new EntityId("c1"), new GetCount(), reply => new CountRead(reply.Count))]),
            Bump.Work(new Reading(), Context));
    }

    [Fact]
    public void Sends_are_equal_when_their_target_and_command_are_equal()
    {
        var send = Context.Send(new EntityId("c1"), new Increment(2), _ => new CounterBumped());

        // The function that turns the reply into a fact is not part of the data.
        Assert.Equal(send, Context.Send(new EntityId("c1"), new Increment(2), _ => new CountRead(0)));
        Assert.NotEqual(send, Context.Send(new EntityId("c2"), new Increment(2), _ => new CounterBumped()));
        Assert.NotEqual(send, Context.Send(new EntityId("c1"), new Increment(3), _ => new CounterBumped()));
    }

    [Fact]
    public void A_give_up_names_the_command_that_was_sent()
    {
        var send = Context.Send(new EntityId("c1"), new Increment(2), _ => new CounterBumped());

        Assert.Equal(nameof(Increment), new WorkGaveUp(send.Effect, GaveUpReason.TimedOut).What);
    }
}
