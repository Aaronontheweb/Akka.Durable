using Akka.Durable.Streams;
using Akka.Durable.Tests.Fixtures;
using Akka.Streams.Dsl;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The stream DSL over a durable entity, hosted through the public API. The counter entity is the fixture: it
/// counts, and it can be told to wait for a gate before it answers.
/// </summary>
public abstract class EntityFlowSpecs(ITestOutputHelper output) : StreamKit(output)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Ask_sends_a_command_per_element_and_emits_the_replies_in_order()
    {
        var batch = new[] { 1, 2, 3 };

        var replies = await Source.From(batch)
            .Via(EntityFlow.Ask<int, CounterCommand, CountIs>(
                EntityRefs<CounterCommand>(),
                by => (new EntityId("counted"), new GetCount()),
                parallelism: 2))
            .RunWith(Sink.Seq<CountIs>(), Materializer);

        Assert.Equal(3, replies.Count);
        Assert.All(replies, r => Assert.Equal(0, r.Count));
    }

    [Fact]
    public async Task Ask_keeps_the_input_order_when_a_later_element_answers_first()
    {
        // The first command waits on the gate, so the second answers first. Output must still be in input order.
        var batch = new[] { "slow", "fast" };

        var asking = Source.From(batch)
            .Via(EntityFlow.Ask<string, CounterCommand, Accepted>(
                EntityRefs<CounterCommand>(),
                which => which == "slow"
                    ? (new EntityId(which), (IReplies<CounterCommand, Accepted>)new IncrementViaService(1, Wait: true))
                    : (new EntityId(which), new Increment(1)),
                parallelism: 2))
            .RunWith(Sink.Seq<Accepted>(), Materializer);

        // Let the fast one finish first, then release the slow one.
        await Task.Delay(TimeSpan.FromMilliseconds(200), TestCancellation);
        Host.Services.GetRequiredService<Gate>().Open();

        var replies = await asking.WaitAsync(Timeout, TestCancellation);
        Assert.Equal(2, replies.Count);
    }

    [Fact]
    public async Task Tell_sends_every_element_and_emits_nothing()
    {
        var batch = new[] { 1, 2, 3 };

        await Source.From(batch)
            .RunWith(EntityFlow.Tell<int, CounterCommand>(
                EntityRefs<CounterCommand>(),
                by => (new EntityId("told"), new Increment(by))), Materializer);

        var count = await CounterRef("told").Ask(new GetCount(), AskCancellation());
        Assert.Equal(6, count.Count);
    }

    [Fact]
    public async Task WaitForState_emits_the_snapshot_that_satisfies_the_condition()
    {
        var counter = CounterRef("waited");
        await counter.Ask(new Increment(1), AskCancellation());

        var snapshots = await Source.Single("waited")
            .Via(EntityFlow.WaitForState<string, CounterCommand, CounterState, CounterEvent>(
                DurableEntityRefs<CounterCommand, CounterState, CounterEvent>(),
                id => new EntityId(id),
                (_, state) => state.Count >= 1,
                parallelism: 1,
                Timeout))
            .RunWith(Sink.Seq<StateSnapshot<CounterState>>(), Materializer);

        Assert.Equal(1, Assert.Single(snapshots).State.Count);
    }

    [Fact]
    public async Task Watch_emits_the_current_state_then_later_ones_without_going_back()
    {
        var counter = CounterRef("watched");

        // The subscription starts when the stream materializes, so wait for the first snapshot before writing.
        var firstSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var watching = EntitySource.Watch(counter)
            .Select(snapshot =>
            {
                firstSeen.TrySetResult();
                return snapshot;
            })
            .Take(2)
            .RunWith(Sink.Seq<StateSnapshot<CounterState>>(), Materializer);

        await firstSeen.Task.WaitAsync(Timeout, TestCancellation);

        await counter.Ask(new Increment(1), AskCancellation());
        await counter.Ask(new Increment(1), AskCancellation());

        var snapshots = await watching.WaitAsync(Timeout, TestCancellation);

        // The subscription conflates: a slow reader gets the newest snapshot it has not read, so it can skip a
        // state. What it guarantees is the current state first, then later ones, never an older one after a newer.
        Assert.Equal(2, snapshots.Count);
        Assert.Equal(0, snapshots[0].State.Count);
        Assert.True(snapshots[1].State.Count >= 1);
        Assert.True(snapshots[1].SequenceNr > snapshots[0].SequenceNr);
    }
}

/// <summary>The stream DSL over an entity hosted in a shard region.</summary>
public sealed class ShardedEntityFlowSpecs(ITestOutputHelper output) : EntityFlowSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>The stream DSL over an entity hosted locally.</summary>
public sealed class LocalEntityFlowSpecs(ITestOutputHelper output) : EntityFlowSpecs(output)
{
    protected override bool Sharded => false;
}
