using Akka.Durable.Streams;
using Akka.Durable.Tests.Fixtures.Echoes;
using Akka.Durable.Tests.Fixtures.Slows;
using Akka.Streams.Dsl;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The stream DSL over a durable workflow, hosted through the public API. The echo workflow is the fixture: it
/// runs one effect and ends with the text it heard, so a spec can watch a workflow run from a stream.
/// </summary>
public abstract class WorkflowFlowSpecs(ITestOutputHelper output) : StreamKit(output)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static Shout Say(string text) => new(text);

    [Fact]
    public async Task Run_starts_a_workflow_per_element_and_emits_its_outcome_in_order()
    {
        var batch = new[] { Say("one"), Say("two"), Say("three") };

        var ended = await Source.From(batch)
            .Via(WorkflowFlow.Run<Shout, Listening, EchoFact, Heard>(
                Echoes(), input => new WorkflowId(input.Text), parallelism: 2, Timeout))
            .RunWith(Sink.Seq<(WorkflowId Id, WorkflowEnded<Heard>)>(), Materializer);

        Assert.Equal(3, ended.Count);
        Assert.Equal(["one", "two", "three"], ended.Select(e => e.Id.Value));

        foreach (var (id, outcome) in ended)
        {
            var finished = Assert.IsType<WorkflowFinished<Heard>>(outcome.Value);
            Assert.Equal(id.Value, finished.Outcome.Text);
        }
    }

    [Fact]
    public async Task Run_emits_the_outcome_of_a_workflow_that_was_already_finished()
    {
        var input = Say("again");
        var id = new WorkflowId("again");

        // Run it once, outside the stream.
        var first = EchoRef("again");
        await first.Start(input, AskCancellation());
        await first.WaitForOutcome(AskCancellation());

        // A replayed element reaches the same workflow and reads its outcome.
        var ended = await Source.Single(input)
            .Via(WorkflowFlow.Run<Shout, Listening, EchoFact, Heard>(
                Echoes(), _ => id, parallelism: 1, Timeout))
            .RunWith(Sink.Seq<(WorkflowId Id, WorkflowEnded<Heard>)>(), Materializer);

        var finished = Assert.IsType<WorkflowFinished<Heard>>(Assert.Single(ended).Item2.Value);
        Assert.Equal("again", finished.Outcome.Text);
    }

    [Fact]
    public async Task Start_emits_each_start_and_does_not_wait_for_the_outcome()
    {
        var batch = new[] { Say("a"), Say("b") };

        var started = await Source.From(batch)
            .Via(WorkflowFlow.Start<Shout, Listening, EchoFact, Heard>(
                Echoes(), input => new WorkflowId(input.Text), parallelism: 2))
            .RunWith(Sink.Seq<(WorkflowId Id, StartResult<Heard>)>(), Materializer);

        Assert.Equal(2, started.Count);
        Assert.All(started, s => Assert.IsType<Started>(s.Item2.Value));

        // The stream did not wait for the outcomes: each workflow has one, and the stream never read it.
        foreach (var (id, _) in started)
            Assert.IsType<WorkflowFinished<Heard>>((await EchoRef(id.Value).GetStateAsync(AskCancellation())).State.Value);
    }

    [Fact]
    public async Task Start_then_Wait_observes_the_outcomes_of_workflows_started_earlier()
    {
        var batch = new[] { Say("x"), Say("y") };

        var started = await Source.From(batch)
            .Via(WorkflowFlow.Start<Shout, Listening, EchoFact, Heard>(
                Echoes(), input => new WorkflowId(input.Text), parallelism: 2))
            .RunWith(Sink.Seq<(WorkflowId Id, StartResult<Heard>)>(), Materializer);

        var ended = await Source.From(started.Select(s => (s.Id, Say(s.Id.Value))))
            .Via(WorkflowFlow.Wait<Shout, Listening, EchoFact, Heard>(Echoes(), breadth: 2, Timeout))
            .RunWith(Sink.Seq<(WorkflowId Id, WorkflowEnded<Heard>)>(), Materializer);

        Assert.Equal(2, ended.Count);
        Assert.All(ended, e => Assert.IsType<WorkflowFinished<Heard>>(e.Item2.Value));
    }

    [Fact]
    public async Task Wait_emits_WorkflowTimedOut_for_a_workflow_that_does_not_end_in_time()
    {
        // The slow workflow's work waits on the gate, which this spec never opens, so it stays in flight.
        var id = new WorkflowId("stuck");
        await Slows().Get(id).Start(new SlowInput("stuck"), AskCancellation());

        var ended = await Source.Single((id, new SlowInput("stuck")))
            .Via(WorkflowFlow.Wait<SlowInput, Waiting, SlowFact, SlowOutcome>(
                Slows(), breadth: 1, TimeSpan.FromMilliseconds(300)))
            .RunWith(Sink.Seq<(WorkflowId Id, WorkflowEnded<SlowOutcome>)>(), Materializer);

        Assert.IsType<WorkflowTimedOut>(Assert.Single(ended).Item2.Value);

        // The workflow carries on: it is durable and still running.
        Assert.IsType<WorkflowRunning<Waiting>>((await Slows().Get(id).GetStateAsync(AskCancellation())).State.Value);
    }

    [Fact]
    public async Task Wait_emits_WorkflowNotStarted_for_a_workflow_that_was_never_started()
    {
        var ended = await Source.Single((new WorkflowId("never"), Say("never")))
            .Via(WorkflowFlow.Wait<Shout, Listening, EchoFact, Heard>(
                Echoes(), breadth: 1, Timeout))
            .RunWith(Sink.Seq<(WorkflowId Id, WorkflowEnded<Heard>)>(), Materializer);

        Assert.IsType<WorkflowNotStarted>(Assert.Single(ended).Item2.Value);
    }

    [Fact]
    public async Task Watch_emits_the_condition_of_a_workflow_and_completes_when_it_finishes()
    {
        var input = Say("watched");
        var workflow = EchoRef("watched");

        var watching = WorkflowSource.Watch<Shout, Listening, EchoFact, Heard>(workflow)
            .RunWith(Sink.Seq<StateSnapshot<WorkflowStatus<Listening, Heard>>>(), Materializer);

        await workflow.Start(input, AskCancellation());

        var snapshots = await watching.WaitAsync(Timeout, TestCancellation);

        Assert.NotEmpty(snapshots);
        Assert.IsType<WorkflowFinished<Heard>>(snapshots[^1].State.Value);
    }
}

/// <summary>The stream DSL over a workflow hosted in a shard region.</summary>
public sealed class ShardedWorkflowFlowSpecs(ITestOutputHelper output) : WorkflowFlowSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>The stream DSL over a workflow hosted locally.</summary>
public sealed class LocalWorkflowFlowSpecs(ITestOutputHelper output) : WorkflowFlowSpecs(output)
{
    protected override bool Sharded => false;
}
