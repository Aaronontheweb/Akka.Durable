using System.Threading.Channels;
using Akka.Actor;
using Akka.Hosting;
using Akka.Durable.Streams;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Streams;
using Akka.Streams.Dsl;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Each public flow and source, hosted through the public API, to show that it is wired to the right call. How the
/// stage behaves (limits, deadlines, cancelling) is shown on the stage itself, in <see cref="DurableStageSpecs"/>.
/// </summary>
public sealed class DurableFlowSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCheckoutHandlers()
            .AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddCounterHandlers();

    protected override void ConfigureMore(AkkaConfigurationBuilder builder) =>
        builder.WithCustomSerializer(CounterSerializer.CreateSerializerDetails).WithDurableEntity(Counter.Definition);

    private IMaterializer Materializer => Sys.Materializer();

    private IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Checkouts() =>
        Host.Services.GetRequiredService<IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>();

    /// <summary>Holds the reserve step open, so that a started checkout stays running.</summary>
    private void HoldReserve() =>
        Services.Reserve = (_, ct) => new TaskCompletionSource<CheckoutFact>().Task.WaitAsync(ct);

    // When the answer's value is itself a union, a switch names that union first, then its cases.
    private static string Say(Answer<StartResult<CheckoutOutcome>> answer) =>
        answer switch
        {
            StartResult<CheckoutOutcome> result => result switch
            {
                Started => "started",
                AlreadyRunning => "already running",
                AlreadyFinished<CheckoutOutcome> => "already finished",
            },
            TimedOut => "timed out",
            Failed failed => failed.Error.Message,
        };

    private static string Say(Answer<DeliveryResult<CheckoutOutcome>> answer) =>
        answer switch
        {
            DeliveryResult<CheckoutOutcome> result => result switch
            {
                Delivered => "delivered",
                Held => "held",
                WorkflowNotStarted => "not started",
                AlreadyFinished<CheckoutOutcome> => "finished",
            },
            TimedOut => "timed out",
            Failed failed => failed.Error.Message,
        };

    [Fact]
    public async Task Ask_pairs_each_input_with_the_reply_of_its_entity()
    {
        await DurableEntityRef<CounterCommand, CounterState, CounterEvent>("a").Ask(new Increment(2), AskCancellation());
        await DurableEntityRef<CounterCommand, CounterState, CounterEvent>("b").Ask(new Increment(5), AskCancellation());

        var replies = await Source.From(new[] { "a", "b" })
            .Via(DurableFlow.Ask(
                EntityRefs<CounterCommand>(),
                (string id) => (new EntityId(id), new GetCount()),
                parallelism: 2,
                Limit))
            .RunWith(Sink.Seq<(string Input, Answer<CountIs> Answer)>(), Materializer);

        Assert.Equal(["a: 2", "b: 5"], replies.Select(r => r.Answer switch
        {
            CountIs counted => $"{r.Input}: {counted.Count}",
            TimedOut => $"{r.Input}: timed out",
            Failed failed => $"{r.Input}: {failed.Error.Message}",
        }));
    }

    [Fact]
    public async Task Start_emits_while_the_workflow_still_runs_and_a_repeated_element_is_already_running()
    {
        HoldReserve();
        var started = await Source.From(new[] { "s1", "s1" })
            .Via(DurableFlow.Start(
                Checkouts(),
                (string id) => (new WorkflowId(id), new PlaceOrder(id, 100)),
                parallelism: 2,
                Limit))
            .RunWith(Sink.Seq<(string Input, Answer<StartResult<CheckoutOutcome>> Answer)>(), Materializer);

        // The reserve step never answers, so the checkout is still running when the stream is done.
        Assert.Equal(["started", "already running"], started.Select(s => Say(s.Answer)));
        Assert.IsType<WorkflowRunning<CheckoutState>>((await CheckoutRef("s1").GetStateAsync(AskCancellation())).State.Value);
    }

    [Fact]
    public async Task Notify_delivers_a_fact_to_a_running_workflow()
    {
        HoldReserve();
        await CheckoutRef("n1").Start(new PlaceOrder("n1", 100), AskCancellation());

        var delivered = await Source.From(new[] { "n1", "n2" })
            .Via(DurableFlow.Notify(
                Checkouts(),
                (string id) => (new WorkflowId(id), new OperatorSaidRetry()),
                parallelism: 2,
                Limit))
            .RunWith(Sink.Seq<(string Input, Answer<DeliveryResult<CheckoutOutcome>> Answer)>(), Materializer);

        // "n1" runs, with its work in flight, so the fact is held. "n2" was never started.
        Assert.Equal(["n1: held", "n2: not started"], delivered.Select(d => $"{d.Input}: {Say(d.Answer)}"));
    }

    /// <summary>One wait on the checkout "u1": for it to be running, or for it to be finished.</summary>
    private sealed record Wait(bool ForFinish);

    [Fact]
    public async Task Until_on_a_workflow_emits_when_the_state_shows_up_and_times_out_for_one_that_never_does()
    {
        HoldReserve();
        var workflow = CheckoutRef("u1");
        await workflow.Start(new PlaceOrder("u1", 100), AskCancellation());

        // The first wait never ends, because the reserve step stays open. It is first in the input and last in the output.
        var answers = await Source.From(new[] { new Wait(ForFinish: true), new Wait(ForFinish: false) })
            .Via(DurableFlow.Until(
                Checkouts(),
                (Wait _) => new WorkflowId("u1"),
                (wait, status) => wait.ForFinish
                    ? status is WorkflowFinished<CheckoutOutcome>
                    : status is WorkflowRunning<CheckoutState>,
                parallelism: 2,
                timeout: TimeSpan.FromMilliseconds(500)))
            .RunWith(Sink.Seq<(Wait Input, Answer<StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>> Answer)>(), Materializer);

        Assert.Equal(["running: state", "finished: timed out"], answers.Select(a =>
            $"{(a.Input.ForFinish ? "finished" : "running")}: {a.Answer switch
            {
                StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>> => "state",
                TimedOut => "timed out",
                Failed failed => failed.Error.Message,
            }}"));

        // The workflow carries on. A missed deadline does not stop it.
        Assert.IsType<WorkflowRunning<CheckoutState>>((await workflow.GetStateAsync(AskCancellation())).State.Value);
    }

    /// <summary>The children of the system guardian. A state watcher is one of them while it lives.</summary>
    private HashSet<ActorPath> SystemActors() =>
        ((ActorRefWithCell)((ExtendedActorSystem)Sys).SystemGuardian).Children.Select(c => c.Path).ToHashSet();

    [Fact]
    public async Task Watch_on_an_entity_gives_each_new_state_and_stopping_the_stream_ends_the_subscription()
    {
        var counter = DurableEntityRef<CounterCommand, CounterState, CounterEvent>("watched");
        await counter.GetStateAsync(AskCancellation());
        var before = SystemActors();

        var snapshots = Channel.CreateUnbounded<StateSnapshot<CounterState>>();
        var (stop, done) = DurableSource.Watch(counter)
            .ViaMaterialized(KillSwitches.Single<StateSnapshot<CounterState>>(), Keep.Right)
            .ToMaterialized(Sink.ForEach<StateSnapshot<CounterState>>(s => snapshots.Writer.TryWrite(s)), Keep.Both)
            .Run(Materializer);

        var current = await snapshots.Reader.ReadAsync(TestCancellation).AsTask().WaitAsync(Limit, TestCancellation);
        Assert.Equal(0, current.State.Count);

        // The watcher is a new actor now. It is the only one.
        Assert.Single(SystemActors().Except(before));

        await counter.Ask(new Increment(1), AskCancellation());
        var later = await snapshots.Reader.ReadAsync(TestCancellation).AsTask().WaitAsync(Limit, TestCancellation);
        Assert.Equal(1, later.State.Count);

        stop.Shutdown();
        await done.WaitAsync(Limit, TestCancellation);

        // The stream was reading when it stopped. The watcher must go away, not wait for the next state.
        await AwaitAssertAsync(
            () => Assert.Empty(SystemActors().Except(before)),
            Limit, cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task Watch_on_a_workflow_completes_after_the_finished_snapshot()
    {
        var workflow = CheckoutRef("watched");

        var watching = DurableSource.Watch(workflow)
            .RunWith(Sink.Seq<StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>>(), Materializer);

        await workflow.Start(new PlaceOrder("watched", 100), AskCancellation());

        var snapshots = await watching.WaitAsync(Limit, TestCancellation);
        Assert.IsType<WorkflowFinished<CheckoutOutcome>>(snapshots[^1].State.Value);
    }
}
