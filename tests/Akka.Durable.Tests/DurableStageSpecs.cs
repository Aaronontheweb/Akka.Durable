using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Durable.Streams;
using Akka.Streams;
using Akka.Streams.Dsl;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// The stage under every flow, on its own: no host, and calls made by hand. A test holds each call open until it
/// ends it, so what the stage does with the calls is the only thing under test.
/// </summary>
public sealed class DurableStageSpecs : IAsyncLifetime
{
    private static readonly TimeSpan MustHappen = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan Plenty = TimeSpan.FromMinutes(5);

    private readonly ActorSystem _system = ActorSystem.Create("durable-stage-specs");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _system.Terminate();

    /// <summary>One call the stage made. The test decides how it ends.</summary>
    private sealed class Call
    {
        private readonly TaskCompletionSource _cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Call(CancellationToken token)
        {
            Token = token;
            token.Register(() => _cancelled.TrySetResult());
        }

        public CancellationToken Token { get; }
        public TaskCompletionSource<int> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task WhenCancelled => _cancelled.Task.WaitAsync(MustHappen, Ct);
    }

    /// <summary>The function the stage calls. It records each call and gives the test its handle.</summary>
    private sealed class Calls
    {
        private readonly ConcurrentDictionary<int, TaskCompletionSource<Call>> _begun = new();
        private readonly ConcurrentQueue<string> _log = new();

        /// <summary>What happened, in order: <c>begin 3</c> when the stage made a call, <c>end 3</c> when the test ended it.</summary>
        public string[] Log => _log.ToArray();

        public Task<int> Begin(int input, CancellationToken token)
        {
            var call = new Call(token);
            _log.Enqueue($"begin {input}");
            Slot(input).TrySetResult(call);
            return call.Result.Task;
        }

        public async Task<Call> BeganAsync(int input) => await Slot(input).Task.WaitAsync(MustHappen, Ct);

        /// <summary>Waits for the call to begin, then ends it with a value. The log gets the line first.</summary>
        public async Task EndAsync(int input, int value)
        {
            var call = await BeganAsync(input);
            _log.Enqueue($"end {input}");
            call.Result.SetResult(value);
        }

        private TaskCompletionSource<Call> Slot(int input) =>
            _begun.GetOrAdd(input, _ => new TaskCompletionSource<Call>(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    private Task<IImmutableList<(int Input, Answer<int> Answer)>> Run(
        Source<int, NotUsed> source, Func<int, CancellationToken, Task<int>> call, int parallelism, TimeSpan timeout, bool ordered) =>
        source
            .Via(new DurableStage<int, int>(call, parallelism, timeout, ordered))
            .RunWith(Sink.Seq<(int Input, Answer<int> Answer)>(), _system.Materializer());

    /// <summary>Runs the stage and lets the test read each answer as it comes out, so it can wait for one before it ends the next call.</summary>
    private (Task Done, Func<Task<string>> Next) RunReading(
        Source<int, NotUsed> source, Func<int, CancellationToken, Task<int>> call, int parallelism, bool ordered)
    {
        var emitted = Channel.CreateUnbounded<string>();
        var done = source
            .Via(new DurableStage<int, int>(call, parallelism, Plenty, ordered))
            .RunWith(Sink.ForEach<(int Input, Answer<int> Answer)>(r => emitted.Writer.TryWrite(Say(r))), _system.Materializer())
            .ContinueWith(_ => emitted.Writer.TryComplete(), TaskScheduler.Default);
        return (done, async () => await emitted.Reader.ReadAsync(Ct).AsTask().WaitAsync(MustHappen, Ct));
    }

    private static string Say((int Input, Answer<int> Answer) result) =>
        result.Answer switch
        {
            int value => $"{result.Input}={value}",
            TimedOut => $"{result.Input} timed out",
            Failed failed => $"{result.Input} failed: {failed.Error.Message}",
        };

    [Fact]
    public async Task Holds_at_most_parallelism_elements_and_asks_for_the_next_only_when_a_slot_is_free()
    {
        var calls = new Calls();
        var done = Run(Source.From(Enumerable.Range(1, 5)), calls.Begin, parallelism: 2, Plenty, ordered: true);

        await calls.BeganAsync(1);
        await calls.BeganAsync(2);
        await calls.EndAsync(1, 10);
        await calls.BeganAsync(3);

        // The third call began after the first one ended. Nothing began ahead of a free slot.
        Assert.Equal(["begin 1", "begin 2", "end 1", "begin 3"], calls.Log);

        foreach (var input in new[] { 2, 3, 4, 5 })
            await calls.EndAsync(input, input * 10);

        var results = await done.WaitAsync(MustHappen, Ct);
        Assert.Equal(["1=10", "2=20", "3=30", "4=40", "5=50"], results.Select(Say));
    }

    [Fact]
    public async Task Ordered_output_waits_for_the_earlier_element_and_counts_the_finished_one_as_held()
    {
        var calls = new Calls();
        var done = Run(Source.From(Enumerable.Range(1, 3)), calls.Begin, parallelism: 2, Plenty, ordered: true);

        // The second call ends first. Its answer waits for the first, and its slot stays taken.
        await calls.EndAsync(2, 20);
        await calls.EndAsync(1, 10);
        await calls.EndAsync(3, 30);

        Assert.Equal(["begin 1", "begin 2", "end 2", "end 1", "begin 3", "end 3"], calls.Log);

        var results = await done.WaitAsync(MustHappen, Ct);
        Assert.Equal(["1=10", "2=20", "3=30"], results.Select(Say));
    }

    [Fact]
    public async Task Unordered_output_emits_each_answer_as_its_call_ends()
    {
        var calls = new Calls();
        var (done, next) = RunReading(Source.From(Enumerable.Range(1, 2)), calls.Begin, parallelism: 2, ordered: false);

        // The second call ends first, and it is read first. The first is still open.
        await calls.EndAsync(2, 20);
        Assert.Equal("2=20", await next());

        await calls.EndAsync(1, 10);
        Assert.Equal("1=10", await next());
        await done.WaitAsync(MustHappen, Ct);
    }

    [Fact]
    public async Task A_passed_deadline_is_an_answer_even_if_the_call_ignores_its_token_and_the_next_element_flows()
    {
        var calls = new Calls();
        var done = Run(Source.From(Enumerable.Range(1, 2)), calls.Begin, parallelism: 1, TimeSpan.FromMilliseconds(200), ordered: true);

        // Call 1 never ends. The stage answers for it at the deadline, which frees the slot for element 2.
        var stuck = await calls.BeganAsync(1);
        await calls.EndAsync(2, 20);

        var results = await done.WaitAsync(MustHappen, Ct);
        Assert.Equal(["1 timed out", "2=20"], results.Select(Say));
        Assert.True(stuck.Token.IsCancellationRequested);
    }

    [Fact]
    public async Task An_exception_is_an_answer_for_that_element_and_Akkas_ask_timeout_is_a_timeout()
    {
        Task<int> Call(int input, CancellationToken token) => input switch
        {
            1 => throw new InvalidOperationException("thrown at once"),
            2 => Task.FromException<int>(new InvalidOperationException("faulted later")),
            3 => Task.FromException<int>(new AskTimeoutException("no reply")),
            _ => Task.FromResult(input * 10),
        };

        var results = await Run(Source.From(Enumerable.Range(1, 4)), Call, parallelism: 2, Plenty, ordered: true)
            .WaitAsync(MustHappen, Ct);

        Assert.Equal(["1 failed: thrown at once", "2 failed: faulted later", "3 timed out", "4=40"], results.Select(Say));
    }

    private static int Break(int _) => throw new InvalidOperationException("upstream broke");

    [Theory]
    [InlineData("downstream cancels")]
    [InlineData("upstream fails")]
    public async Task Stopping_for_any_reason_cancels_the_calls_in_flight(string reason)
    {
        var calls = new Calls();
        var source = reason == "downstream cancels"
            ? Source.From(Enumerable.Range(1, 3))
            : Source.From(Enumerable.Range(1, 3)).Concat(Source.Single(4).Select(Break));

        // Downstream takes the first answer and cancels. In the other case, upstream fails once the stage asks for a fourth element.
        var done = source
            .Via(new DurableStage<int, int>(calls.Begin, parallelism: 4, Plenty, ordered: true))
            .Take(reason == "downstream cancels" ? 1 : 10)
            .RunWith(Sink.Seq<(int Input, Answer<int> Answer)>(), _system.Materializer());

        var second = await calls.BeganAsync(2);
        var third = await calls.BeganAsync(3);
        if (reason == "downstream cancels")
            await calls.EndAsync(1, 10);

        await second.WhenCancelled;
        await third.WhenCancelled;

        if (reason == "upstream fails")
            await Assert.ThrowsAsync<InvalidOperationException>(() => done.WaitAsync(MustHappen, Ct));
        else
            Assert.Equal(["1=10"], (await done.WaitAsync(MustHappen, Ct)).Select(Say));
    }

    [Fact]
    public async Task When_upstream_finishes_with_calls_in_flight_it_emits_them_and_then_completes()
    {
        var calls = new Calls();
        var (done, next) = RunReading(Source.From(Enumerable.Range(1, 2)), calls.Begin, parallelism: 4, ordered: false);

        // Both elements are taken, and upstream has finished, before either call ends.
        await calls.BeganAsync(1);
        await calls.BeganAsync(2);

        await calls.EndAsync(1, 10);
        Assert.Equal("1=10", await next());
        await calls.EndAsync(2, 20);
        Assert.Equal("2=20", await next());

        await done.WaitAsync(MustHappen, Ct);
    }
}
