using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Akka.Durable.Streams;

/// <summary>
/// The one stage every flow is built on. It calls a function for each element, keeps at most
/// <c>parallelism</c> elements, gives each call a deadline, and cancels the calls still open when it stops.
/// </summary>
/// <remarks>
/// <para>
/// <c>SelectAsync</c> cannot do this: it has no way to cancel a call that is in flight, so a wait that nobody
/// reads any more would stay alive until its own limit.
/// </para>
/// <para>
/// All state is touched on the stage's own thread. A call ends on another thread and comes back through an
/// async callback.
/// </para>
/// </remarks>
internal sealed class DurableStage<TIn, TOut> : GraphStage<FlowShape<TIn, (TIn Input, Answer<TOut> Answer)>>
{
    private readonly Func<TIn, CancellationToken, Task<TOut>> _call;
    private readonly int _parallelism;
    private readonly TimeSpan _timeout;
    private readonly bool _ordered;

    private readonly Inlet<TIn> _in = new("DurableStage.in");
    private readonly Outlet<(TIn Input, Answer<TOut> Answer)> _out = new("DurableStage.out");

    public DurableStage(Func<TIn, CancellationToken, Task<TOut>> call, int parallelism, TimeSpan timeout, bool ordered)
    {
        ArgumentNullException.ThrowIfNull(call);
        ArgumentOutOfRangeException.ThrowIfLessThan(parallelism, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        // CancelAfter takes at most this many milliseconds.
        ArgumentOutOfRangeException.ThrowIfGreaterThan(timeout, TimeSpan.FromMilliseconds(uint.MaxValue - 1));

        _call = call;
        _parallelism = parallelism;
        _timeout = timeout;
        _ordered = ordered;
        Shape = new FlowShape<TIn, (TIn Input, Answer<TOut> Answer)>(_in, _out);
    }

    public override FlowShape<TIn, (TIn Input, Answer<TOut> Answer)> Shape { get; }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

    /// <summary>One element, from the moment it is taken until it is emitted.</summary>
    private sealed class Slot(TIn input)
    {
        public TIn Input { get; } = input;
        public Answer<TOut>? Result { get; set; }
    }

    private sealed class Logic : GraphStageLogic
    {
        private readonly DurableStage<TIn, TOut> _stage;
        private readonly CancellationTokenSource _stopping = new();
        private readonly Action<(Slot Slot, Answer<TOut> Answer)> _onAnswered;

        // Elements that are taken and not yet emitted, finished ones too. The parallelism limit counts this.
        private int _held;

        // Ordered output: every held slot, in input order. Unordered output: the finished slots, in the order they finished.
        private readonly Queue<Slot> _ready = new();

        private volatile bool _stopped;

        public Logic(DurableStage<TIn, TOut> stage) : base(stage.Shape)
        {
            _stage = stage;
            _onAnswered = GetAsyncCallback<(Slot Slot, Answer<TOut> Answer)>(Answered);

            SetHandler(stage._in,
                OnPush,
                () =>
                {
                    // Held elements are emitted first. The stage completes from Emit when the last one is out.
                    if (_held == 0)
                        CompleteStage();
                },
                FailStage);

            SetHandler(stage._out,
                Emit,
                _ => CompleteStage());
        }

        public override void PreStart() => Pull(_stage._in);

        public override void PostStop()
        {
            // Set before the cancel: every call that ends because of it must see that the stage is stopping.
            _stopped = true;
            try
            {
                _stopping.Cancel();
            }
            catch (AggregateException)
            {
                // A callback of the caller's own threw while it was told to cancel. The stage stops anyway.
            }

            _stopping.Dispose();
        }

        private void OnPush()
        {
            var slot = new Slot(Grab(_stage._in));
            _held++;
            if (_stage._ordered)
                _ready.Enqueue(slot);

            // Not awaited. It runs up to its first await right here, on the stage's thread.
            _ = Run(slot);
            Emit();
        }

        private async Task Run(Slot slot)
        {
            Answer<TOut> answer;

            // One token source for each element, linked to the stage's. The deadline and the stage's own stop are
            // two causes of one cancellation. The stage tells them apart by its flag.
            using var limit = CancellationTokenSource.CreateLinkedTokenSource(_stopping.Token);
            try
            {
                limit.CancelAfter(_stage._timeout);

                // The call begins here, before the first await, so calls begin in the input's order. A function that
                // throws at once is caught below. WaitAsync ends at the deadline even when the call ignores its token.
                answer = await _stage._call(slot.Input, limit.Token).WaitAsync(limit.Token).ConfigureAwait(false);
            }
            catch (AskTimeoutException)
            {
                // Akka's own ask timeout: the answer did not come in time.
                answer = new TimedOut();
            }
            catch (OperationCanceledException) when (limit.IsCancellationRequested)
            {
                if (_stopped)
                    return;
                answer = new TimedOut();
            }
            catch (Exception error)
            {
                answer = new Failed(error);
            }

            // Back onto the stage's thread. A stage that has stopped has nobody to emit to.
            if (!_stopped)
                _onAnswered((slot, answer));
        }

        private void Answered((Slot Slot, Answer<TOut> Answer) done)
        {
            if (_stopped)
                return;

            done.Slot.Result = done.Answer;
            if (!_stage._ordered)
                _ready.Enqueue(done.Slot);
            Emit();
        }

        /// <summary>Emits what is ready, asks upstream for more when a slot is free, and completes when all is out.</summary>
        private void Emit()
        {
            if (IsAvailable(_stage._out) && TakeReady() is { } ready)
            {
                _held--;
                Push(_stage._out, (ready.Input, ready.Result!.Value));
            }

            if (IsClosed(_stage._in))
            {
                if (_held == 0)
                    CompleteStage();
            }
            else if (_held < _stage._parallelism && !HasBeenPulled(_stage._in))
            {
                Pull(_stage._in);
            }
        }

        private Slot? TakeReady()
        {
            // Ordered: the head leaves when it has its answer. Unordered: the queue holds only finished slots.
            if (_ready.TryPeek(out var next) && next.Result is not null)
                return _ready.Dequeue();
            return null;
        }
    }
}
