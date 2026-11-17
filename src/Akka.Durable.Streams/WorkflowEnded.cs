namespace Akka.Durable.Streams;

/// <summary>
/// How a workflow ended, as a stream element.
/// </summary>
/// <remarks>
/// A workflow that gives up produces its outcome through the definition's <c>GaveUp</c> function, so a failure is
/// a domain outcome and not a separate case here. The two cases that are not outcomes are the stream's own:
/// <see cref="WorkflowTimedOut"/> when the stream stopped waiting, and <see cref="WorkflowNotStarted"/> when
/// nothing was started.
/// </remarks>
public readonly union WorkflowEnded<TOutcome>(
    WorkflowFinished<TOutcome>, WorkflowTimedOut, WorkflowNotStarted);

/// <summary>
/// The stream stopped waiting for the workflow. The workflow itself carries on: it is durable, and its outcome
/// is still in its journal.
/// </summary>
public sealed record WorkflowTimedOut;
