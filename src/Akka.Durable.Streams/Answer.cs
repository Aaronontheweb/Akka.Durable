namespace Akka.Durable.Streams;

/// <summary>
/// What a stream stage got for one element: the value, or <see cref="TimedOut"/>, or <see cref="Failed"/>.
/// </summary>
public readonly union Answer<T>(T, TimedOut, Failed);

/// <summary>
/// The deadline passed before the answer came. The result is not known: the entity or workflow may still have
/// handled the element.
/// </summary>
public sealed record TimedOut;

/// <summary>
/// The call for one element threw. The stream carries on with the next element.
/// </summary>
public sealed record Failed(Exception Error);
