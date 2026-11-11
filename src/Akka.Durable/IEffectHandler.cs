namespace Akka.Durable;

/// <summary>
/// Runs one kind of effect issued with <c>Execute</c> and returns an answer for <c>Decide</c>.
/// </summary>
/// <typeparam name="TEffect">The effect record the decider issues. Plain data, owned by the domain.</typeparam>
/// <typeparam name="TOutcome">
/// For an entity: the entity's command type. <c>Decide</c> decides what the answer means: a decline, a success.
/// For a workflow: the workflow's fact type.
/// </typeparam>
/// <remarks>
/// <para>Handlers are the only impure code in an entity. Register one with
/// <c>services.AddEffectHandler&lt;ChargeCard, InvoiceCommand, ChargeCardHandler&gt;()</c>. Each attempt gets a fresh
/// DI scope. One handler may be registered for each effect type.</para>
/// <para>Return expected failures as answers: a decline is an answer. A thrown exception, a time-out or an empty
/// answer is a failed attempt. For an entity, <c>Decide</c> then gets the give-up command of the effect
/// (<see cref="IEffect{TCommand}.GaveUp"/>). For a workflow, it gets the definition's give-up fact.</para>
/// </remarks>
public interface IEffectHandler<in TEffect, TOutcome> where TEffect : notnull
{
    /// <summary>Runs the effect once.</summary>
    /// <param name="effect">The effect.</param>
    /// <param name="delivery">The effect's stable id and the number of this attempt.</param>
    /// <param name="cancellationToken">Cancelled when the attempt runs past its time limit, or the host stops.</param>
    Task<TOutcome> HandleAsync(TEffect effect, EffectDelivery delivery, CancellationToken cancellationToken);
}

/// <summary>
/// Runs one kind of effect issued with <c>Execute</c>, with no answer for <c>Decide</c>. The effect is acknowledged
/// when <see cref="HandleAsync"/> returns.
/// </summary>
/// <typeparam name="TEffect">The effect record the decider issues.</typeparam>
/// <remarks>
/// Register it with <c>services.AddEffectHandler&lt;PublishPayment, PublishPaymentHandler&gt;()</c>. A thrown exception
/// or a time-out is a failed attempt, and <c>Decide</c> gets the effect's give-up command.
/// </remarks>
public interface IEffectHandler<in TEffect> where TEffect : notnull
{
    /// <summary>Runs the effect once.</summary>
    /// <param name="effect">The effect.</param>
    /// <param name="delivery">The effect's stable id and the number of this attempt.</param>
    /// <param name="cancellationToken">Cancelled when the attempt runs past its time limit, or the host stops.</param>
    Task HandleAsync(TEffect effect, EffectDelivery delivery, CancellationToken cancellationToken);
}

/// <summary>
/// An effect an entity issues with <c>Execute</c>. It says which command <c>Decide</c> gets when the effect gets no
/// answer.
/// </summary>
/// <typeparam name="TCommand">The command type of the entity that issues the effect.</typeparam>
/// <remarks>
/// <para>The effect is journaled with the decision that issues it, so it needs a serializer, like the entity's events.
/// The host checks this when it starts.</para>
/// <para>The mapping is a method on the effect, not a function at the call site, so it still works after a restart,
/// when the library has only the stored effect.</para>
/// </remarks>
public interface IEffect<out TCommand>
{
    /// <summary>The command <c>Decide</c> gets when this effect gets no answer.</summary>
    /// <param name="gaveUp">Why there is no answer, and how many attempts were made.</param>
    TCommand GaveUp(WorkGaveUp gaveUp);
}

/// <summary>The stable id of one effect.</summary>
/// <param name="Value">
/// The id as text. For an entity: <c>"{entity type}/{entity id}/{sequence}"</c>, for example
/// <c>"invoices/inv-42/1187"</c>. For a workflow's work: <c>"{workflow type}/{workflow id}/{entry}.{position}"</c>, for
/// example <c>"checkouts/c-9/52.0"</c>. Pass it as an idempotency key to a service that accepts one.
/// </param>
/// <param name="Sequence">
/// For an entity: the journal sequence number of the record that issued the effect. It grows with each effect the
/// entity issues. For a workflow: the entry, which grows each time the workflow enters a state. Two pieces of work of
/// one state share it.
/// </param>
public readonly record struct EffectId(string Value, long Sequence)
{
    public override string ToString() => Value;
}

/// <summary>What a handler knows about the effect it runs.</summary>
/// <param name="Id">The effect's id. It is the same on every attempt, and after a restart, a snapshot or a shard move.</param>
/// <param name="Attempt">The number of this attempt, starting at 1.</param>
/// <remarks>
/// A new <c>Execute</c> is a new effect with a new id, even when it carries the same data. To ask again "with the same
/// key", put a key of your domain in the effect.
/// </remarks>
public readonly record struct EffectDelivery(EffectId Id, int Attempt);
