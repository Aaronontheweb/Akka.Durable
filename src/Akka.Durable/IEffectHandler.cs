namespace Akka.Durable;

/// <summary>
/// Runs one kind of effect issued with <c>Execute</c> and returns the entity's outcome for it.
/// </summary>
/// <typeparam name="TEffect">The effect record the decider issues. Plain data, owned by the domain.</typeparam>
/// <typeparam name="TOutcome">
/// The entity's command type, when <c>Decide</c> should decide what the result means (a decline, a success,
/// a retry), or the entity's event type, when the result is a fact to persist in every state.
/// </typeparam>
/// <remarks>
/// <para>Handlers are the only impure code in an entity. Register them with the service collection, for example
/// <c>services.AddScoped&lt;IEffectHandler&lt;ChargeCard, InvoiceCommand&gt;, ChargeCardHandler&gt;()</c>; each
/// run gets a fresh scope. Exactly one handler may be registered per effect type.</para>
/// <para>Return expected failures as outcomes. A thrown exception or a timeout is logged and dropped: the entity's
/// state still records that the effect was issued, so its <c>Resume</c> or a retry timer can issue it again.</para>
/// </remarks>
public interface IEffectHandler<in TEffect, TOutcome> where TEffect : notnull
{
    Task<TOutcome> HandleAsync(TEffect effect, CancellationToken cancellationToken);
}
