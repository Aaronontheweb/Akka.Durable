namespace Akka.Durable;

/// <summary>
/// Runs one kind of effect issued with <c>Execute</c> and returns the entity's outcome for it.
/// </summary>
/// <typeparam name="TEffect">The effect record the decider issues. Plain data, owned by the domain.</typeparam>
/// <typeparam name="TOutcome">
/// The entity's command type. <c>Decide</c> decides what the result means: a decline, a success, a retry.
/// For a workflow, the workflow's fact type.
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
