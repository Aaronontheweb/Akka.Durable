using System.Collections.Immutable;
using Akka.Durable.Tests.Fixtures;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>Compile real callers to prove invalid calls cannot escape through overload resolution.</summary>
public sealed class TypedContractSpecs
{
    private static readonly ImmutableArray<MetadataReference> References =
        ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator)
        .Append(typeof(IEntityRef<>).Assembly.Location)
        .Append(typeof(Counter).Assembly.Location)
        .Distinct(StringComparer.Ordinal)
        .Select(path => MetadataReference.CreateFromFile(path))
        .ToImmutableArray<MetadataReference>();

    private static ImmutableArray<Diagnostic> Compile(string body)
    {
        var source = """
            using System;
            using System.Threading;
            using System.Threading.Tasks;
            using Akka.Actor;
            using Akka.Durable;
            using Akka.Durable.Tests.Fixtures;
            public static class Caller
            {
                public static void Call(IEntityRef<CounterCommand> entity,
                    DecisionContext<CounterState, CounterCommand, CounterEvent> ctx)
                {
            """ + body + """
                }
            }
            """;
        return Diagnostics(source).Where(d => d.Severity == DiagnosticSeverity.Error).ToImmutableArray();
    }

    private static ImmutableArray<Diagnostic> Diagnostics(string source) =>
        CSharpCompilation.Create("TypedContractProbe",
                [CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Preview))], References,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
            .GetDiagnostics();

    [Fact]
    public void Command_contract_infers_Ask_and_bound_reply_types()
    {
        var errors = Compile("""
            Task<Accepted> accepted = entity.Ask(new Increment(1));
            Task<CountIs> count = entity.Ask(new GetCount());
            Decision<CounterCommand, CounterEvent> replied = ctx.Persist(new Incremented(1)).Reply(new Increment(1), new Accepted());
            Handled<CounterCommand, CounterEvent> executed = ctx.None()
                .Execute(new Compute(new IncrementViaService(1, false)))
                .Reply(new Increment(1), new Accepted());
            Handled<CounterCommand, CounterEvent> sent = ctx.Persist(new Incremented(1))
                .Send<CounterCommand>(new EntityId("b"), new Increment(1))
                .Schedule("tick", TimeSpan.FromSeconds(1), new Increment(1));
            Handled<CounterCommand, CounterEvent> ran = ctx.None()
                .Run<Gate>((gate, ct) => gate.WaitAsync(ct))
                .Reply(new GetCount(), new CountIs(0))
                .Stop();
            Decision<CounterCommand, CounterEvent> explicitConversion = ctx.None().ToDecision();
            """);

        Assert.True(errors.IsEmpty, string.Join(Environment.NewLine, errors));
    }

    public static TheoryData<string> InvalidCalls =>
    [
        "entity.Ask(new Noop());",
        "entity.Ask<CountIs>(new Increment(1));",
        "entity.Ask(new Adjust(1));",
        "ICanTell untyped = entity;",
        "ctx.None().Reply(new Increment(1), new CountIs(0));",
        "ctx.None().Reply(new Adjust(1), new CountIs(0));",
        "ctx.None().Reply(new Accepted());",
        "ctx.None().Reply(new Noop(), new Accepted());",
        "ctx.None().Reply(new Adjust(1), new Accepted());",
        "ctx.None().Schedule(\"tick\", TimeSpan.FromSeconds(1), new Adjust(1));",
        "ctx.None().Send<CounterCommand>(new EntityId(\"b\"), new Adjust(1));",
        "ctx.None().Run<Gate>((gate, ct) => gate.WaitAsync(ct), onSuccess: next => next);",
        "ctx.Persist(new Added(1));",
        // A give-up becomes one of the entity's own commands: not another entity's, and not any other value.
        "var replaced = Counter.Definition with { GaveUp = gaveUp => new Adjust(1) };",
        "var replaced = Counter.Definition with { GaveUp = gaveUp => gaveUp.Reason };",
        "var defined = new EntityDefinition<CounterState, CounterCommand, CounterEvent>(Counter.TypeName, " +
        "_ => new CounterState(0, false), Counter.Decide, Counter.Evolve, GaveUp: gaveUp => new Adjust(1));",
    ];

    [Theory]
    [MemberData(nameof(InvalidCalls))]
    public void Invalid_calls_fail_compilation(string body) => Assert.NotEmpty(Compile(body));

    /// <summary>
    /// An effect is a plain record: it implements nothing and knows nothing of the entity. What its give-up becomes is
    /// the definition's <c>GaveUp</c> argument, a lambda that returns one case of the command union.
    /// </summary>
    [Fact]
    public void Execute_takes_a_record_that_implements_nothing_and_GaveUp_takes_a_lambda_that_returns_a_union_case()
    {
        var errors = Diagnostics("""
            using Akka.Durable;
            using Akka.Durable.Tests.Fixtures;

            public sealed record PlainEffect(string Name);

            public static class Author
            {
                public static Handled<CounterCommand, CounterEvent> Issue(
                    DecisionContext<CounterState, CounterCommand, CounterEvent> ctx) =>
                    ctx.None().Execute(new PlainEffect("a"));

                public static Decision<CounterCommand, CounterEvent> IssueWithAnEvent(
                    DecisionContext<CounterState, CounterCommand, CounterEvent> ctx) =>
                    ctx.Persist(new Incremented(1)).Execute(new PlainEffect("a")).Execute(new PlainEffect("b"));

                public static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> Defined = new(
                    Counter.TypeName, _ => new CounterState(0, false), Counter.Decide, Counter.Evolve,
                    GaveUp: gaveUp => new EffectGaveUpNoted($"{gaveUp.What}:{gaveUp.Reason}"));

                public static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> Replaced =
                    Counter.Definition with { GaveUp = gaveUp => new Noop() };

                public static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> ByEffect =
                    Counter.Definition with
                    {
                        GaveUp = gaveUp => gaveUp.Work switch
                        {
                            PlainEffect plain => new EffectGaveUpNoted(plain.Name),
                            _ => new Noop(),
                        },
                    };

                public static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> Without =
                    Counter.Definition with { GaveUp = null };
            }
            """).Where(d => d.Severity == DiagnosticSeverity.Error).ToImmutableArray();

        Assert.True(errors.IsEmpty, string.Join(Environment.NewLine, errors));
    }

    public static TheoryData<string, bool> StatusReaders => new()
    {
        { "Reserving => 0, Charging => 1, Undoing => 2, NeedsAttention => 3,", false },
        { "Reserving => 0, Charging => 1, Undoing => 2,", true },
    };

    /// <summary>
    /// A status reader with no catch-all arm: the compiler checks it against every case of the state union, and
    /// reports CS8509 (an error under TreatWarningsAsErrors) when a case has no arm.
    /// </summary>
    [Theory]
    [MemberData(nameof(StatusReaders))]
    public void A_status_reader_without_a_catch_all_arm_is_checked_against_every_state(string arms, bool reported)
    {
        var diagnostics = Diagnostics($$"""
            using Akka.Durable;
            using Akka.Durable.Tests.Fixtures.Checkouts;
            public static class Reader
            {
                public static int Read(WorkflowStatus<CheckoutState, CheckoutOutcome> status) => status switch
                {
                    WorkflowNotStarted => -1,
                    WorkflowRunning<CheckoutState> running => running.State switch { {{arms}} },
                    WorkflowFinished<CheckoutOutcome> => -2,
                };
            }
            """);

        Assert.Empty(diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.Equal(reported, diagnostics.Any(d => d.Id == "CS8509"));
    }
}
