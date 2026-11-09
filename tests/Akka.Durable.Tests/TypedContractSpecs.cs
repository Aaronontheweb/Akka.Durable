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
        var syntax = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Preview));
        return CSharpCompilation.Create("TypedContractProbe", [syntax], References,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
            .GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .ToImmutableArray();
    }

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
    ];

    [Theory]
    [MemberData(nameof(InvalidCalls))]
    public void Invalid_calls_fail_compilation(string body) => Assert.NotEmpty(Compile(body));
}
