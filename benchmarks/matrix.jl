using Dagger
import Dagger: reduceblock
using DistributedArrays

using BenchmarkTools

AmulB_compatible(x,y) =
    try; size(x), size(y), x * y; true; catch err false end

AmulB_compatible(x::Computed,y::Computed) = AmulB_compatible(domain(x.result), domain(y.result))

function AcmulB_compatible(x,y)
    AmulB_compatible(domain(x.result)', domain(y.result))
end

function matrix_suite(ctx, X, f=x->x)
    suite = BenchmarkGroup(["matrix"])

    suite["alloc"] = @benchmarkable (compute($(f(X))); gc())

    Y = compute(ctx, f(X))
    suite["X+1.0"]  = @benchmarkable (compute($ctx, $(f(Y+1))); gc())
    suite["X+X"]    = @benchmarkable (compute($ctx, $(f(Y+Y))); gc())
    suite["X'"]     = @benchmarkable (compute($ctx, $(f(Y'))); gc())
    suite["sin(X)"] = @benchmarkable (compute($ctx, $(f(sin(Y)))); gc())

    if domain(Y.result) == domain(Y.result)'
        @show domain(Y.result)
        suite["X+X'"]   = @benchmarkable (compute($ctx, $(f(Y+Y'))); gc())
    end
    if AmulB_compatible(Y, Y)
        suite["X*X"]    = @benchmarkable (compute($ctx, $(f(Y*Y))); gc())
    end
    if AcmulB_compatible(Y, Y)
        suite["X'*X"]   = @benchmarkable (compute($ctx, $(f(Y'*Y))); gc())
    end
    suite
end

function matrix_variants(NW, f=x->x)
    w = workers()[1:NW]
    suite = BenchmarkGroup()

    matrix(ch, sz) = zeros(Chunks(ch), sz)

    suite["trivial_one_each"] = matrix_suite(Context(w), matrix(1, NW), f)
    suite["trivial_two_each"] = matrix_suite(Context(w), matrix(1, 2NW), f)
    suite["trivial_1k_each"] = matrix_suite(Context(w), matrix(1, 1000NW), f)

 #  suite["128M_one_each"] =
 #      matrix_suite(Context(w), matrix((4000,4000), (4000*NW, 4000)), f)

 #  suite["128M_two_each"] =
 #      matrix_suite(Context(w), matrix((4000,4000), (NW*4000, 2*4000)), f)

 #  suite["128M_uneven"] =
 #      matrix_suite(Context(w), matrix((4000,4000), (5000*NW, 5000)), f)
    suite
end

function matrix_suite_nworkers(NW)
    suite = BenchmarkGroup()
    suite["in-memory"] = matrix_variants(2, x->x)
    suite["on-disk"]   = matrix_variants(2, x -> Dagger.save(x, "tmp$(randstring(5))"))
    suite
end

using JLD
function main()
    if length(ARGS) == 0
        error("Usage: julia -p NWorkers benchmark.jl [tune-file] [output-file]")
    end

    nw = nworkers()
    suite = matrix_suite_nworkers(nw)
    if length(ARGS) > 0
        tune_file = ARGS[1]
        if isfile(tune_file)
            println("Using benchmark tuning data in $tune_file")
            loadparams!(suite, JLD.load(tune_file, "suite"), :evals, :samples)
        else
            println("Creating benchmark tuning file $tune_file")
            tune!(suite)
            JLD.save(tune_file, "suite", params(suite))
        end
    else
        println("Tuning benchmarks. Tip: provide a tune_file argument to save this step.")
        tune!(suite)
    end

    println("running benchmarks")
    res = run(suite)
    @show(res)
    output = get(ARGS, 2, "result-$(nw)worker-$(Dates.now()).jld")
    println("saving results in $output")
    JLD.save(output, "result", res)
    println("Done.")

    if length(ARGS) > 2
        compare_file = ARGS[3]
        println("Comparing with results in $compare_file")
        reference_results = JLD.load(compare_file, "result")
        @show judge(reference_results, res)
    end

end

main()
