# Does Reactant actually make Dagger faster? This compares each algorithm as
# Dagger runs it, as `Dagger.@reactant mode=:inner` runs it, and as
# `Dagger.@reactant mode=:full` runs it.
#
# Run with:
#     julia --project=test/reactantenv -t auto test/reactantenv/bench.jl [size]
#
# where `size` is `small` (the default), `medium`, or `large`.

using Dagger
using Reactant
using LinearAlgebra
using Random
using Printf

Reactant.set_default_backend("cpu")

const SIZES = Dict("small" => (n=512, bs=128, reps=3),
                   "medium" => (n=1024, bs=256, reps=3),
                   "large" => (n=2048, bs=512, reps=2))
const SIZE = get(SIZES, isempty(ARGS) ? "small" : first(ARGS)) do
    error("Unknown size $(repr(first(ARGS))); expected one of $(join(sort(collect(keys(SIZES))), ", "))")
end

"""
    measure(prepare, run!; reps) -> (first, best)

Times `run!(prepare())`, returning the time of the first call - which is where
Reactant compiles, and which is therefore what a one-shot script would see - and
the best of `reps` further calls, which is the steady-state cost once every
program involved has been compiled and cached.

`prepare` runs untimed before each call, as these algorithms overwrite their
inputs.
"""
function measure(prepare, run!; reps::Int)
    first_time = @elapsed run!(prepare())
    best = Inf
    for _ in 1:reps
        state = prepare()
        best = min(best, @elapsed run!(state))
    end
    return first_time, best
end

const RESULTS = Tuple{String,String,Float64,Float64,Bool}[]

function run_variant!(algorithm::String, variant::String, prepare, run!, check; reps::Int)
    Dagger.reactant_cache_clear!()
    correct = check(run!(prepare()))
    first_time, best = measure(prepare, run!; reps)
    push!(RESULTS, (algorithm, variant, first_time, best, correct))
    @printf("  %-26s %8.3fs first  %8.3fs best%s\n",
            variant, first_time, best, correct ? "" : "   [WRONG RESULT]")
    return
end

#############################################################################
# Cholesky factorization of a DMatrix
#############################################################################

chol_dagger(DX) = cholesky(DX)
chol_inner(DX) = Dagger.@reactant cholesky(DX)
chol_full(DX) = Dagger.@reactant mode=:full cholesky(DX)

function bench_cholesky(n, bs, reps)
    println("cholesky($n x $n, $bs x $bs blocks)")
    Random.seed!(1234)
    X = rand(n, n)
    X = X * X' + n * I
    reference = cholesky(copy(X)).U

    prepare() = distribute(copy(X), Blocks(bs, bs))
    check(chol) = isapprox(collect(chol.U), reference; rtol=1e-6)

    run_variant!("cholesky", "Dagger", prepare, chol_dagger, check; reps)
    run_variant!("cholesky", "Reactant (inner)", prepare, chol_inner, check; reps)
    run_variant!("cholesky", "Reactant (full)", prepare, chol_full, check; reps)
    return
end

#############################################################################
# A blocked matrix multiply, written as a Datadeps region
#############################################################################

function blocked_matmul!(state)
    C, A, B, blocks = state
    Dagger.spawn_datadeps() do
        for I in blocks, J in blocks, K in blocks
            Dagger.@spawn mul!(InOut(view(C, I, J)), In(view(A, I, K)), In(view(B, K, J)),
                               1.0, 1.0)
        end
    end
    return C
end
matmul_dagger(state) = blocked_matmul!(state)
matmul_inner(state) = Dagger.@reactant blocked_matmul!(state)
matmul_full(state) = Dagger.@reactant mode=:full blocked_matmul!(state)

function bench_matmul(n, bs, reps)
    nblocks = cld(n, bs)
    println("blocked matmul($n x $n, $(nblocks)x$(nblocks) blocks, $(nblocks^3) tasks)")
    Random.seed!(1234)
    A = rand(n, n)
    B = rand(n, n)
    reference = A * B
    blocks = [idx:min(idx + bs - 1, n) for idx in 1:bs:n]

    prepare() = (zeros(n, n), A, B, blocks)
    check(C) = isapprox(C, reference; rtol=1e-6)

    run_variant!("matmul", "Dagger", prepare, matmul_dagger, check; reps)
    run_variant!("matmul", "Reactant (inner)", prepare, matmul_inner, check; reps)
    run_variant!("matmul", "Reactant (full)", prepare, matmul_full, check; reps)
    return
end

#############################################################################
# An elementwise pipeline over a DArray, which is not a Datadeps region and so
# is only affected by inner mode
#############################################################################

pipeline(DA) = sum(sqrt.(abs.(DA) .+ 1) .* 2)
pipeline_dagger(DA) = pipeline(DA)
pipeline_inner(DA) = Dagger.@reactant pipeline(DA)
pipeline_full(DA) = Dagger.@reactant mode=:full pipeline(DA)

function bench_pipeline(n, bs, reps)
    println("broadcast pipeline($n x $n, $bs x $bs blocks)")
    Random.seed!(1234)
    X = rand(n, n)
    reference = pipeline(X)

    DA = distribute(X, Blocks(bs, bs))
    prepare() = DA
    check(total) = isapprox(total, reference; rtol=1e-6)

    run_variant!("pipeline", "Dagger", prepare, pipeline_dagger, check; reps)
    run_variant!("pipeline", "Reactant (inner)", prepare, pipeline_inner, check; reps)
    run_variant!("pipeline", "Reactant (full)", prepare, pipeline_full, check; reps)
    return
end

#############################################################################
# What XLA itself costs, with no Dagger involved: the ceiling that either mode
# is working towards, and the explanation for most of what the numbers above show
#############################################################################

function bench_reference(n, reps)
    println("reference, without Dagger ($n x $n)")
    Random.seed!(1234)
    X = rand(n, n)
    X = X * X' + n * I
    A = rand(n, n)
    B = rand(n, n)

    rX = Reactant.to_rarray(X)
    rA = Reactant.to_rarray(A)
    rB = Reactant.to_rarray(B)
    chol_program = Reactant.compile(cholesky, (rX,); sync=true)
    mul_program = Reactant.compile(*, (rA, rB); sync=true)

    _, lapack = measure(() -> copy(X), cholesky; reps)
    _, xla = measure(() -> rX, chol_program; reps)
    @printf("  %-26s %8.3fs LAPACK %8.3fs XLA\n", "cholesky", lapack, xla)

    _, blas = measure(() -> (A, B), state -> state[1] * state[2]; reps)
    _, xla = measure(() -> (rA, rB), state -> mul_program(state...); reps)
    @printf("  %-26s %8.3fs BLAS   %8.3fs XLA\n", "matmul", blas, xla)
    return
end

#############################################################################

function summarize()
    println()
    println("Summary (best-of-N, relative to Dagger without Reactant)")
    for algorithm in unique(first.(RESULTS))
        rows = filter(row -> row[1] == algorithm, RESULTS)
        baseline = rows[findfirst(row -> row[2] == "Dagger", rows)][4]
        println("  $algorithm:")
        for (_, variant, first_time, best, correct) in rows
            @printf("    %-26s %8.3fs  %6.2fx%s\n",
                    variant, best, baseline / best, correct ? "" : "   [WRONG RESULT]")
        end
    end
    return
end

@info "Environment" julia=VERSION threads=Threads.nthreads() blas_threads=BLAS.get_num_threads() xla_platform=Reactant.XLA.platform_name(Reactant.XLA.default_backend())

bench_cholesky(SIZE.n, SIZE.bs, SIZE.reps)
bench_matmul(SIZE.n, SIZE.bs, SIZE.reps)
bench_pipeline(SIZE.n, SIZE.bs, SIZE.reps)
bench_reference(SIZE.n, SIZE.reps)
summarize()
