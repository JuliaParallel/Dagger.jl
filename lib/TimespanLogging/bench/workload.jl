# Bounded Datadeps workload used by compare_datadeps.jl.
# Do not fetch_logs! here — we want emit-path overhead only.

using Dagger
using LinearAlgebra

const N = 512
const B = 64
const WARMUP = 3
const RUNS = 3

function measure(f; warmup=WARMUP, runs=RUNS)
    for _ in 1:warmup
        f()
    end
    GC.gc()
    best_t = Inf
    best_a = typemax(Int)
    best_b = typemax(Int)
    for _ in 1:runs
        t0 = time_ns()
        before = Base.gc_num()
        f()
        dt = (time_ns() - t0) / 1e9
        diff = Base.GC_Diff(Base.gc_num(), before)
        best_t = min(best_t, dt)
        best_a = min(best_a, Base.gc_alloc_count(diff))
        best_b = min(best_b, Int(diff.allocd))
    end
    return (time=best_t, allocs=best_a, bytes=best_b)
end

function setup_logging!(on::Bool)
    if on
        Dagger.enable_logging!(;all_task_deps=true)
    else
        Dagger.disable_logging!()
    end
    return nothing
end

function run_workload(logging::Bool)
    setup_logging!(logging)
    A = rand(Blocks(B, B), Float64, N, N)
    C = zeros(Blocks(B, B), Float64, N, N)
    wait(A); wait(C)
    matmul = measure() do
        mul!(C, A, A)
    end
    G = rand(Blocks(B, B), Float64, N, N)
    S = G * G'
    wait(S)
    chol = measure() do
        wait(cholesky(S).factors)
    end
    return (matmul=matmul, cholesky=chol)
end

function print_results(label, logging, results)
    for (op, r) in ((:matmul, results.matmul), (:cholesky, results.cholesky))
        println("RESULT tree=", label,
                " logging=", logging,
                " op=", op,
                " time=", r.time,
                " allocs=", r.allocs,
                " bytes=", r.bytes)
    end
    flush(stdout)
end
