# Benchmark: validate Datadeps disk-spilling on read-only and written slot copies.
#
# Single-worker regions never create Datadeps-managed slot copies (data is
# operated on in place), so the memory-aware spill/reclaim machinery only does
# anything when copies cross a worker boundary. This benchmark forces a large,
# *live*, read-only copy footprint on a remote worker and bounds it under a
# tight budget -- exactly what the disk-spilling path is for.
#
# Setup:
#   * A, B, C are tiled as INDEPENDENT per-tile arrays on worker 1. This matters:
#     a `view(parent, Blocks(...))` DArray shares one parent allocation, and
#     moving any tile-view to another worker materializes the *whole parent* as a
#     single Libc buffer (tiles are views into it). Per-tile `unsafe_free!` then
#     frees a view pointer that was never registered, so nothing is reclaimed.
#     Independent tiles each become their own freeable/spillable Libc copy.
#   * Every tile-multiply C[i,j] += A[i,k]*B[k,j] is forced onto worker 2 via
#     `compute_scope`, so each `In(A[i,k])`/`In(B[k,j])` becomes a read-only copy
#     there. Because A[i,k] is reused across all j (and B[k,j] across all i),
#     those copies stay *live* across many tasks: reclaim can't free a live copy,
#     so bounding the footprint requires spilling live read-only copies to disk
#     and reloading them on next use.
#
# Note: C[i,j] tiles are written (InOut). These are also spillable: a written
# copy's disk image is kept as its source of truth, and the region-end write-back
# reloads spilled written tiles one at a time (incrementally), so they no longer
# form an irreducible in-memory floor. Savings thus come from both the read-only
# A/B copies and the written C copies; the ON peak settles near the budget.
#
# Usage:
#   julia --project=. -t <nthreads> benchmarks/spill_matmul.jl [N] [blocksize] [budget_MiB]
#
# Defaults: N=2048, blocksize=512, budget=24 MiB.

using Distributed, Printf, LinearAlgebra

const N      = length(ARGS) >= 1 ? parse(Int, ARGS[1]) : 2048
const BS     = length(ARGS) >= 2 ? parse(Int, ARGS[2]) : 512
const BUDGET = UInt64((length(ARGS) >= 3 ? parse(Int, ARGS[3]) : 24) * 2^20)
@assert N % BS == 0 "N ($N) must be divisible by blocksize ($BS)"

if nworkers() < 2
    addprocs(1; exeflags="--project=$(Base.active_project())")
end
@everywhere using Dagger, LinearAlgebra
@everywhere gemm_acc!(C, A, B) = (mul!(C, A, B, 1.0, 1.0); C)

humanbytes(n) = n < 1024 ? "$(n) B" :
                n < 1024^2 ? @sprintf("%.1f KiB", n/1024) :
                n < 1024^3 ? @sprintf("%.1f MiB", n/1024^2) :
                             @sprintf("%.2f GiB", n/1024^3)

const NT = N ÷ BS
const w2 = Dagger.scope(worker=2)
const sp2 = Dagger.CPURAMMemorySpace(2)
peak2() = remotecall_fetch(() -> Dagger.libc_array_stats().peak_bytes, 2)

# Independent per-tile arrays on worker 1 (NOT views into a shared parent).
maketiles(f) = [f(i, k) for i in 1:NT, k in 1:NT]

function matmul_region!(Ct, At, Bt)
    Dagger.spawn_datadeps() do
        for i in 1:NT, j in 1:NT
            for k in 1:NT
                Dagger.@spawn compute_scope=w2 gemm_acc!(InOut(Ct[i, j]), In(At[i, k]), In(Bt[k, j]))
            end
        end
    end
    return Ct
end

# Reassemble a full matrix from its tiles (for a dense reference check).
function untile(Tt)
    M = Matrix{Float64}(undef, N, N)
    for i in 1:NT, j in 1:NT
        M[(i-1)*BS+1:i*BS, (j-1)*BS+1:j*BS] = Tt[i, j]
    end
    return M
end

function run_once(At, Bt, Ct)
    foreach(t -> fill!(t, 0.0), Ct)
    remotecall_fetch(Dagger.reset_libc_array_stats!, 2); GC.gc()
    t = @elapsed matmul_region!(Ct, At, Bt)
    return (; time=t, peak=peak2())
end

function main()
    println("="^74)
    println("Datadeps disk-spilling validation: remote read-only matmul copies")
    @printf("  matrices: 3x %d x %d Float64 (%s each), %dx%d tiles of %s\n",
            N, N, humanbytes(N*N*8), NT, NT, humanbytes(BS*BS*8))
    println("  procs:    $(procs())  (compute forced on worker 2)")
    @printf("  budget on worker 2:  %s   (read-only A+B copies: %s, written C copies: %s)\n",
            humanbytes(BUDGET), humanbytes(2*NT*NT*BS*BS*8), humanbytes(NT*NT*BS*BS*8))
    println("="^74)

    At = maketiles((i, k) -> [sin(0.5*((i-1)*BS+r) + ((k-1)*BS+c)) for r in 1:BS, c in 1:BS])
    Bt = maketiles((k, j) -> [cos(((k-1)*BS+r) - 0.5*((j-1)*BS+c)) for r in 1:BS, c in 1:BS])
    Ct = maketiles((i, j) -> zeros(BS, BS))
    ref = untile(At) * untile(Bt)

    println("warmup ..."); matmul_region!(Ct, At, Bt)

    println("\n--- feature OFF (copies accumulate on worker 2) ---")
    Dagger.disable_memory_aware_scheduling!()
    off = run_once(At, Bt, Ct)
    err_off = maximum(abs, untile(Ct) .- ref) / max(1.0, maximum(abs, ref))
    @printf("  time %.2f s   peak copies %s   rel.err %.2e\n",
            off.time, humanbytes(off.peak), err_off)

    println("\n--- feature ON (spill read-only + written copies, budget $(humanbytes(BUDGET))) ---")
    on = nothing; err_on = NaN
    try
        Dagger.enable_memory_aware_scheduling!(;
            limits=Dict{Dagger.MemorySpace,UInt64}(sp2 => BUDGET),
            reassign=false, spill_to_disk=true)
        on = run_once(At, Bt, Ct)
        err_on = maximum(abs, untile(Ct) .- ref) / max(1.0, maximum(abs, ref))
        @printf("  time %.2f s   peak copies %s   rel.err %.2e\n",
                on.time, humanbytes(on.peak), err_on)
    finally
        Dagger.disable_memory_aware_scheduling!()
    end

    println("\n" * "="^74)
    @printf("peak worker-2 copy memory:  OFF %s  ->  ON %s   (%.2fx smaller)\n",
            humanbytes(off.peak), humanbytes(on.peak), off.peak / max(1, on.peak))
    @printf("wall-clock cost of bounding: OFF %.2f s -> ON %.2f s\n", off.time, on.time)
    println("="^74)

    println()
    println("correctness OFF: ", err_off < 1e-8 ? "PASS" : "FAIL (err=$err_off)")
    println("correctness ON:  ", err_on  < 1e-8 ? "PASS" : "FAIL (err=$err_on)")
    println("peak reduced:    ", on.peak < off.peak ? "PASS" : "FAIL")
    # The ON peak settles near the budget: read-only A/B copies and written C
    # copies are both spilled to disk under pressure, and spilled written tiles
    # are reloaded incrementally at region end (one at a time) for write-back, so
    # there is no longer an irreducible in-memory floor at the full C footprint.
end

main()
