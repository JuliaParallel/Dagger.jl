# Benchmark: how much CPU memory does Libc-backed Datadeps freeing reclaim?
#
# Runs a tiled, distributed dense matrix-matrix multiply (`mul!`) across 2
# workers inside a Datadeps region. Datadeps copies tiles of A/B/C onto the
# worker performing each tile-multiply; with Libc-backed allocations these
# copies are tracked and can be eagerly freed via `unsafe_free!` at the end of
# the region (instead of leaking until the GC runs).
#
# We report the total bytes of CPU buffer copies that became eagerly-freeable,
# aggregated across all worker processes.
#
# Usage:
#   julia --project=. benchmarks/libc_free_matmul.jl [N] [blocksize]

using Distributed

const N = length(ARGS) >= 1 ? parse(Int, ARGS[1]) : 2048
const BS = length(ARGS) >= 2 ? parse(Int, ARGS[2]) : 512

# "2-worker": one extra process joins the master, so copies cross a worker
# boundary (which is where Datadeps allocates buffer copies).
if nworkers() < 2
    addprocs(1; exeflags="--project=$(Base.active_project())")
end

@everywhere using Dagger, LinearAlgebra

function gather_stats()
    stats = [(; total_bytes=0, live_bytes=0, peak_bytes=0, num_allocs=0) for _ in procs()]
    for (i, p) in enumerate(procs())
        stats[i] = remotecall_fetch(Dagger.libc_array_stats, p)
    end
    return stats
end

reset_all() = foreach(p -> remotecall_fetch(Dagger.reset_libc_array_stats!, p), procs())

humanbytes(n) = n < 1024 ? "$(n) B" :
                n < 1024^2 ? string(round(n/1024; digits=1), " KiB") :
                n < 1024^3 ? string(round(n/1024^2; digits=1), " MiB") :
                             string(round(n/1024^3; digits=2), " GiB")

function run_matmul(N, BS)
    A = rand(Blocks(BS, BS), N, N)
    B = rand(Blocks(BS, BS), N, N)
    C = zeros(Blocks(BS, BS), N, N)
    mul!(C, A, B)
    return C, A, B
end

println("="^70)
println("Libc-backed Datadeps free benchmark")
println("  matrix: $(N)x$(N) Float64 ($(humanbytes(N*N*8)) each), block $(BS)x$(BS)")
println("  procs: $(procs()) (workers: $(workers()))")
println("="^70)

# Warm up (compile the matmul path; ignore these stats).
run_matmul(min(N, 512), min(BS, 256))
reset_all()

C, A, B = run_matmul(N, BS)

GC.gc() # let any finalizer-based frees settle for an accurate live count
stats = gather_stats()
total = sum(s -> s.total_bytes, stats)
peak = sum(s -> s.peak_bytes, stats)
live = sum(s -> s.live_bytes, stats)
nallocs = sum(s -> s.num_allocs, stats)

println()
for (p, s) in zip(procs(), stats)
    println("  worker $p: allocated $(humanbytes(s.total_bytes)) across $(s.num_allocs) buffers (peak live $(humanbytes(s.peak_bytes)), still live $(humanbytes(s.live_bytes)))")
end
println()
println("TOTAL Libc-backed buffer memory made eagerly-freeable: $(humanbytes(total)) ($(nallocs) buffers)")
println("Peak simultaneously-live Libc-backed buffers:          $(humanbytes(peak))")
println("Still live after region returned:                      $(humanbytes(live))")
println("Per-matrix size for reference:                         $(humanbytes(N*N*8))")

# Correctness sanity check.
err = maximum(abs, collect(C) .- (collect(A) * collect(B)))
println()
println("max abs error vs reference A*B: $err")
@assert err < 1e-6 * N "matmul result is incorrect!"
println("OK")
