# Benchmark: larger-than-RAM Cholesky via an mmap-backed input array.
#
# Datadeps' aliasing is pointer-based: planning unwraps every tile to read its
# data pointer, and the analysis assumes those pointers stay valid for the
# lifetime of the region. That makes a plain in-RAM `distribute`d DArray
# impossible to push past RAM -- MemPool disk caching has to *relocate* tiles to
# spill them, which invalidates the cached pointers (observed end-to-end as
# `ReadOnlyMemoryError`, or `Cannot write ref to LRU` when planning pins every
# tile so nothing can be evicted).
#
# The trick here sidesteps that entirely: allocate the input matrix in an
# mmap-backed file and wrap it as a `DArray` with `view(A, Blocks(...))`. The
# matrix now lives at a *stable* virtual address (so pointer-based aliasing
# stays valid), while the OS transparently pages cold tiles out to the backing
# file. We can therefore factor a matrix far larger than physical RAM, with the
# OS doing the swapping for the input and our own disk-spilling machinery
# handling any Datadeps-managed slot copies on top.
#
# Usage:
#   julia --project=. -t <nthreads> benchmarks/mmap_cholesky.jl [N] [blocksize] [nworkers]
#
# Defaults: N=8192, blocksize=1024, nworkers=1 (single worker, multi-threaded).
#
# Run it on this branch and on `jps/dsparsematrix` to compare: the branch-only
# memory-aware hooks are feature-detected, so the same file runs on both.

using Distributed, Mmap, Printf

const N  = length(ARGS) >= 1 ? parse(Int, ARGS[1]) : 8192
const BS = length(ARGS) >= 2 ? parse(Int, ARGS[2]) : 1024
const NW = length(ARGS) >= 3 ? parse(Int, ARGS[3]) : 1

@assert N % BS == 0 "N ($N) must be divisible by blocksize ($BS) for square tiling"

if NW > 1 && nworkers() < NW
    addprocs(NW - nworkers(); exeflags="--project=$(Base.active_project())")
end

@everywhere using Dagger, LinearAlgebra

humanbytes(n) = n < 1024 ? "$(n) B" :
                n < 1024^2 ? @sprintf("%.1f KiB", n/1024) :
                n < 1024^3 ? @sprintf("%.1f MiB", n/1024^2) :
                             @sprintf("%.2f GiB", n/1024^3)

# Feature-detect the branch-only memory-aware API so this file also runs on
# `jps/dsparsematrix` (where these symbols don't exist).
const HAS_MEMAWARE = isdefined(Dagger, :enable_memory_aware_scheduling!)
const HAS_LIBCSTATS = isdefined(Dagger, :libc_array_stats)

# Sum of peak resident set size across all processes (bytes).
peak_rss() = sum(p -> remotecall_fetch(Sys.maxrss, p), procs())

# Allocate an mmap-backed `N x N` Float64 matrix and fill it (in place, across
# threads) with a symmetric, strongly diagonally-dominant -> SPD matrix:
#   A = ones(N,N) (off-diagonal) + (2N) I (diagonal).
# Deterministic, cheap to compute, and well-conditioned for Cholesky.
function make_mmap_spd(path, N)
    io = open(path, "w+")
    A = Mmap.mmap(io, Matrix{Float64}, (N, N))
    Threads.@threads for j in 1:N
        @inbounds for i in 1:N
            A[i, j] = i == j ? Float64(2N + 1) : 1.0
        end
    end
    Mmap.sync!(A)
    return A, io
end

function factor!(DA)
    # Factor in place; `cholesky`/`cholcopy` would `copy(A)` and drop the mmap
    # backing, so call the in-place tiled kernel directly.
    _, info = LinearAlgebra._chol!(DA, LinearAlgebra.UpperTriangular)
    return info
end

function validate()
    # Small end-to-end correctness check of the mmap + view + in-place factor
    # path against a dense LAPACK reference.
    n, bs = 512, 128
    path, io0 = mktemp()
    close(io0)
    A, io = make_mmap_spd(path, n)
    Aref = Matrix(A)               # small enough to keep a dense copy
    DA = view(A, Blocks(bs, bs))
    factor!(DA)
    U = UpperTriangular(collect(DA))
    resid = norm(U' * U - Aref) / norm(Aref)
    finalize(A); close(io); rm(path; force=true)
    @assert resid < 1e-10 "mmap Cholesky incorrect (relative residual = $resid)"
    return resid
end

function run_bench()
    println("="^72)
    println("Larger-than-RAM Cholesky (mmap-backed input)")
    @printf("  matrix:   %d x %d Float64  (%s),  block %d x %d\n",
            N, N, humanbytes(N * N * 8), BS, BS)
    println("  procs:    $(procs())  (workers: $(workers()), threads/worker: $(Threads.nthreads()))")
    println("  memaware: $(HAS_MEMAWARE ? "available" : "NOT available (baseline)")")
    println("="^72)

    print("validating mmap+view+in-place factor on a 512x512 case ... ")
    resid = validate()
    @printf("ok (rel. residual %.2e)\n", resid)

    if HAS_MEMAWARE
        Dagger.enable_memory_aware_scheduling!(; verbose=false)
    end

    path, io0 = mktemp()
    close(io0)
    print("building $(humanbytes(N*N*8)) mmap-backed SPD matrix ... ")
    t_build = @elapsed (A, io) = make_mmap_spd(path, N)
    @printf("%.2f s\n", t_build)

    DA = view(A, Blocks(BS, BS))

    if HAS_LIBCSTATS
        foreach(p -> remotecall_fetch(Dagger.reset_libc_array_stats!, p), procs())
    end

    println("factoring ...")
    GC.gc()
    info = -1
    t_fac = @elapsed (info = factor!(DA))
    rss = peak_rss()

    println()
    @printf("  factorization time:        %.2f s\n", t_fac)
    @printf("  cholesky info (0 == ok):   %d\n", info)
    @printf("  peak RSS (all procs):      %s\n", humanbytes(rss))
    @printf("  matrix size on disk:       %s\n", humanbytes(N * N * 8))
    @printf("  peak RSS / matrix size:    %.2f  (<1 => OS paged tiles to disk)\n",
            rss / (N * N * 8))
    if HAS_LIBCSTATS
        peak_copies = sum(p -> remotecall_fetch(() -> Dagger.libc_array_stats().peak_bytes, p), procs())
        tot_copies  = sum(p -> remotecall_fetch(() -> Dagger.libc_array_stats().total_bytes, p), procs())
        @printf("  Datadeps slot copies:      peak %s, total %s\n",
                humanbytes(peak_copies), humanbytes(tot_copies))
    end

    finalize(A); close(io); rm(path; force=true)
    @assert info == 0 "Cholesky failed (info = $info)"
    println("\nOK")
end

run_bench()
