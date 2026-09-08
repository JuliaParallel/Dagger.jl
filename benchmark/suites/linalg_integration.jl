# Comparison benches: Dagger linalg vs the non-Dagger ecosystem equivalent.
#
# Multi-threaded (one node):
#   julia --project=test -t 16 benchmark/suites/linalg_integration.jl
#
# MPI (every rank runs the same script; hostfile from batchctl):
#   LINALG_BENCH_MODE=mpi mpiexec --hostfile $WORK/hostfile -n 4 \
#       julia --project=test -t 4 benchmark/suites/linalg_integration.jl
#
# Environment:
#   LINALG_BENCH_MODE     mt | mpi          (default mt)
#   LINALG_BENCH_OUT      output directory  (default pwd/results)
#   LINALG_BENCH_ONLY     comma-separated feature keys (default: all)
#   LINALG_BENCH_WARMUP   warmup iters      (default 8)
#   LINALG_BENCH_SAMPLES  timed iters       (default 5)
#   LINALG_BENCH_SCALE    default | small | mpi
#
# Profile mode (attribution, not the published table):
#   LINALG_BENCH_PROFILE=1|cpu|alloc|logs|all  → linalg_profile.jl via the driver
#
# Methodology (AGENTS.md lesson 4): deep warmup, GC.gc(), min of timed runs.
# Dagger uses BLAS threads = 1 (task parallelism). Host dense uses BLAS = nthreads.
# Iterative methods share atol/rtol/itmax; tables report iterations + true residual.

const MODE = lowercase(get(ENV, "LINALG_BENCH_MODE", "mt"))
const WARMUP = parse(Int, get(ENV, "LINALG_BENCH_WARMUP", MODE == "mpi" ? "5" : "8"))
const SAMPLES = parse(Int, get(ENV, "LINALG_BENCH_SAMPLES", "5"))
const SCALE = lowercase(get(ENV, "LINALG_BENCH_SCALE", MODE == "mpi" ? "mpi" : "default"))
const ONLY = let s = strip(get(ENV, "LINALG_BENCH_ONLY", ""))
    isempty(s) ? nothing : Set(strip.(split(s, ',')))
end

using Dates
using LinearAlgebra
using Random
using SparseArrays
using Statistics

using Dagger
using Krylov
using AlgebraicMultigrid
using IncompleteLU
using PureKLU
using PureUMFPACK
using LinearSolve
using LinearSolve: KrylovJL_GMRES, PureUMFPACKFactorization, PureKLUFactorization,
                   UMFPACKFactorization, KLUFactorization

const MPI_MODE = MODE == "mpi"
if MPI_MODE
    using MPI
    isdefined(Dagger, :accelerate!) || error("Dagger.accelerate! is required for MPI benches")
    Dagger.accelerate!(:mpi)
    const COMM = MPI.COMM_WORLD
    const RANK = MPI.Comm_rank(COMM)
    const NRANKS = MPI.Comm_size(COMM)
    # First datadeps/MPI compile can exceed the 120s hang detector. Planning
    # runs on this task, so a TaskLocalValue write here is the one that matters.
    let ext = Base.get_extension(Dagger, :MPIExt)
        if ext !== nothing
            ext.DEADLOCK_TIMEOUT_PERIOD[] = parse(Float64, get(ENV, "DAGGER_MPI_DEADLOCK_TIMEOUT", "900"))
            ext.DEADLOCK_WARN_PERIOD[] = parse(Float64, get(ENV, "DAGGER_MPI_DEADLOCK_WARN", "60"))
        end
    end
else
    const RANK = 0
    const NRANKS = 1
end

is_root() = RANK == 0
# Do not mix MPI.Barrier with Dagger's tagged P2P (deadlocks under compile
# skew). Dagger SPMD ops already synchronize; host baselines run on every rank.
maybe_barrier() = nothing

const NTHREADS = Threads.nthreads()
const BLAS_DAGGER = 1
const BLAS_BASELINE = max(1, NTHREADS)

function with_blas(f, n::Integer)
    old = BLAS.get_num_threads()
    BLAS.set_num_threads(n)
    try
        return f()
    finally
        BLAS.set_num_threads(old)
    end
end

function timed_min(f; warmup=WARMUP, samples=SAMPLES, sync=true)
    for _ in 1:warmup
        f()
    end
    GC.gc()
    best = Inf
    for _ in 1:samples
        t = @elapsed f()
        best = min(best, t)
    end
    return best
end

# Host baselines run on every rank (SPMD). timed_min_root is a compatibility
# alias — it no longer skips work on non-root, which used to let other ranks
# enter the next Dagger collective while rank 0 was still in serial host code.
timed_min_root(f; kwargs...) = timed_min(f; kwargs...)

function instance_type()
    env = get(ENV, "LINALG_BENCH_INSTANCE", "")
    isempty(env) || return env
    try
        tok = read(`curl -fsS -X PUT http://169.254.169.254/latest/api/token
                    -H X-aws-ec2-metadata-token-ttl-seconds: 60 --max-time 2`, String)
        return strip(read(`curl -fsS -H "X-aws-ec2-metadata-token: $tok"
                           http://169.254.169.254/latest/meta-data/instance-type --max-time 2`, String))
    catch
        return "unknown"
    end
end

function git_sha()
    try
        return strip(readchomp(`git -C $(dirname(dirname(@__DIR__))) rev-parse HEAD`))
    catch
        return get(ENV, "LINALG_BENCH_SHA", "unknown")
    end
end

# --- problem constructors ---------------------------------------------------

laplacian_1d(T, n) = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(4), n),
     1 => fill(-one(T), n - 1),
)

function laplacian_2d(T, nx, ny=nx)
    Tx = SparseArrays.spdiagm(-1 => fill(-one(T), nx - 1),
                               0 => fill(T(2), nx),
                               1 => fill(-one(T), nx - 1))
    Ty = SparseArrays.spdiagm(-1 => fill(-one(T), ny - 1),
                               0 => fill(T(2), ny),
                               1 => fill(-one(T), ny - 1))
    Ix = SparseMatrixCSC{T,Int}(I, nx, nx)
    Iy = SparseMatrixCSC{T,Int}(I, ny, ny)
    return kron(Iy, Tx) + kron(Ty, Ix)
end

advection_1d(T, n) = laplacian_1d(T, n) + SparseArrays.spdiagm(
    -1 => fill(T(-3) / 10, n - 1),
     1 => fill(T(3) / 10, n - 1),
)

function dense_spd(T, n)
    G = rand(T, n, n)
    return G * G' + T(n) * I
end

function wait_d(x)
    x isa Dagger.DArray && wait(x)
    return x
end

function wait_factor(F)
    # Use hasfield: Cholesky exposes `U` as a property but not a field
    # (FieldError if we getfield(:U)).
    if hasfield(typeof(F), :factors)
        fac = getfield(F, :factors)
        fac isa Dagger.DArray && wait(fac)
    end
    if hasfield(typeof(F), :U)
        U = getfield(F, :U)
        U isa Dagger.DArray && wait(U)
    end
    if hasfield(typeof(F), :V)
        V = getfield(F, :V)
        V isa Dagger.DArray && wait(V)
    end
    if hasfield(typeof(F), :Vt)
        Vt = getfield(F, :Vt)
        Vt isa Dagger.DArray && wait(Vt)
    end
    return F
end

function true_relres(A, x, b)
    y = A * x
    wait_d(y)
    yh = y isa Dagger.DArray ? collect(y) : y
    xh = x isa Dagger.DArray ? collect(x) : x
    bh = b isa Dagger.DArray ? collect(b) : b
    return LinearAlgebra.norm(yh .- bh) / max(LinearAlgebra.norm(bh), eps())
end

# --- host preconditioner stand-ins (when no ecosystem distributed PC exists)

struct HostBlockPC{F}
    factors::Vector{F}
    ranges::Vector{UnitRange{Int}}
end

function HostBlockPC(A::AbstractMatrix, bs::Integer)
    n = size(A, 1)
    factors = Any[]
    ranges = UnitRange{Int}[]
    for s in 1:bs:n
        r = s:min(s + bs - 1, n)
        push!(factors, lu(Matrix(A[r, r])))
        push!(ranges, r)
    end
    return HostBlockPC(factors, ranges)
end

function LinearAlgebra.mul!(y::AbstractVector, P::HostBlockPC, x::AbstractVector)
    for (F, r) in zip(P.factors, P.ranges)
        y[r] = F \ view(x, r)
    end
    return y
end

struct HostRAS{F}
    factors::Vector{F}
    interiors::Vector{UnitRange{Int}}
    omegas::Vector{UnitRange{Int}}
end

function HostRAS(A::AbstractMatrix, bs::Integer, overlap::Integer)
    n = size(A, 1)
    factors = Any[]
    interiors = UnitRange{Int}[]
    omegas = UnitRange{Int}[]
    for s in 1:bs:n
        interior = s:min(s + bs - 1, n)
        Ω = max(1, first(interior) - overlap):min(n, last(interior) + overlap)
        push!(factors, lu(A[Ω, Ω]))
        push!(interiors, interior)
        push!(omegas, Ω)
    end
    return HostRAS(factors, interiors, omegas)
end

struct HostRASBasic{F}
    factors::Vector{F}
    omegas::Vector{UnitRange{Int}}
end

function HostRASBasic(A::AbstractMatrix, bs::Integer, overlap::Integer)
    n = size(A, 1)
    factors = Any[]
    omegas = UnitRange{Int}[]
    for s in 1:bs:n
        interior = s:min(s + bs - 1, n)
        Ω = max(1, first(interior) - overlap):min(n, last(interior) + overlap)
        push!(factors, lu(A[Ω, Ω]))
        push!(omegas, Ω)
    end
    return HostRASBasic(factors, omegas)
end

function LinearAlgebra.mul!(y::AbstractVector, P::HostRASBasic, x::AbstractVector)
    fill!(y, zero(eltype(y)))
    for (F, Ω) in zip(P.factors, P.omegas)
        yΩ = F \ view(x, Ω)
        y[Ω] .+= yΩ
    end
    return y
end

function LinearAlgebra.mul!(y::AbstractVector, P::HostRAS, x::AbstractVector)
    fill!(y, zero(eltype(y)))
    for (F, interior, Ω) in zip(P.factors, P.interiors, P.omegas)
        yΩ = F \ view(x, Ω)
        off = first(interior) - first(Ω)
        copyto!(view(y, interior), view(yΩ, (off + 1):(off + length(interior))))
    end
    return y
end

# Projected apply on host: P = I - N N', y = P A P x, N orthonormal columns.
function host_projected_mul!(y, A, N, x)
    # N is n×k with orthonormal columns
    xt = x - N * (N' * x)
    yt = A * xt
    y .= yt - N * (N' * yt)
    return y
end

# --- sizes -----------------------------------------------------------------

const S = if SCALE == "small"
    (gemm_n=512, gemm_b=256,
     lu_n=512, lu_b=256,
     qr_n=512, qr_b=256,
     chol_n=512, chol_b=256,
     svd_n=128, svd_b=64,
     spmv_n=8_192, spmv_b=1_024,
     spgemm_n=800, spgemm_b=200, spgemm_p=0.02,
     krylov_grid=32, krylov_b=64,
     direct_grid=32, direct_b=64,
     assembly_grid=64, assembly_b=128,
     op_n=256, op_b=64)
elseif SCALE == "mpi"
    # Tile sides chosen so a 4-rank run is ~2×2 (dense) / 4×4 (Krylov), matching
    # the working contrib/mpi/run_matmul.jl pattern. Fine tiles hung in MPI
    # aliasing bcasts on the first pass.
    (gemm_n=2048, gemm_b=1024,
     lu_n=1024, lu_b=512,
     qr_n=1024, qr_b=512,
     chol_n=1024, chol_b=512,
     svd_n=256, svd_b=128,
     spmv_n=65_536, spmv_b=16_384,
     spgemm_n=1_600, spgemm_b=800, spgemm_p=0.008,
     krylov_grid=64, krylov_b=1024,
     direct_grid=64, direct_b=1024,
     assembly_grid=128, assembly_b=1024,
     op_n=1_024, op_b=512)
else
    (gemm_n=4096, gemm_b=512,
     lu_n=2048, lu_b=256,
     qr_n=2048, qr_b=256,
     chol_n=2048, chol_b=256,
     svd_n=256, svd_b=128,
     spmv_n=160_000, spmv_b=20_000,
     spgemm_n=2_500, spgemm_b=625, spgemm_p=0.008,
     krylov_grid=64, krylov_b=1024,
     direct_grid=80, direct_b=1280,
     assembly_grid=200, assembly_b=2_500,
     op_n=2_048, op_b=256)
end

const ATOL = 1e-10
const RTOL = 1e-8
const ITMAX = 500
const GMRES_MEM = 50

# --- result plumbing -------------------------------------------------------

const OUTDIR = let d = get(ENV, "LINALG_BENCH_OUT", "")
    isempty(d) ? joinpath(pwd(), "results") : abspath(d)
end
is_root() && mkpath(OUTDIR)

const META = Dict{String,Any}(
    "mode" => MODE,
    "scale" => SCALE,
    "commit" => git_sha(),
    "julia" => string(VERSION),
    "threads" => NTHREADS,
    "ranks" => NRANKS,
    "instance" => instance_type(),
    "date" => string(Dates.today()),
    "datetime" => string(Dates.now()),
    "warmup" => WARMUP,
    "samples" => SAMPLES,
    "blas_dagger" => BLAS_DAGGER,
    "blas_baseline" => BLAS_BASELINE,
    "atol" => ATOL,
    "rtol" => RTOL,
    "itmax" => ITMAX,
)

const ROWS = Dict{String,Any}[]

function push_row!(; feature, key, problem, dagger_s, baseline_s, baseline_name,
                   notes="", iters_d=nothing, iters_b=nothing,
                   relres_d=nothing, relres_b=nothing, setup_d=nothing, setup_b=nothing,
                   error=nothing)
    speedup = (baseline_s isa Real && dagger_s isa Real && dagger_s > 0 &&
               isfinite(baseline_s) && isfinite(dagger_s)) ?
              baseline_s / dagger_s : nothing
    row = Dict{String,Any}(
        "feature" => feature,
        "key" => key,
        "problem" => problem,
        "dagger_s" => dagger_s,
        "baseline_s" => baseline_s,
        "baseline_name" => baseline_name,
        "speedup" => speedup,
        "notes" => notes,
        "iters_d" => iters_d,
        "iters_b" => iters_b,
        "relres_d" => relres_d,
        "relres_b" => relres_b,
        "setup_d" => setup_d,
        "setup_b" => setup_b,
        "error" => error,
    )
    push!(ROWS, row)
    if is_root()
        _flush_outputs()
        sp = speedup === nothing ? "n/a" : string(round(speedup; digits=3))
        ds = dagger_s isa Real && isfinite(dagger_s) ? string(round(dagger_s; digits=4)) : string(dagger_s)
        bs = baseline_s isa Real && isfinite(baseline_s) ? string(round(baseline_s; digits=4)) : string(baseline_s)
        println("ROW ", key, "  D=", ds, "s  B=", bs, "s  speedup=", sp,
                error === nothing ? "" : "  ERR=$(error)")
        flush(stdout)
    end
    return row
end

function fmt_s(x)
    x isa Real && isfinite(x) || return "—"
    if x < 1e-3
        return string(round(x * 1e6; digits=1), " µs")
    elseif x < 1
        return string(round(x * 1e3; digits=2), " ms")
    else
        return string(round(x; digits=3), " s")
    end
end

function fmt_sp(x)
    x isa Real && isfinite(x) || return "—"
    return string(round(x; digits=2), "×")
end

function markdown_table(rows)
    io = IOBuffer()
    println(io, "| Feature | Problem | Dagger | Baseline (name) | Time D / Time B | Speedup | Notes |")
    println(io, "|---|---|---|---|---|---|---|")
    for r in rows
        notes = String(r["notes"])
        extra = String[]
        r["iters_d"] !== nothing && push!(extra, "iters D/B=$(r["iters_d"])/$(r["iters_b"])")
        r["relres_d"] !== nothing && push!(extra, "‖Ax−b‖/‖b‖ D/B=$(round(r["relres_d"]; sigdigits=3))/$(r["relres_b"] === nothing ? "—" : round(r["relres_b"]; sigdigits=3))")
        r["setup_d"] !== nothing && push!(extra, "PC setup D=$(fmt_s(r["setup_d"]))")
        r["setup_b"] !== nothing && push!(extra, "B=$(fmt_s(r["setup_b"]))")
        r["error"] !== nothing && push!(extra, "ERROR: $(r["error"])")
        note = join(filter(!isempty, [notes, join(extra, "; ")]), "; ")
        println(io, "| ", r["feature"], " | ", r["problem"], " | ", fmt_s(r["dagger_s"]),
                " | ", fmt_s(r["baseline_s"]), " (", r["baseline_name"], ") | ",
                fmt_s(r["dagger_s"]), " / ", fmt_s(r["baseline_s"]), " | ",
                fmt_sp(r["speedup"]), " | ", note, " |")
    end
    return String(take!(io))
end

function _flush_outputs()
    is_root() || return
    tag = MODE == "mpi" ? "mpi" : "mt"
    json_path = joinpath(OUTDIR, "linalg_integration_$(tag).json")
    md_path = joinpath(OUTDIR, "linalg_integration_$(tag).md")
    payload = Dict("meta" => META, "rows" => ROWS)
    # Avoid a JSON dep: write a Julia-parseable Dict via repr, plus a simple JSON.
    open(json_path, "w") do io
        _write_json(io, payload)
    end
    open(md_path, "w") do io
        println(io, markdown_table(ROWS))
    end
    return nothing
end

function _write_json(io, x)
    if x === nothing
        write(io, "null")
    elseif x isa Bool
        write(io, x ? "true" : "false")
    elseif x isa Integer
        write(io, string(x))
    elseif x isa AbstractFloat
        isfinite(x) ? write(io, string(x)) : write(io, "null")
    elseif x isa AbstractString
        write(io, '"')
        for c in x
            if c == '\\'
                write(io, "\\\\")
            elseif c == '"'
                write(io, "\\\"")
            elseif c == '\n'
                write(io, "\\n")
            elseif c == '\r'
                write(io, "\\r")
            elseif c == '\t'
                write(io, "\\t")
            elseif iscntrl(c)
                write(io, "\\u", lpad(string(UInt32(c); base=16), 4, '0'))
            else
                write(io, c)
            end
        end
        write(io, '"')
    elseif x isa AbstractVector
        write(io, '[')
        for (i, v) in enumerate(x)
            i > 1 && write(io, ',')
            _write_json(io, v)
        end
        write(io, ']')
    elseif x isa AbstractDict
        write(io, '{')
        first = true
        for (k, v) in x
            first || write(io, ',')
            first = false
            _write_json(io, string(k))
            write(io, ':')
            _write_json(io, v)
        end
        write(io, '}')
    else
        _write_json(io, string(x))
    end
end

wanted(key) = ONLY === nothing || key in ONLY

macro safe_bench(key, ex)
    quote
        if wanted($(esc(key)))
            try
                $(esc(ex))
            catch err
                is_root() && @error "bench $($(esc(key))) failed" exception = (err, catch_backtrace())
                push_row!(feature=$(esc(key)), key=$(esc(key)), problem="(failed)",
                          dagger_s=nothing, baseline_s=nothing, baseline_name="—",
                          error=sprint(showerror, err))
            end
            GC.gc()
            maybe_barrier()
        end
    end
end

# --- benches ---------------------------------------------------------------

function bench_dense_gemm()
    n, b = S.gemm_n, S.gemm_b
    td = with_blas(BLAS_DAGGER) do
        A = rand(Blocks(b, b), Float64, n, n)
        B = rand(Blocks(b, b), Float64, n, n)
        timed_min() do
            C = A * B
            wait(C)
        end
    end
    tb = with_blas(BLAS_BASELINE) do
        Ah = rand(n, n)
        Bh = rand(n, n)
        timed_min() do
            Ah * Bh
        end
    end
    push_row!(feature="Dense GEMM / mul!", key="dense_gemm",
              problem="n=$(n), tile=$(b)×$(b), Float64, C←A*B",
              dagger_s=td, baseline_s=tb,
              baseline_name="LinearAlgebra.*(::Matrix, ::Matrix) OpenBLAS",
              notes="Dagger BLAS=$(BLAS_DAGGER); host BLAS=$(BLAS_BASELINE); A*B not mul!(C,A,A) (MPI aliasing)")
end

function bench_dense_factor(feature, key, n, b, dagger_f, host_f, host_name)
    td = with_blas(BLAS_DAGGER) do
        A = rand(Blocks(b, b), Float64, n, n); wait(A)
        bv = rand(Blocks(b), Float64, n); wait(bv)
        timed_min() do
            F = dagger_f(A)
            wait_factor(F)
            x = F \ bv
            wait_d(x)
        end
    end
    tb = with_blas(BLAS_BASELINE) do
        Ah = rand(n, n)
        bh = rand(n)
        timed_min() do
            F = host_f(Ah)
            F \ bh
        end
    end
    push_row!(feature=feature, key=key,
              problem="n=$(n), tile=$(b)×$(b), Float64, factor + \\",
              dagger_s=td, baseline_s=tb, baseline_name=host_name,
              notes="Dagger BLAS=$(BLAS_DAGGER); host BLAS=$(BLAS_BASELINE)")
end

function bench_dense_chol()
    n, b = S.chol_n, S.chol_b
    td = with_blas(BLAS_DAGGER) do
        G = rand(Blocks(b, b), Float64, n, n); wait(G)
        A = G * G'
        wait(A)
        # ensure PD at the type/tile level
        bv = rand(Blocks(b), Float64, n); wait(bv)
        timed_min() do
            F = cholesky(A)
            wait_factor(F)
            x = F \ bv
            wait_d(x)
        end
    end
    tb = with_blas(BLAS_BASELINE) do
        Ah = dense_spd(Float64, n)
        bh = rand(n)
        timed_min() do
            F = cholesky(Ah)
            F \ bh
        end
    end
    push_row!(feature="Dense Cholesky + \\", key="dense_chol",
              problem="n=$(n), tile=$(b)×$(b), SPD G*G', factor + \\",
              dagger_s=td, baseline_s=tb,
              baseline_name="LinearAlgebra.cholesky(::Matrix) LAPACK potrf",
              notes="Dagger BLAS=$(BLAS_DAGGER); host BLAS=$(BLAS_BASELINE)")
end

function bench_dense_svd()
    n, b = S.svd_n, S.svd_b
    td = with_blas(BLAS_DAGGER) do
        A = rand(Blocks(b, b), Float64, n, n); wait(A)
        timed_min() do
            F = svd(A)
            wait_factor(F)
        end
    end
    tb = with_blas(BLAS_BASELINE) do
        Ah = rand(n, n)
        timed_min() do
            svd(Ah)
        end
    end
    push_row!(feature="Dense SVD", key="dense_svd",
              problem="n=$(n), tile=$(b)×$(b), Float64, svd only",
              dagger_s=td, baseline_s=tb,
              baseline_name="LinearAlgebra.svd(::Matrix) LAPACK",
              notes="modest size (tiled Jacobi vs LAPACK gesdd); Dagger BLAS=$(BLAS_DAGGER); host BLAS=$(BLAS_BASELINE)")
end

function bench_spmv()
    n, b = S.spmv_n, S.spmv_b
    # 1-D Laplacian: nnz = 3n, honest SparseArrays SpMV vs tiled SpMV.
    Random.seed!(1234)
    Ah = laplacian_1d(Float64, n)
    xh = rand(n)
    td = with_blas(BLAS_DAGGER) do
        A = distribute(Ah, Blocks(b, b)); wait(A)
        x = distribute(xh, Blocks(b)); wait(x)
        y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
        timed_min() do
            mul!(y, A, x); wait(y)
        end
    end
    tb = timed_min_root() do
        Ah * xh
    end
    nnzA = nnz(Ah)
    push_row!(feature="Sparse SpMV", key="sparse_spmv",
              problem="1-D Laplacian n=$(n), nnz=$(nnzA), tile=$(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="SparseArrays *(::CSC, ::Vector)",
              notes="host CSC SpMV is single-threaded")
end

function bench_spgemm()
    n, b, p = S.spgemm_n, S.spgemm_b, S.spgemm_p
    Random.seed!(1234)
    Ah = sprand(Float64, n, n, p)
    Bh = sprand(Float64, n, n, p)
    td = with_blas(BLAS_DAGGER) do
        A = distribute(Ah, Blocks(b, b))
        B = distribute(Bh, Blocks(b, b))
        timed_min() do
            C = A * B
            wait(C)
        end
    end
    tb = timed_min() do
        Ah * Bh
    end
    push_row!(feature="Sparse SpGEMM", key="sparse_spgemm",
              problem="sprand n=$(n), p=$(p), nnz=$(nnz(Ah)), tile=$(b)×$(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="SparseArrays *(::CSC, ::CSC)",
              notes="host CSC×CSC is single-threaded")
end

function _krylov_pair(; feature, key, grid, b, solver_d, solver_b, host_name,
                      build_d=nothing, build_b=nothing, notes="",
                      Ahost=nothing, method_notes="", host_ldiv=false)
    n = grid * grid
    Random.seed!(1234)
    Ah = Ahost === nothing ? laplacian_2d(Float64, grid) : Ahost
    bh = rand(n)
    DA = nothing
    Db = nothing
    Pd = nothing
    setup_d = nothing
    td = with_blas(BLAS_DAGGER) do
        DA = distribute(Ah, Blocks(b, b)); wait(DA)
        Db = distribute(bh, Blocks(b)); wait(Db)
        if build_d !== nothing
            setup_d = @elapsed begin
                Pd = build_d(DA)
            end
            maybe_barrier()
        end
        # one compile/warmup solve happens inside timed_min
        return timed_min() do
            x, _ = solver_d(DA, Db, Pd)
            wait_d(x)
        end
    end
    # residual / iters from one extra solve (not timed)
    iters_d = nothing
    rel_d = nothing
    with_blas(BLAS_DAGGER) do
        x, st = solver_d(DA, Db, Pd)
        wait_d(x)
        iters_d = hasproperty(st, :niter) ? st.niter : nothing
        rel_d = true_relres(DA, x, Db)
    end

    Pb = nothing
    setup_b = nothing
    if build_b !== nothing
        setup_b = @elapsed begin
            Pb = build_b(Ah)
        end
    end
    tb = timed_min() do
        solver_b(Ah, bh, Pb; ldiv=host_ldiv)
    end
    xh, stb = solver_b(Ah, bh, Pb; ldiv=host_ldiv)
    iters_b = hasproperty(stb, :niter) ? stb.niter : nothing
    rel_b = true_relres(Ah, xh, bh)
    note = join(filter(!isempty, [notes, method_notes,
                                  "square tiles; same atol=$(ATOL) rtol=$(RTOL) itmax=$(ITMAX)"]), "; ")
    push_row!(feature=feature, key=key,
              problem="2-D Laplacian $(grid)×$(grid) (n=$(n), nnz=$(nnz(Ah))), tile=$(b)×$(b)",
              dagger_s=td, baseline_s=tb, baseline_name=host_name,
              notes=note, iters_d=iters_d, iters_b=iters_b,
              relres_d=rel_d, relres_b=rel_b, setup_d=setup_d, setup_b=setup_b)
end

_cg_d(A, b, M) = M === nothing ?
    Krylov.cg(A, b; atol=ATOL, rtol=RTOL, itmax=ITMAX) :
    Krylov.cg(A, b; M=M, ldiv=false, atol=ATOL, rtol=RTOL, itmax=ITMAX)
_gmres_d(A, b, M) = M === nothing ?
    Krylov.gmres(A, b; atol=ATOL, rtol=RTOL, itmax=ITMAX, memory=GMRES_MEM) :
    Krylov.gmres(A, b; M=M, ldiv=false, atol=ATOL, rtol=RTOL, itmax=ITMAX, memory=GMRES_MEM)
function _cg_b(A, b, M; ldiv=false)
    M === nothing && return Krylov.cg(A, b; atol=ATOL, rtol=RTOL, itmax=ITMAX)
    return Krylov.cg(A, b; M=M, ldiv=ldiv, atol=ATOL, rtol=RTOL, itmax=ITMAX)
end
function _gmres_b(A, b, M; ldiv=false)
    M === nothing && return Krylov.gmres(A, b; atol=ATOL, rtol=RTOL, itmax=ITMAX, memory=GMRES_MEM)
    return Krylov.gmres(A, b; M=M, ldiv=ldiv, atol=ATOL, rtol=RTOL, itmax=ITMAX, memory=GMRES_MEM)
end

function bench_krylov_nopc()
    grid, b = S.krylov_grid, S.krylov_b
    wanted("krylov_cg") && _krylov_pair(; feature="Krylov CG (no PC)", key="krylov_cg",
                 grid=grid, b=b, solver_d=_cg_d, solver_b=_cg_b,
                 host_name="Krylov.cg(::CSC)", notes="no preconditioner")
    wanted("krylov_gmres") && _krylov_pair(; feature="Krylov GMRES (no PC)", key="krylov_gmres",
                 grid=grid, b=b, solver_d=_gmres_d, solver_b=_gmres_b,
                 host_name="Krylov.gmres(::CSC)", notes="no preconditioner; memory=$(GMRES_MEM)")
end

function bench_krylov_jacobi()
    _krylov_pair(; feature="Krylov CG + Jacobi", key="krylov_jacobi",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_cg_d, solver_b=_cg_b,
                 host_name="Krylov.cg + Diagonal(1./diag)",
                 build_d=Dagger.JacobiPreconditioner,
                 build_b=A -> Diagonal(1.0 ./ diag(A)),
                 host_ldiv=true,
                 notes="host M = Diagonal (ldiv); Dagger JacobiPreconditioner (mul!)")
end

function bench_krylov_blockjacobi()
    _krylov_pair(; feature="Krylov CG + BlockJacobi", key="krylov_blockjacobi",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_cg_d, solver_b=_cg_b,
                 host_name="Krylov.cg + hand-rolled block LU",
                 build_d=Dagger.BlockJacobiPreconditioner,
                 build_b=A -> HostBlockPC(A, S.krylov_b),
                 notes="host is serial per-block LU (no ecosystem BlockJacobi)")
end

function bench_krylov_blockilu()
    _krylov_pair(; feature="Krylov GMRES + BlockILU", key="krylov_blockilu",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_gmres_d, solver_b=_gmres_b,
                 host_name="Krylov.gmres + IncompleteLU.ilu (whole matrix)",
                 build_d=Dagger.BlockILUPreconditioner,
                 build_b=A -> IncompleteLU.ilu(A),
                 host_ldiv=true,
                 notes="host ILU is global (stronger than per-tile); Dagger is block-diagonal ILU")
end

function bench_krylov_amg_tile()
    _krylov_pair(; feature="Krylov GMRES + per-tile AMG", key="krylov_amg_tile",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_gmres_d, solver_b=_gmres_b,
                 host_name="Krylov.gmres + AlgebraicMultigrid RS (global)",
                 build_d=A -> Dagger.AMGPreconditioner(A; method=:ruge_stuben),
                 build_b=A -> AlgebraicMultigrid.aspreconditioner(AlgebraicMultigrid.ruge_stuben(A)),
                 host_ldiv=true,
                 notes="Dagger AMGPreconditioner is block-diagonal (lesson 19); host is true global AMG. Check ‖Ax−b‖, not only stats.solved")
end

function bench_krylov_globalamg()
    _krylov_pair(; feature="Krylov GMRES + GlobalAMG", key="krylov_globalamg",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_gmres_d, solver_b=_gmres_b,
                 host_name="Krylov.gmres + AlgebraicMultigrid SA (global)",
                 build_d=A -> Dagger.SmoothedAggregationPreconditioner(A; presweeps=2, postsweeps=2),
                 build_b=A -> AlgebraicMultigrid.aspreconditioner(AlgebraicMultigrid.smoothed_aggregation(A)),
                 host_ldiv=true,
                 notes="2+2 damped-Jacobi sweeps (lesson 32); host SA is process-local")
end

function bench_krylov_asm()
    _krylov_pair(; feature="Additive Schwarz (RAS)", key="krylov_asm",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_gmres_d, solver_b=_gmres_b,
                 host_name="Krylov.gmres + hand-rolled RAS (serial)",
                 build_d=A -> Dagger.AdditiveSchwarzPreconditioner(A; overlap=1),
                 build_b=A -> HostRAS(A, S.krylov_b, 1),
                 notes="overlap=1, PC_ASM_RESTRICT; no Julia-ecosystem distributed RAS — host is serial pre-factored RAS")
end

function bench_sparse_direct()
    grid, b = S.direct_grid, S.direct_b
    n = grid * grid
    Random.seed!(1234)
    Ah = laplacian_2d(Float64, grid)
    bh = rand(n)
    DA = with_blas(BLAS_DAGGER) do
        A = distribute(Ah, Blocks(b, b)); wait(A)
        A
    end
    Db = distribute(bh, Blocks(b)); wait(Db)

    if wanted("sparse_chol")
        td = with_blas(BLAS_DAGGER) do
            timed_min() do
                F = cholesky(DA)
                x = F \ Db
                wait_d(x)
            end
        end
        tb = timed_min_root() do
            F = cholesky(Ah)
            F \ bh
        end
        push_row!(feature="Sparse cholesky + \\", key="sparse_chol",
                  problem="2-D Laplacian $(grid)×$(grid) (n=$(n), nnz=$(nnz(Ah))), tile=$(b)×$(b)",
                  dagger_s=td, baseline_s=tb,
                  baseline_name="CHOLMOD cholesky(::CSC)",
                  notes="Dagger gathers then CHOLMOD on one worker; both fit in RAM")
    end

    if wanted("sparse_klu")
        try
            td = with_blas(BLAS_DAGGER) do
                timed_min() do
                    F = Dagger.klu(DA)
                    x = F \ Db
                    wait_d(x)
                end
            end
            tb = timed_min_root() do
                F = PureKLU.klu(Ah)
                F \ bh
            end
            push_row!(feature="Sparse klu + \\", key="sparse_klu",
                      problem="2-D Laplacian $(grid)×$(grid) (n=$(n), nnz=$(nnz(Ah))), tile=$(b)×$(b)",
                      dagger_s=td, baseline_s=tb,
                      baseline_name="PureKLU.klu(::CSC)",
                      notes="Dagger.klu gathers to one worker; host is already local CSC")
        catch err
            push_row!(feature="Sparse klu + \\", key="sparse_klu", problem="(failed)",
                      dagger_s=nothing, baseline_s=nothing, baseline_name="PureKLU.klu(::CSC)",
                      error=sprint(showerror, err))
        end
    end

    if wanted("sparse_splu")
        try
            td = with_blas(BLAS_DAGGER) do
                timed_min() do
                    F = Dagger.splu(DA)
                    x = F \ Db
                    wait_d(x)
                end
            end
            tb = timed_min_root() do
                F = lu(Ah)
                F \ bh
            end
            push_row!(feature="Sparse splu + \\", key="sparse_splu",
                      problem="2-D Laplacian $(grid)×$(grid) (n=$(n), nnz=$(nnz(Ah))), tile=$(b)×$(b)",
                      dagger_s=td, baseline_s=tb,
                      baseline_name="SparseArrays.lu(::CSC) UMFPACK",
                      notes="Dagger.splu gathers; host UMFPACK on local CSC")
        catch err
            push_row!(feature="Sparse splu + \\", key="sparse_splu", problem="(failed)",
                      dagger_s=nothing, baseline_s=nothing, baseline_name="SparseArrays.lu(::CSC) UMFPACK",
                      error=sprint(showerror, err))
        end
    end
end

function bench_assembly()
    grid, b = S.assembly_grid, S.assembly_b
    n = grid * grid
    Random.seed!(1234)
    Ah = laplacian_2d(Float64, grid)
    I, J, V = findnz(Ah)
    part = Blocks(b, b)
    td = with_blas(BLAS_DAGGER) do
        timed_min() do
            A = SparseArrays.sparse(I, J, V, n, n, part)
            wait(A)
        end
    end
    tb = with_blas(BLAS_DAGGER) do
        # baseline: serial CSC then distribute — the path assembly replaces
        timed_min() do
            S = SparseArrays.sparse(I, J, V, n, n)
            A = distribute(S, part)
            wait(A)
        end
    end
    push_row!(feature="Incremental sparse(I,J,V, Blocks)", key="assembly",
              problem="2-D Laplacian COO $(grid)×$(grid) (n=$(n), nnz=$(length(V))), tile=$(b)×$(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="sparse(I,J,V) then distribute",
              notes="baseline includes host CSC construction + distribute")
end

function bench_linearsolve()
    grid, b = S.krylov_grid, S.krylov_b
    n = grid * grid
    Random.seed!(1234)
    Ah = laplacian_2d(Float64, grid)
    bh = rand(n)
    DA = distribute(Ah, Blocks(b, b)); wait(DA)
    Db = distribute(bh, Blocks(b)); wait(Db)

    td = with_blas(BLAS_DAGGER) do
        timed_min() do
            sol = LinearSolve.solve(LinearProblem(DA, Db), KrylovJL_GMRES();
                                    abstol=ATOL, reltol=RTOL)
            wait_d(sol.u)
        end
    end
    tb = timed_min_root() do
        LinearSolve.solve(LinearProblem(Ah, bh), KrylovJL_GMRES();
                          abstol=ATOL, reltol=RTOL)
    end
    push_row!(feature="LinearSolve KrylovJL_GMRES", key="linearsolve_krylov",
              problem="2-D Laplacian $(grid)×$(grid) (n=$(n)), tile=$(b)×$(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="LinearSolve KrylovJL_GMRES(::CSC)",
              notes="same LinearSolve algorithm; Dagger defaultalg would also pick KrylovJL_GMRES for this size if forced, but we pin the alg")

    grid2, b2 = S.direct_grid, S.direct_b
    n2 = grid2 * grid2
    Ah2 = laplacian_2d(Float64, grid2)
    bh2 = rand(n2)
    DA2 = distribute(Ah2, Blocks(b2, b2)); wait(DA2)
    Db2 = distribute(bh2, Blocks(b2)); wait(Db2)
    td = with_blas(BLAS_DAGGER) do
        timed_min() do
            sol = LinearSolve.solve(LinearProblem(DA2, Db2), PureUMFPACKFactorization())
            wait_d(sol.u)
        end
    end
    tb = timed_min_root() do
        LinearSolve.solve(LinearProblem(Ah2, bh2), UMFPACKFactorization())
    end
    push_row!(feature="LinearSolve sparse direct", key="linearsolve_direct",
              problem="2-D Laplacian $(grid2)×$(grid2) (n=$(n2)), tile=$(b2)×$(b2)",
              dagger_s=td, baseline_s=tb,
              baseline_name="LinearSolve UMFPACKFactorization(::CSC)",
              notes="Dagger PureUMFPACKFactorization (gather+splu) vs host SuiteSparse UMFPACK")
end

function bench_operators()
    n, b = S.op_n, S.op_b
    Random.seed!(1234)
    Ah = laplacian_1d(Float64, n)
    Nhost = fill(1 / sqrt(n), n)
    xh = rand(n)
    td = with_blas(BLAS_DAGGER) do
        DA = distribute(Ah, Blocks(b, b)); wait(DA)
        DN = distribute(ones(n), Blocks(b)); wait(DN)
        PA = Dagger.Projected(DA, DN)
        x = distribute(xh, Blocks(b)); wait(x)
        y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
        timed_min() do
            mul!(y, PA, x); wait(y)
        end
    end
    tb = timed_min_root() do
        y = similar(xh)
        host_projected_mul!(y, Ah, reshape(Nhost, n, 1), xh)
    end
    push_row!(feature="Projected mul!", key="projected",
              problem="1-D Laplacian n=$(n), tile=$(b), constant nullspace",
              dagger_s=td, baseline_s=tb,
              baseline_name="serial P A P (orthonormal ones)",
              notes="correctness-adjacent; constructor orthonormalizes")

    n1 = n ÷ 2
    A11h = Matrix(laplacian_1d(Float64, n1))
    A22h = Matrix(laplacian_1d(Float64, n1)) + 3 * I
    A12h = fill(0.05, n1, n1)
    A21h = Matrix(A12h')
    Ahost = [A11h A12h; A21h A22h]
    zh = rand(n)
    td = with_blas(BLAS_DAGGER) do
        A11 = distribute(A11h, Blocks(b, b)); wait(A11)
        A12 = distribute(A12h, Blocks(b, b)); wait(A12)
        A21 = distribute(A21h, Blocks(b, b)); wait(A21)
        A22 = distribute(A22h, Blocks(b, b)); wait(A22)
        BO = Dagger.BlockOperator(A11, A12, A21, A22)
        z = distribute(zh, Blocks(b)); wait(z)
        y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
        timed_min() do
            mul!(y, BO, z); wait(y)
        end
    end
    tb = timed_min_root() do
        Ahost * zh
    end
    push_row!(feature="BlockOperator mul!", key="blockoperator",
              problem="2-field nest n=$(n) (2×$(n1)), tile=$(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="serial *(::Matrix) of assembled nest",
              notes="correctness-adjacent; hvcat would assemble, this stays matrix-free")
end

function bench_krylov_asm_basic()
    _krylov_pair(; feature="Additive Schwarz (BASIC)", key="krylov_asm_basic",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_cg_d, solver_b=_cg_b,
                 host_name="Krylov.cg + hand-rolled ASM BASIC (serial)",
                 build_d=A -> Dagger.AdditiveSchwarzPreconditioner(A; overlap=1, type=:basic),
                 build_b=A -> HostRASBasic(A, S.krylov_b, 1),
                 notes="type=:basic (SPD); overlap=1; host is serial pre-factored ASM")
end

function bench_gmg()
    _krylov_pair(; feature="Krylov CG + GeometricMultigrid", key="krylov_gmg",
                 grid=S.krylov_grid, b=S.krylov_b,
                 solver_d=_cg_d, solver_b=_cg_b,
                 host_name="Krylov.cg + AlgebraicMultigrid SA (no Julia GMG)",
                 build_d=A -> Dagger.GeometricMultigrid(A; grid=(S.krylov_grid, S.krylov_grid),
                                                       presweeps=2, postsweeps=2),
                 build_b=A -> AlgebraicMultigrid.aspreconditioner(AlgebraicMultigrid.smoothed_aggregation(A)),
                 host_ldiv=true,
                 notes="2+2 damped-Jacobi; host baseline is algebraic SA (no ecosystem GMG)")
end

function bench_csr_spmv()
    n, b = S.spmv_n, S.spmv_b
    Random.seed!(1234)
    Ah = laplacian_1d(Float64, n)
    xh = rand(n)
    # Prefer a 2-D-sized 1-D operator already used for SpMV; CSR vs CSC tiles.
    csr_ok = try
        @eval using SparseMatricesCSR
        true
    catch
        false
    end
    csr_ok || (push_row!(feature="CSR SpMV", key="csr_spmv", problem="(skipped)",
                         dagger_s=nothing, baseline_s=nothing, baseline_name="—",
                         error="SparseMatricesCSR not in environment"); return)
    td = with_blas(BLAS_DAGGER) do
        A = SparseMatricesCSR.sparsecsr(Ah, Blocks(b, b)); wait(A)
        x = distribute(xh, Blocks(b)); wait(x)
        y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
        timed_min() do
            mul!(y, A, x); wait(y)
        end
    end
    tb = timed_min_root() do
        Ah * xh
    end
    push_row!(feature="CSR SpMV", key="csr_spmv",
              problem="1-D Laplacian n=$(n), nnz=$(nnz(Ah)), CSR tiles $(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="SparseArrays *(::CSC, ::Vector)",
              notes="Dagger SparseMatrixCSR tiles vs host CSC; compare also to CSC SpMV row")
end

function bench_eigen()
    grid, b = 32, 256
    n = grid * grid
    Random.seed!(1234)
    Ah = laplacian_2d(Float64, grid)
    td = with_blas(BLAS_DAGGER) do
        A = distribute(Ah, Blocks(b, b)); wait(A)
        timed_min() do
            F = eigen(A; nev=1, which=:SR)
            wait_d(F.vectors)
        end
    end
    tb = timed_min_root() do
        eigen(Symmetric(Matrix(Ah)))
    end
    push_row!(feature="eigen (LOBPCG)", key="eigen",
              problem="2-D Laplacian $(grid)×$(grid) (n=$(n)), tile=$(b)×$(b), nev=1",
              dagger_s=td, baseline_s=tb,
              baseline_name="LinearAlgebra.eigen(::Symmetric) dense geev",
              notes="Dagger is LOBPCG (1 pair); host is full dense geev — different work")
end

function bench_numeric_refactor()
    grid, b = S.direct_grid, S.direct_b
    n = grid * grid
    Random.seed!(1234)
    Ah = laplacian_2d(Float64, grid)
    # Same sparsity, different values (KLU numeric update, not symbolic).
    Ah2 = Ah + SparseArrays.spdiagm(0 => fill(0.1, n))
    DA = distribute(Ah, Blocks(b, b)); wait(DA)
    DA2 = distribute(Ah2, Blocks(b, b)); wait(DA2)
    F = Dagger.klu(DA)
    wait_factor(F)
    td = with_blas(BLAS_DAGGER) do
        timed_min() do
            lu!(F, DA2)
            wait_factor(F)
        end
    end
    Fh = PureKLU.klu(Ah)
    tb = timed_min() do
        PureKLU.klu!(Fh, Ah2)
    end
    push_row!(feature="Numeric refactor (klu!)", key="numeric_refactor",
              problem="2-D Laplacian $(grid)×$(grid) (n=$(n)), same pattern, tile=$(b)×$(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="PureKLU.klu!(::CSC)",
              notes="setup excluded; Dagger lu!(F, A) vs host klu!; same sparsity")
end

function bench_mixed_mul()
    n, b = S.spmv_n, S.spmv_b
    Random.seed!(1234)
    Ah = laplacian_1d(Float32, n)
    xh = rand(Float64, n)
    td = with_blas(BLAS_DAGGER) do
        A = distribute(Ah, Blocks(b, b)); wait(A)
        x = distribute(xh, Blocks(b)); wait(x)
        y = Dagger.zeros(Blocks(b), Float64, n); wait(y)
        timed_min() do
            mul!(y, A, x); wait(y)
        end
    end
    tb = timed_min_root() do
        Ah * xh
    end
    push_row!(feature="Mixed-eltype SpMV", key="mixed_mul",
              problem="1-D Laplacian n=$(n), A::Float32 * x::Float64, tile=$(b)",
              dagger_s=td, baseline_s=tb,
              baseline_name="SparseArrays *(::CSC{Float32}, ::Vector{Float64})",
              notes="mixed-eltype mul!; compare also to Float64 SpMV row")
end

function bench_multi_rhs()
    grid, b, nrhs = 32, 256, 4
    n = grid * grid
    Random.seed!(1234)
    Ah = laplacian_2d(Float64, grid)
    Bh = rand(n, nrhs)
    DA = distribute(Ah, Blocks(b, b)); wait(DA)
    DB = distribute(Bh, Blocks(b, nrhs)); wait(DB)
    td = with_blas(BLAS_DAGGER) do
        timed_min() do
            X = DA \ DB
            wait_d(X)
        end
    end
    tb = timed_min_root() do
        Ah \ Bh
    end
    push_row!(feature="Multi-RHS A \\ B", key="multi_rhs",
              problem="2-D Laplacian $(grid)×$(grid) (n=$(n)), $(nrhs) RHS, tile=$(b)×$(nrhs)",
              dagger_s=td, baseline_s=tb,
              baseline_name="SparseArrays \\ (::CSC, ::Matrix) UMFPACK",
              notes="Dagger sparse A\\B is block_gmres; host is UMFPACK multi-RHS — different work")
end

# --- main ------------------------------------------------------------------

function main()
    if is_root()
        println("linalg_integration  mode=", MODE, " scale=", SCALE,
                " threads=", NTHREADS, " ranks=", NRANKS)
        println("julia ", VERSION, "  commit ", META["commit"],
                "  instance ", META["instance"])
        println("warmup=", WARMUP, " samples=", SAMPLES,
                " out=", OUTDIR)
        flush(stdout)
    end
    maybe_barrier()

    @safe_bench "dense_gemm" bench_dense_gemm()
    @safe_bench "dense_lu" bench_dense_factor("Dense LU + \\", "dense_lu", S.lu_n, S.lu_b,
        A -> lu(A, RowMaximum()), A -> lu(A, RowMaximum()),
        "LinearAlgebra.lu(::Matrix) LAPACK getrf")
    @safe_bench "dense_qr" bench_dense_factor("Dense QR + \\", "dense_qr", S.qr_n, S.qr_b,
        qr, qr, "LinearAlgebra.qr(::Matrix) LAPACK geqrf")
    @safe_bench "dense_chol" bench_dense_chol()
    @safe_bench "dense_svd" bench_dense_svd()
    @safe_bench "sparse_spmv" bench_spmv()
    @safe_bench "sparse_spgemm" bench_spgemm()
    if wanted("krylov_cg") || wanted("krylov_gmres")
        try
            bench_krylov_nopc()
        catch err
            is_root() && @error "bench krylov_nopc failed" exception = (err, catch_backtrace())
            push_row!(feature="Krylov no-PC", key="krylov_nopc", problem="(failed)",
                      dagger_s=nothing, baseline_s=nothing, baseline_name="—",
                      error=sprint(showerror, err))
        end
        GC.gc(); maybe_barrier()
    end
    @safe_bench "krylov_jacobi" bench_krylov_jacobi()
    @safe_bench "krylov_blockjacobi" bench_krylov_blockjacobi()
    @safe_bench "krylov_blockilu" bench_krylov_blockilu()
    @safe_bench "krylov_amg_tile" bench_krylov_amg_tile()
    @safe_bench "krylov_globalamg" bench_krylov_globalamg()
    @safe_bench "krylov_asm" bench_krylov_asm()
    if wanted("sparse_chol") || wanted("sparse_klu") || wanted("sparse_splu")
        try
            bench_sparse_direct()
        catch err
            is_root() && @error "bench sparse_direct failed" exception = (err, catch_backtrace())
            push_row!(feature="sparse_direct", key="sparse_direct", problem="(failed)",
                      dagger_s=nothing, baseline_s=nothing, baseline_name="—",
                      error=sprint(showerror, err))
        end
        GC.gc(); maybe_barrier()
    end
    @safe_bench "assembly" bench_assembly()
    @safe_bench "linearsolve_krylov" bench_linearsolve()
    @safe_bench "projected" bench_operators()
    @safe_bench "krylov_asm_basic" bench_krylov_asm_basic()
    @safe_bench "krylov_gmg" bench_gmg()
    @safe_bench "csr_spmv" bench_csr_spmv()
    @safe_bench "eigen" bench_eigen()
    @safe_bench "numeric_refactor" bench_numeric_refactor()
    @safe_bench "mixed_mul" bench_mixed_mul()
    @safe_bench "multi_rhs" bench_multi_rhs()

    if is_root()
        println()
        println(markdown_table(ROWS))
        println("Wrote ", joinpath(OUTDIR, "linalg_integration_$(MODE == "mpi" ? "mpi" : "mt").json"))
    end
    return ROWS
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
