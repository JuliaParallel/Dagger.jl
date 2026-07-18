# Benchmark suites, expressed as Autotune operations.
#
# This file is the bridge between the regression benchmark's suites and the
# shared benchmark engine in `src/autotune/`. Instead of BenchmarkTools
# `@benchmarkable`s, each benchmark case is registered as a `benchmark_only`
# Autotune `OperationSpec` (never runtime-selectable by `invoke_best`) plus a
# single algorithm that runs the exact Dagger/native operation. The Autotune
# engine then provides the timing loop, per-sample measurement, subprocess/OOM
# orchestration, and result storage that used to live in `benchmarks.jl`.
#
# Execution variants (exec × backend)
# -----------------------------------
# The `BENCHMARK` spec `suite:method[+accel]` selects, per suite, a set of
# variants encoded in each trial's *features*:
#   * exec    ∈ {"dagger", "raw"}   (method): Dagger `DArray` vs a native array
#   * backend ∈ {"cpu","cuda","amdgpu","opencl","metal","oneapi"} (accel)
# A GPU backend runs Dagger ops inside `Dagger.with_options(;scope=<gpu scope>)`
# (so DArray chunks are GPU-allocated), or, for `raw`, on native GPU arrays with
# no Dagger. `raw` variants exist only for ops that provide a native path (dense
# array/linalg); stencil/sparse are Dagger-only. Backends whose package is
# absent/non-functional are gated out by `supports`.
#
# This file is included by BOTH the orchestrator (to enumerate trials) and every
# benchmark worker (to actually run them), so it must be side-effect-idempotent
# and assumes Dagger + Dagger.Autotune are already loaded (plus any requested
# accel package, which activates the corresponding Dagger GPU extension).
# Registration of newer features (@stencil, sparse LA, Dagger.cg) is
# capability-guarded so an older baseline revision simply doesn't register them.

using Dagger
using LinearAlgebra
using SparseArrays
# `Distributed` must be imported in this module (not merely loaded) because
# `_bench_finalize` uses the `Distributed.@everywhere` macro, which is expanded
# when that function is *defined* (at include time), not when it runs.
import Distributed

const AT = Dagger.Autotune
const T_BENCH = Float64

# op symbol => (suite, case) label, for reconstructing AirspeedVelocity keypaths.
const SUITE_OP_META = Dict{Symbol,NamedTuple{(:suite, :case),Tuple{String,String}}}()
# suite name => op symbols registered under it (in registration order).
const SUITE_OPS_BY_SUITE = Dict{String,Vector{Symbol}}()

# ---------------------------------------------------------------------------
# Accelerator backends
#
# GPU support is built into Dagger via package extensions: loading the backend
# package (e.g. `AMDGPU`) activates the extension and registers its GPU
# processors. A scope built with `Dagger.scope(<scope_kw>=1)` restricts a
# computation to that backend's GPU, and allocations/ops inside
# `Dagger.with_options(;scope)` run on (and allocate on) the GPU.

struct BenchAccel
    pkg::String            # package to load ("AMDGPU"); activates the Dagger ext
    array_type::String     # native array type name ("ROCArray")
    scope_kw::Symbol       # Dagger.scope keyword (:rocm_gpu)
    functional::Function   # (mod) -> Bool : is a usable device present?
    sync::Function         # (mod) -> nothing : block until device work completes
    # Optional driver/runtime packages loaded best-effort alongside `pkg` (e.g. a
    # CPU OpenCL ICD when no system/GPU driver is installed). Absent ones are
    # silently ignored.
    drivers::Vector{String}
end
BenchAccel(pkg, at, kw, fn, sy; drivers=String[]) =
    BenchAccel(pkg, at, kw, fn, sy, drivers)

const BENCH_ACCELS = Dict{String,BenchAccel}(
    "cuda"   => BenchAccel("CUDA", "CuArray", :cuda_gpu,
                           m -> Bool(m.functional()), m -> m.synchronize()),
    "amdgpu" => BenchAccel("AMDGPU", "ROCArray", :rocm_gpu,
                           m -> Bool(m.functional()), m -> m.synchronize()),
    "opencl" => BenchAccel("OpenCL", "CLArray", :cl_device,
                           m -> !isempty(m.cl.platforms()),
                           m -> m.cl.finish(m.cl.queue()); drivers=["pocl_jll"]),
    "metal"  => BenchAccel("Metal", "MtlArray", :metal_gpu,
                           m -> Bool(m.functional()), m -> m.synchronize()),
    "oneapi" => BenchAccel("oneAPI", "oneArray", :intel_gpu,
                           m -> Bool(m.functional()), m -> m.synchronize()),
)

_accel_mod(name) = AT.loaded_module(BENCH_ACCELS[name].pkg)

function _accel_functional(name)
    haskey(BENCH_ACCELS, name) || return false
    m = _accel_mod(name)
    m === nothing && return false
    try
        BENCH_ACCELS[name].functional(m)
    catch
        false
    end
end

_accel_scope(name) = Dagger.scope(; BENCH_ACCELS[name].scope_kw => 1)

function _accel_sync(name)
    m = _accel_mod(name)
    m === nothing && return nothing
    try
        BENCH_ACCELS[name].sync(m)
    catch
    end
    return nothing
end

# Allocate a native (non-Dagger) NxN/Nx1 array of `kind` on `backend`. For a GPU
# backend it constructs the backend's array type from a host array (uniform
# across backends; `rand`/`ones`/`zeros` device constructors vary).
function _native_alloc(backend::AbstractString, kind::Symbol, ::Type{T}, dims...) where {T}
    host = kind === :rand ? rand(T, dims...) :
           kind === :ones ? ones(T, dims...) : zeros(T, dims...)
    backend == "cpu" && return host
    m = _accel_mod(backend)
    m === nothing && error("accelerator package for '$backend' not loaded")
    return getfield(m, Symbol(BENCH_ACCELS[backend].array_type))(host)
end

# Requested (exec, backend) variants for a suite, parsed from the `BENCHMARK`
# spec (`benches`, defined in common.jl). Empty accels => cpu.
function _suite_variants(suite::AbstractString)
    out = Tuple{String,String}[]
    for e in get(benches, suite, [])
        exec = String(e.method)
        if isempty(e.accels)
            push!(out, (exec, "cpu"))
        else
            for acc in e.accels
                push!(out, (exec, String(acc)))
            end
        end
    end
    return isempty(out) ? [("dagger", "cpu")] : unique(out)
end

# Distributed-aware per-sample cleanup: mirrors the old suites' `teardown`
# (`@everywhere GC.gc()`), reclaiming each sample's distributed garbage.
_bench_finalize() = begin
    if Distributed.nprocs() > 1
        try
            Distributed.@everywhere GC.gc()
        catch
            GC.gc()
        end
    else
        GC.gc()
    end
    nothing
end

_dwait(x) = (x isa Dagger.DArray && wait(x); x)

"""
    register_bench_op!(; suite, case, op, gen, invoke, peak_bytes,
                         structure="dense", raw_gen=nothing, raw_invoke=nothing)

Register one benchmark case as a `benchmark_only` Autotune operation. The Dagger
path uses `gen(features) -> Tuple` (builds the distributed operands) and
`invoke(ctx, operands...)` (the timed Dagger call, which should `wait`/collect so
the work is complete). GPU-`dagger` variants reuse these, wrapped in the
backend's GPU scope. If `raw_gen(features, backend) -> Tuple` and
`raw_invoke(ctx, operands...)` are supplied, `raw` (non-Dagger, native-array)
variants are also offered (device sync is added automatically).

`peak_bytes(features)` is the estimated peak *resident* footprint; it is divided
by three to feed the engine's `input_bytes * 3` memory-budget heuristic, so the
effective skip threshold matches the old `fits_budget(peak)` decision exactly.
"""
function register_bench_op!(; suite::String, case::String, op::Symbol,
                              gen::Function, invoke::Function,
                              peak_bytes::Function, structure::String="dense",
                              raw_gen=nothing, raw_invoke=nothing)
    # `block` (and sparse `density`) are deterministic functions of `N`, so they
    # are NOT stored as features: doing so would make the engine's
    # monotonic-scale "series" key (which excludes only the size keys m/n/k/...)
    # vary per scale, fragmenting each op into one single-scale series and
    # defeating the OOM scale-skip. They're recovered from `n` on demand (see
    # `_blk`/`_density` and the keypath assembly in `benchmarks.jl`).
    enumf = function (axes)
        out = Dict{String,Any}[]
        for (exec, backend) in _suite_variants(suite)
            atype = exec == "dagger" ? "DArray" :
                    backend == "cpu" ? "Array" : BENCH_ACCELS[backend].array_type
            for N in get(axes, "size", scales)
                push!(out, Dict{String,Any}(
                    "exec" => exec, "backend" => backend, "array_type" => atype,
                    "structure" => structure, "eltype" => "Float64",
                    "locality" => "local", "m" => Int(N), "n" => Int(N)))
            end
        end
        return out
    end

    generate_inputs = function (f)
        exec = get(f, "exec", "dagger"); backend = get(f, "backend", "cpu")
        if exec == "raw"
            ops = raw_gen === nothing ? () : raw_gen(f, backend)
            backend == "cpu" || _accel_sync(backend)
            return ops
        elseif backend == "cpu"
            return gen(f)
        else
            ops = Dagger.with_options(; scope=_accel_scope(backend)) do
                gen(f)
            end
            _accel_sync(backend)
            return ops
        end
    end

    run_invoke = function (ctx, args...)
        f = ctx.features; exec = get(f, "exec", "dagger"); backend = get(f, "backend", "cpu")
        if exec == "raw"
            r = raw_invoke(ctx, args...)
            backend == "cpu" || _accel_sync(backend)
            return r
        elseif backend == "cpu"
            return invoke(ctx, args...)
        else
            r = Dagger.with_options(; scope=_accel_scope(backend)) do
                invoke(ctx, args...)
            end
            _accel_sync(backend)
            return r
        end
    end

    supports = function (f)
        exec = get(f, "exec", "dagger"); backend = get(f, "backend", "cpu")
        exec == "raw" && raw_gen === nothing && return false
        backend == "cpu" && return true
        return _accel_functional(backend)
    end

    opspec = AT.OperationSpec(;
        name = op, benchmark_only = true,
        scale = f -> Float64(f["n"]),
        input_bytes = f -> peak_bytes(f) / 3,
        output_bytes = f -> 0.0,
        extract_features = (xs...; accuracy=nothing) -> Dict{String,Any}(),
        generate_inputs = generate_inputs,
        enumerate_features = enumf,
        default_axes = Dict{String,Vector}("size" => collect(scales)),
        finalize_sample = _bench_finalize,
    )
    AT.register_operation!(opspec)
    AT.register_algorithm!(AT.AlgorithmSpec(;
        name = :bench, op = op, package = "Dagger", input_form = :DArray,
        supports = supports, invoke = run_invoke))
    SUITE_OP_META[op] = (suite = suite, case = case)
    push!(get!(() -> Symbol[], SUITE_OPS_BY_SUITE, suite), op)
    return op
end

# Feature helpers. `block`/`density` are derived from the problem size `N`
# (never stored in the feature dict; see `enumf`), so a suite op forms one
# monotonic-scale series across all of its scales.
_N(f) = Int(f["n"])
_blk(f) = get(f, "structure", "dense") == "sparse" ? banded_block(_N(f)) : square_block(_N(f))
_density(f) = min(0.1, 16 / _N(f))

# ---------------------------------------------------------------------------
# array: elementwise / reduction DArray ops
#
# All cases hold at most the input plus a same-size result (nmats=2), matching
# the old `array_suite` budget.

let peak = f -> dense_bytes(_N(f); nmats=2, T=T_BENCH)
    mkX = f -> (b = _blk(f); N = _N(f); X = rand(Blocks(b, b), T_BENCH, N, N); wait(X); (X,))
    # Native (non-Dagger) operand: a plain (host or device) NxN matrix.
    rawX = (f, backend) -> (_native_alloc(backend, :rand, T_BENCH, _N(f), _N(f)),)

    register_bench_op!(suite="array", case="alloc (rand)", op=:array_alloc,
        gen = f -> (), peak_bytes = peak,
        invoke = (ctx) -> (f = ctx.features; b = _blk(f); N = _N(f);
                           wait(rand(Blocks(b, b), T_BENCH, N, N))),
        raw_gen = (f, backend) -> (),
        raw_invoke = (ctx) -> (f = ctx.features;
                               _native_alloc(f["backend"], :rand, T_BENCH, _N(f), _N(f))))
    register_bench_op!(suite="array", case="broadcast (X .+ 1)", op=:array_broadcast,
        gen = mkX, peak_bytes = peak, invoke = (ctx, X) -> wait(X .+ 1),
        raw_gen = rawX, raw_invoke = (ctx, X) -> X .+ 1)
    register_bench_op!(suite="array", case="add (X + X)", op=:array_add,
        gen = mkX, peak_bytes = peak, invoke = (ctx, X) -> wait(X + X),
        raw_gen = rawX, raw_invoke = (ctx, X) -> X + X)
    register_bench_op!(suite="array", case="map (sin.(X))", op=:array_map,
        gen = mkX, peak_bytes = peak, invoke = (ctx, X) -> wait(sin.(X)),
        raw_gen = rawX, raw_invoke = (ctx, X) -> sin.(X))
    register_bench_op!(suite="array", case="transpose (permutedims)", op=:array_transpose,
        gen = mkX, peak_bytes = peak, invoke = (ctx, X) -> wait(permutedims(X)),
        raw_gen = rawX, raw_invoke = (ctx, X) -> permutedims(X))
    register_bench_op!(suite="array", case="reduce (sum)", op=:array_reduce,
        gen = mkX, peak_bytes = peak, invoke = (ctx, X) -> sum(X),
        raw_gen = rawX, raw_invoke = (ctx, X) -> sum(X))
    register_bench_op!(suite="array", case="norm", op=:array_norm,
        gen = mkX, peak_bytes = peak, invoke = (ctx, X) -> norm(X),
        raw_gen = rawX, raw_invoke = (ctx, X) -> norm(X))
end

# ---------------------------------------------------------------------------
# linalg: dense distributed linear algebra
#
# Square matrices with a square block grid. Operands are built (distributed) in
# `gen` (untimed) and the factorization/product is timed; the non-bang
# factorizations copy internally, so reusing the operand across samples is safe.

let mkA = f -> (b = _blk(f); N = _N(f); A = rand(Blocks(b, b), T_BENCH, N, N); wait(A); (A,))
    # gemm/matvec need A (+ result) resident; factorizations copy internally.
    peak3 = f -> dense_bytes(_N(f); nmats=3, T=T_BENCH)
    peak1 = f -> dense_bytes(_N(f); nmats=1, T=T_BENCH)
    peak4 = f -> dense_bytes(_N(f); nmats=4, T=T_BENCH)

    # Native (non-Dagger) operands. Dense linalg on the GPU array types dispatches
    # to the vendor BLAS/LAPACK (rocBLAS/rocSOLVER, CUBLAS/CUSOLVER, ...) via the
    # generic LinearAlgebra methods, so the timed calls below are backend-agnostic.
    rawA = (f, backend) -> (_native_alloc(backend, :rand, T_BENCH, _N(f), _N(f)),)
    rawAb = (f, backend) -> (_native_alloc(backend, :rand, T_BENCH, _N(f), _N(f)),
                             _native_alloc(backend, :rand, T_BENCH, _N(f)))
    # G*Gᵀ is SPD for a full-rank random G (true w.h.p.), so cholesky succeeds.
    rawSPD = (f, backend) -> (G = _native_alloc(backend, :rand, T_BENCH, _N(f), _N(f)); (G * G',))

    register_bench_op!(suite="linalg", case="matmul (A*A)", op=:linalg_matmul,
        gen = mkA, peak_bytes = peak3, invoke = (ctx, A) -> wait(A * A),
        raw_gen = rawA, raw_invoke = (ctx, A) -> A * A)
    register_bench_op!(suite="linalg", case="syrk (A'*A)", op=:linalg_syrk,
        gen = mkA, peak_bytes = peak3, invoke = (ctx, A) -> wait(A' * A),
        raw_gen = rawA, raw_invoke = (ctx, A) -> A' * A)
    register_bench_op!(suite="linalg", case="lu", op=:linalg_lu,
        gen = mkA, peak_bytes = peak3,
        invoke = (ctx, A) -> wait(lu(A, RowMaximum()).factors),
        raw_gen = rawA, raw_invoke = (ctx, A) -> lu(A))
    register_bench_op!(suite="linalg", case="qr", op=:linalg_qr,
        gen = mkA, peak_bytes = peak3, invoke = (ctx, A) -> wait(qr(A).factors),
        raw_gen = rawA, raw_invoke = (ctx, A) -> qr(A))
    register_bench_op!(suite="linalg", case="solve (A\\b via lu)", op=:linalg_solve,
        gen = f -> (b = _blk(f); N = _N(f);
                    A = rand(Blocks(b, b), T_BENCH, N, N);
                    rhs = rand(Blocks(b), T_BENCH, N); wait(A); (A, rhs)),
        peak_bytes = peak3, invoke = (ctx, A, rhs) -> wait(lu(A, RowMaximum()) \ rhs),
        raw_gen = rawAb, raw_invoke = (ctx, A, rhs) -> A \ rhs)
    register_bench_op!(suite="linalg", case="cholesky", op=:linalg_cholesky,
        gen = f -> (b = _blk(f); N = _N(f);
                    G = rand(Blocks(b, b), T_BENCH, N, N); A = G * G'; wait(A); (A,)),
        peak_bytes = peak4, invoke = (ctx, A) -> wait(cholesky(A).factors),
        raw_gen = rawSPD, raw_invoke = (ctx, A) -> cholesky(Hermitian(A)))
    register_bench_op!(suite="linalg", case="matvec (A*x)", op=:linalg_matvec,
        gen = f -> (b = _blk(f); N = _N(f);
                    A = rand(Blocks(b, b), T_BENCH, N, N);
                    x = rand(Blocks(b), T_BENCH, N); wait(A); (A, x)),
        peak_bytes = peak1, invoke = (ctx, A, x) -> wait(A * x),
        raw_gen = rawAb, raw_invoke = (ctx, A, x) -> A * x)
end

# ---------------------------------------------------------------------------
# sparse: sparse distributed linear algebra (banded operators, nnz ~ O(N))

laplacian_1d(::Type{T}, n) where {T} = SparseArrays.spdiagm(
    -1 => fill(-one(T), n - 1),
     0 => fill(T(4), n),
     1 => fill(-one(T), n - 1),
)

let spmv_ok = try
        S = distribute(sprand(T_BENCH, 8, 8, 0.5), Blocks(4, 4))
        x = distribute(rand(T_BENCH, 8), Blocks(4)); wait(S * x); true
    catch err
        @warn "Skipping sparse spmv (unsupported by this Dagger)" exception=err
        false
    end
    spgemm_ok = try
        S = distribute(sprand(T_BENCH, 8, 8, 0.5), Blocks(4, 4)); wait(S * S); true
    catch err
        @warn "Skipping sparse spgemm (unsupported by this Dagger)" exception=err
        false
    end
    cg_ok = isdefined(Dagger, :cg) && try
        A = distribute(laplacian_1d(T_BENCH, 8), Blocks(4, 4))
        rhs = distribute(rand(T_BENCH, 8), Blocks(4))
        wait(first(Dagger.cg(A, rhs; atol=1e-8, rtol=1e-6, itmax=50))); true
    catch err
        @warn "Skipping sparse cg solve (Krylov/Dagger.cg unavailable)" exception=err
        false
    end

    if spmv_ok
        register_bench_op!(suite="sparse", case="spmv (S*x)", op=:sparse_spmv,
            structure="sparse",
            gen = f -> (b = _blk(f); N = _N(f); d = _density(f);
                        S = distribute(sprand(T_BENCH, N, N, d), Blocks(b, b));
                        x = distribute(rand(T_BENCH, N), Blocks(b)); wait(S); (S, x)),
            peak_bytes = f -> sparse_bytes(_N(f); nmats=2, density=_density(f), T=T_BENCH),
            invoke = (ctx, S, x) -> wait(S * x))
    end
    if spgemm_ok
        register_bench_op!(suite="sparse", case="spgemm (S*S)", op=:sparse_spgemm,
            structure="sparse",
            gen = f -> (b = _blk(f); N = _N(f); d = _density(f);
                        S = distribute(sprand(T_BENCH, N, N, d), Blocks(b, b)); wait(S); (S,)),
            peak_bytes = f -> sparse_bytes(_N(f); nmats=6, density=_density(f), T=T_BENCH),
            invoke = (ctx, S) -> wait(S * S))
    end
    if cg_ok
        register_bench_op!(suite="sparse", case="cg solve (laplacian)", op=:sparse_cg,
            structure="sparse",
            gen = f -> (b = _blk(f); N = _N(f);
                        A = distribute(laplacian_1d(T_BENCH, N), Blocks(b, b));
                        rhs = distribute(rand(T_BENCH, N), Blocks(b)); wait(A); (A, rhs)),
            peak_bytes = f -> sparse_bytes(_N(f); nmats=2, density=3 / _N(f), T=T_BENCH),
            invoke = (ctx, A, rhs) ->
                wait(first(Dagger.cg(A, rhs; atol=1e-8, rtol=1e-6, itmax=200))))
    end
end

# ---------------------------------------------------------------------------
# stencil: @stencil kernels
#
# Kernels are plain functions (BenchmarkTools couldn't parse @stencil inside
# @benchmarkable; the engine has no such restriction, but keeping them as
# functions mirrors the old suite and isolates the capability guard). In-place
# kernels fully overwrite (or monotonically accumulate into) their output each
# call, so the amount of work per sample is identical without re-priming.

let stencil_ok = try
        @eval import Dagger: @stencil, Wrap, Pad, Reflect, Clamp
        true
    catch err
        @warn "Skipping stencil suite (@stencil unavailable in this Dagger)" exception=err
        false
    end

    if stencil_ok
        @eval begin
            function _st_assign!(B, ::Type{S}) where {S}
                @stencil B[idx] = one(S)
                return B
            end
            function _st_neighbors_wrap!(A, B)
                @stencil B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
                return B
            end
            function _st_neighbors_pad!(A, B)
                @stencil B[idx] = sum(@neighbors(A[idx], 1, Pad(0)))
                return B
            end
            function _st_neighbors_clamp!(A, B)
                @stencil B[idx] = sum(@neighbors(A[idx], 1, Clamp()))
                return B
            end
            function _st_neighbors_reflect!(A, B)
                @stencil B[idx] = sum(@neighbors(A[idx], 1, Reflect(true)))
                return B
            end
            _st_alloc_wrap(A) = @stencil sum(@neighbors(A[idx], 1, Wrap()))
            function _st_update_plus!(A, B)
                @stencil B[idx] = B[idx] + A[idx]
                return B
            end
            function _st_multi!(A, B, ::Type{S}) where {S}
                @stencil begin
                    A[idx] = one(S)
                    B[idx] = A[idx] * 2
                end
                return B
            end
        end

        peak2 = f -> dense_bytes(_N(f); nmats=2, T=T_BENCH)
        peak3 = f -> dense_bytes(_N(f); nmats=3, T=T_BENCH)
        mkAB = f -> (b = _blk(f); N = _N(f);
                     A = ones(Blocks(b, b), T_BENCH, N, N);
                     B = zeros(Blocks(b, b), T_BENCH, N, N); wait(A); (A, B))
        mkB = f -> (b = _blk(f); N = _N(f); B = zeros(Blocks(b, b), T_BENCH, N, N); wait(B); (B,))
        mkA = f -> (b = _blk(f); N = _N(f); A = ones(Blocks(b, b), T_BENCH, N, N); wait(A); (A,))

        register_bench_op!(suite="stencil", case="assign (const)", op=:stencil_assign,
            gen = mkB, peak_bytes = peak2, invoke = (ctx, B) -> _st_assign!(B, T_BENCH))
        register_bench_op!(suite="stencil", case="neighbors (Wrap)", op=:stencil_wrap,
            gen = mkAB, peak_bytes = peak2, invoke = (ctx, A, B) -> _st_neighbors_wrap!(A, B))
        register_bench_op!(suite="stencil", case="neighbors (Pad)", op=:stencil_pad,
            gen = mkAB, peak_bytes = peak2, invoke = (ctx, A, B) -> _st_neighbors_pad!(A, B))
        register_bench_op!(suite="stencil", case="neighbors (Clamp)", op=:stencil_clamp,
            gen = mkAB, peak_bytes = peak2, invoke = (ctx, A, B) -> _st_neighbors_clamp!(A, B))
        register_bench_op!(suite="stencil", case="neighbors (Reflect)", op=:stencil_reflect,
            gen = mkAB, peak_bytes = peak2, invoke = (ctx, A, B) -> _st_neighbors_reflect!(A, B))
        register_bench_op!(suite="stencil", case="update (+)", op=:stencil_update,
            gen = mkAB, peak_bytes = peak2, invoke = (ctx, A, B) -> _st_update_plus!(A, B))
        register_bench_op!(suite="stencil", case="multi-expr", op=:stencil_multi,
            gen = mkAB, peak_bytes = peak2, invoke = (ctx, A, B) -> _st_multi!(A, B, T_BENCH))
        register_bench_op!(suite="stencil", case="alloc (neighbors Wrap)", op=:stencil_alloc,
            gen = mkA, peak_bytes = peak3, invoke = (ctx, A) -> wait(_st_alloc_wrap(A)))
    end
end

nothing
