# registry.jl
#
# Three registries drive everything:
#
#   OPERATIONS  - what can be autotuned (:lu, :cholesky, :qr, :solve, :gemm,
#                 :transfer, ...): how to extract features from real inputs,
#                 how to synthesize benchmark inputs from a feature dict, how
#                 to estimate work (the interpolation coordinate), and how to
#                 validate results (e.g. solve residuals).
#
#   ALGORITHMS  - candidate implementations per operation. Each declares its
#                 owning package (checked against `Base.loaded_modules`, never
#                 hard-depended-on), the container form it consumes, a
#                 `supports` predicate over features, per-algorithm FREE
#                 configuration axes (e.g. DArray block size), and an invoke
#                 function. Dagger is *not* privileged: BLAS, CUDA, KLU,
#                 Krylov, LinearSolve, ... algorithms are first-class.
#
#   CONVERTERS  - single-hop data movement between container forms, each
#                 tagged with a "channel" (:h2d, :d2h, :distribute, :collect,
#                 :memcpy) whose bandwidth is itself benchmarked (op
#                 :transfer) so movement costs are predicted from measurement.

# ---------------------------------------------------------------------------
# Raw implementation hooks
#
# Dagger's own algorithms must be invoked through these hooks rather than
# through the public entrypoints, because the public entrypoints call back
# into Autotune (infinite recursion otherwise). Dagger's __init__ populates
# them:
#
#     Autotune.set_raw_impl!(:dagger_lu, A -> Dagger._lu_nopivot!(copy(A)))
#     Autotune.set_raw_impl!(:dagger_gemm, (A, B) -> Dagger._matmul(A, B))
#     ...

const RAW_IMPLS = Dict{Symbol,Function}()

set_raw_impl!(name::Symbol, f::Function) = (RAW_IMPLS[name] = f; nothing)
has_raw_impl(name::Symbol) = haskey(RAW_IMPLS, name)
function raw_impl(name::Symbol)
    haskey(RAW_IMPLS, name) && return RAW_IMPLS[name]
    error("Autotune: no raw implementation registered for $name. " *
          "Call Autotune.set_raw_impl!(:$name, f) during package init.")
end

# ---------------------------------------------------------------------------
# Operations

"""
    OperationSpec

Describes an autotunable operation.

  * `nargs`             - number of positional array inputs.
  * `scale(features)`   - work estimate (~flops or bytes). Used as the
                          interpolation coordinate in the cost model, so
                          near-affine time-vs-scale is expected within one
                          (algorithm, categorical-config) group. Mild
                          misestimates are harmless; actual timings always
                          come from the benchmark database.
  * `input_bytes(features)`  - memory footprint estimate, for OOM avoidance.
  * `output_bytes(features)` - result footprint, `0.0` when the result isn't
                          a plain array (e.g. factorization objects).
  * `array_output`      - whether the result is a plain array that can be
                          converted back to the caller's container form.
  * `extract_features(inputs...; accuracy)` - features from *real* inputs.
  * `generate_inputs(features)` - synthesize benchmark inputs in the *base*
                          form (`Array`/`SparseMatrixCSC`); the worker then
                          converts to `features["array_type"]`.
  * `enumerate_features(axes)` - expand benchmark axes into feature dicts.
  * `check(features, base_inputs, result)` - `nothing` if the result is
                          acceptable, else an error string (e.g. residual
                          tolerance not met). Receives the CPU-collected
                          result.
  * `mutated_args`       - positional input indices (1-based) that an
                          algorithm's `invoke` mutates in place rather than
                          returning a freshly-allocated result (e.g. `[1]`
                          for `:lu!`'s `A`, `[1]` for `:gemm!`'s `C`, `[1,2]`
                          for `:solve!`'s `A` and `b`). Empty for ordinary
                          out-of-place operations. Two consequences:
                            - `Autotune.benchmark` re-primes these arguments
                              from a pristine copy before every timed sample
                              (repeatedly re-invoking an in-place algorithm on
                              its own output would otherwise measure garbage,
                              e.g. re-factoring an already-factored matrix).
                            - `invoke_best`/`execute_plan` copy the algorithm's
                              (possibly container-converted) result back into
                              the caller's original array(s) after invoking,
                              so the `!` in-place contract holds regardless of
                              which container form ended up being fastest.
"""
Base.@kwdef struct OperationSpec
    name::Symbol
    nargs::Int = 1
    scale::Function
    input_bytes::Function
    output_bytes::Function = f -> 0.0
    array_output::Bool = false
    output_ndims::Int = 2
    extract_features::Function
    generate_inputs::Function
    enumerate_features::Function
    default_axes::Dict{String,Vector} = Dict{String,Vector}()
    check::Function = (features, base_inputs, result) -> nothing
    mutated_args::Vector{Int} = Int[]
end

const OPERATIONS = Dict{Symbol,OperationSpec}()

register_operation!(op::OperationSpec) = (OPERATIONS[op.name] = op; op)
operation(name::Symbol) = get(OPERATIONS, name) do
    error("Autotune: unknown operation :$name (registered: $(collect(keys(OPERATIONS))))")
end

# ---------------------------------------------------------------------------
# Algorithms

"""
    InvokeContext

Handed to every algorithm invoke function. `mod` is the algorithm's owning
module (resolved from `Base.loaded_modules`; `nothing` for stdlib/hook-based
algorithms), `config` holds chosen FREE/MUTABLE configuration values (e.g.
`"blocksize"`), and `accuracy` the requested tolerance (if any).
"""
struct InvokeContext
    mod::Union{Module,Nothing}
    features::Dict{String,Any}
    config::Dict{String,Any}
    accuracy::Union{Nothing,Float64}
end

config_get(ctx::InvokeContext, key::String, default) = get(ctx.config, key, default)
accuracy_or(ctx::InvokeContext, default) = ctx.accuracy === nothing ? default : ctx.accuracy

"""
    AlgorithmSpec

A candidate implementation of an operation. `prepare` runs untimed before
benchmarking (e.g. staging data onto a device for `:transfer/:d2h`);
`invoke` is the timed/selected call.

  * `in_place` - documents that `invoke` mutates its arguments (per the
                owning `OperationSpec.mutated_args`) rather than allocating a
                fresh result; purely informational (used by `report`), the
                actual re-priming/copy-back behavior is driven by
                `OperationSpec.mutated_args`.
"""
Base.@kwdef struct AlgorithmSpec
    name::Symbol
    op::Symbol
    package::Union{Nothing,String} = nothing
    input_form::Symbol = :Array
    supports::Function = features -> true
    prepare::Function = (ctx, inputs...) -> inputs
    invoke::Function
    free_config::Dict{String,Vector} = Dict{String,Vector}()
    in_place::Bool = false
end

# op => (name => spec)
const ALGORITHMS = Dict{Symbol,Dict{Symbol,AlgorithmSpec}}()

function register_algorithm!(alg::AlgorithmSpec)
    d = get!(() -> Dict{Symbol,AlgorithmSpec}(), ALGORITHMS, alg.op)
    d[alg.name] = alg
    return alg
end

algorithms_for(op::Symbol) = collect(values(get(ALGORITHMS, op, Dict{Symbol,AlgorithmSpec}())))
function algorithm(op::Symbol, name::Symbol)
    d = get(ALGORITHMS, op, nothing)
    d === nothing && return nothing
    return get(d, name, nothing)
end

algorithm_available(alg::AlgorithmSpec) =
    package_available(alg.package) &&
    (!startswith(String(alg.name), "dagger_") || has_raw_impl(alg.name))

algorithm_module(alg::AlgorithmSpec) =
    alg.package === nothing ? nothing : loaded_module(alg.package)

# GPU functionality gates, keyed by package name. Consulted by the benchmark
# worker after loading the package (a machine can have CUDA.jl installed but
# no usable GPU).
const FUNCTIONAL_CHECKS = Dict{String,Function}(
    "CUDA"   => mod -> Bool(mod.functional()),
    "AMDGPU" => mod -> Bool(mod.functional()),
)

# Per-form device synchronization, called inside the timed region so GPU
# kernels are actually measured. Extend for new device array types.
const SYNC_HOOKS = Dict{Symbol,Function}(
    :CuArray  => () -> (m = loaded_module("CUDA"); m === nothing || m.synchronize(); nothing),
    :ROCArray => () -> (m = loaded_module("AMDGPU"); m === nothing || m.synchronize(); nothing),
)

sync_form(form::Symbol) = begin
    hook = get(SYNC_HOOKS, form, nothing)
    hook === nothing || hook()
    nothing
end

# ---------------------------------------------------------------------------
# Converters

"""
    Converter

Single-hop data movement between container forms. `convert(x, config)` may
consult FREE configuration (e.g. `"blocksize"` for `Array -> DArray`).
`bytes(x)` estimates the payload for bandwidth-based cost prediction.
"""
struct Converter
    from::Symbol
    to::Symbol
    package::Union{Nothing,String}
    channel::Symbol
    convert::Function
    bytes::Function
end

const CONVERTERS = Dict{Tuple{Symbol,Symbol},Converter}()

register_converter!(c::Converter) = (CONVERTERS[(c.from, c.to)] = c; c)
converter(from::Symbol, to::Symbol) = get(CONVERTERS, (from, to), nothing)

converter_available(::Nothing) = false
converter_available(c::Converter) = package_available(c.package)

# Fallback bandwidths (bytes/second) when no :transfer benchmarks exist yet.
const FALLBACK_BANDWIDTH = Dict{Symbol,Float64}(
    :h2d        => 8.0e9,
    :d2h        => 8.0e9,
    :memcpy     => 2.0e10,
    :distribute => 4.0e9,
    :collect    => 4.0e9,
)

dense_bytes(x::AbstractArray) = Float64(length(x) * sizeof(eltype(x)))
function payload_bytes(x)
    if x isa SparseArrays.AbstractSparseMatrix
        return Float64(SparseArrays.nnz(x) * (sizeof(eltype(x)) + sizeof(Int)))
    elseif x isa AbstractArray
        return dense_bytes(x)
    else
        return 0.0
    end
end

# ---------------------------------------------------------------------------
# Feature enumeration helpers (benchmark-time)

_as_dims(sz::Integer, ndims::Int) = ntuple(_ -> Int(sz), ndims)
_as_dims(sz::Union{Tuple,AbstractVector}, ndims::Int) =
    ntuple(i -> Int(sz[min(i, length(sz))]), ndims)

"""
    matrix_feature_enumerator(; dims=2, sparse_ok=false, accuracy_axis=false,
                                spd_axis=false)

Build the default `enumerate_features` function for a matrix operation.
Crosses `array_type × size × eltype` (plus `density` for sparse forms,
`accuracy`/`spd` when enabled). `dims=2` produces `m,n`; `dims=3` produces
`m,k,n` (GEMM). Size axis entries may be integers (square) or tuples.
"""
function matrix_feature_enumerator(; dims::Int=2, sparse_ok::Bool=false,
                                     accuracy_axis::Bool=false, spd_axis::Bool=false)
    dimkeys = dims == 3 ? ("m", "k", "n") : ("m", "n")
    return function (axes::Dict{String,Vector})
        feats = Dict{String,Any}[]
        for at in axes["array_type"]
            form = Symbol(at)
            structure = structure_of(form)
            structure == "sparse" && !sparse_ok && continue
            sizes = structure == "sparse" ? get(axes, "sparse_size", axes["size"]) :
                                            axes["size"]
            densities = structure == "sparse" ? axes["density"] : [nothing]
            accuracies = accuracy_axis ? axes["accuracy"] : [nothing]
            spds = spd_axis ? axes["spd"] : [nothing]
            for sz in sizes, et in axes["eltype"], den in densities,
                acc in accuracies, spd in spds
                d = Dict{String,Any}(
                    "array_type" => String(at),
                    "structure"  => structure,
                    "eltype"     => String(et),
                    "locality"   => "local",
                )
                dt = _as_dims(sz, length(dimkeys))
                for (k, v) in zip(dimkeys, dt)
                    d[k] = v
                end
                den === nothing || (d["density"] = Float64(den))
                acc === nothing || (d["accuracy"] = Float64(acc))
                spd === nothing || (d["spd"] = Bool(spd))
                push!(feats, d)
            end
        end
        return feats
    end
end

# ---------------------------------------------------------------------------
# Benchmark input generation (base forms: Array / SparseMatrixCSC)

_trial_rng(features) = Random.MersenneTwister(0x0DA66E5 + hash(features) % 100_000)

function _gen_dense(rng, T, dims...)
    return rand(rng, T, dims...)
end

# Diagonally-dominant square matrix: well-conditioned for LU and iterative
# methods alike, without being trivially easy.
function _gen_dd(rng, T, n)
    A = rand(rng, T, n, n)
    for i in 1:n
        A[i, i] += T(n)
    end
    return A
end

function _gen_spd_dense(rng, T, n)
    A = rand(rng, T, n, n)
    S = A' * A
    for i in 1:n
        S[i, i] += T(n)
    end
    return (S + S') / 2
end

function _gen_sparse(rng, T, m, n, density)
    return SparseArrays.sprand(rng, T, m, n, density)
end

function _gen_sparse_dd(rng, T, n, density)
    A = SparseArrays.sprand(rng, T, n, n, density)
    return A + T(2) * T(n) * SparseArrays.sparse(LinearAlgebra.I, n, n)
end

function _gen_sparse_spd(rng, T, n, density)
    A = SparseArrays.sprand(rng, T, n, n, density / 2)
    S = A + A' + T(2) * T(n) * SparseArrays.sparse(LinearAlgebra.I, n, n)
    return S
end

_is_sparse_feat(f) = get(f, "structure", "dense") == "sparse"
_feat_T(f) = eltype_from_string(f["eltype"])
_feat_density(f) = Float64(get(f, "density", 1.0))

# ---------------------------------------------------------------------------
# Default operations

_dense_mat_bytes(f) = Float64(f["m"]) * Float64(f["n"]) * sizeof(_feat_T(f))
_sparse_mat_bytes(f) = Float64(f["m"]) * Float64(f["n"]) * _feat_density(f) *
                       (sizeof(_feat_T(f)) + sizeof(Int))
_mat_bytes(f) = _is_sparse_feat(f) ? _sparse_mat_bytes(f) : _dense_mat_bytes(f)

# Sparse factorization work is fill-in dependent; nnz*sqrt(n) is only an
# interpolation coordinate (see OperationSpec docstring).
_sparse_work(f) = Float64(f["m"]) * Float64(f["n"]) * _feat_density(f) * sqrt(Float64(f["n"]))

function _factor_features(A; accuracy=nothing)
    return common_features(A)
end

function _solve_features(A, b; accuracy=nothing)
    d = common_features(A; accuracy)
    if get(d, "structure", "dense") == "dense" || !profile_sparsity[]
        d["spd"] = false  # unknown without an expensive check; dense direct
                          # solvers don't care, and callers can pass hints
    else
        d["spd"] = Bool(get(d, "symmetric", false))
    end
    return d
end

function _solve_check(features, base_inputs, result)
    A, b = base_inputs
    x = result
    r = try
        Float64(LinearAlgebra.norm(A * x - b) / max(LinearAlgebra.norm(b), eps()))
    catch err
        return "residual check failed: $(sprint(showerror, err))"
    end
    tol = Float64(get(features, "accuracy", 1.0e-8))
    limit = max(100 * tol, 1.0e-6)
    return r <= limit ? nothing : "residual $r exceeds limit $limit (accuracy=$tol)"
end

# ---------------------------------------------------------------------------
# Benchmark input generation for the in-place (`!`) operations. Dense-only;
# see the registration comment below for why sparse is excluded.

_lu_dense_generate_inputs(f) = (_gen_dd(_trial_rng(f), _feat_T(f), f["n"]),)
_cholesky_dense_generate_inputs(f) = (_gen_spd_dense(_trial_rng(f), _feat_T(f), f["n"]),)
_qr_dense_generate_inputs(f) = (_gen_dense(_trial_rng(f), _feat_T(f), f["m"], f["n"]),)

function _solve_dense_generate_inputs(f)
    rng = _trial_rng(f); T = _feat_T(f); n = f["n"]
    A = Bool(get(f, "spd", false)) ? _gen_spd_dense(rng, T, n) : _gen_dd(rng, T, n)
    return (A, rand(rng, T, n))
end

function _gemm_inplace_features(C, A, B; accuracy=nothing)
    d = common_features(A)
    d["k"] = size(A, 2)
    d["n"] = size(B, 2)
    return d
end

function _gemm_inplace_generate_inputs(f)
    rng = _trial_rng(f); T = _feat_T(f)
    C = zeros(T, f["m"], f["n"])
    A = _gen_dense(rng, T, f["m"], f["k"])
    B = _gen_dense(rng, T, f["k"], f["n"])
    return (C, A, B)
end

function _gemm_inplace_check(features, base_inputs, result)
    _, A, B = base_inputs
    expected = A * B
    actual = result
    Tf = real(float(eltype(expected)))
    tol = 100 * sqrt(eps(Tf)) * max(one(Tf), Float64(LinearAlgebra.norm(expected)))
    err = try
        Float64(LinearAlgebra.norm(actual - expected))
    catch err
        return "gemm! check failed: $(sprint(showerror, err))"
    end
    return err <= tol ? nothing : "gemm! result mismatch: ||C - A*B|| = $err exceeds tolerance $tol"
end

function register_default_operations!()
    common_dense_axes = Dict{String,Vector}(
        "size"        => Any[256, 1024, 4096, 16384],
        "sparse_size" => Any[10_000, 100_000, 1_000_000],
        "density"     => Any[1.0e-4, 1.0e-3],
        "eltype"      => Any["Float32", "Float64"],
        "array_type"  => Any["Array"],  # extended automatically by the
                                        # benchmark driver when CUDA/AMDGPU/
                                        # Dagger are usable (see benchmark.jl)
    )

    register_operation!(OperationSpec(;
        name = :lu, nargs = 1,
        scale = f -> _is_sparse_feat(f) ? _sparse_work(f) :
                     Float64(f["m"]) * Float64(f["n"]) * min(Float64(f["m"]), Float64(f["n"])),
        input_bytes = _mat_bytes,
        extract_features = _factor_features,
        generate_inputs = f -> begin
            rng = _trial_rng(f); T = _feat_T(f)
            _is_sparse_feat(f) ? (_gen_sparse_dd(rng, T, f["n"], _feat_density(f)),) :
                                 (_gen_dd(rng, T, f["n"]),)
        end,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=true),
        default_axes = merge(common_dense_axes,
                             Dict{String,Vector}("array_type" => Any["Array", "SparseMatrixCSC"])),
    ))

    register_operation!(OperationSpec(;
        name = :cholesky, nargs = 1,
        scale = f -> _is_sparse_feat(f) ? _sparse_work(f) : Float64(f["n"])^3 / 3,
        input_bytes = _mat_bytes,
        extract_features = _factor_features,
        generate_inputs = f -> begin
            rng = _trial_rng(f); T = _feat_T(f)
            _is_sparse_feat(f) ? (_gen_sparse_spd(rng, T, f["n"], _feat_density(f)),) :
                                 (_gen_spd_dense(rng, T, f["n"]),)
        end,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=true),
        default_axes = merge(common_dense_axes,
                             Dict{String,Vector}("array_type" => Any["Array", "SparseMatrixCSC"])),
    ))

    register_operation!(OperationSpec(;
        name = :qr, nargs = 1,
        scale = f -> Float64(f["m"]) * Float64(f["n"])^2,
        input_bytes = _mat_bytes,
        extract_features = _factor_features,
        generate_inputs = f -> begin
            rng = _trial_rng(f); T = _feat_T(f)
            (_gen_dense(rng, T, f["m"], f["n"]),)
        end,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=false),
        default_axes = common_dense_axes,
    ))

    register_operation!(OperationSpec(;
        name = :solve, nargs = 2,
        scale = f -> (_is_sparse_feat(f) ? _sparse_work(f) : (2 / 3) * Float64(f["n"])^3) +
                     2 * Float64(f["n"])^2,
        input_bytes = f -> _mat_bytes(f) + Float64(f["n"]) * sizeof(_feat_T(f)),
        output_bytes = f -> Float64(f["n"]) * sizeof(_feat_T(f)),
        array_output = true,
        output_ndims = 1,
        extract_features = _solve_features,
        generate_inputs = f -> begin
            rng = _trial_rng(f); T = _feat_T(f); n = f["n"]
            spd = Bool(get(f, "spd", false))
            A = if _is_sparse_feat(f)
                spd ? _gen_sparse_spd(rng, T, n, _feat_density(f)) :
                      _gen_sparse_dd(rng, T, n, _feat_density(f))
            else
                spd ? _gen_spd_dense(rng, T, n) : _gen_dd(rng, T, n)
            end
            (A, rand(rng, T, n))
        end,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=true,
                                                         accuracy_axis=true, spd_axis=true),
        default_axes = merge(common_dense_axes, Dict{String,Vector}(
            "array_type" => Any["Array", "SparseMatrixCSC"],
            "accuracy"   => Any[1.0e-8],
            "spd"        => Any[false, true],
        )),
        check = _solve_check,
    ))

    register_operation!(OperationSpec(;
        name = :gemm, nargs = 2,
        scale = f -> 2 * Float64(f["m"]) * Float64(f["k"]) * Float64(f["n"]),
        input_bytes = f -> (Float64(f["m"]) * Float64(f["k"]) +
                            Float64(f["k"]) * Float64(f["n"])) * sizeof(_feat_T(f)),
        output_bytes = f -> Float64(f["m"]) * Float64(f["n"]) * sizeof(_feat_T(f)),
        array_output = true,
        extract_features = (A, B; accuracy=nothing) -> begin
            d = common_features(A)
            d["k"] = size(A, 2)
            d["n"] = size(B, 2)
            d
        end,
        generate_inputs = f -> begin
            rng = _trial_rng(f); T = _feat_T(f)
            (_gen_dense(rng, T, f["m"], f["k"]), _gen_dense(rng, T, f["k"], f["n"]))
        end,
        enumerate_features = matrix_feature_enumerator(; dims=3, sparse_ok=false),
        default_axes = merge(common_dense_axes,
                             Dict{String,Vector}("size" => Any[256, 1024, 4096, 8192])),
    ))

    # -----------------------------------------------------------------
    # In-place counterparts (`:lu!`, `:cholesky!`, `:qr!`, `:solve!`,
    # `:gemm!`). Out-of-place algorithms above are written with an
    # allocating caller in mind (`lu(A)`, `A \ b`, `A * B`, ...); when the
    # caller instead already owns the output container (`lu!(A)`,
    # `mul!(C, A, B)`, `ldiv!(A, b)`), skipping the internal allocation/copy
    # every out-of-place algorithm otherwise performs is a real win. There is
    # no meaningful in-place *sparse* factorization -- UMFPACK/KLU/CHOLMOD
    # always allocate a fresh opaque factor object regardless of whether `A`
    # is reused -- so only dense BLAS/LAPACK/CUDA/Dagger algorithms are
    # registered here.
    register_operation!(OperationSpec(;
        name = :lu!, nargs = 1,
        scale = f -> Float64(f["m"]) * Float64(f["n"]) * min(Float64(f["m"]), Float64(f["n"])),
        input_bytes = _dense_mat_bytes,
        extract_features = _factor_features,
        generate_inputs = _lu_dense_generate_inputs,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=false),
        default_axes = common_dense_axes,
        mutated_args = [1],
    ))

    register_operation!(OperationSpec(;
        name = :cholesky!, nargs = 1,
        scale = f -> Float64(f["n"])^3 / 3,
        input_bytes = _dense_mat_bytes,
        extract_features = _factor_features,
        generate_inputs = _cholesky_dense_generate_inputs,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=false),
        default_axes = common_dense_axes,
        mutated_args = [1],
    ))

    register_operation!(OperationSpec(;
        name = :qr!, nargs = 1,
        scale = f -> Float64(f["m"]) * Float64(f["n"])^2,
        input_bytes = _dense_mat_bytes,
        extract_features = _factor_features,
        generate_inputs = _qr_dense_generate_inputs,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=false),
        default_axes = common_dense_axes,
        mutated_args = [1],
    ))

    register_operation!(OperationSpec(;
        name = :solve!, nargs = 2,
        scale = f -> (2 / 3) * Float64(f["n"])^3 + 2 * Float64(f["n"])^2,
        input_bytes = f -> _dense_mat_bytes(f) + Float64(f["n"]) * sizeof(_feat_T(f)),
        extract_features = _solve_features,
        generate_inputs = _solve_dense_generate_inputs,
        enumerate_features = matrix_feature_enumerator(; sparse_ok=false,
                                                         accuracy_axis=true, spd_axis=true),
        default_axes = merge(common_dense_axes, Dict{String,Vector}(
            "accuracy" => Any[1.0e-8],
            "spd"      => Any[false, true],
        )),
        check = _solve_check,
        mutated_args = [1, 2],
    ))

    register_operation!(OperationSpec(;
        name = :gemm!, nargs = 3,
        scale = f -> 2 * Float64(f["m"]) * Float64(f["k"]) * Float64(f["n"]),
        input_bytes = f -> (Float64(f["m"]) * Float64(f["k"]) +
                            Float64(f["k"]) * Float64(f["n"]) +
                            Float64(f["m"]) * Float64(f["n"])) * sizeof(_feat_T(f)),
        extract_features = _gemm_inplace_features,
        generate_inputs = _gemm_inplace_generate_inputs,
        enumerate_features = matrix_feature_enumerator(; dims=3, sparse_ok=false),
        default_axes = merge(common_dense_axes,
                             Dict{String,Vector}("size" => Any[256, 1024, 4096, 8192])),
        check = _gemm_inplace_check,
        mutated_args = [1],
    ))

    # Pseudo-operation measuring data-movement bandwidth per channel. Results
    # feed `movement_cost` in model.jl.
    register_operation!(OperationSpec(;
        name = :transfer, nargs = 1,
        scale = f -> Float64(f["bytes"]),
        input_bytes = f -> 3.0 * Float64(f["bytes"]),
        extract_features = (x; accuracy=nothing) -> Dict{String,Any}(
            "bytes" => payload_bytes(x), "eltype" => string(eltype(x)),
            "array_type" => "Array", "structure" => "dense", "locality" => "local"),
        generate_inputs = f -> begin
            rng = _trial_rng(f)
            (rand(rng, Float64, max(1, Int(f["bytes"]) ÷ 8)),)
        end,
        enumerate_features = axes -> begin
            feats = Dict{String,Any}[]
            for b in axes["bytes"]
                push!(feats, Dict{String,Any}(
                    "bytes" => Int(b), "eltype" => "Float64",
                    "array_type" => "Array", "structure" => "dense",
                    "locality" => "local"))
            end
            feats
        end,
        default_axes = Dict{String,Vector}("bytes" => Any[2^20, 2^24, 2^27]),
    ))

    return nothing
end

# ---------------------------------------------------------------------------
# Default algorithms

_dense_only(form) = f -> f["array_type"] == String(form) &&
                         get(f, "structure", "dense") == "dense"
_sparse_only(form) = f -> f["array_type"] == String(form) &&
                          get(f, "structure", "dense") == "sparse"
# Dense + a BLAS/LAPACK-supported element type. The vendor LU backends (MKL,
# OpenBLAS, BLIS) only have `getrf` for the four `BlasFloat`s; likewise
# RecursiveFactorization targets those.
_blas_dense_only(form) = f -> _dense_only(form)(f) &&
                              (eltype_from_string(f["eltype"]) <: LinearAlgebra.BlasFloat)

const DAGGER_BLOCKSIZES = Any[256, 512, 1024, 2048]

function register_default_algorithms!()
    # ------------------------------------------------------------------ :lu
    register_algorithm!(AlgorithmSpec(; name = :blas_lu, op = :lu,
        input_form = :Array, supports = _dense_only(:Array),
        invoke = (ctx, A) -> LinearAlgebra.lu(A)))

    register_algorithm!(AlgorithmSpec(; name = :umfpack_lu, op = :lu,
        input_form = :SparseMatrixCSC, supports = _sparse_only(:SparseMatrixCSC),
        invoke = (ctx, A) -> LinearAlgebra.lu(A)))

    register_algorithm!(AlgorithmSpec(; name = :klu_lu, op = :lu,
        package = "KLU", input_form = :SparseMatrixCSC,
        supports = _sparse_only(:SparseMatrixCSC),
        invoke = (ctx, A) -> ctx.mod.klu(A)))

    register_algorithm!(AlgorithmSpec(; name = :cuda_lu, op = :lu,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        invoke = (ctx, A) -> LinearAlgebra.lu(A)))

    register_algorithm!(AlgorithmSpec(; name = :dagger_lu, op = :lu,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        # `raw_impl(:dagger_lu)` mutates its argument in place; :lu is
        # out-of-place, so copy first (the genuine in-place algorithm lives
        # under :lu! below, sharing the same raw impl without the copy).
        invoke = (ctx, A) -> raw_impl(:dagger_lu)(copy(A))))

    # Vendor dense-LU backends. These `getrf!` in place, so the out-of-place
    # `:lu` must hand them a private copy (see `_external_getrf_lu!` in
    # lu_backends.jl). RecursiveFactorization is a pure-Julia recursive LU.
    register_algorithm!(AlgorithmSpec(; name = :mkl_lu, op = :lu,
        package = "MKL_jll", input_form = :Array, supports = _blas_dense_only(:Array),
        invoke = (ctx, A) -> _external_lu_invoke(_mkl_getrf_lib, copy(A))))

    register_algorithm!(AlgorithmSpec(; name = :openblas_lu, op = :lu,
        package = "OpenBLAS_jll", input_form = :Array, supports = _blas_dense_only(:Array),
        invoke = (ctx, A) -> _external_lu_invoke(_openblas_getrf_lib, copy(A))))

    register_algorithm!(AlgorithmSpec(; name = :blis_lu, op = :lu,
        package = "blis_jll", input_form = :Array, supports = _blas_dense_only(:Array),
        invoke = (ctx, A) -> _external_lu_invoke(_blis_getrf_lib, copy(A))))

    register_algorithm!(AlgorithmSpec(; name = :recursive_lu, op = :lu,
        package = "RecursiveFactorization", input_form = :Array,
        supports = _blas_dense_only(:Array),
        invoke = (ctx, A) -> ctx.mod.lu(A)))

    # ------------------------------------------------------------ :cholesky
    register_algorithm!(AlgorithmSpec(; name = :blas_cholesky, op = :cholesky,
        input_form = :Array, supports = _dense_only(:Array),
        invoke = (ctx, A) -> LinearAlgebra.cholesky(LinearAlgebra.Hermitian(A))))

    register_algorithm!(AlgorithmSpec(; name = :cholmod_cholesky, op = :cholesky,
        input_form = :SparseMatrixCSC, supports = _sparse_only(:SparseMatrixCSC),
        invoke = (ctx, A) -> LinearAlgebra.cholesky(LinearAlgebra.Hermitian(A))))

    register_algorithm!(AlgorithmSpec(; name = :cuda_cholesky, op = :cholesky,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        invoke = (ctx, A) -> LinearAlgebra.cholesky(LinearAlgebra.Hermitian(A))))

    register_algorithm!(AlgorithmSpec(; name = :dagger_cholesky, op = :cholesky,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        # See the :dagger_lu comment above: copy first for the out-of-place op.
        invoke = (ctx, A) -> raw_impl(:dagger_cholesky)(copy(A))))

    # ------------------------------------------------------------------ :qr
    register_algorithm!(AlgorithmSpec(; name = :blas_qr, op = :qr,
        input_form = :Array, supports = _dense_only(:Array),
        invoke = (ctx, A) -> LinearAlgebra.qr(A)))

    register_algorithm!(AlgorithmSpec(; name = :cuda_qr, op = :qr,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        invoke = (ctx, A) -> LinearAlgebra.qr(A)))

    register_algorithm!(AlgorithmSpec(; name = :dagger_qr, op = :qr,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        # See the :dagger_lu comment above: copy first for the out-of-place op.
        invoke = (ctx, A) -> raw_impl(:dagger_qr)(copy(A))))

    # --------------------------------------------------------------- :solve
    register_algorithm!(AlgorithmSpec(; name = :dense_backslash, op = :solve,
        input_form = :Array, supports = _dense_only(:Array),
        invoke = (ctx, A, b) -> A \ b))

    register_algorithm!(AlgorithmSpec(; name = :umfpack_solve, op = :solve,
        input_form = :SparseMatrixCSC, supports = _sparse_only(:SparseMatrixCSC),
        invoke = (ctx, A, b) -> A \ b))

    register_algorithm!(AlgorithmSpec(; name = :klu_solve, op = :solve,
        package = "KLU", input_form = :SparseMatrixCSC,
        supports = _sparse_only(:SparseMatrixCSC),
        invoke = (ctx, A, b) -> ctx.mod.klu(A) \ b))

    register_algorithm!(AlgorithmSpec(; name = :cholmod_solve, op = :solve,
        input_form = :SparseMatrixCSC,
        supports = f -> _sparse_only(:SparseMatrixCSC)(f) && get(f, "spd", false) == true,
        invoke = (ctx, A, b) -> LinearAlgebra.cholesky(LinearAlgebra.Hermitian(A)) \ b))

    for (name, form) in ((:krylov_gmres_sparse, :SparseMatrixCSC),
                         (:krylov_gmres_dense, :Array))
        register_algorithm!(AlgorithmSpec(; name = name, op = :solve,
            package = "Krylov", input_form = form,
            supports = f -> f["array_type"] == String(form),
            invoke = (ctx, A, b) -> begin
                rtol = accuracy_or(ctx, 1.0e-8)
                x, _stats = ctx.mod.gmres(A, b; rtol = rtol, atol = 0.0)
                x
            end))
    end

    register_algorithm!(AlgorithmSpec(; name = :krylov_cg, op = :solve,
        package = "Krylov", input_form = :SparseMatrixCSC,
        supports = f -> _sparse_only(:SparseMatrixCSC)(f) && get(f, "spd", false) == true,
        invoke = (ctx, A, b) -> begin
            rtol = accuracy_or(ctx, 1.0e-8)
            x, _stats = ctx.mod.cg(A, b; rtol = rtol, atol = 0.0)
            x
        end))

    register_algorithm!(AlgorithmSpec(; name = :cuda_solve, op = :solve,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        invoke = (ctx, A, b) -> A \ b))

    register_algorithm!(AlgorithmSpec(; name = :linearsolve_default, op = :solve,
        package = "LinearSolve", input_form = :Array, supports = _dense_only(:Array),
        invoke = (ctx, A, b) -> ctx.mod.solve(ctx.mod.LinearProblem(A, b)).u))

    register_algorithm!(AlgorithmSpec(; name = :dagger_solve, op = :solve,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        invoke = (ctx, A, b) -> raw_impl(:dagger_solve)(A, b)))

    # ---------------------------------------------------------------- :gemm
    register_algorithm!(AlgorithmSpec(; name = :blas_gemm, op = :gemm,
        input_form = :Array, supports = _dense_only(:Array),
        invoke = (ctx, A, B) -> A * B))

    register_algorithm!(AlgorithmSpec(; name = :cuda_gemm, op = :gemm,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        invoke = (ctx, A, B) -> A * B))

    register_algorithm!(AlgorithmSpec(; name = :dagger_gemm, op = :gemm,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        invoke = (ctx, A, B) -> raw_impl(:dagger_gemm)(A, B)))

    # ----------------------------------------------------------------- :lu!
    register_algorithm!(AlgorithmSpec(; name = :blas_lu!, op = :lu!,
        input_form = :Array, supports = _dense_only(:Array), in_place = true,
        invoke = (ctx, A) -> LinearAlgebra.lu!(A)))

    register_algorithm!(AlgorithmSpec(; name = :cuda_lu!, op = :lu!,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        in_place = true, invoke = (ctx, A) -> LinearAlgebra.lu!(A)))

    register_algorithm!(AlgorithmSpec(; name = :dagger_lu!, op = :lu!,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        in_place = true, invoke = (ctx, A) -> raw_impl(:dagger_lu)(A)))

    # Vendor dense-LU backends, in-place: `getrf!` overwrites `A` directly.
    register_algorithm!(AlgorithmSpec(; name = :mkl_lu!, op = :lu!,
        package = "MKL_jll", input_form = :Array, supports = _blas_dense_only(:Array),
        in_place = true, invoke = (ctx, A) -> _external_lu_invoke(_mkl_getrf_lib, A)))

    register_algorithm!(AlgorithmSpec(; name = :openblas_lu!, op = :lu!,
        package = "OpenBLAS_jll", input_form = :Array, supports = _blas_dense_only(:Array),
        in_place = true, invoke = (ctx, A) -> _external_lu_invoke(_openblas_getrf_lib, A)))

    register_algorithm!(AlgorithmSpec(; name = :blis_lu!, op = :lu!,
        package = "blis_jll", input_form = :Array, supports = _blas_dense_only(:Array),
        in_place = true, invoke = (ctx, A) -> _external_lu_invoke(_blis_getrf_lib, A)))

    register_algorithm!(AlgorithmSpec(; name = :recursive_lu!, op = :lu!,
        package = "RecursiveFactorization", input_form = :Array,
        supports = _blas_dense_only(:Array),
        in_place = true, invoke = (ctx, A) -> ctx.mod.lu!(A)))

    # ----------------------------------------------------------- :cholesky!
    register_algorithm!(AlgorithmSpec(; name = :blas_cholesky!, op = :cholesky!,
        input_form = :Array, supports = _dense_only(:Array), in_place = true,
        invoke = (ctx, A) -> LinearAlgebra.cholesky!(LinearAlgebra.Hermitian(A))))

    register_algorithm!(AlgorithmSpec(; name = :cuda_cholesky!, op = :cholesky!,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        in_place = true,
        invoke = (ctx, A) -> LinearAlgebra.cholesky!(LinearAlgebra.Hermitian(A))))

    register_algorithm!(AlgorithmSpec(; name = :dagger_cholesky!, op = :cholesky!,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        in_place = true, invoke = (ctx, A) -> raw_impl(:dagger_cholesky)(A)))

    # ----------------------------------------------------------------- :qr!
    register_algorithm!(AlgorithmSpec(; name = :blas_qr!, op = :qr!,
        input_form = :Array, supports = _dense_only(:Array), in_place = true,
        invoke = (ctx, A) -> LinearAlgebra.qr!(A)))

    register_algorithm!(AlgorithmSpec(; name = :cuda_qr!, op = :qr!,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        in_place = true, invoke = (ctx, A) -> LinearAlgebra.qr!(A)))

    register_algorithm!(AlgorithmSpec(; name = :dagger_qr!, op = :qr!,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        in_place = true, invoke = (ctx, A) -> raw_impl(:dagger_qr)(A)))

    # -------------------------------------------------------------- :solve!
    register_algorithm!(AlgorithmSpec(; name = :dense_ldiv!, op = :solve!,
        input_form = :Array, supports = _dense_only(:Array), in_place = true,
        invoke = (ctx, A, b) -> LinearAlgebra.ldiv!(LinearAlgebra.lu!(A), b)))

    register_algorithm!(AlgorithmSpec(; name = :cuda_ldiv!, op = :solve!,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        in_place = true,
        invoke = (ctx, A, b) -> LinearAlgebra.ldiv!(LinearAlgebra.lu!(A), b)))

    register_algorithm!(AlgorithmSpec(; name = :dagger_solve!, op = :solve!,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        # The algorithm name must match its raw impl key (`algorithm_available`
        # looks up `has_raw_impl(alg.name)` for any `dagger_`-prefixed algorithm).
        in_place = true, invoke = (ctx, A, b) -> raw_impl(:dagger_solve!)(A, b)))

    # -------------------------------------------------------------- :gemm!
    register_algorithm!(AlgorithmSpec(; name = :blas_gemm!, op = :gemm!,
        input_form = :Array, supports = _dense_only(:Array), in_place = true,
        invoke = (ctx, C, A, B) -> LinearAlgebra.mul!(C, A, B)))

    register_algorithm!(AlgorithmSpec(; name = :cuda_gemm!, op = :gemm!,
        package = "CUDA", input_form = :CuArray, supports = _dense_only(:CuArray),
        in_place = true, invoke = (ctx, C, A, B) -> LinearAlgebra.mul!(C, A, B)))

    register_algorithm!(AlgorithmSpec(; name = :dagger_gemm!, op = :gemm!,
        package = "Dagger", input_form = :DArray, supports = _dense_only(:DArray),
        free_config = Dict{String,Vector}("blocksize" => DAGGER_BLOCKSIZES),
        in_place = true, invoke = (ctx, C, A, B) -> raw_impl(:dagger_gemm!)(C, A, B)))

    # ------------------------------------------------------------ :transfer
    # Bandwidth microbenchmarks; algorithm names double as channel names.
    register_algorithm!(AlgorithmSpec(; name = :memcpy, op = :transfer,
        input_form = :Array, invoke = (ctx, x) -> copy(x)))

    register_algorithm!(AlgorithmSpec(; name = :h2d, op = :transfer,
        package = "CUDA", input_form = :Array,
        invoke = (ctx, x) -> ctx.mod.CuArray(x)))

    register_algorithm!(AlgorithmSpec(; name = :d2h, op = :transfer,
        package = "CUDA", input_form = :Array,
        prepare = (ctx, x) -> (ctx.mod.CuArray(x),),
        invoke = (ctx, xd) -> Array(xd)))

    register_algorithm!(AlgorithmSpec(; name = :distribute, op = :transfer,
        package = "Dagger", input_form = :Array,
        invoke = (ctx, x) -> _distribute_array(x, Dict{String,Any}())))

    register_algorithm!(AlgorithmSpec(; name = :collect, op = :transfer,
        package = "Dagger", input_form = :Array,
        prepare = (ctx, x) -> (_distribute_array(x, Dict{String,Any}()),),
        invoke = (ctx, xd) -> collect(xd)))

    return nothing
end

# ---------------------------------------------------------------------------
# Default converters

_default_blocksize(x::AbstractMatrix) = max(1, cld(minimum(size(x)), 4))
_default_blocksize(x::AbstractVector) = max(1, cld(length(x), 4))
_default_blocksize(x) = 512

function _distribute_array(x, config::Dict{String,Any})
    dagger = loaded_module("Dagger")
    dagger === nothing && error("Autotune: Dagger not loaded; cannot distribute")
    bs = Int(get(config, "blocksize", _default_blocksize(x)))
    blocks = x isa AbstractMatrix ? dagger.Blocks(bs, bs) : dagger.Blocks(bs)
    return dagger.distribute(x, blocks)
end

function register_default_converters!()
    register_converter!(Converter(:Array, :CuArray, "CUDA", :h2d,
        (x, cfg) -> loaded_module("CUDA").CuArray(x), payload_bytes))
    register_converter!(Converter(:CuArray, :Array, "CUDA", :d2h,
        (x, cfg) -> Array(x), payload_bytes))

    register_converter!(Converter(:Array, :ROCArray, "AMDGPU", :h2d,
        (x, cfg) -> loaded_module("AMDGPU").ROCArray(x), payload_bytes))
    register_converter!(Converter(:ROCArray, :Array, "AMDGPU", :d2h,
        (x, cfg) -> Array(x), payload_bytes))

    register_converter!(Converter(:Array, :DArray, "Dagger", :distribute,
        _distribute_array, payload_bytes))
    # `DArray -> Array`: if the `DArray` is just a non-copying `view` over a
    # dense parent (`view(A, Blocks(...))`), hand back the parent directly
    # instead of collecting a fresh copy -- and price the movement at zero
    # bytes so the cost model doesn't penalize dense algorithms for a copy
    # that never happens.
    register_converter!(Converter(:DArray, :Array, "Dagger", :collect,
        (x, cfg) -> (p = view_parent(x); p === nothing ? collect(x) : p),
        x -> view_parent(x) === nothing ? payload_bytes(x) : 0.0))

    # Sparse <-> dense conversion is intentionally *not* auto-registered:
    # densifying a large sparse matrix can be a semantic/memory catastrophe,
    # not just a performance choice. Register it explicitly if you want the
    # selector to consider it for your workload.
    return nothing
end
