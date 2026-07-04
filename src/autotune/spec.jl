# spec.jl
#
# Turning user inputs into a flat, serializable feature dictionary
# (Dict{String,Any}) that the benchmark database is keyed on. All values must
# be TOML-representable (String, Int, Float64, Bool). Standard keys:
#
#   "m","n","k"     - dimensions (op-specific; see registry.jl)
#   "eltype"        - "Float64", "Float32", "ComplexF64", ...
#   "array_type"    - container *form*: "Array", "SparseMatrixCSC", "CuArray",
#                     "ROCArray", "DArray", ...
#   "structure"     - "dense" | "sparse" (derived from array_type)
#   "locality"      - "local" | "remote" (partially/wholly off-process chunks)
#   "density"       - nnz/(m*n) for sparse inputs
#   "bandwidth"     - structural bandwidth for sparse inputs
#   "symmetric"     - structural symmetry for sparse inputs
#   "accuracy"      - requested relative residual tolerance (solve-like ops)
#   "bytes"         - payload size (transfer pseudo-op)

# ---------------------------------------------------------------------------
# Module lookup without hard dependencies

"""
    loaded_module(name::AbstractString) -> Union{Module,Nothing}

Find a top-level loaded package module by name, without depending on it.
"""
function loaded_module(name::AbstractString)
    for (id, mod) in Base.loaded_modules
        id.name == name && return mod
    end
    return nothing
end

package_available(::Nothing) = true
package_available(name::AbstractString) = loaded_module(name) !== nothing

# ---------------------------------------------------------------------------
# Array forms

# Forms considered sparse containers.
const SPARSE_FORMS = Set{Symbol}([:SparseMatrixCSC, :CuSparseMatrixCSR])

# typename => form overrides for wrappers we want to see through.
# (Adjoint/Transpose/Symmetric etc. report their parent's form.)
const WRAPPER_TYPES = Set{Symbol}([:Adjoint, :Transpose, :Symmetric, :Hermitian,
                                   :UpperTriangular, :LowerTriangular, :SubArray])

"""
    array_form(x) -> Symbol

The container *form* of `x`, used to key benchmark data and converters:
`:Array`, `:SparseMatrixCSC`, `:CuArray`, `:ROCArray`, `:DArray`, ... Falls
back to `nameof(typeof(x))` for unknown containers (which simply means no
benchmark data will match until such data is generated for that form).
"""
function array_form(x::AbstractArray)
    T = typeof(x)
    name = nameof(T)
    if name in WRAPPER_TYPES
        return array_form(parent(x))
    end
    return name
end
array_form(x) = nameof(typeof(x))

structure_of(form::Symbol) = form in SPARSE_FORMS ? "sparse" : "dense"

# ---------------------------------------------------------------------------
# Locality

# Per-form locality hooks: form => (x -> Bool), true when all data is on the
# current process. Dagger's integration should install a `:DArray` hook (see
# `install_darray_locality_hook!` below for a best-effort default).
const LOCALITY_HOOKS = Dict{Symbol,Function}()

"""
    is_all_local(x) -> Bool

Whether all of `x`'s data lives on the current process. Non-distributed
containers are trivially local; distributed containers are checked via a
registered hook (defaulting to `true` if no hook exists).
"""
function is_all_local(x)
    form = array_form(x)
    hook = get(LOCALITY_HOOKS, form, nothing)
    hook === nothing && return true
    return try
        hook(x)::Bool
    catch
        true
    end
end

# ---------------------------------------------------------------------------
# View-backed container unwrapping
#
# A `Dagger.DArray` produced by `view(A, Blocks(...))` merely *tiles* an
# existing dense array `A` (its chunks are `SubArray`s of `A`); no data was
# copied. When such a `DArray` has to be converted to the `:Array` form for a
# dense algorithm, materializing it with `collect` would allocate and copy a
# whole second array for nothing -- we can just hand back the parent `A`
# directly. This hook lets Autotune recognize that case without depending on
# Dagger: Dagger installs a precise implementation in its `__init__`; the
# default treats nothing as unwrappable.
const VIEW_PARENT_HOOK = Ref{Function}(_ -> nothing)

"""
    view_parent(x) -> Union{AbstractArray,Nothing}

The dense parent array a distributed container is a non-copying view over, or
`nothing` if `x` isn't such a whole-array view (so it must be materialized).
"""
view_parent(x) = try
    VIEW_PARENT_HOOK[](x)
catch
    nothing
end

set_view_parent_hook!(f::Function) = (VIEW_PARENT_HOOK[] = f; nothing)

"""
    install_darray_locality_hook!()

Best-effort default locality check for `Dagger.DArray`, inspecting chunk
handles' owners. Dagger's `__init__` may replace this with a precise
implementation (`LOCALITY_HOOKS[:DArray] = A -> ...`).
"""
function install_darray_locality_hook!()
    LOCALITY_HOOKS[:DArray] = A -> begin
        me = Distributed.myid()
        all(A.chunks) do chunk
            owner = try
                chunk.handle.owner
            catch
                me
            end
            owner == me
        end
    end
    return nothing
end

# ---------------------------------------------------------------------------
# Sparsity summaries

"""
    sparsity_features!(d::Dict{String,Any}, A) -> d

Record density, structural bandwidth, and structural symmetry of a sparse
matrix into `d`. O(nnz); callers on hot paths can skip via
`profile_sparsity[]`.
"""
function sparsity_features!(d::Dict{String,Any}, A::SparseArrays.AbstractSparseMatrix)
    m, n = size(A)
    nz = SparseArrays.nnz(A)
    d["density"] = m * n == 0 ? 0.0 : nz / (m * n)
    bw = 0
    rows = SparseArrays.rowvals(A)
    for col in 1:n
        for idx in SparseArrays.nzrange(A, col)
            bw = max(bw, abs(rows[idx] - col))
        end
    end
    d["bandwidth"] = bw
    d["symmetric"] = m == n && LinearAlgebra.issymmetric(A)
    return d
end
sparsity_features!(d::Dict{String,Any}, A) = d

# Skip O(nnz) sparsity profiling at runtime if desired.
const profile_sparsity = Ref{Bool}(true)

# ---------------------------------------------------------------------------
# Common feature extraction

"""
    common_features(A; accuracy=nothing) -> Dict{String,Any}

Features shared by every matrix operation, extracted from the primary input.
"""
function common_features(A; accuracy=nothing)
    d = Dict{String,Any}()
    form = array_form(A)
    d["array_type"] = String(form)
    d["structure"] = structure_of(form)
    d["eltype"] = string(eltype(A))
    if A isa AbstractMatrix
        d["m"], d["n"] = size(A)
    elseif A isa AbstractVector
        d["m"] = length(A)
        d["n"] = 1
    end
    d["locality"] = is_all_local(A) ? "local" : "remote"
    if d["structure"] == "sparse" && profile_sparsity[]
        sparsity_features!(d, A isa SparseArrays.AbstractSparseMatrix ? A : parent(A))
    end
    if accuracy !== nothing
        d["accuracy"] = Float64(accuracy)
    end
    return d
end

# ---------------------------------------------------------------------------
# Element types

const ELTYPE_MAP = Dict{String,DataType}(
    "Float16"    => Float16,
    "Float32"    => Float32,
    "Float64"    => Float64,
    "ComplexF32" => ComplexF32,
    "ComplexF64" => ComplexF64,
    "Int32"      => Int32,
    "Int64"      => Int64,
)

function eltype_from_string(s::AbstractString)
    haskey(ELTYPE_MAP, s) && return ELTYPE_MAP[s]
    # Last resort for user-registered eltypes living in Base/Core.
    try
        T = getfield(Base, Symbol(s))
        T isa Type && return T
    catch
    end
    throw(ArgumentError("Autotune: unknown eltype string \"$s\"; register it in Autotune.ELTYPE_MAP"))
end

# ---------------------------------------------------------------------------
# Machine fingerprint

"""
    machine_fingerprint() -> Dict{String,Any}

Identifying information for the machine the benchmark ran on, stored in the
database and checked (with a warning, not an error) at load time.
"""
function machine_fingerprint()
    d = Dict{String,Any}(
        "hostname"     => gethostname_safe(),
        "cpu"          => Sys.cpu_info()[1].model,
        "cpu_threads"  => Sys.CPU_THREADS,
        "memory_bytes" => Int(Sys.total_memory()),
        "julia"        => string(VERSION),
        "os"           => string(Sys.KERNEL),
    )
    return d
end

gethostname_safe() = try
    String(Base.Libc.gethostname())
catch
    "unknown"
end
