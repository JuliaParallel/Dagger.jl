"""
    DSparseArray{T,N} <: AbstractArray{T,N}

A sparse array container, for which the contained array may be replaced with a
new one to support in-place operations. Designed to work well with Datadeps
algorithms: writes that reallocate (and grow/shrink) the inner sparse storage
are hidden behind the wrapper's stable identity, so Datadeps aliasing tracking
remains valid (see `aliases_as_whole`).

`DSparseVector{T}` and `DSparseMatrix{T}` are aliases for the 1- and 2-dimensional
cases. The wrapper is general over `N` so it can hold sparse vectors, matrices,
and (eventually) higher-order sparse tensors (e.g. Finch tensors).
"""
mutable struct DSparseArray{T,N} <: AbstractArray{T,N}
    mat
end
const DSparseVector{T} = DSparseArray{T,1}
const DSparseMatrix{T} = DSparseArray{T,2}

# Convenience constructor inferring element type and dimensionality from `x`.
DSparseArray(x) = DSparseArray{eltype(x),ndims(x)}(x)

_sparse_collect(M) = collect(M)
# Allocate a destination tile matching the inner storage's sparse backend. The
# default delegates to the storage type's own `similar`, which carries the
# concrete backend (e.g. `SparseMatrixCSC`) forward to the destination -- this is
# how `similar(::DArray)` propagates a tile's sparse type to a newly-allocated
# result DArray. Backends whose `similar` is unsuitable (e.g. Finch, whose generic
# `similar` yields tensor formats that destabilize later kernels) override this.
_sparse_similar(mat, ::Type{T}, dims::Dims) where {T} = similar(mat, T, dims)

Base.eltype(M::DSparseArray{T}) where T = T
Base.size(M::DSparseArray) = size(M.mat)
Base.iterate(M::DSparseArray) = iterate(M.mat)
Base.iterate(M::DSparseArray, state) = iterate(M.mat, state)
Base.similar(M::DSparseArray, ::Type{T}, dims::Dims) where T =
    DSparseArray{T,length(dims)}(_sparse_similar(M.mat, T, dims))
Base.collect(M::DSparseArray) = _sparse_collect(M.mat)
# See `_collect_cat` in array/darray.jl: gathering a sparse DArray densifies
# each tile, rather than `cat`ing sparse tiles element-by-element.
_collect_cat_tile(M::DSparseArray) = Array(_sparse_collect(M.mat))

# N.B. hash and aliasing shouldn't change even if M.mat changes.
# This is the key property that makes `DSparseArray` safe for Datadeps:
# the aliasing identity is tied to the (stable) wrapper object, not to the
# inner storage, which may be reallocated (and grow/shrink) on writes.
Base.hash(M::DSparseArray, h::UInt) = hash(objectid(M), hash(DSparseArray, h))

# Sparse containers must alias as a single, indivisible unit. Their storage is
# reallocated (and may grow/shrink) on writes, so it is unsafe to alias any
# *part* of the container independently. We therefore force *all* access to a
# `DSparseArray` to resolve to the container's stable whole-object
# `ObjectAliasing` -- including access via views, transposes, adjoints, and
# reshapes -- rather than e.g. a `StridedAliasing` over storage that may have
# since moved. `aliases_as_whole` opts the type into this behavior, and Datadeps'
# `aliasing_root` resolves any wrapper of a `DSparseArray` back to the container
# before computing aliasing. Note: this must return a *bare* `ObjectAliasing`, as
# `aliases_as_whole(::ObjectAliasing)` relies on it to drive whole-object copies.
#
# The ObjectAliasing is stamped with the inner storage's memory space so GPU-
# resident sparse tiles (e.g. CuSparseMatrixCSC / ROCSparseMatrixCSC, or
# DeviceSparseMatrixCSC) are tracked in VRAM rather than host RAM. The pointer
# itself is still the host wrapper's `pointer_from_objref` (identity only);
# Datadeps never span-copies these objects (see `aliases_as_whole` / `FullCopy`).
function aliasing(M::DSparseArray, _=identity)
    space = value_memory_space(M.mat)
    ptr = RemotePtr{Cvoid}(UInt(pointer_from_objref(M)), space)
    return ObjectAliasing(ptr, sizeof(typeof(M)))
end
aliases_as_whole(::DSparseArray) = true
# Chunk space follows the inner sparse storage (host CSC → CPURAM, GPU sparse → VRAM).
# `memory_space` (not only `value_memory_space`) must resolve correctly: Datadeps'
# `aliased_object!` uses `memory_space(x)` when deciding whether a same-space
# slot can share the original object. The generic fallback labels unknown values
# as CPURAM, which would force a discarded GPU copy and drop SpGEMM writes.
value_memory_space(M::DSparseArray) = value_memory_space(M.mat)
memory_space(M::DSparseArray) = value_memory_space(M)

# A `DSparseArray` has no meaningful raw data pointer (its storage may be
# reallocated/resized). This trap ensures that if aliasing ever tries to treat a
# wrapper of a `DSparseArray` as strided memory -- e.g. a new array-wrapper type
# that `aliasing_root` failed to resolve via `Base.parent` -- it fails loudly
# instead of silently corrupting Datadeps' aliasing tracking.
function Base.pointer(::DSparseArray)
    throw(ArgumentError("`pointer(::DSparseArray)` is intentionally unsupported: \
        a DSparseArray may reallocate its storage, so it must be aliased as a \
        whole object via `Dagger.aliasing`. If you reached here through Datadeps \
        aliasing of an array wrapper, ensure that wrapper implements `Base.parent` \
        so that `Dagger.aliasing_root` can resolve it to the DSparseArray."))
end

# Forward indexing to the inner array. Ranges/colons are supported so that
# views (used by copy-buffering between mismatched partitionings) work.
Base.getindex(M::DSparseArray, I...) = getindex(M.mat, I...)
Base.setindex!(M::DSparseArray, v, I...) = setindex!(M.mat, v, I...)

# Whole-tile copy. This is how Datadeps moves a tile between workers via
# `move!`: we *reallocate* the inner storage rather than mutating in place,
# since sparse storage cannot generally be updated in place. The wrapper's
# identity (and thus its aliasing) is preserved.
function Base.copyto!(dst::DSparseArray, src::DSparseArray)
    dst.mat = _sparse_copy(src.mat)
    return dst
end
# Backend-overridable deep copy of the inner storage (Finch tensors don't define
# `Base.copy`).
_sparse_copy(mat) = copy(mat)

# Adapt preserves the `DSparseArray` wrapper while adapting the inner storage
# (e.g. SparseMatrixCSC → CuSparseMatrixCSC under `adapt(CuArray, …)`). GPU
# package extensions override `move` for sparse types so OpenCL/Metal/oneAPI do
# not densify via the generic `adapt(DeviceArray, SparseMatrixCSC)` path.
Adapt.adapt_structure(to, M::DSparseArray) = DSparseArray(Adapt.adapt(to, M.mat))

# Whole-object in-place move for Datadeps `FullCopy`. Reassigns `to.mat` from a
# space-converted copy of `from.mat` rather than span-copying storage.
#
# N.B. This hooks the `dep_mod` arity rather than the 4-argument one that other
# array types use. Every GPU extension claims the 4-argument form for its own
# spaces (`move!(::TheirVRAMSpace, ::CPURAMMemorySpace, ::AbstractArray{T,N},
# ::AbstractArray{T,N})`), and a `DSparseArray` method with unconstrained spaces
# is genuinely ambiguous with those -- more specific in the value arguments, less
# specific in the space arguments -- so it would need a tie-breaker per backend
# per space pair. The 5-argument form is only ever defined in core for arrays,
# and it is what every Datadeps copy path calls, so hooking here wins outright.
# `dep_mod` is always `identity`: `aliases_as_whole` means no remainder copies.
function move!(dep_mod, to_space::MemorySpace, from_space::MemorySpace, to::DSparseArray{T,N}, from::DSparseArray{T,N}) where {T,N}
    if to_space == from_space
        to.mat = _sparse_copy(from.mat)
    else
        to_proc = first(processors(to_space))
        from_proc = first(processors(from_space))
        moved = move(from_proc, to_proc, from)
        to.mat = moved.mat
    end
    return
end

"""
    wraps_as_sparse_tile(x) -> Bool

Whether `x` is a bare sparse container that Datadeps must see through a
[`DSparseArray`](@ref) wrapper rather than directly.

Such containers reallocate their storage on write and (for `SparseMatrixCSC` and
Finch tensors) are *immutable* structs, so they have no stable object identity to
hang whole-object aliasing off of, and no meaningful data pointer for the span
machinery. `DSparseArray` supplies both.

Sparse backends opt their storage types in (see `ext/SparseArraysExt.jl`,
`ext/FinchExt.jl`, and the GPU sparse extensions); this single trait drives both
[`maybe_wrap_tile`](@ref) and Datadeps' automatic wrapping of user-provided
sparse arguments.
"""
wraps_as_sparse_tile(@nospecialize x) = false

"""
    maybe_wrap_tile(x)

Wrapping hook used when materializing tiles (e.g. in `distribute`) and when
adopting a user-provided sparse argument in Datadeps. Sparse storage is wrapped
in a [`DSparseArray`](@ref) that Datadeps can track; everything else is copied
as-is.

Either way the result owns storage that is *unaliased* from `x`. That matters for
the wrapper case: a `DSparseArray`'s aliasing identity is the wrapper's own
`objectid`, so two wrappers sharing inner storage would look disjoint to
Datadeps, which frees its region-internal slots eagerly at region end -- a
use-after-free, not merely a stale read.
"""
maybe_wrap_tile(x) = wraps_as_sparse_tile(x) ? wrap_sparse_tile(x) : copy(x)

# Wrap sparse storage, taking ownership of a private copy of it (see
# `maybe_wrap_tile`). `_sparse_copy` is the backend-overridable deep copy, so
# this works for host CSC, device CSC, and Finch tensors alike.
wrap_sparse_tile(x) = DSparseArray(_sparse_copy(x))

# Partial-range tile copy used by copy-buffering (e.g. when matmul operands need
# to be repartitioned) and by `repartition`. Some backends (Finch) forbid
# `setindex!`, so we route through a backend hook that returns the (possibly
# reallocated) inner storage. `DSparseArray` hides the reallocation from Datadeps.
function copyto_view!(Bpart::DSparseArray, Brange, Apart, Arange)
    if _sparse_off_host(Bpart.mat) || _sparse_off_host(Apart)
        return _copyto_view_hosted!(Bpart, Brange, Apart, Arange)
    end
    Bpart.mat = _sparse_copyto_view!(Bpart.mat, Brange, view(Apart, Arange))
    return
end
# Default: in-place element-wise copy (works for `setindex!`-capable backends
# such as `SparseArrays`). May reallocate and return new storage for backends
# that can't update in place, so callers must use the returned value.
function _sparse_copyto_view!(mat, Brange, src)
    copyto!(view(mat, Brange), src)
    return mat
end

_sparse_off_host(x) = !(value_memory_space(x) isa CPURAMMemorySpace)

# Sub-range tile copy staged through host sparse storage, for device-resident
# tiles.
#
# A device sparse tile supports *neither* end of an element-wise sub-range copy:
# reading it goes through `getindex`, which is scalar indexing on the storage
# vectors, and writing it would have to insert nonzeros into a CSC in place.
# Only a whole-tile write (`copyto!`/`move!`, which replace `mat` outright) is
# available on a device, so a partial one has to happen on the host: gather both
# ends, copy there, and re-upload with the same `move` every Datadeps transfer
# uses. Going through `move` is what keeps this backend-agnostic -- each GPU
# sparse extension already defines `move(::CPUProc, ::TheirProc, ::DSparseArray)`
# for its own storage type, so no backend needs a method here.
#
# It costs a host round trip per overlapping tile pair. That is the price of the
# fallback existing at all: without it, a device-resident sparse operand simply
# cannot be re-tiled, so a partitioning mismatch is an error rather than a
# slowdown.
function _copyto_view_hosted!(Bpart::DSparseArray, Brange, Apart, Arange)
    to_space = value_memory_space(Bpart.mat)
    Bhost = _sparse_off_host(Bpart.mat) ? _sparse_collect(Bpart.mat) : Bpart.mat
    Bhost = _sparse_copyto_view!(Bhost, Brange, view(_sparse_host_side(Apart), Arange))
    Bpart.mat = if to_space isa CPURAMMemorySpace
        Bhost
    else
        move(OSProc(), first(processors(to_space)), DSparseArray(Bhost)).mat
    end
    return
end
# The host-readable stand-in for a copy *source*, gathering it only if it is not
# already on the host (in which case the source is passed through untouched, so
# the host path stays exactly what it was).
_sparse_host_side(A::DSparseArray) = _sparse_off_host(A.mat) ? _sparse_collect(A.mat) : A
_sparse_host_side(A) = _sparse_off_host(A) ? Adapt.adapt(Array, A) : A

#Base.BroadcastStyle(::Type{<:DSparseArray}) = FinchStyle()

LinearAlgebra.norm2(M::DSparseArray) = LinearAlgebra.norm2(M.mat)

# Matrix-specific operations dispatch on the 2-D alias `DSparseMatrix`.
function matmatmul!(
    C::DSparseMatrix,
    transA::Char,
    transB::Char,
    A::DSparseMatrix,
    B::DSparseMatrix,
    alpha,
    beta
)
    # Forward to sparse matmatmul!, but allow in-place update of C
    return matmatmul!(C, transA, transB, A.mat, B.mat, alpha, beta)
end

# Sparse matrix-vector multiply: `C = alpha*op(A)*B + beta*C` with a sparse
# matrix tile `A` and dense vectors `B`/`C`. Unwrap to the inner storage so each
# backend can provide an efficient (in-place on dense `C`) method.
function matvecmul!(C, transA::Char, A::DSparseMatrix, B, alpha, beta)
    return matvecmul!(C, transA, A.mat, B, alpha, beta)
end

# Factorize the inner sparse storage (e.g. sparse LU/UMFPACK) for block-Jacobi,
# rather than the `DSparseArray` wrapper (whose generic `lu` would densify).
_factorize_tile(M::DSparseArray) = LinearAlgebra.lu(M.mat)

# Expose the inner sparse storage so preconditioner backends (ILU, AMG) build on
# the actual `SparseMatrixCSC` rather than the `DSparseArray` wrapper.
_tile_matrix(M::DSparseArray) = M.mat

function transpose_tile end
function copytile!(A::DSparseMatrix, B::DSparseMatrix)
    A.mat = transpose_tile(B.mat)
    return
end
function copydiagtile!(A::DSparseMatrix, uplo)
    A.mat = transpose_tile(A.mat, uplo)
    return
end

#==============================================================================
  Device-resident CSC for GPU backends without a vendor sparse library
  (OpenCL / Metal / oneAPI). Storage vectors live in device memory; SpGEMM/SpMV
  kernels fall back to host SparseArrays (see SparseArraysExt). CUDA/ROCm use
  their native CuSparse/ROCSparse types instead.
==============================================================================#

"""
    DeviceSparseMatrixCSC{Tv,Ti,V,I}

A CSC sparse matrix whose `colptr`/`rowval`/`nzval` live in device arrays
(`CLArray`, `MtlArray`, `oneArray`, …). Used as the inner storage of a
[`DSparseArray`](@ref) on GPU backends that lack a vendor sparse library.
"""
struct DeviceSparseMatrixCSC{Tv,Ti,V<:AbstractVector{Tv},I<:AbstractVector{Ti}} <: AbstractMatrix{Tv}
    m::Int
    n::Int
    colptr::I
    rowval::I
    nzval::V
end
Base.size(A::DeviceSparseMatrixCSC) = (A.m, A.n)
Base.eltype(::DeviceSparseMatrixCSC{Tv}) where Tv = Tv
Base.IndexStyle(::Type{<:DeviceSparseMatrixCSC}) = IndexCartesian()
function Base.getindex(A::DeviceSparseMatrixCSC{Tv}, i::Integer, j::Integer) where Tv
    @boundscheck checkbounds(A, i, j)
    # Scalar device indexing; acceptable for rare host-side inspection.
    col_start = Int(A.colptr[j])
    col_end = Int(A.colptr[j + 1]) - 1
    for p in col_start:col_end
        Int(A.rowval[p]) == i && return A.nzval[p]
    end
    return zero(Tv)
end
# Memory space follows the nonzero values buffer.
value_memory_space(A::DeviceSparseMatrixCSC) = value_memory_space(A.nzval)
memory_space(A::DeviceSparseMatrixCSC) = value_memory_space(A)
# Whole-object aliasing (storage may be replaced on write); stamp with device space.
function aliasing(A::DeviceSparseMatrixCSC, _=identity)
    space = value_memory_space(A)
    ptr = RemotePtr{Cvoid}(UInt(pointer_from_objref(A)), space)
    return ObjectAliasing(ptr, sizeof(typeof(A)))
end
aliases_as_whole(::DeviceSparseMatrixCSC) = true
function Base.pointer(::DeviceSparseMatrixCSC)
    throw(ArgumentError("`pointer(::DeviceSparseMatrixCSC)` is unsupported; \
        alias as a whole object via `Dagger.aliasing`."))
end

# Allocation hooks used by `spzeros`/`sprand` under a GPU compute scope.
# GPU Exts override these for their processor type.
allocate_sparse_zeros(proc::Processor, ::Type{T}, dims::Dims) where T =
    allocate_sparse_zeros_default(proc, T, dims)
allocate_sparse_rand(proc::Processor, ::Type{T}, dims::Dims, sparsity::AbstractFloat) where T =
    allocate_sparse_rand_default(proc, T, dims, sparsity)
# Defaults are filled in by SparseArraysExt (host SparseMatrixCSC / SparseVector).
function allocate_sparse_zeros_default end
function allocate_sparse_rand_default end
# Upload host CSC onto device vectors of type `Arr` (filled in by SparseArraysExt).
function device_sparse_from_host end

# One empty sparse tile, in whatever backend the executing processor calls for.
# N.B. A named function, not a closure: `AllocateArray` spawns it, and under MPI
# an anonymous closure hashes differently per rank.
_sparse_zeros_tile(::Type{T}, dims::Dims) where T =
    DSparseArray(allocate_sparse_zeros(task_processor(), T, dims))

# `allocate_tiled` for sparse-backed arrays: keeps a re-tiling sparse. See the
# docstring in array/copy.jl for why dispatching on the tile type matters.
function allocate_tiled(::Type{<:DSparseArray}, ::Type{T}, part::Blocks{N}, dims::Dims{N}) where {T,N}
    d = ArrayDomain(map(x->1:x, dims))
    a = AllocateArray(T, _sparse_zeros_tile, false, d, partition(part, d), part, nothing;
                      return_type=DSparseArray{T,N})
    return _to_darray(a)
end
