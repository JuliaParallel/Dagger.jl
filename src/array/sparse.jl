"""
    DSparseMatrix{T} <: AbstractMatrix{T}

A sparse matrix container, for which the contained matrix may be replaced with
a new one to support in-place operations. Designed to work well with
Datadeps algorithms.
"""
mutable struct DSparseMatrix{T} <: AbstractMatrix{T}
    mat
end
function DSparseMatrix{T}(undef, dims::NTuple{N, Int}) where {T,N}
    M = sparse_mode()
    return DSparseMatrix{T}(_sparse_alloc(Val(M), T, dims))
end

function _sparse_not_loaded(::Val{M}) where M
    if M == :sparsearrays
        throw(ArgumentError("SparseArrays must be loaded to use SparseMatrixCSC"))
    elseif M == :finch
        throw(ArgumentError("Finch must be loaded to use Finch.Tensor"))
    elseif M == :none
        throw(ArgumentError("Sparse mode not set\nSet it with `sparse_mode!(M)` where M is :sparsearrays or :finch, and load the corresponding package"))
    else
        throw(ArgumentError("Unknown sparse mode $M\nOptions are :sparsearrays and :finch"))
    end
end
_sparse_alloc(::Val{M}, T::Type, dims::Dims) where M = _sparse_not_loaded(Val(M))
_sparse_collect(M) = collect(M)
const SPARSE_MODE = TaskLocalValue{Symbol}(()->:none)
sparse_mode() = SPARSE_MODE[]
sparse_mode(::T) where T = error("Unknown sparse mode for type $T")
set_sparse_mode!(mode::Symbol) = SPARSE_MODE[] = mode

Base.eltype(M::DSparseMatrix{T}) where T = T
Base.size(M::DSparseMatrix) = size(M.mat)
Base.ndims(M::DSparseMatrix) = 2
Base.iterate(M::DSparseMatrix) = iterate(M.mat)
Base.iterate(M::DSparseMatrix, state) = iterate(M.mat, state)
Base.similar(M::DSparseMatrix, ::Type{T}, dims::Tuple{Int, Int}) where T =
    DSparseMatrix{T}(_sparse_alloc(Val(sparse_mode(M.mat)), T, dims))
Base.collect(M::DSparseMatrix) = _sparse_collect(M.mat)

# N.B. hash and aliasing shouldn't change even if M.mat changes.
# This is the key property that makes `DSparseMatrix` safe for Datadeps:
# the aliasing identity is tied to the (stable) wrapper object, not to the
# inner storage, which may be reallocated (and grow/shrink) on writes.
Base.hash(M::DSparseMatrix, h::UInt) = hash(objectid(M), hash(DSparseMatrix, h))

# Sparse containers must alias as a single, indivisible unit. Their storage is
# reallocated (and may grow/shrink) on writes, so it is unsafe to alias any
# *part* of the container independently. We therefore force *all* access to a
# `DSparseMatrix` to resolve to the container's stable whole-object
# `ObjectAliasing` -- including access via views, transposes, adjoints, and
# reshapes -- rather than e.g. a `StridedAliasing` over storage that may have
# since moved.
aliasing(M::DSparseMatrix, _=identity) = ObjectAliasing(M)

# Resolve the root `DSparseMatrix` beneath a stack of array wrappers (returns
# `nothing` if there is no sparse container at the root).
sparse_alias_root(@nospecialize(x)) = nothing
sparse_alias_root(M::DSparseMatrix) = M
sparse_alias_root(x::SubArray) = sparse_alias_root(parent(x))
sparse_alias_root(x::Base.ReshapedArray) = sparse_alias_root(parent(x))
sparse_alias_root(x::LinearAlgebra.Transpose) = sparse_alias_root(parent(x))
sparse_alias_root(x::LinearAlgebra.Adjoint) = sparse_alias_root(parent(x))
sparse_alias_root(x::Base.PermutedDimsArray) = sparse_alias_root(parent(x))

# Views/reshapes over a sparse container alias the *whole* container; any other
# (e.g. dense `Array`-backed) parent defers to the existing handling. Transpose
# and adjoint already forward aliasing to their parent, so the sparse-rooted case
# is handled there (and recursively via `sparse_alias_root`).
function aliasing(x::SubArray)
    root = sparse_alias_root(x)
    root === nothing && return invoke(aliasing, Tuple{Any}, x)
    return aliasing(root)
end
function aliasing(x::Base.ReshapedArray)
    root = sparse_alias_root(x)
    root === nothing && return invoke(aliasing, Tuple{Any}, x)
    return aliasing(root)
end

# Forward indexing to the inner matrix. Ranges/colons are supported so that
# views (used by copy-buffering between mismatched partitionings) work.
Base.getindex(M::DSparseMatrix, I...) = getindex(M.mat, I...)
Base.setindex!(M::DSparseMatrix, v, I...) = setindex!(M.mat, v, I...)

# Whole-tile copy. This is how Datadeps moves a tile between workers via
# `move!`: we *reallocate* the inner storage rather than mutating in place,
# since sparse storage cannot generally be updated in place. The wrapper's
# identity (and thus its aliasing) is preserved.
function Base.copyto!(dst::DSparseMatrix, src::DSparseMatrix)
    dst.mat = _sparse_copy(src.mat)
    return dst
end
# Backend-overridable deep copy of the inner storage (Finch tensors don't define
# `Base.copy`).
_sparse_copy(mat) = copy(mat)

# Wrapping hook used when materializing tiles (e.g. in `distribute`). Backends
# overload this (e.g. in package extensions) to wrap freshly-created tiles in a
# container that Datadeps can track (such as `DSparseMatrix` for sparse tiles);
# everything else is left untouched.
maybe_wrap_tile(x) = x

# Partial-range tile copy used by copy-buffering (e.g. when matmul operands need
# to be repartitioned). Some backends (Finch) forbid `setindex!`, so we route
# through a backend hook that returns the (possibly reallocated) inner storage.
# `DSparseMatrix` hides the reallocation from Datadeps.
function copyto_view!(Bpart::DSparseMatrix, Brange, Apart, Arange)
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

#Base.BroadcastStyle(::Type{<:DSparseMatrix}) = FinchStyle()

LinearAlgebra.norm2(M::DSparseMatrix) = LinearAlgebra.norm2(M.mat)

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

function transpose_tile end
function copytile!(A::DSparseMatrix, B::DSparseMatrix)
    A.mat = transpose_tile(B.mat)
    return
end
function copydiagtile!(A::DSparseMatrix, uplo)
    A.mat = transpose_tile(A.mat, uplo)
    return
end