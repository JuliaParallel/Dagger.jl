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
    #DSparseMatrix{T}(similar(M.mat, T, dims))
    DSparseMatrix{T}(_sparse_alloc(Val(sparse_mode(M.mat)), T, dims))
Base.collect(M::DSparseMatrix) = _sparse_collect(M.mat)

# N.B. hash and aliasing shouldn't change even if M.mat changes
Base.hash(M::DSparseMatrix, h::UInt) = hash(objectid(M), hash(DSparseMatrix, h))
aliasing(M::DSparseMatrix, _=identity) =
    ObjectAliasing(M)

Base.getindex(M::DSparseMatrix{T}, I::Int) where T =
    getindex(M.mat, I)
Base.getindex(M::DSparseMatrix{T}, I::Vararg{Int,N}) where {T,N} =
    getindex(M.mat, I...)
Base.setindex!(M::DSparseMatrix{T}, v, I::Int) where T =
    setindex!(m.mat, v, I)
Base.setindex!(M::DSparseMatrix{T}, v, I::Vararg{Int,N}) where {T,N} =
    setindex!(M.mat, v, I...)

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