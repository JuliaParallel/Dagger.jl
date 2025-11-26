module SparseArraysExt

import SparseArrays
import SparseArrays: SparseMatrixCSC
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseMatrix

Dagger.sparse_mode(::SparseMatrixCSC) = :sparsearrays
Dagger._sparse_alloc(::Val{:sparsearrays}, T::Type, dims::Dims) =
    SparseArrays.spzeros(T, dims...)

function SparseArrays.spzeros(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    a = Dagger.AllocateArray(T, SparseArrays.spzeros, false, d, Dagger.partition(p, d), p, assignment)
    return Dagger._to_darray(a)
end
SparseArrays.spzeros(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.spzeros(p, T, dims; assignment)
SparseArrays.spzeros(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.spzeros(p, Float64, dims; assignment)
SparseArrays.spzeros(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) =
    SparseArrays.spzeros(p, Float64, dims; assignment)
SparseArrays.spzeros(::AutoBlocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    SparseArrays.spzeros(Dagger.auto_blocks(dims), T, dims; assignment)

function SparseArrays.sprand(p::Blocks, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseMatrix{T}(SparseArrays.sprand(T, _dims..., sparsity)), false, d, Dagger.partition(p, d), p, assignment)
    return Dagger._to_darray(a)
end
SparseArrays.sprand(p::BlocksOrAuto, T::Type, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.sprand(p, T, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
SparseArrays.sprand(p::BlocksOrAuto, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.sprand(p, Float64, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
SparseArrays.sprand(p::BlocksOrAuto, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    SparseArrays.sprand(p, Float64, dims, sparsity; assignment)
SparseArrays.sprand(::AutoBlocks, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    SparseArrays.sprand(Dagger.auto_blocks(dims), T, dims, sparsity; assignment)

function Dagger.matmatmul!(
    C::DSparseMatrix,
    transA::Char,
    transB::Char,
    A::SparseMatrixCSC,
    B::SparseMatrixCSC,
    alpha,
    beta
)
    # Use fallback implementation
    # TODO: Optimize this further
    C.mat = alpha * A * B + beta * C.mat

    return C
end

function Dagger.transpose_tile(B::SparseMatrixCSC)
    return SparseArrays.sparse(B')
end

end # module SparseArraysExt
