module SparseArraysExt

import SparseArrays
import SparseArrays: SparseMatrixCSC
import LinearAlgebra
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseMatrix

# Keep tiles sparse through `collect`/`cat`; the outer `collect` densifies.
Dagger._sparse_collect(M::SparseMatrixCSC) = copy(M)
# Wrap bare sparse tiles (e.g. from `distribute`) so Datadeps sees a stable container.
Dagger.maybe_wrap_tile(x::SparseMatrixCSC) = DSparseMatrix{eltype(x)}(x)

function SparseArrays.spzeros(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseMatrix{T}(SparseArrays.spzeros(T, _dims...)), false, d, Dagger.partition(p, d), p, assignment)
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

_apply_trans(X, t::Char) =
    t == 'N' ? X :
    t == 'T' ? transpose(X) :
    t == 'C' ? adjoint(X) :
    throw(ArgumentError("Invalid trans char: $t"))

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
    opA = _apply_trans(A, transA)
    opB = _apply_trans(B, transB)
    C.mat = alpha * (opA * opB) + beta * C.mat

    return C
end

# Off-diagonal tile copy in `copytri!`: produce the (conjugate) transpose tile.
function Dagger.transpose_tile(B::SparseMatrixCSC)
    return SparseArrays.sparse(B')
end
# Diagonal tile symmetrization in `copytri!`: build the full Hermitian tile from
# its `uplo` triangle (matching the dense `copydiagtile!` semantics).
function Dagger.transpose_tile(B::SparseMatrixCSC, uplo::Char)
    if uplo == 'U'
        Bt = SparseArrays.triu(B)
    elseif uplo == 'L'
        Bt = SparseArrays.tril(B)
    else
        throw(ArgumentError("uplo must be 'U' or 'L', got $uplo"))
    end
    C = Bt + Bt'
    # The shared diagonal was added twice; restore the original tile's diagonal.
    for i in 1:LinearAlgebra.checksquare(B)
        C[i, i] = B[i, i]
    end
    return C
end

end # module SparseArraysExt
