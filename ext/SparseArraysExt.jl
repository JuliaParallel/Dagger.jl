module SparseArraysExt

import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseArray, DSparseMatrix

# Keep tiles sparse through `collect`/`cat`; the outer `collect` densifies.
Dagger._sparse_collect(M::SparseMatrixCSC) = copy(M)
# Wrap bare sparse tiles (e.g. from `distribute`) so Datadeps sees a stable container.
Dagger.maybe_wrap_tile(x::SparseMatrixCSC) = DSparseArray(x)
Dagger.maybe_wrap_tile(x::SparseVector) = DSparseArray(x)

# --- Eager freeing / Libc-backing of `SparseArrays` tile storage ------------
#
# A `SparseMatrixCSC`/`SparseVector` is just a handful of dense vectors, so it
# gets the same Libc-backed-allocation + eager-free treatment as a dense `Array`
# (see `Dagger.libc_backed`/`Dagger.unsafe_free!`). This lets the memory-aware
# Datadeps planner reclaim and disk-spill sparse tiles, not just dense ones.

# Free the backing vectors (no-op for any that are not Libc-backed). The struct
# itself is immutable and GC-managed; we only reclaim its buffers.
function Dagger.unsafe_free!(A::SparseMatrixCSC)
    Dagger.unsafe_free!(A.colptr)
    Dagger.unsafe_free!(A.rowval)
    Dagger.unsafe_free!(A.nzval)
    return
end
function Dagger.unsafe_free!(A::SparseVector)
    Dagger.unsafe_free!(A.nzind)
    Dagger.unsafe_free!(A.nzval)
    return
end

# Rebuild the tile sharing its structure but with Libc-backed buffers, so it can
# be freed eagerly and spilled to disk. The CSC/sparse-vector invariants are
# preserved (same indices/values, just copied into Libc memory); buffers already
# Libc-backed are returned as-is by `libc_backed`, so this is effectively a no-op
# the second time around.
Dagger.libc_backed(A::SparseMatrixCSC) =
    SparseMatrixCSC(A.m, A.n,
                    Dagger.libc_backed(A.colptr),
                    Dagger.libc_backed(A.rowval),
                    Dagger.libc_backed(A.nzval))
Dagger.libc_backed(A::SparseVector) =
    SparseVector(A.n, Dagger.libc_backed(A.nzind), Dagger.libc_backed(A.nzval))

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
    opA = _apply_trans(A, transA)
    opB = _apply_trans(B, transB)
    # Sparse*sparse yields a freshly-allocated sparse matrix, which we reassign
    # into the wrapper (`DSparseMatrix` hides this reallocation from Datadeps).
    # `SparseArrays` provides no efficient 5-arg `mul!` into a sparse `C` -- the
    # output sparsity pattern is determined by the product -- so we form the
    # product out-of-place and apply only the alpha/beta scaling that is actually
    # needed. The transposed-operand products dispatch to specialized SparseArrays
    # methods, so `opA`/`opB` are not materialized.
    AB = opA * opB
    prod = isone(alpha) ? AB : alpha * AB
    if iszero(beta)
        C.mat = prod
    elseif isone(beta)
        C.mat = prod + C.mat
    else
        C.mat = prod + beta * C.mat
    end

    return C
end

# Sparse matrix-vector multiply tile kernel: `C = alpha*op(A)*B + beta*C` with a
# `SparseMatrixCSC` `A` and dense vectors `B`/`C`. SparseArrays provides an
# efficient 5-arg `mul!` (SpMV) into a dense output, including for transposed and
# adjoint operands, so this updates `C` in place with no allocation.
function Dagger.matvecmul!(C::AbstractVector, transA::Char, A::SparseMatrixCSC, B::AbstractVector, alpha, beta)
    LinearAlgebra.mul!(C, _apply_trans(A, transA), B, alpha, beta)
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
