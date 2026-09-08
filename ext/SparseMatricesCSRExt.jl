module SparseMatricesCSRExt

# Host CSR tiles for sparse `DArray`s. `SparseMatrixCSR` is the ecosystem type
# (SparseMatricesCSR.jl); this extension makes it a first-class `DSparseArray`
# inner format so tiled `mul!` / SpMV / SpGEMM stay sparse.
#
# BSR is a follow-up: Julia has no host block-sparse type (`BlockArrays` is
# dense mortar; vendor BSR is GPU-only). Do not invent `Dagger.BSR`.

import SparseMatricesCSR
import SparseMatricesCSR: SparseMatrixCSR, sparsecsr, spzeroscsr
import SparseArrays
import SparseArrays: SparseMatrixCSC
import LinearAlgebra
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType
import Dagger: DSparseArray, DSparseMatrix, DArray, DMatrix

#------------------------------------------------------------------------------
# CSC ↔ CSR (no densify)
#------------------------------------------------------------------------------

# CSR(m, n, rowptr, colval, nzval) is the transpose of
# CSC(n, m, rowptr, colval, nzval). Shift `Bi`-based pointers/indices to 1-based
# first; `copy(transpose(::SparseMatrixCSC))` is SparseArrays' ftranspose.
function _csr_to_csc(A::SparseMatrixCSR{Bi,Tv,Ti}) where {Bi,Tv,Ti}
    o = 1 - Bi
    rowptr = copy(A.rowptr)
    colval = copy(A.colval)
    if o != 0
        rowptr .+= o
        colval .+= o
    end
    At = SparseMatrixCSC{Tv,Ti}(A.n, A.m, rowptr, colval, copy(A.nzval))
    return copy(transpose(At))
end

function SparseArrays.SparseMatrixCSC{Tv,Ti}(A::SparseMatrixCSR) where {Tv,Ti}
    S = _csr_to_csc(A)
    return SparseMatrixCSC{Tv,Ti}(S)
end
SparseArrays.SparseMatrixCSC(A::SparseMatrixCSR{Bi,Tv,Ti}) where {Bi,Tv,Ti} =
    SparseMatrixCSC{Tv,Ti}(A)

_as_host_csc(A::SparseMatrixCSC) = A
_as_host_csc(A::SparseMatrixCSR) = _csr_to_csc(A)
_as_host_csc(A) = SparseMatrixCSC(Dagger._sparse_collect(A))

function _restore_format(::SparseMatrixCSR{Bi,Tv,Ti}, S::SparseMatrixCSC) where {Bi,Tv,Ti}
    return convert(SparseMatrixCSR{Bi,Tv,Ti}, S)
end
_restore_format(::SparseMatrixCSC, S::SparseMatrixCSC) = S
_restore_format(_, S::SparseMatrixCSC) = S

# Range/vector `getindex` is missing on `SparseMatrixCSR` (scalar only). The
# generic AbstractArray path `similar`s a dense `Matrix` and then `setindex!`s
# structural zeros, which throws. `distribute(csr, Blocks)` slices through
# `ArrayDomain` UnitRanges, so this method is what keeps those tiles sparse.
function Base.getindex(A::SparseMatrixCSR{Bi,Tv,Ti},
                       I::AbstractVector, J::AbstractVector) where {Bi,Tv,Ti}
    return convert(SparseMatrixCSR{Bi,Tv,Ti}, _csr_to_csc(A)[I, J])
end

#------------------------------------------------------------------------------
# DSparseArray hooks
#------------------------------------------------------------------------------

Dagger.wraps_as_sparse_tile(::SparseMatrixCSR) = true
Dagger._sparse_collect(A::SparseMatrixCSR) = _csr_to_csc(A)
Dagger._sparse_copy(A::SparseMatrixCSR) = copy(A)
function Dagger._sparse_similar(::SparseMatrixCSR, ::Type{T}, dims::Dims{2}) where T
    return spzeroscsr(T, dims...)
end

# CSR `setindex!` refuses structural inserts, so the default in-place view copy
# cannot grow a tile during repartition. Rebuild through CSC.
function Dagger._sparse_copyto_view!(mat::SparseMatrixCSR{Bi,Tv,Ti}, Brange, src) where {Bi,Tv,Ti}
    host = _csr_to_csc(mat)
    copyto!(view(host, Brange), src)
    return convert(SparseMatrixCSR{Bi,Tv,Ti}, host)
end

_apply_trans(X, t::Char) =
    t == 'N' ? X :
    t == 'T' ? transpose(X) :
    t == 'C' ? adjoint(X) :
    throw(ArgumentError("Invalid trans char: $t"))

# SpMV: keep the 'N' path on CSR (row-wise). Transposed/adjoint fall back to
# CSC so we pick up SparseArrays' 5-arg methods (CSR only has 5-arg for `'N'`,
# and its Adjoint 3-arg method skips conjugation).
function Dagger.matvecmul!(C::AbstractVector, transA::Char, A::SparseMatrixCSR, B::AbstractVector, alpha, beta)
    if transA == 'N'
        LinearAlgebra.mul!(C, A, B, alpha, beta)
    else
        LinearAlgebra.mul!(C, _apply_trans(_csr_to_csc(A), transA), B, alpha, beta)
    end
    return C
end

function _assign_product!(C::DSparseMatrix, prod::SparseMatrixCSC, beta)
    if iszero(beta)
        result = prod
    else
        Ch = _as_host_csc(C.mat)
        result = isone(beta) ? prod + Ch : prod + beta * Ch
    end
    C.mat = _restore_format(C.mat, SparseMatrixCSC(result))
    return C
end

function _csr_spgemm!(C::DSparseMatrix, transA::Char, transB::Char, A, B, alpha, beta)
    opA = _apply_trans(_as_host_csc(A), transA)
    opB = _apply_trans(_as_host_csc(B), transB)
    AB = opA * opB
    prod = isone(alpha) ? SparseMatrixCSC(AB) : SparseMatrixCSC(alpha * AB)
    return _assign_product!(C, prod, beta)
end

# CSR × CSR, plus mixed CSC/CSR. CSC × CSC stays on SparseArraysExt.
function Dagger.matmatmul!(C::DSparseMatrix, transA::Char, transB::Char,
                           A::SparseMatrixCSR, B::SparseMatrixCSR, alpha, beta)
    return _csr_spgemm!(C, transA, transB, A, B, alpha, beta)
end
function Dagger.matmatmul!(C::DSparseMatrix, transA::Char, transB::Char,
                           A::SparseMatrixCSR, B::SparseMatrixCSC, alpha, beta)
    return _csr_spgemm!(C, transA, transB, A, B, alpha, beta)
end
function Dagger.matmatmul!(C::DSparseMatrix, transA::Char, transB::Char,
                           A::SparseMatrixCSC, B::SparseMatrixCSR, alpha, beta)
    return _csr_spgemm!(C, transA, transB, A, B, alpha, beta)
end

function Dagger.transpose_tile(B::SparseMatrixCSR)
    return SparseMatrixCSR(Dagger.transpose_tile(_csr_to_csc(B)))
end
function Dagger.transpose_tile(B::SparseMatrixCSR, uplo::Char)
    return SparseMatrixCSR(Dagger.transpose_tile(_csr_to_csc(B), uplo))
end

#------------------------------------------------------------------------------
# convert / sparsecsr / spzeroscsr on DMatrix
#------------------------------------------------------------------------------

# SparseMatricesCSR's `convert(SparseMatrixCSR, ::AbstractMatrix)` transposes
# the DMatrix and densifies. Gather through the existing CSC assemble instead.
function SparseMatricesCSR.sparsecsr(A::DMatrix)
    return sparsecsr(SparseArrays.sparse(A))
end
function SparseMatricesCSR.sparsecsr(::Val{Bi}, A::DMatrix) where Bi
    return sparsecsr(Val(Bi), SparseArrays.sparse(A))
end

function Base.convert(::Type{SparseMatrixCSR{Bi,Tv,Ti}}, A::DMatrix) where {Bi,Tv,Ti}
    return convert(SparseMatrixCSR{Bi,Tv,Ti}, SparseArrays.sparse(A))
end
Base.convert(::Type{SparseMatrixCSR}, A::DMatrix) = sparsecsr(A)

# Named kernel (MPI-stable): convert one tile to host CSR without densifying.
function _tile_to_csr(tile)
    if tile isa DSparseArray
        mat = tile.mat
        mat isa SparseMatrixCSR && return DSparseArray(copy(mat))
        return DSparseArray(SparseMatrixCSR(Dagger._sparse_collect(mat)))
    end
    return DSparseArray(SparseMatrixCSR(SparseArrays.sparse(tile)))
end

function _csr_from_tiles(A::DMatrix{T}) where T
    Ac = A.chunks
    new_chunks = Array{Dagger.DTask}(undef, size(Ac))
    for I in eachindex(Ac)
        new_chunks[I] = Dagger.@spawn return_type=DSparseArray{T,2} _tile_to_csr(Ac[I])
    end
    return DArray(T, A.domain, A.subdomains, new_chunks, A.partitioning, A.concat)
end

"""
    sparsecsr(A::DMatrix, part::Blocks)

A new `DMatrix` with the same values as `A` and `SparseMatrixCSR` tiles under
`part`. This is the distributed, tile-preserving convert; `sparsecsr(A)`
without `part` gathers to one host `SparseMatrixCSR`, matching
`sparse(::DMatrix)` → `SparseMatrixCSC`.
"""
function SparseMatricesCSR.sparsecsr(A::DMatrix, part::Blocks{2})
    A = fetch(A)
    B = A.partitioning == part ? A : Dagger.repartition(A, part)
    return _csr_from_tiles(B)
end

function SparseMatricesCSR.sparsecsr(A::AbstractMatrix, part::Blocks{2})
    return Dagger.distribute(sparsecsr(A), part)
end

const _COOIndexVec = AbstractVector{<:Integer}

function SparseMatricesCSR.sparsecsr(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                                     m::Integer, n::Integer, combine::Function, part::Blocks{2};
                                     assignment::AssignmentType=:arbitrary)
    A = SparseArrays.sparse(I, J, V, Int(m), Int(n), combine, part; assignment)
    return sparsecsr(A, part)
end
SparseMatricesCSR.sparsecsr(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                            m::Integer, n::Integer, part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    sparsecsr(I, J, V, m, n, +, part; assignment)
SparseMatricesCSR.sparsecsr(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                            part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    sparsecsr(I, J, V, isempty(I) ? 0 : Int(maximum(I)), isempty(J) ? 0 : Int(maximum(J)), +, part; assignment)
SparseMatricesCSR.sparsecsr(part::Blocks{2}, I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                            m::Integer, n::Integer; assignment::AssignmentType=:arbitrary) =
    sparsecsr(I, J, V, m, n, +, part; assignment)
SparseMatricesCSR.sparsecsr(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                            m::Integer, n::Integer, combine::Function, ::AutoBlocks; assignment::AssignmentType=:arbitrary) =
    sparsecsr(I, J, V, m, n, combine, Dagger.auto_blocks((Int(m), Int(n))); assignment)

# Named allocator so MPI hashes the same function on every rank.
_csr_zeros_tile(::Type{T}, dims::Dims) where T = DSparseArray(spzeroscsr(T, dims...))

function SparseMatricesCSR.spzeroscsr(p::Blocks, T::Type, dims::Dims{2}; assignment::AssignmentType=:arbitrary)
    d = Dagger.ArrayDomain(map(x -> 1:x, dims))
    a = Dagger.AllocateArray(T, _csr_zeros_tile, false, d, Dagger.partition(p, d), p, assignment;
                             return_type=DSparseArray{T,2})
    return Dagger._to_darray(a)
end
SparseMatricesCSR.spzeroscsr(p::BlocksOrAuto, T::Type, m::Integer, n::Integer; assignment::AssignmentType=:arbitrary) =
    spzeroscsr(p, T, (Int(m), Int(n)); assignment)
SparseMatricesCSR.spzeroscsr(p::BlocksOrAuto, m::Integer, n::Integer; assignment::AssignmentType=:arbitrary) =
    spzeroscsr(p, Float64, (Int(m), Int(n)); assignment)
SparseMatricesCSR.spzeroscsr(::AutoBlocks, T::Type, dims::Dims{2}; assignment::AssignmentType=:arbitrary) =
    spzeroscsr(Dagger.auto_blocks(dims), T, dims; assignment)

function SparseArrays.spzeros(::Type{<:SparseMatrixCSR}, p::Blocks{2}, T::Type, dims::Dims{2};
                              assignment::AssignmentType=:arbitrary)
    return spzeroscsr(p, T, dims; assignment)
end
SparseArrays.spzeros(::Type{<:SparseMatrixCSR}, p::Blocks{2}, T::Type, m::Integer, n::Integer;
                     assignment::AssignmentType=:arbitrary) =
    spzeroscsr(p, T, (Int(m), Int(n)); assignment)
SparseArrays.spzeros(::Type{<:SparseMatrixCSR}, p::Blocks{2}, m::Integer, n::Integer;
                     assignment::AssignmentType=:arbitrary) =
    spzeroscsr(p, Float64, (Int(m), Int(n)); assignment)

end # module SparseMatricesCSRExt
