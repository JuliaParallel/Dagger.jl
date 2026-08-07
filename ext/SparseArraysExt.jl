module SparseArraysExt

import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseArray, DSparseMatrix

# Keep tiles sparse through `collect`/`cat`; the outer `collect` densifies.
Dagger._sparse_collect(M::SparseMatrixCSC) = copy(M)

# Assemble already-local tiles into one global `SparseMatrixCSC` without
# densifying: unwrap each tile, offset its (i,j) by the precomputed subdomain
# offsets, and build from triplets. Intended to run inside a worker-scoped task
# (the scheduler moves the tile chunks there); used by `Dagger.klu` / `Dagger.splu`.
function Dagger._gather_sparse(::Type{T}, tiles, row_offsets, col_offsets, m, n) where T
    Is = Int[]; Js = Int[]; Vs = T[]
    for k in 1:length(tiles)
        tile = SparseMatrixCSC(Dagger._tile_matrix(tiles[k]))
        ti, tj, tv = SparseArrays.findnz(tile)
        append!(Is, ti .+ row_offsets[k])
        append!(Js, tj .+ col_offsets[k])
        append!(Vs, tv)
    end
    return SparseArrays.sparse(Is, Js, Vs, m, n)
end

# Dense → sparse for Stage-4c Schur complements (fill-in is expected).
Dagger._sparse_copy_of(S::AbstractMatrix) = SparseArrays.sparse(S)
Dagger._sparse_copy_of(S::SparseMatrixCSC) = S

# Wrap bare sparse tiles (e.g. from `distribute`) so Datadeps sees a stable container.
Dagger.maybe_wrap_tile(x::SparseMatrixCSC) = DSparseArray(x)
Dagger.maybe_wrap_tile(x::SparseVector) = DSparseArray(x)
Dagger.maybe_wrap_tile(x::Dagger.DeviceSparseMatrixCSC) = DSparseArray(x)

# Host defaults for scoped sparse allocation (GPU Exts override per-processor).
Dagger.allocate_sparse_zeros_default(::Dagger.Processor, ::Type{T}, dims::Dims{2}) where T =
    SparseArrays.spzeros(T, dims...)
Dagger.allocate_sparse_zeros_default(::Dagger.Processor, ::Type{T}, dims::Dims{1}) where T =
    SparseArrays.spzeros(T, dims...)
Dagger.allocate_sparse_rand_default(::Dagger.Processor, ::Type{T}, dims::Dims{2}, sparsity::AbstractFloat) where T =
    SparseArrays.sprand(T, dims..., sparsity)
Dagger.allocate_sparse_rand_default(::Dagger.Processor, ::Type{T}, dims::Dims{1}, sparsity::AbstractFloat) where T =
    SparseArrays.sprand(T, dims..., sparsity)

# DeviceSparseMatrixCSC ↔ SparseMatrixCSC
function SparseArrays.SparseMatrixCSC(A::Dagger.DeviceSparseMatrixCSC{Tv,Ti}) where {Tv,Ti}
    return SparseMatrixCSC{Tv,Ti}(A.m, A.n, Array(A.colptr), Array(A.rowval), Array(A.nzval))
end
"""
    device_sparse_from_host(Arr, S::SparseMatrixCSC) -> DeviceSparseMatrixCSC

Upload a host CSC onto device vectors of type `Arr` (e.g. `CLArray`, `MtlArray`,
`oneArray`). Indices are converted to `Int32` for device friendliness.
"""
function Dagger.device_sparse_from_host(::Type{Arr}, S::SparseMatrixCSC{Tv}) where {Tv,Arr}
    colptr = Arr(Int32.(S.colptr))
    rowval = Arr(Int32.(S.rowval))
    nzval = Arr(Array(S.nzval))
    return Dagger.DeviceSparseMatrixCSC(S.m, S.n, colptr, rowval, nzval)
end
Base.copy(A::Dagger.DeviceSparseMatrixCSC) =
    Dagger.DeviceSparseMatrixCSC(A.m, A.n, copy(A.colptr), copy(A.rowval), copy(A.nzval))
Dagger._sparse_copy(A::Dagger.DeviceSparseMatrixCSC) = copy(A)
Dagger._sparse_collect(A::Dagger.DeviceSparseMatrixCSC) = SparseMatrixCSC(A)
function Dagger._sparse_similar(A::Dagger.DeviceSparseMatrixCSC{Tv,Ti}, ::Type{T}, dims::Dims{2}) where {Tv,Ti,T}
    n = dims[2]
    colptr = similar(A.colptr, Ti, n + 1)
    fill!(colptr, one(Ti))
    rowval = similar(A.rowval, Ti, 0)
    nzval = similar(A.nzval, T, 0)
    return Dagger.DeviceSparseMatrixCSC(dims[1], n, colptr, rowval, nzval)
end

# Rebuild a DeviceSparseMatrixCSC on the same device array type as `like`.
function _to_device_sparse(like::Dagger.DeviceSparseMatrixCSC, S::SparseMatrixCSC)
    colptr = similar(like.colptr, eltype(like.colptr), length(S.colptr))
    rowval = similar(like.rowval, eltype(like.rowval), length(S.rowval))
    nzval = similar(like.nzval, eltype(S), length(S.nzval))
    copyto!(colptr, eltype(colptr).(S.colptr))
    copyto!(rowval, eltype(rowval).(S.rowval))
    copyto!(nzval, S.nzval)
    return Dagger.DeviceSparseMatrixCSC(S.m, S.n, colptr, rowval, nzval)
end

function SparseArrays.spzeros(p::BlocksOrAuto, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    part, assign, plan = Dagger.Tapes.resolve_partitioning(T, dims, p, assignment)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    N = length(dims)
    # Route through `allocate_sparse_zeros` so a GPU compute scope yields
    # device-resident sparse tiles (vendor sparse or DeviceSparseMatrixCSC).
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseArray(Dagger.allocate_sparse_zeros(Dagger.task_processor(), T, _dims)), false, d, Dagger.partition(part, d), part, assign;
                             return_type=DSparseArray{T,N})
    return Dagger.Tapes.track!(Dagger._to_darray(a), plan)
end
SparseArrays.spzeros(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.spzeros(p, T, dims; assignment)
SparseArrays.spzeros(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.spzeros(p, Float64, dims; assignment)
SparseArrays.spzeros(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) =
    SparseArrays.spzeros(p, Float64, dims; assignment)

function SparseArrays.sprand(p::BlocksOrAuto, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary)
    part, assign, plan = Dagger.Tapes.resolve_partitioning(T, dims, p, assignment)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    N = length(dims)
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseArray(Dagger.allocate_sparse_rand(Dagger.task_processor(), T, _dims, sparsity)), false, d, Dagger.partition(part, d), part, assign;
                             return_type=DSparseArray{T,N})
    return Dagger.Tapes.track!(Dagger._to_darray(a), plan)
end
SparseArrays.sprand(p::BlocksOrAuto, T::Type, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.sprand(p, T, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
SparseArrays.sprand(p::BlocksOrAuto, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    SparseArrays.sprand(p, Float64, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
SparseArrays.sprand(p::BlocksOrAuto, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    SparseArrays.sprand(p, Float64, dims, sparsity; assignment)

_apply_trans(X, t::Char) =
    t == 'N' ? X :
    t == 'T' ? transpose(X) :
    t == 'C' ? adjoint(X) :
    throw(ArgumentError("Invalid trans char: $t"))

function _sparse_gemm_assign!(C::DSparseMatrix, prod, beta)
    if iszero(beta)
        C.mat = prod
    elseif isone(beta)
        C.mat = prod + C.mat
    else
        C.mat = prod + beta * C.mat
    end
    return C
end

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
    return _sparse_gemm_assign!(C, prod, beta)
end

# DeviceSparseMatrixCSC SpGEMM: gather to host, multiply, scatter back.
function Dagger.matmatmul!(
    C::DSparseMatrix,
    transA::Char,
    transB::Char,
    A::Dagger.DeviceSparseMatrixCSC,
    B::Dagger.DeviceSparseMatrixCSC,
    alpha,
    beta
)
    Ah = SparseMatrixCSC(A)
    Bh = SparseMatrixCSC(B)
    Ch = C.mat isa Dagger.DeviceSparseMatrixCSC ? SparseMatrixCSC(C.mat) :
         C.mat isa SparseMatrixCSC ? C.mat : SparseMatrixCSC(C.mat)
    opA = _apply_trans(Ah, transA)
    opB = _apply_trans(Bh, transB)
    AB = opA * opB
    prod = isone(alpha) ? AB : alpha * AB
    if iszero(beta)
        result = prod
    elseif isone(beta)
        result = prod + Ch
    else
        result = prod + beta * Ch
    end
    C.mat = _to_device_sparse(A, SparseMatrixCSC(result))
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

# DeviceSparseMatrixCSC SpMV: host fallback (works for any dense vector type
# that supports Array(::)/copyto!).
function Dagger.matvecmul!(C::AbstractVector, transA::Char, A::Dagger.DeviceSparseMatrixCSC, B::AbstractVector, alpha, beta)
    Ah = SparseMatrixCSC(A)
    Bh = Array(B)
    Ch = Array(C)
    LinearAlgebra.mul!(Ch, _apply_trans(Ah, transA), Bh, alpha, beta)
    copyto!(C, Ch)
    return C
end

# Off-diagonal tile copy in `copytri!`: produce the (conjugate) transpose tile.
function Dagger.transpose_tile(B::SparseMatrixCSC)
    return SparseArrays.sparse(B')
end
function Dagger.transpose_tile(B::Dagger.DeviceSparseMatrixCSC)
    return _to_device_sparse(B, SparseArrays.sparse(SparseMatrixCSC(B)'))
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
function Dagger.transpose_tile(B::Dagger.DeviceSparseMatrixCSC, uplo::Char)
    return _to_device_sparse(B, Dagger.transpose_tile(SparseMatrixCSC(B), uplo))
end

end # module SparseArraysExt
