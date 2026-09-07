module SparseArraysExt

import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseArray, DSparseMatrix
import Dagger: DArray, DMatrix, In, InOut, SparseCOOBucket

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

# Wrap bare sparse tiles (e.g. from `distribute`, or a user's sparse Datadeps
# argument) so Datadeps sees a stable container.
Dagger.wraps_as_sparse_tile(::SparseMatrixCSC) = true
Dagger.wraps_as_sparse_tile(::SparseVector) = true
Dagger.wraps_as_sparse_tile(::Dagger.DeviceSparseMatrixCSC) = true

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

function SparseArrays.spzeros(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    N = length(dims)
    # Route through `allocate_sparse_zeros` so a GPU compute scope yields
    # device-resident sparse tiles (vendor sparse or DeviceSparseMatrixCSC).
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseArray(Dagger.allocate_sparse_zeros(Dagger.task_processor(), T, _dims)), false, d, Dagger.partition(p, d), p, assignment;
                             return_type=DSparseArray{T,N})
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
    N = length(dims)
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseArray(Dagger.allocate_sparse_rand(Dagger.task_processor(), T, _dims, sparsity)), false, d, Dagger.partition(p, d), p, assignment;
                             return_type=DSparseArray{T,N})
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

#==============================================================================
  Incremental / one-shot sparse assembly (`sparse` / `sparse!` + `Blocks`)
==============================================================================#

# Restamp a host CSC as the executing processor's sparse tile. CPU `move` is
# identity; GPU extensions upload to CuSparse / ROCSparse / DeviceSparseMatrixCSC.
function Dagger._store_assembled_tile(S::SparseMatrixCSC)
    return Dagger.move(Dagger.OSProc(), Dagger.task_processor(), Dagger.DSparseArray(S))
end

function _buckets_to_csc(::Type{T}, tm::Integer, tn::Integer, combine, buckets) where T
    n = 0
    for b in buckets
        n += length(b.V)
    end
    I = Vector{Int}(undef, n)
    J = Vector{Int}(undef, n)
    V = Vector{T}(undef, n)
    p = 0
    for b in buckets
        len = length(b.V)
        len == 0 && continue
        copyto!(I, p + 1, b.I, 1, len)
        copyto!(J, p + 1, b.J, 1, len)
        copyto!(V, p + 1, b.V, 1, len)
        p += len
    end
    return SparseArrays.sparse(resize!(I, p), resize!(J, p), resize!(V, p), tm, tn, combine)
end

function _combine_host_csc(A::SparseMatrixCSC, S::SparseMatrixCSC, combine)
    nnz(A) == 0 && return S
    nnz(S) == 0 && return A
    if combine === +
        return A + S
    end
    I1, J1, V1 = SparseArrays.findnz(A)
    I2, J2, V2 = SparseArrays.findnz(S)
    return SparseArrays.sparse(vcat(I1, I2), vcat(J1, J2), vcat(V1, V2),
                               size(A, 1), size(A, 2), combine)
end

# Named kernel: mutate a `DSparseArray` tile by SparseArrays-combining the
# incoming local-index buckets. Whole-tile replace of `dest.mat` keeps Datadeps
# aliasing on the wrapper. Device tiles gather to host CSC, combine, re-upload.
function Dagger._assemble_coo_into_tile(dest, combine, buckets...)
    dest isa Dagger.DSparseArray || throw(ArgumentError(
        "sparse! destination tiles must be sparse; allocate with \
         spzeros(Blocks(...), T, m, n)"))
    tm, tn = size(dest)
    T = eltype(dest)
    S = _buckets_to_csc(T, tm, tn, combine, buckets)
    host = dest.mat isa SparseMatrixCSC ? dest.mat : Dagger._sparse_collect(dest.mat)
    combined = _combine_host_csc(host, S, combine)
    dest.mat = Dagger._store_assembled_tile(combined).mat
    return dest
end

function _coo_eltype_checked(V)
    T = eltype(V)
    T === Any && throw(ArgumentError(
        "V must have a concrete eltype; pass T via spzeros(Blocks, T, m, n) and sparse!"))
    return T
end

function _sparse_add_local_coo!(A::DArray{T,2}, I, J, V, combine) where T
    m, n = size(A)
    row_cum = A.subdomains.cumlength[1]
    col_cum = A.subdomains.cumlength[2]
    buckets = Dagger._bucket_coo_chunk(I, J, V, row_cum, col_cum, m, n)
    ntr, ntc = size(A.chunks)
    Dagger.spawn_datadeps() do
        for tj in 1:ntc, ti in 1:ntr
            dest = A.chunks[ti, tj]
            Dagger.@spawn return_type=DSparseArray{T,2} Dagger._assemble_coo_into_tile(
                InOut(dest), combine, buckets[ti, tj])
        end
    end
    return A
end

function _sparse_add_darray_coo!(A::DArray{T,2}, I::DArray, J::DArray, V::DArray, combine) where T
    size(I.chunks) == size(J.chunks) == size(V.chunks) ||
        throw(ArgumentError("I, J, V must have the same chunk layout"))
    m, n = size(A)
    row_cum = copy(A.subdomains.cumlength[1])
    col_cum = copy(A.subdomains.cumlength[2])
    Ichunks, Jchunks, Vchunks = I.chunks, J.chunks, V.chunks
    ncoo = length(Ichunks)
    ntr, ntc = size(A.chunks)
    Tv = eltype(V)
    Dagger.spawn_datadeps() do
        bucket_tasks = Vector{Dagger.DTask}(undef, ncoo)
        for c in 1:ncoo
            bucket_tasks[c] = Dagger.@spawn return_type=Matrix{SparseCOOBucket{Tv}} Dagger._bucket_coo_chunk(
                In(Ichunks[c]), In(Jchunks[c]), In(Vchunks[c]),
                row_cum, col_cum, m, n)
        end
        for tj in 1:ntc, ti in 1:ntr
            extracts = Vector{Dagger.DTask}(undef, ncoo)
            for c in 1:ncoo
                extracts[c] = Dagger.@spawn return_type=SparseCOOBucket{Tv} Dagger._extract_coo_bucket(
                    In(bucket_tasks[c]), ti, tj)
            end
            dest = A.chunks[ti, tj]
            in_extracts = ntuple(c -> In(extracts[c]), ncoo)
            Dagger.spawn(Dagger._assemble_coo_into_tile,
                         Dagger.Options(; return_type=DSparseArray{T,2}),
                         InOut(dest), combine, in_extracts...)
        end
    end
    return A
end

"""
    sparse!(A::DArray, I, J, V, combine=+)

Add COO triplets `(I[k], J[k], V[k])` into the sparse tiled `DArray` `A`
(typically from `spzeros(Blocks(...), T, m, n)`). Duplicate `(i,j)` entries are
combined with `combine`, matching `SparseArrays.sparse`. `I`, `J`, `V` may be
local vectors or `DArray`s of the same chunk layout; overlap rows are sent to
the owning tile rather than assembled on the producer.
"""
function SparseArrays.sparse!(A::DArray{T,2}, I::AbstractVector, J::AbstractVector,
                              V::AbstractVector, combine::Function=+) where T
    A.partitioning isa Blocks{2} || throw(ArgumentError(
        "sparse! requires a Blocks-partitioned DMatrix"))
    length(I) == length(J) == length(V) ||
        throw(ArgumentError("I, J, V must have the same length"))
    if I isa DArray && J isa DArray && V isa DArray
        return _sparse_add_darray_coo!(A, I, J, V, combine)
    elseif I isa DArray || J isa DArray || V isa DArray
        throw(ArgumentError("I, J, V must all be DArrays or all be local vectors"))
    else
        return _sparse_add_local_coo!(A, I, J, V, combine)
    end
end

"""
    sparse(I, J, V, m, n, [combine=+,] part::Blocks; assignment=:arbitrary)
    sparse(part::Blocks, I, J, V, m, n, [combine=+]; assignment=:arbitrary)

Assemble a sparse `DMatrix` from COO triplets without building a global
`SparseMatrixCSC` on one process. `part` is the output tiling; `I`, `J`, `V`
may be local vectors or `DArray`s. Duplicates use `combine` (`+` by default),
matching `SparseArrays.sparse`. Existing `sparse(I, J, V)` / `distribute`
behavior is unchanged.
"""
function SparseArrays.sparse(I::AbstractVector, J::AbstractVector, V::AbstractVector,
                             m::Integer, n::Integer, combine::Function, part::Blocks{2};
                             assignment::AssignmentType=:arbitrary)
    A = SparseArrays.spzeros(part, _coo_eltype_checked(V), Int(m), Int(n); assignment)
    return SparseArrays.sparse!(A, I, J, V, combine)
end
SparseArrays.sparse(I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    m::Integer, n::Integer, part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, +, part; assignment)
SparseArrays.sparse(I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    m::Integer, n::Integer, combine::Function, ::AutoBlocks; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, combine, Dagger.auto_blocks((Int(m), Int(n))); assignment)
SparseArrays.sparse(I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    m::Integer, n::Integer, ::AutoBlocks; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, +, AutoBlocks(); assignment)

function _coo_extent(I::AbstractVector)
    isempty(I) && return 0
    return Int(maximum(I))
end

SparseArrays.sparse(I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, _coo_extent(I), _coo_extent(J), +, part; assignment)
SparseArrays.sparse(I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    combine::Function, part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, _coo_extent(I), _coo_extent(J), combine, part; assignment)

SparseArrays.sparse(part::Blocks{2}, I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    m::Integer, n::Integer; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, +, part; assignment)
SparseArrays.sparse(part::Blocks{2}, I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    m::Integer, n::Integer, combine::Function; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, combine, part; assignment)
SparseArrays.sparse(part::AutoBlocks, I::AbstractVector, J::AbstractVector, V::AbstractVector,
                    m::Integer, n::Integer; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, part; assignment)

end # module SparseArraysExt
