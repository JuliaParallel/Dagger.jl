module SparseArraysExt

import SparseArrays
import SparseArrays: SparseMatrixCSC, SparseVector
import LinearAlgebra
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseArray, DSparseMatrix
import Dagger: DArray, DMatrix, DVector, SparseCOOBucket, SparseMatrixBSR, sparsebsr
import Dagger: GeometricMultigrid, GeometricMGLevel

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

# Overlapping ASM: assemble the halo-expanded block from already-host tiles
# without densifying. Offsets are relative to the bounding box of the
# intersecting tiles; the result is then sliced down to `Ω`.
function Dagger._asm_assemble_sparse(Ω::UnitRange{Int}, row_ranges, col_ranges, hosts)
    T = eltype(first(hosts))
    r0 = first(first(row_ranges))
    r1 = last(last(row_ranges))
    c0 = first(first(col_ranges))
    c1 = last(last(col_ranges))
    m = r1 - r0 + 1
    n = c1 - c0 + 1
    ntiles = length(hosts)
    row_offsets = Vector{Int}(undef, ntiles)
    col_offsets = Vector{Int}(undef, ntiles)
    tiles = Vector{Any}(undef, ntiles)
    k = 0
    for jr in eachindex(row_ranges), jc in eachindex(col_ranges)
        k += 1
        row_offsets[k] = first(row_ranges[jr]) - r0
        col_offsets[k] = first(col_ranges[jc]) - c0
        tiles[k] = hosts[k]
    end
    S = Dagger._gather_sparse(T, tiles, row_offsets, col_offsets, m, n)
    ri = (first(Ω) - r0 + 1):(last(Ω) - r0 + 1)
    ci = (first(Ω) - c0 + 1):(last(Ω) - c0 + 1)
    return S[ri, ci]
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
    SparseArrays.nnz(A) == 0 && return S
    SparseArrays.nnz(S) == 0 && return A
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

# Regular spawn (not datadeps): SparseCOOBucket is not a Datadeps-movable
# container, and assembly is a construction graph like AllocateArray.
function _with_replaced_chunks(A::DArray{T,N}, new_chunks) where {T,N}
    if eltype(A.chunks) >: eltype(new_chunks)
        copyto!(A.chunks, new_chunks)
        return A
    end
    return Dagger.DArray(T, A.domain, A.subdomains, new_chunks, A.partitioning, A.concat)
end

function _sparse_add_local_coo!(A::DArray{T,2}, I, J, V, combine) where T
    m, n = size(A)
    row_cum = A.subdomains.cumlength[1]
    col_cum = A.subdomains.cumlength[2]
    buckets = Dagger._bucket_coo_chunk(I, J, V, row_cum, col_cum, m, n)
    ntr, ntc = size(A.chunks)
    new_chunks = Matrix{Dagger.DTask}(undef, ntr, ntc)
    for tj in 1:ntc, ti in 1:ntr
        dest = A.chunks[ti, tj]
        new_chunks[ti, tj] = Dagger.@spawn return_type=DSparseArray{T,2} Dagger._assemble_coo_into_tile(
            dest, combine, buckets[ti, tj])
    end
    return _with_replaced_chunks(A, new_chunks)
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
    bucket_tasks = Vector{Dagger.DTask}(undef, ncoo)
    for c in 1:ncoo
        bucket_tasks[c] = Dagger.@spawn return_type=Matrix{SparseCOOBucket{Tv}} Dagger._bucket_coo_chunk(
            Ichunks[c], Jchunks[c], Vchunks[c], row_cum, col_cum, m, n)
    end
    new_chunks = Matrix{Dagger.DTask}(undef, ntr, ntc)
    for tj in 1:ntc, ti in 1:ntr
        extracts = Vector{Dagger.DTask}(undef, ncoo)
        for c in 1:ncoo
            extracts[c] = Dagger.@spawn return_type=SparseCOOBucket{Tv} Dagger._extract_coo_bucket(
                bucket_tasks[c], ti, tj)
        end
        dest = A.chunks[ti, tj]
        new_chunks[ti, tj] = Dagger.spawn(Dagger._assemble_coo_into_tile,
                                          Dagger.Options(; return_type=DSparseArray{T,2}),
                                          dest, combine, extracts...)
    end
    return _with_replaced_chunks(A, new_chunks)
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

# I/J are Integer-eltype so these win against SparseArrays'
# `sparse(I, J, V, m, n, combine)` (otherwise `Blocks` vs `combine` is ambiguous).
const _COOIndexVec = AbstractVector{<:Integer}

"""
    sparse(I, J, V, m, n, [combine=+,] part::Blocks; assignment=:arbitrary)
    sparse(part::Blocks, I, J, V, m, n, [combine=+]; assignment=:arbitrary)

Assemble a sparse `DMatrix` from COO triplets without building a global
`SparseMatrixCSC` on one process. `part` is the output tiling; `I`, `J`, `V`
may be local vectors or `DArray`s. Duplicates use `combine` (`+` by default),
matching `SparseArrays.sparse`. Existing `sparse(I, J, V)` / `distribute`
behavior is unchanged.
"""
function SparseArrays.sparse(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                             m::Integer, n::Integer, combine::Function, part::Blocks{2};
                             assignment::AssignmentType=:arbitrary)
    A = SparseArrays.spzeros(part, _coo_eltype_checked(V), Int(m), Int(n); assignment)
    return SparseArrays.sparse!(A, I, J, V, combine)
end
SparseArrays.sparse(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    m::Integer, n::Integer, part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, +, part; assignment)
SparseArrays.sparse(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    m::Integer, n::Integer, combine::Function, ::AutoBlocks; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, combine, Dagger.auto_blocks((Int(m), Int(n))); assignment)
SparseArrays.sparse(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    m::Integer, n::Integer, ::AutoBlocks; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, +, AutoBlocks(); assignment)

function _coo_extent(I::AbstractVector)
    isempty(I) && return 0
    return Int(maximum(I))
end

SparseArrays.sparse(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, _coo_extent(I), _coo_extent(J), +, part; assignment)
SparseArrays.sparse(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    combine::Function, part::Blocks{2}; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, _coo_extent(I), _coo_extent(J), combine, part; assignment)

SparseArrays.sparse(part::Blocks{2}, I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    m::Integer, n::Integer; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, +, part; assignment)
SparseArrays.sparse(part::Blocks{2}, I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    m::Integer, n::Integer, combine::Function; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, combine, part; assignment)
SparseArrays.sparse(part::AutoBlocks, I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                    m::Integer, n::Integer; assignment::AssignmentType=:arbitrary) =
    SparseArrays.sparse(I, J, V, m, n, part; assignment)

"""
    sparse(A::DMatrix) -> SparseMatrixCSC

Gather a tiled `DMatrix` into one host `SparseMatrixCSC`. Sparse tiles are
concatenated from their existing nonzeros (no densifying `cat`); a dense
`DMatrix` is `collect`ed and then converted. `collect(A)` itself still
returns a dense `Array`.
"""
function SparseArrays.sparse(A::Dagger.DMatrix{T}) where T
    A = fetch(A)
    isempty(A.chunks) && return SparseArrays.spzeros(T, size(A)...)
    c0 = Dagger._resolved_chunk(first(A.chunks))
    if Dagger.chunktype(c0) <: Dagger.DSparseArray
        return Dagger.uniform_execution() ? _sparse_from_tiles(A) :
               Dagger._collect_sparse_dmatrix(A)
    else
        return SparseArrays.sparse(Base.collect(A))
    end
end

function _sparse_from_tiles(A::Dagger.DMatrix{T}) where T
    m, n = size(A)
    Ac = A.chunks
    mt, nt = size(Ac)
    ntiles = mt * nt
    row_offsets = Vector{Int}(undef, ntiles)
    col_offsets = Vector{Int}(undef, ntiles)
    tiles = Vector{Any}(undef, ntiles)
    idx = 1
    for i in 1:mt, j in 1:nt
        dom = A.subdomains[i, j]
        row_offsets[idx] = first(dom.indexes[1]) - 1
        col_offsets[idx] = first(dom.indexes[2]) - 1
        tiles[idx] = Ac[i, j]
        idx += 1
    end
    scope = Dagger._select_factor_scope(A)
    return fetch(Dagger.spawn(Dagger._gather_sparse_from_tiles,
                              Dagger.Options(; compute_scope=scope),
                              T, row_offsets, col_offsets, m, n, tiles...))
end

# ---- Incomplete Cholesky (IC(0)) --------------------------------------------
# Right-looking no-fill Cholesky on the Hermitian lower triangle. The apply is
# `y ← (L L')⁻¹ x` via CSC forward / back substitution. Used as the per-tile
# operator for `BlockICPreconditioner` / `ichol`.

struct IC0Factor{Tv,Ti}
    L::SparseMatrixCSC{Tv,Ti}
end

_ic_as_sparse(A::SparseMatrixCSC) = A
_ic_as_sparse(A::AbstractMatrix) = SparseArrays.sparse(A)

function _hermitian_tril(A::SparseMatrixCSC{T}) where T
    LinearAlgebra.checksquare(A)
    H = T <: Complex ? (A + adjoint(A)) / 2 : (A + SparseArrays.transpose(A)) / 2
    return SparseArrays.tril(H)
end

function _ic0_factorize!(L::SparseMatrixCSC{T}) where T
    n = size(L, 1)
    colptr, rowval, nzval = L.colptr, L.rowval, L.nzval
    @inbounds for j in 1:n
        p0 = colptr[j]
        p1 = colptr[j+1] - 1
        (p0 <= p1 && rowval[p0] == j) || throw(LinearAlgebra.PosDefException(j))
        d = real(nzval[p0])
        d > 0 || throw(LinearAlgebra.PosDefException(j))
        s = sqrt(d)
        nzval[p0] = convert(T, s)
        for p in (p0 + 1):p1
            nzval[p] /= s
        end
        for p in (p0 + 1):p1
            k = rowval[p]
            lkj = nzval[p]
            pk = colptr[k]
            pk_end = colptr[k + 1] - 1
            for q in p:p1
                i = rowval[q]
                lij = nzval[q]
                while pk <= pk_end && rowval[pk] < i
                    pk += 1
                end
                if pk <= pk_end && rowval[pk] == i
                    nzval[pk] -= lij * conj(lkj)
                end
            end
        end
    end
    return L
end

function _ic0_forward!(y, L::SparseMatrixCSC)
    colptr, rowval, nzval = L.colptr, L.rowval, L.nzval
    n = size(L, 1)
    @inbounds for j in 1:n
        p0 = colptr[j]
        p1 = colptr[j+1] - 1
        y[j] /= nzval[p0]
        yj = y[j]
        for p in (p0 + 1):p1
            y[rowval[p]] -= nzval[p] * yj
        end
    end
    return y
end

function _ic0_backward!(y, L::SparseMatrixCSC)
    colptr, rowval, nzval = L.colptr, L.rowval, L.nzval
    n = size(L, 1)
    @inbounds for j in n:-1:1
        p0 = colptr[j]
        p1 = colptr[j+1] - 1
        acc = y[j]
        for p in (p0 + 1):p1
            acc -= conj(nzval[p]) * y[rowval[p]]
        end
        y[j] = acc / conj(nzval[p0])
    end
    return y
end

function LinearAlgebra.ldiv!(y::AbstractVector, F::IC0Factor, x::AbstractVector)
    copyto!(y, x)
    _ic0_forward!(y, F.L)
    _ic0_backward!(y, F.L)
    return y
end

function _ic0_operator(tile)
    S = _ic_as_sparse(Dagger._tile_matrix(tile))
    return IC0Factor(_ic0_factorize!(_hermitian_tril(S)))
end

function Dagger.BlockICPreconditioner(A::Dagger.DMatrix)
    return Dagger._build_block_preconditioner(Dagger.BlockICPreconditioner, A, _ic0_operator)
end

Dagger.ichol(A::Dagger.DMatrix) = Dagger.BlockICPreconditioner(A)

# Prefer in-place `ldiv!` over the generic Factorization `\` path.
Dagger._apply_inverse!(y, F::IC0Factor, x) = LinearAlgebra.ldiv!(y, F, x)

#==============================================================================
  Geometric multigrid (`GeometricMultigrid`)

  Transfer operators are assembled with `sparse(I, J, V, m, n, Blocks)` so they
  stay tiled and sparse (lesson 29). The V-cycle apply lives in
  `src/array/gmg.jl` and does not need SparseArrays.
==============================================================================#

function _gmg_assemble_coo(I, J, V, m::Integer, n::Integer, row_bs::Integer, col_bs::Integer)
    return SparseArrays.sparse(I, J, V, m, n, Blocks(row_bs, col_bs))
end

function _gmg_as_transfer(M, ::Type{T}, m::Integer, n::Integer,
                          row_bs::Integer, col_bs::Integer) where T
    size(M) == (m, n) || throw(DimensionMismatch(
        "transfer is $(size(M, 1))×$(size(M, 2)) but expected $m×$n"))
    part = Blocks(row_bs, col_bs)
    if M isa Dagger.DMatrix
        eltype(M) === T || throw(ArgumentError(
            "transfer eltype $(eltype(M)) does not match operator eltype $T"))
        return M.partitioning == part ? M : Dagger.repartition(M, part)
    end
    S = M isa SparseMatrixCSC ? M : SparseArrays.sparse(M)
    if eltype(S) !== T
        SI, SJ, SV = SparseArrays.findnz(S)
        S = SparseArrays.sparse(SI, SJ, T.(SV), size(S, 1), size(S, 2))
    end
    return Dagger.distribute(S, part)
end

function _gmg_make_R(::Type{T}, spec, grid, n::Integer, nc::Integer, k::Integer) where T
    if spec isa Symbol
        I, J, V, nc2, n2 = Dagger._gmg_restriction_coo(T, grid, spec)
        (nc2, n2) == (nc, n) || throw(DimensionMismatch(
            "restriction COO is $(nc2)×$(n2) but expected $nc×$n"))
        return _gmg_assemble_coo(I, J, V, nc, n, k, k)
    end
    return _gmg_as_transfer(spec, T, nc, n, k, k)
end

function _gmg_make_P(::Type{T}, spec, grid, n::Integer, nc::Integer, k::Integer) where T
    if spec isa Symbol
        I, J, V, n2, nc2 = Dagger._gmg_prolongation_coo(T, grid, spec)
        (n2, nc2) == (n, nc) || throw(DimensionMismatch(
            "prolongation COO is $(n2)×$(nc2) but expected $n×$nc"))
        return _gmg_assemble_coo(I, J, V, n, nc, k, k)
    end
    return _gmg_as_transfer(spec, T, n, nc, k, k)
end

function _gmg_galerkin(A::Dagger.DMatrix{T}, R::Dagger.DMatrix{T}, P::Dagger.DMatrix{T}) where T
    n = size(A, 1)
    nc = size(P, 2)
    k = Int(A.partitioning.blocksize[1])
    kc = Int(P.partitioning.blocksize[2])
    kr = Int(R.partitioning.blocksize[1])
    TT = Dagger.darray_tiletype(A)
    AP = Dagger.allocate_tiled(TT, T, Blocks(k, kc), (n, nc))
    LinearAlgebra.mul!(AP, A, P)
    Ac = Dagger.allocate_tiled(TT, T, Blocks(kr, kc), (nc, nc))
    LinearAlgebra.mul!(Ac, R, AP)
    return Ac
end

function _gmg_level(A::Dagger.DMatrix{T}, R::Dagger.DMatrix{T}, P::Dagger.DMatrix{T}) where T
    n = size(A, 1)
    nc = size(P, 2)
    k = Int(A.partitioning.blocksize[1])
    kc = Int(P.partitioning.blocksize[2])
    dinv = Dagger._jacobi_dinv(A)
    res = DVector{T}(undef, Blocks(k), n)
    coarse_x = DVector{T}(undef, Blocks(kc), nc)
    coarse_b = DVector{T}(undef, Blocks(kc), nc)
    return GeometricMGLevel(A, R, P, dinv, res, coarse_x, coarse_b)
end

function Dagger.GeometricMultigrid(A::Dagger.DMatrix{T};
                                   grid=nothing,
                                   restriction=:full_weighting,
                                   prolongation=nothing,
                                   max_levels::Integer=3,
                                   max_coarse::Integer=32,
                                   relax::Real=2 / 3,
                                   presweeps::Integer=2,
                                   postsweeps::Integer=2) where T
    max_levels >= 1 || throw(ArgumentError("max_levels must be ≥ 1"))
    max_coarse >= 1 || throw(ArgumentError("max_coarse must be ≥ 1"))
    presweeps >= 0 && postsweeps >= 0 || throw(ArgumentError(
        "presweeps and postsweeps must be ≥ 0"))

    n, A = Dagger._square_tiled_dmatrix(A)
    g0 = Dagger._gmg_normalize_grid(n, grid)
    g = g0
    k = Int(A.partitioning.blocksize[1])
    r_show = Dagger._gmg_kind_symbol(restriction)
    p_show = prolongation === nothing ? Dagger._gmg_default_prolongation(g0) :
             Dagger._gmg_kind_symbol(prolongation)

    levels = GeometricMGLevel[]
    first_level = true
    while length(levels) + 1 < max_levels && Dagger._gmg_can_coarsen(g, max_coarse)
        rspec = if first_level
            restriction
        else
            restriction isa Symbol ? restriction : :full_weighting
        end
        pspec = if first_level
            prolongation === nothing ? Dagger._gmg_default_prolongation(g) : prolongation
        elseif prolongation isa Symbol
            prolongation
        else
            Dagger._gmg_default_prolongation(g)
        end
        g_next = Dagger._gmg_coarse_grid(g)
        nf = prod(g)
        nc = prod(g_next)
        if !(rspec isa Symbol)
            size(rspec, 2) == nf || throw(DimensionMismatch(
                "restriction is $(size(rspec, 1))×$(size(rspec, 2)) but A is $(nf)×$(nf)"))
            nc = size(rspec, 1)
            nc == prod(g_next) || (g_next = (nc,))
        end
        if !(pspec isa Symbol)
            size(pspec) == (nf, nc) || throw(DimensionMismatch(
                "prolongation is $(size(pspec, 1))×$(size(pspec, 2)) but expected $nf×$nc"))
        end
        R = _gmg_make_R(T, rspec, g, nf, nc, k)
        P = _gmg_make_P(T, pspec, g, nf, nc, k)
        Ac = _gmg_galerkin(A, R, P)
        push!(levels, _gmg_level(A, R, P))
        A = Ac
        g = g_next
        first_level = false
    end
    coarse = Dagger._spawn_direct_factorization(A, LinearAlgebra.lu)
    part = Blocks(Int((isempty(levels) ? A : levels[1].A).partitioning.blocksize[1]))
    return GeometricMultigrid(levels, coarse, A, Float64(relax), Int(presweeps),
                              Int(postsweeps), n, part, g0, r_show, p_show)
end

#------------------------------------------------------------------------------
# SparseMatrixBSR (host block-CSR tiles)
#------------------------------------------------------------------------------

function SparseArrays.SparseMatrixCSC{Tv,Ti}(A::SparseMatrixBSR) where {Tv,Ti}
    I, J, V = Dagger._bsr_findnz(A)
    return SparseArrays.sparse(Tv.(I), Ti.(J), Tv.(V), size(A)...)
end
SparseArrays.SparseMatrixCSC(A::SparseMatrixBSR{Tv,Ti}) where {Tv,Ti} =
    SparseArrays.SparseMatrixCSC{Tv,Ti}(A)
SparseArrays.sparse(A::SparseMatrixBSR) = SparseArrays.SparseMatrixCSC(A)
SparseArrays.findnz(A::SparseMatrixBSR) = Dagger._bsr_findnz(A)
SparseArrays.nnz(A::SparseMatrixBSR) = length(A.nzval)

Dagger._sparse_collect(A::SparseMatrixBSR) = SparseArrays.SparseMatrixCSC(A)

function Dagger.matvecmul!(C::AbstractVector, transA::Char, A::SparseMatrixBSR, B::AbstractVector, alpha, beta)
    if transA == 'N'
        LinearAlgebra.mul!(C, A, B, alpha, beta)
    else
        LinearAlgebra.mul!(C, _apply_trans(SparseArrays.SparseMatrixCSC(A), transA), B, alpha, beta)
    end
    return C
end

function _bsr_spgemm!(C::DSparseMatrix, transA::Char, transB::Char, A, B, alpha, beta)
    opA = _apply_trans(SparseArrays.SparseMatrixCSC(A isa SparseMatrixBSR ? A : Dagger._sparse_collect(A)), transA)
    opB = _apply_trans(SparseArrays.SparseMatrixCSC(B isa SparseMatrixBSR ? B : Dagger._sparse_collect(B)), transB)
    AB = opA * opB
    prod = isone(alpha) ? SparseMatrixCSC(AB) : SparseMatrixCSC(alpha * AB)
    bs = C.mat isa SparseMatrixBSR ? C.mat.blocksize :
         A isa SparseMatrixBSR ? A.blocksize : (1, 1)
    if iszero(beta)
        C.mat = SparseMatrixBSR(prod, bs)
    else
        Ch = SparseArrays.SparseMatrixCSC(C.mat isa SparseMatrixBSR ? C.mat : Dagger._sparse_collect(C.mat))
        result = isone(beta) ? prod + Ch : prod + beta * Ch
        C.mat = SparseMatrixBSR(SparseMatrixCSC(result), bs)
    end
    return C
end

function Dagger.matmatmul!(C::DSparseMatrix, transA::Char, transB::Char,
                           A::SparseMatrixBSR, B::SparseMatrixBSR, alpha, beta)
    return _bsr_spgemm!(C, transA, transB, A, B, alpha, beta)
end
function Dagger.matmatmul!(C::DSparseMatrix, transA::Char, transB::Char,
                           A::SparseMatrixBSR, B::SparseMatrixCSC, alpha, beta)
    return _bsr_spgemm!(C, transA, transB, A, B, alpha, beta)
end
function Dagger.matmatmul!(C::DSparseMatrix, transA::Char, transB::Char,
                           A::SparseMatrixCSC, B::SparseMatrixBSR, alpha, beta)
    return _bsr_spgemm!(C, transA, transB, A, B, alpha, beta)
end

# Gather convert without densifying (overrides the core `collect` fallback).
function Dagger.sparsebsr(A::DMatrix, blocksize::Tuple{Integer,Integer})
    return SparseMatrixBSR(SparseArrays.sparse(A), (Int(blocksize[1]), Int(blocksize[2])))
end

function Dagger.sparsebsr(I::_COOIndexVec, J::_COOIndexVec, V::AbstractVector,
                          m::Integer, n::Integer, blocksize::Tuple{Integer,Integer},
                          part::Blocks{2}; assignment::AssignmentType=:arbitrary)
    A = SparseArrays.sparse(I, J, V, Int(m), Int(n), +, part; assignment)
    return sparsebsr(A, part, blocksize)
end

function SparseArrays.spzeros(::Type{<:SparseMatrixBSR}, p::Blocks{2}, T::Type, dims::Dims{2};
                              blocksize::Tuple{Integer,Integer}=(1, 1),
                              assignment::AssignmentType=:arbitrary)
    return Dagger._spzeros_bsr(p, T, dims, (Int(blocksize[1]), Int(blocksize[2])); assignment)
end
SparseArrays.spzeros(::Type{<:SparseMatrixBSR}, p::Blocks{2}, T::Type, m::Integer, n::Integer;
                     blocksize::Tuple{Integer,Integer}=(1, 1),
                     assignment::AssignmentType=:arbitrary) =
    SparseArrays.spzeros(SparseMatrixBSR, p, T, (Int(m), Int(n)); blocksize, assignment)
SparseArrays.spzeros(::Type{<:SparseMatrixBSR}, p::Blocks{2}, m::Integer, n::Integer;
                     blocksize::Tuple{Integer,Integer}=(1, 1),
                     assignment::AssignmentType=:arbitrary) =
    SparseArrays.spzeros(SparseMatrixBSR, p, Float64, (Int(m), Int(n)); blocksize, assignment)

end # module SparseArraysExt
