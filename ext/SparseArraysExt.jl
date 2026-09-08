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
  Sparse-aware `@stencil` sweep (`@stencil sparse=true`)

  A dense sweep evaluates the kernel at every index of a tile. When the kernel
  is zero-preserving -- it maps an all-zero neighborhood to zero, which is what
  `sparse=true` asserts -- an output element can only be nonzero if some stored
  entry of an operand lies within the neighborhood distance of it. So

      support(output) subset of  dilate(support(operands), neigh_dist)

  the morphological dilation of the operands' nonzero pattern by the stencil's
  `(2w+1)^N` box. Every index outside that set is provably zero: no kernel
  evaluation, and no slot in the result.

  Because CSC keeps each column's row indices sorted, the dilated pattern is
  computed a column at a time and the result is emitted straight into
  `colptr`/`rowval`/`nzval` -- one allocation, rather than `nnz` scalar
  `setindex!`s that each memmove the tail of the storage.
==============================================================================#

# Where an operand's stored entries are, and how far a stencil access reaches
# from them. A `HaloArray` reaches `halo_width`; a plain tile is read at `idx`
# only, so it reaches nothing. `nothing` means "no pattern available" -- a dense
# operand can be nonzero anywhere, so the sweep must decline.
_stencil_pattern_source(A::SparseMatrixCSC{Tv,Ti}, ::Val{N}) where {Tv,Ti,N} = (A, ntuple(_ -> 0, Val(N)))
_stencil_pattern_source(A::SparseVector{Tv,Ti}, ::Val{N}) where {Tv,Ti,N} = (A, ntuple(_ -> 0, Val(N)))
function _stencil_pattern_source(A::Dagger.HaloArray{T,N}, ::Val{N}) where {T,N}
    c = A.center
    (c isa SparseMatrixCSC || c isa SparseVector) || return nothing
    return (c, A.halo_width)
end
_stencil_pattern_source(@nospecialize(x), ::Val) = nothing

"""
    _StencilAccumulator

Stands in for the output tile while a sparse sweep runs. The kernel generated by
`@stencil` performs its own store (`out[idx] = ...`), so rather than special-case
the kernel, the sweep hands it this: `setindex!` appends to the result's storage
vectors (dropping zeros), and `getindex` reads the *previous* tile, which is what
an in-place `A[idx] = f(A[idx], ...)` expects to see.

Correctness rests on the sweep visiting candidates in column-major order and
finalizing each column before starting the next, so `rowval` comes out sorted
within each column and `colptr` can be filled in as it goes.
"""
struct _StencilAccumulator{Tv,Ti,N,S} <: AbstractArray{Tv,N}
    dims::NTuple{N,Int}
    old::S
    rowval::Vector{Ti}
    nzval::Vector{Tv}
end
function _StencilAccumulator{Tv,Ti}(dims::NTuple{N,Int}, old::S) where {Tv,Ti,N,S}
    return _StencilAccumulator{Tv,Ti,N,S}(dims, old, Ti[], Tv[])
end
Base.size(A::_StencilAccumulator) = A.dims
Base.IndexStyle(::Type{<:_StencilAccumulator}) = IndexCartesian()
@inline Base.getindex(A::_StencilAccumulator{Tv,Ti,N}, I::Vararg{Int,N}) where {Tv,Ti,N} =
    A.old[I...]
@inline function Base.setindex!(A::_StencilAccumulator{Tv,Ti,N}, v, I::Vararg{Int,N}) where {Tv,Ti,N}
    if !iszero(v)
        push!(A.rowval, I[1])
        push!(A.nzval, v)
    end
    return v
end

function Dagger.try_sparse_output_sweep!(proc, style, f, output::Dagger.DSparseArray, read_vars)
    old = output.mat
    (old isa SparseMatrixCSC || old isa SparseVector) || return false
    _sparse_output_sweep!(_restrictable(style, old, read_vars), f, output, old, read_vars)
    return true
end

# Whether this expression can actually restrict which indices it visits. Only a
# `ZeroPreservingSweep` may, and only when every operand contributes a pattern to
# dilate; otherwise the sweep still runs -- it just visits every index, which is
# what `FullSweep` does anyway. Returns the style to sweep with, paired with the
# sources it needs (empty for a full sweep).
_restrictable(::Dagger.FullSweep, old, read_vars) = (Dagger.FullSweep(), ())
function _restrictable(style::Dagger.ZeroPreservingSweep, old, read_vars)
    N = ndims(old)
    sources = map(v -> _stencil_pattern_source(v, Val(N)), Tuple(values(read_vars)))
    # A dense operand can be nonzero anywhere, so there is no pattern to dilate
    # and every index is a candidate -- which is exactly a full sweep.
    any(isnothing, sources) && return (Dagger.FullSweep(), ())
    if isempty(sources)
        # The output is the only operand (`B[idx] = B[idx] * 2`, common as a
        # follow-up expression in a stencil block): its own pattern is the
        # candidate set, undilated.
        sources = ((old, ntuple(_ -> 0, Val(N))),)
    end
    # Operands are indexed at the same `idx` as the output, so a size mismatch
    # would make the dilated pattern meaningless.
    all(src -> size(src[1]) == size(old), sources) || return (Dagger.FullSweep(), ())
    return (style, sources)
end

# Rows of column `j` this sweep must visit, as clipped intervals. A full sweep
# visits the whole column; only the zero-preserving style restricts.
@inline function _column_intervals!(ivals, ::Dagger.FullSweep, sources, j, m, n, shell)
    empty!(ivals)
    push!(ivals, (1, m))
    return ivals
end
@inline _column_intervals!(ivals, ::Dagger.ZeroPreservingSweep, sources, j, m, n, shell) =
    _stencil_column_intervals!(ivals, sources, j, m, n, shell)

# Widest halo reach over all operands, per dimension. A full sweep visits every
# index anyway, so it needs no shell.
_stencil_shell(::Dagger.FullSweep, sources) = ()
function _stencil_shell(::Dagger.ZeroPreservingSweep, sources)
    N = length(sources[1][2])
    return ntuple(N) do d
        w = 0
        for (_, hw) in sources
            w = max(w, hw[d])
        end
        w
    end
end

# Candidate rows of column `j`: the operands' stored entries dilated by their halo
# reach, plus the boundary shell, as sorted clipped intervals.
function _stencil_column_intervals!(ivals, sources, j, m, n, shell)
    empty!(ivals)
    if _in_shell(j, n, shell[2])
        push!(ivals, (1, m))
        return ivals
    end
    shell[1] > 0 && push!(ivals, (1, min(m, shell[1])))
    shell[1] > 0 && push!(ivals, (max(1, m - shell[1] + 1), m))
    for (A, hw) in sources
        _push_dilated!(ivals, A, hw, j, m, n)
    end
    sort!(ivals; by=first)
    return ivals
end

# Stored entries of a *neighboring* chunk reach this tile's outermost `shell`
# elements. Rather than extract each halo region's own pattern, treat that shell
# as candidate wholesale: it over-approximates by O(w * perimeter) indices,
# against the O(prod(size)) the sweep is avoiding.
@inline _in_shell(i, n, w) = w > 0 && (i <= w || i > n - w)

_sparse_output_sweep!((style, sources)::Tuple, f, output, old, read_vars) =
    _sparse_output_sweep!(style, sources, f, output, old, read_vars)

function _sparse_output_sweep!(style, sources, f::F, output, old::SparseMatrixCSC{Tv,Ti},
                               read_vars) where {F,Tv,Ti}
    m, n = size(old)
    shell = _stencil_shell(style, sources)
    acc = _StencilAccumulator{Tv,Ti}((m, n), old)
    colptr = Vector{Ti}(undef, n + 1)
    colptr[1] = 1
    ivals = Tuple{Int,Int}[]
    w = Dagger._max_halo_width(values(read_vars), (0, 0))
    interior_vars = map(Dagger._interior_var, read_vars)

    for j in 1:n
        _column_intervals!(ivals, style, sources, j, m, n, shell)
        # Walk the sorted intervals, merging overlaps, and sweep each maximal run.
        lo = 0; hi = -1
        for (a, b) in ivals
            if a > hi + 1
                lo > 0 && _sweep_run!(f, acc, read_vars, interior_vars, w, m, n, lo, hi, j)
                lo, hi = a, b
            else
                hi = max(hi, b)
            end
        end
        lo > 0 && _sweep_run!(f, acc, read_vars, interior_vars, w, m, n, lo, hi, j)
        colptr[j+1] = length(acc.nzval) + 1
    end

    output.mat = SparseMatrixCSC{Tv,Ti}(m, n, colptr, acc.rowval, acc.nzval)
    return
end

# Rows of column `j` that a stored entry of `A` can reach, as clipped intervals.
@inline function _push_dilated!(ivals, A::SparseMatrixCSC, hw, j, m, n)
    wr, wc = hw[1], hw[2]
    @inbounds for jj in max(1, j - wc):min(n, j + wc)
        for p in A.colptr[jj]:(A.colptr[jj+1] - 1)
            i = Int(A.rowval[p])
            push!(ivals, (max(1, i - wr), min(m, i + wr)))
        end
    end
    return
end

# Sweep rows `lo:hi` of column `j`, splitting off the part whose whole
# neighborhood lands inside the center array.
#
# This is the same interior/shell split `cpu_stencil_sweep!` makes, and it has to
# be made here too: assembling a sparse output requires visiting indices in
# column-major order, so this sweep cannot reuse that one, and without the split
# every `@neighbors` access would pay `HaloArray`'s region-code dispatch even
# where it provably cannot reach a halo.
@inline function _sweep_run!(f::F, acc, read_vars, interior_vars, w, m, n,
                             lo::Int, hi::Int, j::Int) where F
    if w[2] < j <= n - w[2]
        ilo = max(lo, w[1] + 1)
        ihi = min(hi, m - w[1])
        if ilo <= ihi
            for i in lo:(ilo - 1)
                @inline f(CartesianIndex(i, j), acc, read_vars)
            end
            for i in ilo:ihi
                @inline f(CartesianIndex(i, j), acc, interior_vars)
            end
            for i in (ihi + 1):hi
                @inline f(CartesianIndex(i, j), acc, read_vars)
            end
            return
        end
    end
    for i in lo:hi
        @inline f(CartesianIndex(i, j), acc, read_vars)
    end
    return
end

function _sparse_output_sweep!(style, sources, f::F, output, old::SparseVector{Tv,Ti},
                               read_vars) where {F,Tv,Ti}
    n = length(old)
    acc = _StencilAccumulator{Tv,Ti}((n,), old)
    ivals = Tuple{Int,Int}[]
    _vector_intervals!(ivals, style, sources, n)
    w = Dagger._max_halo_width(values(read_vars), (0,))
    interior_vars = map(Dagger._interior_var, read_vars)

    lo = 0; hi = -1
    for (a, b) in ivals
        if a > hi + 1
            lo > 0 && _sweep_run_1d!(f, acc, read_vars, interior_vars, w, n, lo, hi)
            lo, hi = a, b
        else
            hi = max(hi, b)
        end
    end
    lo > 0 && _sweep_run_1d!(f, acc, read_vars, interior_vars, w, n, lo, hi)

    output.mat = SparseVector{Tv,Ti}(n, acc.rowval, acc.nzval)
    return
end

@inline function _vector_intervals!(ivals, ::Dagger.FullSweep, sources, n)
    push!(ivals, (1, n))
    return ivals
end
function _vector_intervals!(ivals, ::Dagger.ZeroPreservingSweep, sources, n)
    w = _stencil_shell(Dagger.ZeroPreservingSweep(), sources)[1]
    w > 0 && push!(ivals, (1, min(n, w)))
    w > 0 && push!(ivals, (max(1, n - w + 1), n))
    for (A, hw) in sources
        @inbounds for i in SparseArrays.nonzeroinds(A)
            push!(ivals, (max(1, i - hw[1]), min(n, i + hw[1])))
        end
    end
    sort!(ivals; by=first)
    return ivals
end

@inline function _sweep_run_1d!(f::F, acc, read_vars, interior_vars, w, n,
                                lo::Int, hi::Int) where F
    ilo = max(lo, w[1] + 1)
    ihi = min(hi, n - w[1])
    if ilo <= ihi
        for i in lo:(ilo - 1)
            @inline f(CartesianIndex(i), acc, read_vars)
        end
        for i in ilo:ihi
            @inline f(CartesianIndex(i), acc, interior_vars)
        end
        for i in (ihi + 1):hi
            @inline f(CartesianIndex(i), acc, read_vars)
        end
        return
    end
    for i in lo:hi
        @inline f(CartesianIndex(i), acc, read_vars)
    end
    return
end

end # module SparseArraysExt
