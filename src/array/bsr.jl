export SparseMatrixBSR, sparsebsr, BSR

"""
    SparseMatrixBSR{Tv,Ti} <: AbstractMatrix{Tv}

Block Sparse Row matrix (block-CSR). Storage matches the usual ecosystem
shape so the type can move out of Dagger later:

- `blocksize = (br, bc)` — each stored entry is a dense `br × bc` block
- `rowptr` — length `mb+1`, 1-based, like CSR `rowptr`
- `colval` — block-column indices (1-based)
- `nzval` — `br × bc × nnzblocks` array (column-major within each block)

`Dagger.BSR` is an alias for this type; do not treat `BSR` as the canonical
name. CSC remains the default sparse tile; host CSR stays on
SparseMatricesCSR. GPU tiles may stay CSC.

Construct with [`sparsebsr`](@ref) or `SparseMatrixBSR(A, blocksize)`,
distribute with `distribute(A, Blocks(...))`, and multiply with
`mul!(y, A, x)` / `A * x`.
"""
struct SparseMatrixBSR{Tv,Ti} <: AbstractMatrix{Tv}
    m::Int
    n::Int
    blocksize::Tuple{Int,Int}
    rowptr::Vector{Ti}
    colval::Vector{Ti}
    nzval::Array{Tv,3}
    function SparseMatrixBSR{Tv,Ti}(m::Integer, n::Integer, blocksize::Tuple{Int,Int},
                                    rowptr::Vector{Ti}, colval::Vector{Ti},
                                    nzval::Array{Tv,3}) where {Tv,Ti}
        br, bc = Int(blocksize[1]), Int(blocksize[2])
        br > 0 && bc > 0 || throw(ArgumentError("blocksize must be positive, got $blocksize"))
        size(nzval, 1) == br && size(nzval, 2) == bc ||
            throw(DimensionMismatch("nzval is $(size(nzval)), blocksize is $blocksize"))
        length(rowptr) == cld(Int(m), br) + 1 ||
            throw(DimensionMismatch("rowptr length $(length(rowptr)) does not match $(cld(Int(m), br))+1"))
        size(nzval, 3) == length(colval) ||
            throw(DimensionMismatch("colval length $(length(colval)) != nnzblocks $(size(nzval, 3))"))
        return new{Tv,Ti}(Int(m), Int(n), (br, bc), rowptr, colval, nzval)
    end
end

const BSR = SparseMatrixBSR

SparseMatrixBSR(m::Integer, n::Integer, blocksize::Tuple{Int,Int}, rowptr, colval, nzval) =
    SparseMatrixBSR{eltype(nzval),eltype(rowptr)}(m, n, blocksize, rowptr, colval, nzval)

function SparseMatrixBSR{Tv,Ti}(m::Integer, n::Integer, blocksize::Tuple{Int,Int}) where {Tv,Ti}
    br, bc = Int(blocksize[1]), Int(blocksize[2])
    br > 0 && bc > 0 || throw(ArgumentError("blocksize must be positive, got $blocksize"))
    mb = cld(Int(m), br)
    return SparseMatrixBSR{Tv,Ti}(Int(m), Int(n), (br, bc), ones(Ti, mb + 1), Ti[],
                                  Array{Tv,3}(undef, br, bc, 0))
end
SparseMatrixBSR(m::Integer, n::Integer, blocksize::Tuple{Int,Int}) =
    SparseMatrixBSR{Float64,Int}(m, n, blocksize)

Base.size(A::SparseMatrixBSR) = (A.m, A.n)
Base.eltype(::SparseMatrixBSR{Tv}) where Tv = Tv
Base.IndexStyle(::Type{<:SparseMatrixBSR}) = IndexCartesian()
function Base.copy(A::SparseMatrixBSR{Tv,Ti}) where {Tv,Ti}
    return SparseMatrixBSR{Tv,Ti}(A.m, A.n, A.blocksize, copy(A.rowptr), copy(A.colval), copy(A.nzval))
end

function Base.show(io::IO, A::SparseMatrixBSR)
    print(io, "SparseMatrixBSR{", eltype(A), "}(", A.m, ", ", A.n, ", blocksize=", A.blocksize,
          ", nnzblocks=", size(A.nzval, 3), ")")
end

function _bsr_block_index(A::SparseMatrixBSR, i::Integer, j::Integer)
    br, bc = A.blocksize
    bi = fld(Int(i) - 1, br) + 1
    bj = fld(Int(j) - 1, bc) + 1
    li = Int(i) - (bi - 1) * br
    lj = Int(j) - (bj - 1) * bc
    return bi, bj, li, lj
end

function Base.getindex(A::SparseMatrixBSR{Tv}, i::Integer, j::Integer) where Tv
    @boundscheck checkbounds(A, i, j)
    bi, bj, li, lj = _bsr_block_index(A, i, j)
    lo = Int(A.rowptr[bi])
    hi = Int(A.rowptr[bi + 1]) - 1
    for p in lo:hi
        if Int(A.colval[p]) == bj
            return A.nzval[li, lj, p]
        end
    end
    return zero(Tv)
end

"""
    sparsebsr(A, blocksize)
    sparsebsr(I, J, V, m, n, blocksize)

Build a [`SparseMatrixBSR`](@ref) from a dense or sparse matrix, or from COO
triplets. `blocksize` is `(br, bc)`.
"""
sparsebsr(A::AbstractMatrix, blocksize::Tuple{Integer,Integer}) =
    SparseMatrixBSR(A, (Int(blocksize[1]), Int(blocksize[2])))
sparsebsr(A::AbstractMatrix, br::Integer, bc::Integer) = sparsebsr(A, (Int(br), Int(bc)))

function SparseMatrixBSR(A::AbstractMatrix, blocksize::Tuple{Int,Int})
    return SparseMatrixBSR{eltype(A),Int}(A, blocksize)
end

function SparseMatrixBSR{Tv,Ti}(A::AbstractMatrix, blocksize::Tuple{Int,Int}) where {Tv,Ti}
    br, bc = Int(blocksize[1]), Int(blocksize[2])
    br > 0 && bc > 0 || throw(ArgumentError("blocksize must be positive, got $blocksize"))
    m, n = size(A)
    mb = cld(m, br)
    nb = cld(n, bc)
    rowptr = Vector{Ti}(undef, mb + 1)
    colval = Ti[]
    blocks = Matrix{Tv}[]
    rowptr[1] = 1
    for bi in 1:mb
        i0 = (bi - 1) * br
        for bj in 1:nb
            j0 = (bj - 1) * bc
            blk = zeros(Tv, br, bc)
            nonempty = false
            for lj in 1:bc
                j = j0 + lj
                j > n && continue
                for li in 1:br
                    i = i0 + li
                    i > m && continue
                    v = Tv(A[i, j])
                    if !iszero(v)
                        nonempty = true
                        blk[li, lj] = v
                    end
                end
            end
            if nonempty
                push!(colval, Ti(bj))
                push!(blocks, blk)
            end
        end
        rowptr[bi + 1] = Ti(length(colval) + 1)
    end
    nnzblocks = length(blocks)
    nzval = Array{Tv,3}(undef, br, bc, nnzblocks)
    for p in 1:nnzblocks
        nzval[:, :, p] = blocks[p]
    end
    return SparseMatrixBSR{Tv,Ti}(m, n, (br, bc), rowptr, colval, nzval)
end

function sparsebsr(I::AbstractVector{<:Integer}, J::AbstractVector{<:Integer}, V::AbstractVector,
                   m::Integer, n::Integer, blocksize::Tuple{Integer,Integer})
    length(I) == length(J) == length(V) || throw(ArgumentError("I, J, V must have the same length"))
    br, bc = Int(blocksize[1]), Int(blocksize[2])
    Tv = eltype(V)
    blocks = Dict{Tuple{Int,Int},Matrix{Tv}}()
    for k in eachindex(I)
        i, j = Int(I[k]), Int(J[k])
        (1 <= i <= m && 1 <= j <= n) || throw(BoundsError((m, n), (i, j)))
        bi = fld(i - 1, br) + 1
        bj = fld(j - 1, bc) + 1
        li = i - (bi - 1) * br
        lj = j - (bj - 1) * bc
        blk = get!(() -> zeros(Tv, br, bc), blocks, (bi, bj))
        blk[li, lj] += V[k]
    end
    mb = cld(Int(m), br)
    rowptr = Vector{Int}(undef, mb + 1)
    colval = Int[]
    nzlist = Matrix{Tv}[]
    rowptr[1] = 1
    for bi in 1:mb
        cols = sort!(collect(bj for (bii, bj) in keys(blocks) if bii == bi))
        for bj in cols
            push!(colval, bj)
            push!(nzlist, blocks[(bi, bj)])
        end
        rowptr[bi + 1] = length(colval) + 1
    end
    nnzblocks = length(nzlist)
    nzval = Array{Tv,3}(undef, br, bc, nnzblocks)
    for p in 1:nnzblocks
        nzval[:, :, p] = nzlist[p]
    end
    return SparseMatrixBSR{Tv,Int}(Int(m), Int(n), (br, bc), rowptr, colval, nzval)
end

function _bsr_findnz(A::SparseMatrixBSR)
    I = Int[]
    J = Int[]
    V = eltype(A)[]
    br, bc = A.blocksize
    m, n = size(A)
    mb = length(A.rowptr) - 1
    for bi in 1:mb
        i0 = (bi - 1) * br
        for p in Int(A.rowptr[bi]):(Int(A.rowptr[bi + 1]) - 1)
            j0 = (Int(A.colval[p]) - 1) * bc
            for lj in 1:bc, li in 1:br
                i = i0 + li
                j = j0 + lj
                (i > m || j > n) && continue
                v = A.nzval[li, lj, p]
                iszero(v) && continue
                push!(I, i)
                push!(J, j)
                push!(V, v)
            end
        end
    end
    return I, J, V
end

function _bsr_to_dense(A::SparseMatrixBSR{Tv}) where Tv
    B = zeros(Tv, A.m, A.n)
    br, bc = A.blocksize
    mb = length(A.rowptr) - 1
    for bi in 1:mb
        i0 = (bi - 1) * br
        for p in Int(A.rowptr[bi]):(Int(A.rowptr[bi + 1]) - 1)
            j0 = (Int(A.colval[p]) - 1) * bc
            for lj in 1:bc, li in 1:br
                i = i0 + li
                j = j0 + lj
                (i > A.m || j > A.n) && continue
                B[i, j] = A.nzval[li, lj, p]
            end
        end
    end
    return B
end
Base.Matrix(A::SparseMatrixBSR) = _bsr_to_dense(A)
Base.Array(A::SparseMatrixBSR) = _bsr_to_dense(A)

function _bsr_slice_blocksize(A::SparseMatrixBSR, I, J)
    br, bc = A.blocksize
    m, n = length(I), length(J)
    if m % br == 0 && n % bc == 0
        return (br, bc)
    end
    br2 = m == 0 ? 1 : gcd(br, m)
    bc2 = n == 0 ? 1 : gcd(bc, n)
    return (max(br2, 1), max(bc2, 1))
end

# Range getindex keeps a BSR (needed by `distribute` / `ArrayDomain` slices).
# The generic AbstractArray path would `similar` a dense Matrix.
function Base.getindex(A::SparseMatrixBSR, I::AbstractVector, J::AbstractVector)
    B = zeros(eltype(A), length(I), length(J))
    @inbounds for (jj, j) in enumerate(J), (ii, i) in enumerate(I)
        B[ii, jj] = A[i, j]
    end
    return SparseMatrixBSR(B, _bsr_slice_blocksize(A, I, J))
end

function LinearAlgebra.mul!(y::AbstractVector, A::SparseMatrixBSR, x::AbstractVector,
                            α::Number, β::Number)
    m, n = size(A)
    length(y) == m || throw(DimensionMismatch("mul!: y has length $(length(y)), A is $m×$n"))
    length(x) == n || throw(DimensionMismatch("mul!: x has length $(length(x)), A is $m×$n"))
    if iszero(β)
        fill!(y, zero(eltype(y)))
    elseif !isone(β)
        y .*= β
    end
    iszero(α) && return y
    br, bc = A.blocksize
    mb = length(A.rowptr) - 1
    @inbounds for bi in 1:mb
        i0 = (bi - 1) * br
        i1 = min(i0 + br, m)
        nr = i1 - i0
        for p in Int(A.rowptr[bi]):(Int(A.rowptr[bi + 1]) - 1)
            bj = Int(A.colval[p])
            j0 = (bj - 1) * bc
            j1 = min(j0 + bc, n)
            nc = j1 - j0
            for li in 1:nr
                acc = zero(eltype(y))
                for lj in 1:nc
                    acc += A.nzval[li, lj, p] * x[j0 + lj]
                end
                y[i0 + li] += α * acc
            end
        end
    end
    return y
end
LinearAlgebra.mul!(y::AbstractVector, A::SparseMatrixBSR, x::AbstractVector) =
    LinearAlgebra.mul!(y, A, x, true, false)

function Base.:*(A::SparseMatrixBSR, x::AbstractVector)
    T = promote_type(eltype(A), eltype(x))
    y = zeros(T, size(A, 1))
    return LinearAlgebra.mul!(y, A, x)
end

# Tile kernel used by distributed SpMV. Transposed/adjoint fall back to a dense
# tile unless SparseArraysExt adds a more-specific `_bsr_matvecmul_trans!`.
function matvecmul!(C::AbstractVector, transA::Char, A::SparseMatrixBSR, B::AbstractVector, alpha, beta)
    if transA == 'N'
        return LinearAlgebra.mul!(C, A, B, alpha, beta)
    end
    return _bsr_matvecmul_trans!(C, transA, A, B, alpha, beta)
end
# Unconstrained `A` so SparseArraysExt can add a SparseMatrixBSR CSC path
# without overwriting this method (precompilation forbids the overwrite).
function _bsr_matvecmul_trans!(C::AbstractVector, transA::Char, A, B::AbstractVector, alpha, beta)
    Ah = A isa SparseMatrixBSR ? _bsr_to_dense(A) : Array(A)
    op = transA == 'T' ? transpose(Ah) : adjoint(Ah)
    return LinearAlgebra.mul!(C, op, B, alpha, beta)
end

wraps_as_sparse_tile(::SparseMatrixBSR) = true
_sparse_copy(A::SparseMatrixBSR) = copy(A)
function _sparse_similar(A::SparseMatrixBSR, ::Type{T}, dims::Dims{2}) where T
    br, bc = A.blocksize
    bs = (dims[1] % br == 0 && dims[2] % bc == 0) ? A.blocksize : (1, 1)
    return SparseMatrixBSR{T,eltype(A.rowptr)}(dims[1], dims[2], bs)
end
function _sparse_copyto_view!(mat::SparseMatrixBSR, Brange, src)
    host = _bsr_to_dense(mat)
    copyto!(view(host, Brange), src)
    return SparseMatrixBSR(host, mat.blocksize)
end

function transpose_tile(B::SparseMatrixBSR)
    I, J, V = _bsr_findnz(B)
    return sparsebsr(J, I, V, B.n, B.m, (B.blocksize[2], B.blocksize[1]))
end
function transpose_tile(B::SparseMatrixBSR, uplo::Char)
    return transpose_tile(B)
end

# Named MPI-stable tile convert.
function _tile_to_bsr(tile, blocksize::Tuple{Int,Int})
    if tile isa DSparseArray
        mat = tile.mat
        if mat isa SparseMatrixBSR && mat.blocksize == blocksize
            return DSparseArray(copy(mat))
        end
        src = mat isa AbstractMatrix ? mat : _sparse_collect(mat)
        return DSparseArray(SparseMatrixBSR(src, blocksize))
    end
    src = tile isa AbstractMatrix ? tile : Array(tile)
    return DSparseArray(SparseMatrixBSR(src, blocksize))
end

function _bsr_from_tiles(A::DMatrix{T}, blocksize::Tuple{Int,Int}) where T
    Ac = A.chunks
    new_chunks = Array{DTask}(undef, size(Ac))
    for I in eachindex(Ac)
        new_chunks[I] = Dagger.@spawn return_type=DSparseArray{T,2} _tile_to_bsr(Ac[I], blocksize)
    end
    return DArray(T, A.domain, A.subdomains, new_chunks, A.partitioning, A.concat)
end

"""
    sparsebsr(A::DMatrix, part, blocksize)

A new `DMatrix` with [`SparseMatrixBSR`](@ref) tiles under `part`. Without
`part`, `sparsebsr(A, blocksize)` gathers to one host `SparseMatrixBSR`
(matching `sparse(::DMatrix)` → one `SparseMatrixCSC`).
"""
function sparsebsr(A::DMatrix, part::Blocks{2}, blocksize::Tuple{Integer,Integer})
    bs = (Int(blocksize[1]), Int(blocksize[2]))
    A = fetch(A)
    B = A.partitioning == part ? A : repartition(A, part)
    return _bsr_from_tiles(B, bs)
end
sparsebsr(A::DMatrix, part::Blocks{2}, br::Integer, bc::Integer) = sparsebsr(A, part, (br, bc))

# More specific than `AbstractMatrix` so we do not scalar-index a DMatrix.
# Default gathers densely; SparseArraysExt adds `_dmatrix_host_sparse(::DMatrix)`.
_dmatrix_host_sparse(A) = collect(A)
function sparsebsr(A::DMatrix, blocksize::Tuple{Integer,Integer})
    return SparseMatrixBSR(_dmatrix_host_sparse(A), (Int(blocksize[1]), Int(blocksize[2])))
end
sparsebsr(A::DMatrix, br::Integer, bc::Integer) = sparsebsr(A, (br, bc))

function sparsebsr(A::AbstractMatrix, part::Blocks{2}, blocksize::Tuple{Integer,Integer})
    return distribute(sparsebsr(A, blocksize), part)
end

struct BSRZerosAlloc
    blocksize::Tuple{Int,Int}
end
(a::BSRZerosAlloc)(::Type{T}, dims::Dims) where T =
    DSparseArray(SparseMatrixBSR{T,Int}(dims[1], dims[2], a.blocksize))

function _spzeros_bsr(p::Blocks{2}, T::Type, dims::Dims{2}, blocksize::Tuple{Int,Int};
                      assignment::AssignmentType=:arbitrary)
    d = ArrayDomain(map(x -> 1:x, dims))
    a = AllocateArray(T, BSRZerosAlloc(blocksize), false, d, partition(p, d), p, assignment;
                      return_type=DSparseArray{T,2})
    return _to_darray(a)
end
