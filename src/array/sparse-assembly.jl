# Incremental / one-shot sparse assembly from COO triplets.
#
# The public API lives on SparseArrays (`sparse` / `sparse!` with a `Blocks`
# argument or DArray I,J,V) in SparseArraysExt. This file holds the
# rank-uniform named kernels those methods spawn: bucket global (i,j,v) by
# destination tile, extract one tile's bucket, and (via the extension) assemble
# a per-tile CSC. Overlap is implicit — a triplet is sent to the tile that owns
# that (row, col), not to the process that produced it.

# Per-tile COO fragment in the destination tile's local (1-based) coordinates.
struct SparseCOOBucket{T}
    I::Vector{Int}
    J::Vector{Int}
    V::Vector{T}
end
SparseCOOBucket{T}() where T = SparseCOOBucket{T}(Int[], Int[], T[])

# Partition global-index triplets onto the Blocks grid described by
# row_cum/col_cum (DomainBlocks.cumlength). Named so MPI ranks hash identically.
function _bucket_coo_chunk(I, J, V, row_cum, col_cum, m::Integer, n::Integer)
    length(I) == length(J) == length(V) ||
        throw(ArgumentError("I, J, V must have the same length"))
    T = eltype(V)
    ntr = length(row_cum)
    ntc = length(col_cum)
    buckets = [SparseCOOBucket{T}() for _ in 1:ntr, _ in 1:ntc]
    @inbounds for k in 1:length(I)
        i = Int(I[k])
        j = Int(J[k])
        v = V[k]
        (1 <= i <= m && 1 <= j <= n) || throw(ArgumentError(
            "sparse index ($i, $j) is outside the $(m)×$(n) matrix"))
        ti = searchsortedfirst(row_cum, i)
        tj = searchsortedfirst(col_cum, j)
        i0 = ti == 1 ? 0 : row_cum[ti - 1]
        j0 = tj == 1 ? 0 : col_cum[tj - 1]
        b = buckets[ti, tj]
        push!(b.I, i - i0)
        push!(b.J, j - j0)
        push!(b.V, v)
    end
    return buckets
end

# Pull one tile's bucket out of a chunk-local bucket matrix so only that
# fragment is moved to the owning tile (PETSc-style overlap send).
_extract_coo_bucket(buckets, ti::Integer, tj::Integer) = buckets[Int(ti), Int(tj)]

# Implemented in SparseArraysExt: assemble buckets into `dest.mat` with
# SparseArrays combine semantics, then restamp the tile for the executing
# processor (host CSC or GPU sparse).
function _assemble_coo_into_tile end
function _store_assembled_tile end
