# Graph partitioning of sparse operators.
#
# `Blocks(k, k)` is geometric: tiles are index-range slabs. Unstructured meshes
# need a vertex permutation of the adjacency graph so those slabs contain few
# off-diagonal nonzeros. This file adds that permutation to the existing
# `repartition` / `distribute` APIs (`partitioner=` / `perm=`) rather than a
# new `Dagger.metis` entry point.
#
# The partitioner currently gathers the pattern (METIS is serial). Schur LU
# still uses `_nested_dissection_partition` (separator construction); that path
# shares only the k-way helper in MetisExt.

"""
    partition_graph(partitioner, A, nparts) -> Vector{Int}

Vertex-to-part assignment (`1:nparts`) of the undirected adjacency of square
sparse `A` (`A + Aᵀ`). `A` may be a `SparseMatrixCSC` or a sparse-backed
[`DMatrix`](@ref) (the latter is gathered for the pattern only).

`partitioner` is `Metis` or `Metis.partition` after `using Metis`, or any
function `f(A, nparts) -> AbstractVector{<:Integer}` (the hook for a KaHIP
wrapper or a custom assignment). Empty parts are allowed.

See [`partition_perm`](@ref) to turn the assignment into a symmetric
permutation, and [`repartition`](@ref) / [`distribute`](@ref) to apply it.
"""
function partition_graph(partitioner, A, nparts)
    if A isa DMatrix
        is_sparse_backed(A) || throw(ArgumentError(
            "graph partitioning requires a sparse-backed DMatrix"))
        return partition_graph(partitioner, _collect_sparse_dmatrix(A), nparts)
    end
    if partitioner isa Module
        return _partition_graph_module(Val(nameof(partitioner)), partitioner, A, nparts)
    end
    throw(ArgumentError(_partition_graph_missing(partitioner, A)))
end

function partition_graph(f::Function, A, nparts)
    if A isa DMatrix
        is_sparse_backed(A) || throw(ArgumentError(
            "graph partitioning requires a sparse-backed DMatrix"))
        return partition_graph(f, _collect_sparse_dmatrix(A), nparts)
    end
    n = LinearAlgebra.checksquare(A)
    return _as_partvec(f(A, nparts), n, nparts)
end

function _partition_graph_module(::Val{name}, partitioner, A, nparts) where name
    throw(ArgumentError(_partition_graph_missing(partitioner, A)))
end

_partition_graph_missing(partitioner, A) =
    "no graph partitioner method for $(typeof(partitioner)) on $(typeof(A)). \
    Load Metis.jl (`using Metis`) and pass `partitioner=Metis` or \
    `partitioner=Metis.partition`, or pass a function `f(A, nparts) -> Vector{Int}`."

function _as_partvec(parts, n::Integer, nparts)
    parts = Int.(parts)
    nparts = Int(nparts)
    nparts >= 1 || throw(ArgumentError("nparts must be ≥ 1, got $nparts"))
    length(parts) == n || throw(DimensionMismatch(
        "partitioner returned $(length(parts)) assignments, expected $n"))
    @inbounds for p in parts
        1 <= p <= nparts || throw(ArgumentError(
            "part assignment $p is outside 1:$nparts"))
    end
    return parts
end

"""
    partition_perm(parts::AbstractVector{<:Integer}) -> Vector{Int}

Permutation that groups vertices by `parts[v]` (stable within each part).
Applying `A[p, p]` then a geometric [`Blocks`](@ref) tiling makes each tile
cover one (or a remainder of one) graph part.

The inverse `invperm(p)` maps a solution in the partitioned ordering back to
the original numbering.
"""
function partition_perm(parts::AbstractVector{<:Integer})
    n = length(parts)
    n == 0 && return Int[]
    nparts = Int(maximum(parts))
    nparts >= 1 || throw(ArgumentError("part assignment is empty"))
    buckets = [Int[] for _ in 1:nparts]
    @inbounds for (v, p) in enumerate(parts)
        1 <= p <= nparts || throw(ArgumentError(
            "part assignment $p is outside 1:$nparts"))
        push!(buckets[p], v)
    end
    return reduce(vcat, buckets)
end

_nparts_from_blocks(n::Integer, part::Blocks) =
    max(1, cld(Int(n), Int(part.blocksize[1])))

function _require_partitioner_xor_perm(partitioner, perm)
    if partitioner !== nothing && perm !== nothing
        throw(ArgumentError("pass partitioner or perm, not both"))
    end
    return nothing
end

function _checked_perm(perm, n::Integer)
    p = Vector{Int}(perm)
    length(p) == n || throw(DimensionMismatch(
        "permutation length $(length(p)) does not match size $n"))
    return p
end

# Geometric `repartition` / `distribute` after an optional graph permutation.
# Defined here so `copy.jl` / `darray.jl` can take the keywords without pulling
# the gather hook in at include time; those files are loaded first.
function _repartition_with_perm(A::DArray{T,N}, part::Blocks{N};
                                partitioner=nothing, perm=nothing) where {T,N}
    _require_partitioner_xor_perm(partitioner, perm)
    if N == 1
        partitioner !== nothing && throw(ArgumentError(
            "graph partitioner requires a matrix; permute a vector with `perm=`"))
        perm === nothing && throw(ArgumentError("perm is required for a vector"))
        p = _checked_perm(perm, length(A))
        p == 1:length(A) && A.partitioning == part && return A
        return distribute(collect(A)[p], part)
    elseif N == 2
        n = LinearAlgebra.checksquare(A)
        if perm === nothing
            is_sparse_backed(A) || throw(ArgumentError(
                "graph partitioning requires a sparse-backed DMatrix"))
            Al = _collect_sparse_dmatrix(A)
            p = partition_perm(partition_graph(partitioner, Al,
                                              _nparts_from_blocks(n, part)))
            p == 1:n && A.partitioning == part && return A
            return distribute(Al[p, p], part)
        else
            p = _checked_perm(perm, n)
            p == 1:n && A.partitioning == part && return A
            is_sparse_backed(A) || throw(ArgumentError(
                "`perm=` on a DMatrix requires sparse tiles (avoids densifying)"))
            Al = _collect_sparse_dmatrix(A)
            return distribute(Al[p, p], part)
        end
    else
        throw(ArgumentError("partitioner=/perm= support 1- and 2-dimensional DArrays"))
    end
end

function _distribute_with_perm(A::AbstractArray{T,N}, dist::Blocks{N}, assignment;
                               partitioner=nothing, perm=nothing) where {T,N}
    _require_partitioner_xor_perm(partitioner, perm)
    if N == 1
        partitioner !== nothing && throw(ArgumentError(
            "graph partitioner requires a matrix; permute a vector with `perm=`"))
        perm === nothing && throw(ArgumentError("perm is required for a vector"))
        p = _checked_perm(perm, length(A))
        return distribute(A[p], dist, assignment)
    elseif N == 2
        n = LinearAlgebra.checksquare(A)
        if perm === nothing
            p = partition_perm(partition_graph(partitioner, A,
                                              _nparts_from_blocks(n, dist)))
        else
            p = _checked_perm(perm, n)
        end
        p == 1:n && return distribute(A, dist, assignment)
        return distribute(A[p, p], dist, assignment)
    else
        throw(ArgumentError("partitioner=/perm= support 1- and 2-dimensional arrays"))
    end
end
