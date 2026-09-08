module MetisExt

import Metis
import SparseArrays
import SparseArrays: SparseMatrixCSC, nnz, rowvals, nzrange
import LinearAlgebra
import Dagger

# Shared METIS k-way of the undirected adjacency of `A + Aᵀ`. Used by
# `partition_graph` (sparse `repartition` / `distribute`) and by the Stage 4c
# Schur separator. Recursive / nested dissection is future work.

# Symmetric adjacency pattern of `A + Aᵀ` (structure only, unit weights).
function _symmetrize_pattern(A::SparseMatrixCSC{Tv,Ti}) where {Tv,Ti}
    n = size(A, 1)
    S = A + transpose(A)
    return SparseMatrixCSC{Tv,Ti}(n, n, S.colptr, S.rowval, ones(Tv, nnz(S)))
end

function _metis_too_small(n::Int, nparts::Int)
    return nparts == 1 || n < 4 || n <= nparts
end

# Returns (part, pattern). `pattern` is `nothing` on the trivial path so the
# Schur caller can skip separator construction.
function _metis_kway(A::SparseMatrixCSC, nparts::Integer)
    n = LinearAlgebra.checksquare(A)
    nparts = Int(nparts)
    nparts >= 1 || throw(ArgumentError("nparts must be ≥ 1, got $nparts"))
    if _metis_too_small(n, nparts)
        return ones(Int, n), nothing
    end
    pattern = _symmetrize_pattern(A)
    g = Metis.graph(pattern)
    return Int.(Metis.partition(g, nparts)), pattern
end

function _metis_partvec(A::SparseMatrixCSC, nparts::Integer)
    part, _ = _metis_kway(A, nparts)
    return part
end

function Dagger.partition_graph(::typeof(Metis.partition), A::SparseMatrixCSC,
                                nparts::Integer)
    n = LinearAlgebra.checksquare(A)
    return Dagger._as_partvec(_metis_partvec(A, nparts), n, nparts)
end

function Dagger._partition_graph_module(::Val{:Metis}, ::Module, A::SparseMatrixCSC,
                                        nparts::Integer)
    n = LinearAlgebra.checksquare(A)
    return Dagger._as_partvec(_metis_partvec(A, nparts), n, nparts)
end

"""
    Dagger._nested_dissection_partition(A, nparts) -> (; perm, interiors, separator)

K-way METIS partition of the undirected adjacency of `A + Aᵀ`, then a vertex
separator `Γ` = every vertex with a neighbor in a different part. Interiors are
the remaining vertices grouped by part (empty parts dropped). The permutation is
`[I₁; …; Iₖ; Γ]`.

For tiny graphs (or `nparts == 1`), returns a trivial partition: one interior
covering `1:n` and an empty separator (caller should fall back to a serial
factorization).
"""
function Dagger._nested_dissection_partition(A::SparseMatrixCSC, nparts::Integer)
    n = LinearAlgebra.checksquare(A)
    nparts = Int(nparts)
    nparts >= 1 || throw(ArgumentError("nparts must be ≥ 1, got $nparts"))

    part, pattern = _metis_kway(A, nparts)
    if pattern === nothing
        return (; perm=collect(1:n), interiors=[collect(1:n)], separator=Int[])
    end

    is_sep = falses(n)
    rows = rowvals(pattern)
    @inbounds for v in 1:n
        for k in nzrange(pattern, v)
            u = rows[k]
            u == v && continue
            if part[u] != part[v]
                is_sep[v] = true
                break
            end
        end
    end

    separator = findall(is_sep)
    interiors = Vector{Vector{Int}}()
    for p in 1:nparts
        Ii = findall(i -> !is_sep[i] && part[i] == p, 1:n)
        isempty(Ii) || push!(interiors, Ii)
    end

    # Everything landed in the separator (or no interiors) → serial fallback.
    if isempty(interiors)
        return (; perm=collect(1:n), interiors=[collect(1:n)], separator=Int[])
    end

    perm = vcat(interiors..., separator)
    return (; perm, interiors, separator)
end

end # module MetisExt
