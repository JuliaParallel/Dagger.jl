module MetisExt

import Metis
import SparseArrays
import SparseArrays: SparseMatrixCSC, nnz, rowvals, nzrange
import LinearAlgebra
import Dagger

# Single-level (non-recursive) METIS k-way partition + vertex-separator
# construction for Stage 4c Schur-complement domain decomposition.
# Recursive / nested dissection is future work.

# Symmetric adjacency pattern of `A + Aᵀ` (structure only, unit weights).
function _symmetrize_pattern(A::SparseMatrixCSC{Tv,Ti}) where {Tv,Ti}
    n = size(A, 1)
    S = A + transpose(A)
    return SparseMatrixCSC{Tv,Ti}(n, n, S.colptr, S.rowval, ones(Tv, nnz(S)))
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

    # Too small to partition usefully → trivial (serial) partition.
    if nparts == 1 || n < 4 || n <= nparts
        return (; perm=collect(1:n), interiors=[collect(1:n)], separator=Int[])
    end

    pattern = _symmetrize_pattern(A)
    g = Metis.graph(pattern)
    part = Int.(Metis.partition(g, nparts))

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
