module PureUMFPACKExt

import PureUMFPACK
import SparseArrays
import SparseArrays: SparseMatrixCSC
import Dagger
import Dagger: DMatrix
import LinearAlgebra

# Sparse direct (UMFPACK-style, pure-Julia) integration. Like KLU, these
# factorizations are plain Julia data (movable), backing both a whole-matrix
# direct solve (`Dagger.splu`) and per-tile block direct solves
# (`BlockUMFPACKPreconditioner`). With `distributed=true`:
#   - `method=:trsv` (default) — Stage 4b tiled triangular solves
#   - `method=:schur` — Stage 4c Schur-complement domain decomposition

_as_sparse(A::SparseMatrixCSC) = A
_as_sparse(A::AbstractMatrix) = SparseArrays.sparse(A)

function _umfpack_factorize(; kwargs...)
    return S -> PureUMFPACK.splu(_as_sparse(S); kwargs...)
end

# Whole-matrix direct solve: gather+factor on one worker, pin the factor there.
# Opt-in distributed paths select Stage 4b (`:trsv`) or Stage 4c (`:schur`).
function Dagger.splu(A::DMatrix; distributed::Bool=false, method::Symbol=:trsv,
                     blocksize=nothing, nparts=nothing, kwargs...)
    factorize = _umfpack_factorize(; kwargs...)
    if !distributed
        return Dagger._spawn_direct_factorization(A, factorize)
    end
    if method === :trsv
        F = Dagger._spawn_direct_factorization(A, factorize)
        return Dagger._make_distributed_sparse_lu(F, A; blocksize)
    elseif method === :schur
        return Dagger._make_schur_sparse_lu(A, factorize; nparts)
    else
        throw(ArgumentError(
            "Dagger.splu distributed method must be :trsv or :schur, got $(repr(method))"))
    end
end

# `(Rs .* A)[p, q] == L * U` with unit-lower `L` (explicit stored ones on the
# diagonal; solve treats it as unit diagonal via `UnitLowerTriangular`).
function Dagger._extract_lu_factors(F::PureUMFPACK.PureLU)
    return (F.L, F.U, copy(F.p), copy(F.q), copy(F.Rs))
end

# Per-tile block direct preconditioner.
function Dagger.BlockUMFPACKPreconditioner(A::DMatrix; kwargs...)
    build = tile -> PureUMFPACK.splu(_as_sparse(Dagger._tile_matrix(tile)); kwargs...)
    return Dagger._build_block_preconditioner(Dagger.BlockUMFPACKPreconditioner, A,
                                              "BlockUMFPACKPreconditioner", build)
end

# `PureLU` provides `\` (no `ldiv!`); the default `_block_apply!` (`y .= op \ x`)
# already uses it, so no apply override is needed.

# The factorization is read-only and pinned to its worker; treat it as
# non-aliasing rather than recursing into its internals.
Dagger.aliasing(::PureUMFPACK.PureLU) = Dagger.NoAliasing()

end # module PureUMFPACKExt
