module PureKLUExt

import PureKLU
import SparseArrays
import SparseArrays: SparseMatrixCSC
import Dagger
import Dagger: DMatrix
import LinearAlgebra

# Sparse direct (KLU) integration. KLU factorizations are pure-Julia data
# (serializable/movable), so they back both a whole-matrix direct solve
# (`Dagger.klu`) and per-tile block direct solves (`BlockKLUPreconditioner`),
# the latter via Dagger's block-preconditioner machinery.

_as_sparse(A::SparseMatrixCSC) = A
_as_sparse(A::AbstractMatrix) = SparseArrays.sparse(A)

# Whole-matrix direct solve: gather+factor on one worker, pin the factor there.
function Dagger.klu(A::DMatrix; kwargs...)
    return Dagger._spawn_direct_factorization(A, S -> PureKLU.klu(S; kwargs...))
end

# Preferential hook for `lu` / `factorize` / `\\` on a sparse-backed DMatrix.
# UMFPACK wins when both extensions are loaded (`Val{:splu}` is tried first).
Dagger._try_sparse_direct_lu(::Val{:klu}, A::DMatrix) = Dagger.klu(A)

# Numeric reuse: same CSC pattern → `klu!` (symbolic + workspace stay).
# A pattern change (e.g. a stored entry became a structural zero after gather)
# falls back to a full `klu` into the pinned box.
function Dagger._update_sparse_lu!(F::PureKLU.KLUFactorization, S; kwargs...)
    S = _as_sparse(S)
    try
        return PureKLU.klu!(F, S; kwargs...)
    catch e
        if e isa ArgumentError
            msg = e.msg
            if occursin("pattern", msg) || occursin("Sizes of K and S", msg)
                return PureKLU.klu(S; kwargs...)
            end
        end
        rethrow()
    end
end

# Per-tile block direct preconditioner.
function Dagger.BlockKLUPreconditioner(A::DMatrix; kwargs...)
    build = tile -> PureKLU.klu(_as_sparse(Dagger._tile_matrix(tile)); kwargs...)
    return Dagger._build_block_preconditioner(Dagger.BlockKLUPreconditioner, A, build)
end

# KLU supports in-place `ldiv!`, so apply each block solve without allocating a
# copy of the RHS (which the generic `Factorization`/`\` path would).
Dagger._apply_inverse!(y, F::PureKLU.KLUFactorization, x) =
    LinearAlgebra.ldiv!(y, F, x)

end # module PureKLUExt
