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

# Per-tile block direct preconditioner.
function Dagger.BlockKLUPreconditioner(A::DMatrix; kwargs...)
    build = tile -> PureKLU.klu(_as_sparse(Dagger._tile_matrix(tile)); kwargs...)
    return Dagger._build_block_preconditioner(Dagger.BlockKLUPreconditioner, A,
                                              "BlockKLUPreconditioner", build)
end

# KLU supports in-place `ldiv!`, so apply each block solve without allocating.
Dagger._block_apply!(y, F::PureKLU.KLUFactorization, x) =
    (LinearAlgebra.ldiv!(y, F, x); return nothing)

# The factorization is read-only and pinned to its worker; treat it as
# non-aliasing rather than recursing into its internals.
Dagger.aliasing(::PureKLU.KLUFactorization) = Dagger.NoAliasing()

end # module PureKLUExt
