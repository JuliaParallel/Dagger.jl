module IncompleteLUExt

import IncompleteLU
import SparseArrays
import Dagger
import Dagger: DMatrix
import LinearAlgebra

# Block incomplete-LU preconditioner, built per diagonal tile on top of Dagger's
# block-preconditioner machinery (see `src/array/iterativesolvers.jl`). Each tile
# gets an ILU factorization (drop tolerance `τ`) once at construction; the apply
# is a forward/backward substitution per block.

_as_sparse(A::SparseArrays.SparseMatrixCSC) = A
_as_sparse(A::AbstractMatrix) = SparseArrays.sparse(A)

function _ilu_operator(tile; τ=0.001, kwargs...)
    S = _as_sparse(Dagger._tile_matrix(tile))
    return IncompleteLU.ilu(S; τ=τ, kwargs...)
end

function Dagger.BlockILUPreconditioner(A::DMatrix; kwargs...)
    build = tile -> _ilu_operator(tile; kwargs...)
    return Dagger._build_block_preconditioner(Dagger.BlockILUPreconditioner, A,
                                              "BlockILUPreconditioner", build)
end

# An ILU factorization applies via `ldiv!` (`\` is not defined).
Dagger._block_apply!(y, F::IncompleteLU.ILUFactorization, x) =
    (LinearAlgebra.ldiv!(y, F, x); return nothing)

# The factorization is read-only and pinned to its worker; treat it as
# non-aliasing rather than recursing into its internals.
Dagger.aliasing(::IncompleteLU.ILUFactorization) = Dagger.NoAliasing()

end # module IncompleteLUExt
