module IncompleteLUExt

import IncompleteLU
import SparseArrays
import Dagger
import LinearAlgebra

# Host ILU factory for [`Dagger.BlockILUPreconditioner`](@ref). The constructor
# lives in core and calls `_ilu_tile`; GPU sparse extensions add more-specific
# methods (vendor ILU0) that win when the tile is device-resident.

_as_sparse(A::SparseArrays.SparseMatrixCSC) = A
_as_sparse(A::AbstractMatrix) = SparseArrays.sparse(A)

function Dagger._ilu_tile(tile; τ=0.001, kwargs...)
    S = _as_sparse(Dagger._tile_matrix(tile))
    return IncompleteLU.ilu(S; τ=τ, kwargs...)
end

# An ILU factorization applies via `ldiv!`. It subtypes `Factorization`, so this
# steers it away from the generic `\` path `_apply_inverse!` would otherwise pick
# (which would allocate a copy of the RHS per apply).
Dagger._apply_inverse!(y, F::IncompleteLU.ILUFactorization, x) =
    LinearAlgebra.ldiv!(y, F, x)

end # module IncompleteLUExt
