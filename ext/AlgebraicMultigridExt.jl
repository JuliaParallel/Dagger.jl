module AlgebraicMultigridExt

import AlgebraicMultigrid
import SparseArrays
import Dagger
import Dagger: DMatrix
import LinearAlgebra

# Algebraic-multigrid preconditioner, built per diagonal tile on top of Dagger's
# block-preconditioner machinery (see `src/array/iterativesolvers.jl`). The AMG
# hierarchy's expensive setup runs once per tile at construction; each apply runs
# one V-cycle (a sequence of SpMVs, smoother sweeps, and a coarse solve).

_as_sparse(A::SparseArrays.SparseMatrixCSC) = A
_as_sparse(A::AbstractMatrix) = SparseArrays.sparse(A)

function _amg_operator(tile, method::Symbol; kwargs...)
    S = _as_sparse(Dagger._tile_matrix(tile))
    ml = if method === :ruge_stuben
        AlgebraicMultigrid.ruge_stuben(S; kwargs...)
    elseif method === :smoothed_aggregation
        AlgebraicMultigrid.smoothed_aggregation(S; kwargs...)
    else
        throw(ArgumentError("AMGPreconditioner: unknown method $(method); use \
            :ruge_stuben or :smoothed_aggregation"))
    end
    return AlgebraicMultigrid.aspreconditioner(ml)
end

function Dagger.AMGPreconditioner(A::DMatrix; method::Symbol=:ruge_stuben, kwargs...)
    build = tile -> _amg_operator(tile, method; kwargs...)
    return Dagger._build_block_preconditioner(Dagger.AMGPreconditioner, A, build)
end

end # module AlgebraicMultigridExt
