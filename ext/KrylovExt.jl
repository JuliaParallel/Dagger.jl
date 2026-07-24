module KrylovExt

import Krylov
import Dagger
import Dagger: DVector, DMatrix
import LinearAlgebra

# Distributed iterative solvers, implemented on top of Krylov.jl.
#
# We never form `A⁻¹`: Krylov only needs `mul!(y, A, x)` (and `mul!(y, A', x)`
# for two-sided methods), which Dagger provides over `DVector`s via its
# distributed SpMV/`gemv_dagger!` path. The remaining requirements -- `dot`,
# `norm`, `axpy!`, `axpby!`, `rmul!`, `copyto!`, `fill!`, broadcasting -- are the
# distributed BLAS-1 ops in `Dagger`.
#
# Workspace vectors are allocated through a `KrylovConstructor` built from
# `similar(b)`, so every internal vector inherits `b`'s element type *and*
# partitioning (`similar` preserves both). This keeps all `mul!`/`dot`/`axpy!`
# operands on a compatible chunk layout, which the distributed kernels require.

function _dagger_krylov(method::Symbol, A, b::DVector; kwargs...)
    kc = Krylov.KrylovConstructor(similar(b))
    workspace = Krylov.krylov_workspace(Val(method), kc)
    Krylov.krylov_solve!(workspace, A, b; kwargs...)
    return Krylov.solution(workspace), Krylov.statistics(workspace)
end

Dagger.krylov_solve(method::Symbol, A, b::DVector; kwargs...) =
    _dagger_krylov(method, A, b; kwargs...)

Dagger.cg(A, b::DVector; kwargs...)       = _dagger_krylov(:cg, A, b; kwargs...)
Dagger.minres(A, b::DVector; kwargs...)   = _dagger_krylov(:minres, A, b; kwargs...)
Dagger.gmres(A, b::DVector; kwargs...)    = _dagger_krylov(:gmres, A, b; kwargs...)
Dagger.bicgstab(A, b::DVector; kwargs...) = _dagger_krylov(:bicgstab, A, b; kwargs...)

end # module KrylovExt
