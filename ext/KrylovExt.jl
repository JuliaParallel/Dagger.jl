module KrylovExt

import Krylov
import Krylov: KrylovConstructor
import Dagger
import Dagger: DVector, DMatrix, Blocks
import LinearAlgebra

# Krylov.jl on Dagger arrays.
#
# Krylov's methods are already generic over the vector type: they only need
# `mul!(y, A, x)` (and `mul!(y, A', x)` for two-sided methods) plus the BLAS-1
# kernels `kdot`/`knorm`/`kaxpy!`/`kaxpby!`/`kscal!`/`kcopy!`/`kfill!`, all of
# which fall back to `dot`/`norm`/`axpy!`/`axpby!`/`rmul!`/`copyto!`/`fill!` for
# a generic `AbstractVector`. Dagger implements every one of those distributed
# (`src/array/linalg.jl`, `src/array/mul.jl`), so no `k*` methods are needed here.
#
# The one thing that does *not* work out of the box is workspace allocation.
# Krylov's `XWorkspace(A, b)` constructors allocate through `S(undef, n)` where
# `S = ktypeof(b)`, and a `DVector`'s type carries neither its block size nor its
# chunk layout -- so that call has no method, and even if it did the workspace
# vectors would not share `b`'s partitioning, forcing a repartitioning copy on
# every `mul!`/`dot`/`axpy!` of every iteration.
#
# Krylov's answer to exactly this is `KrylovConstructor`, which allocates each
# workspace vector with `similar` from a prototype. So the only thing this
# extension does is route `XWorkspace(A, b::DVector)` through a
# `KrylovConstructor` built from `b`. That single hook is what every entry point
# funnels through, so all of these then work directly on Dagger arrays:
#
#     Krylov.cg(A, b)                      # out-of-place
#     Krylov.cg!(workspace, A, b)          # in-place
#     Krylov.krylov_solve(Val(:cg), A, b)  # generic, by method symbol
#     Krylov.krylov_workspace(Val(:cg), A, b)
#
# and user code written against Krylov.jl runs on Dagger by passing Dagger arrays
# in, with no Dagger-specific branch. `Dagger.cg` and friends are kept as thin
# aliases (see below) for callers that used them.

"""
    _square_constructor(A, b::DVector) -> KrylovConstructor

Build the workspace prototype for a *square* operator: every workspace vector is
`similar(b)`, so all of them inherit `b`'s element type, block size, and chunk
placement.

`A` is not consulted at all, which is deliberate -- it lets matrix-free operators
work as long as they implement `mul!` and `size`.
"""
_square_constructor(A, b::DVector) = KrylovConstructor(similar(b))

"""
    _rect_constructor(A, b::DVector) -> KrylovConstructor

Build the workspace prototype for a *rectangular* operator (the least-squares and
least-norm methods). These need a second prototype of length `size(A, 2)`, and it
must be partitioned to match `A`'s columns, since it is what `mul!(y, A, x)`
consumes and `mul!(x, A', y)` produces.

Only a `DMatrix` (or its adjoint/transpose) exposes that column partitioning, so
matrix-free rectangular operators must construct the workspace themselves.
"""
function _rect_constructor(A, b::DVector)
    vn = _domain_vector(A, eltype(b))
    return KrylovConstructor(similar(b), vn)
end

# A zero vector over `A`'s column (domain) space, blocked like `A`'s columns.
_domain_vector(A::DMatrix, ::Type{T}) where {T} =
    Dagger.zeros(Blocks(A.partitioning.blocksize[2]), T, size(A, 2))
_domain_vector(A::LinearAlgebra.AdjOrTrans{<:Any,<:DMatrix}, ::Type{T}) where {T} =
    Dagger.zeros(Blocks(parent(A).partitioning.blocksize[1]), T, size(A, 2))
_domain_vector(A, ::Type{T}) where {T} = throw(ArgumentError(
    "Cannot infer the column partitioning of a $(typeof(A)) operator, which the \
    rectangular Krylov methods need for their length-n workspace vectors. Build \
    the workspace explicitly with a matching prototype, e.g. \
    `Krylov.krylov_workspace(Val(:lsqr), Krylov.KrylovConstructor(similar(b), xproto))`, \
    and solve with `Krylov.krylov_solve!(workspace, A, b)`."))

# Square operators: `Sm == Sn`, so one prototype suffices. Every workspace here
# takes `(A, b)`; kwargs (`memory`, `window`) are forwarded untouched.
for W in (:BicgstabWorkspace, :BilqWorkspace, :BilqrWorkspace, :CarWorkspace,
          :CgWorkspace, :CgLanczosWorkspace, :CgsWorkspace, :CrWorkspace,
          :DiomWorkspace, :DqgmresWorkspace, :FgmresWorkspace, :FomWorkspace,
          :GmresWorkspace, :MinaresWorkspace, :MinresWorkspace,
          :MinresQlpWorkspace, :QmrWorkspace, :SymmlqWorkspace)
    @eval Krylov.$W(A, b::DVector; kwargs...) =
        Krylov.$W(_square_constructor(A, b); kwargs...)
end
# `bilqr` solves the dual system `Aᴴt = c` alongside `Ax = b`; both right-hand
# sides live in the same space, so `c` needs no separate prototype.
Krylov.BilqrWorkspace(A, b::DVector, c) = Krylov.BilqrWorkspace(_square_constructor(A, b))
# Shifted Lanczos carries the shift count through to the workspace.
Krylov.CgLanczosShiftWorkspace(A, b::DVector, nshifts::Integer) =
    Krylov.CgLanczosShiftWorkspace(_square_constructor(A, b), nshifts)

# Rectangular operators: `Sm` (length m, like `b`) and `Sn` (length n, like `A`'s
# columns) differ, so both prototypes are needed.
for W in (:CglsWorkspace, :CgneWorkspace, :CraigWorkspace, :CraigmrWorkspace,
          :CrlsWorkspace, :CrmrWorkspace, :LnlqWorkspace, :LslqWorkspace,
          :LsmrWorkspace, :LsqrWorkspace)
    @eval Krylov.$W(A, b::DVector; kwargs...) =
        Krylov.$W(_rect_constructor(A, b); kwargs...)
end
Krylov.CglsLanczosShiftWorkspace(A, b::DVector, nshifts::Integer) =
    Krylov.CglsLanczosShiftWorkspace(_rect_constructor(A, b), nshifts)

# `Dagger.cg` and friends predate the direct Krylov support above and are kept as
# aliases. New code can call `Krylov.cg(A, b)` directly.
Dagger.krylov_solve(method::Symbol, A, b::DVector; kwargs...) =
    Krylov.krylov_solve(Val(method), A, b; kwargs...)

Dagger.cg(A, b::DVector; kwargs...)       = Krylov.cg(A, b; kwargs...)
Dagger.minres(A, b::DVector; kwargs...)   = Krylov.minres(A, b; kwargs...)
Dagger.gmres(A, b::DVector; kwargs...)    = Krylov.gmres(A, b; kwargs...)
Dagger.bicgstab(A, b::DVector; kwargs...) = Krylov.bicgstab(A, b; kwargs...)

end # module KrylovExt
