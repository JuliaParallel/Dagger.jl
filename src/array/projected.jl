# Projected operator: attach a known nullspace and drop it from every product.
#
# Julia's `LinearAlgebra.nullspace` *computes* a basis; solvers need the other
# half of PETSc `MatNullSpace` — hold that basis and apply
# `x ← x - N(N'x)` (orthonormal columns) so a Krylov iteration on a Neumann
# Poisson / Stokes / Maxwell operator stays in the orthogonal complement.
# The wrapper is the API: `Krylov.cg(Projected(A, N), b)` is enough. There is
# no `attach_nullspace` hook, and users never have to call [`project!`](@ref)
# themselves.
#
# Not an `AbstractMatrix`. `Adjoint` *is* one regardless of its parent (see
# AGENTS.md), so two-sided Krylov (`A'`) would scalar-index unless `mul!` is
# defined on `Adjoint{<:Any,<:Projected}` / `Transpose{<:Any,<:Projected}`.

"""
    Projected(A, N; orthonormalize=true)
    Projected(A, left, right; orthonormalize=true)

Wrap the operator `A` so that each `mul!(y, ·, x)` over `DVector`s projects
out a known nullspace. `A` is any object with `mul!(y, A, x)` (a `DMatrix` or
a matrix-free operator); `N` is a `DVector` (one basis vector) or a `DMatrix`
whose *columns* are the basis, partitioned like the vectors Krylov will
multiply.

This is the solver-side counterpart of PETSc `MatNullSpace`. After each
product the wrapper applies `y ← y - N(N'y)` for orthonormal columns, so

```julia
N = distribute(ones(n), Blocks(k))          # constant mode; orthonormalized
x, stats = Krylov.minres(Dagger.Projected(A, N), b)
```

needs no extra `project!` call. One `N` is the symmetric case (Neumann
Poisson). Pass `left` and `right` separately when the left and right
nullspaces differ: `left` is removed from `A*x` (range / residual), `right`
from the input of `A*x` and from `A'*x`.

Columns are orthonormalized at construction by default, so a raw `ones(n)`
is a valid Poisson nullspace (`‖ones‖ = √n`, not 1). Set
`orthonormalize=false` only when the columns are already orthonormal.

The stored basis is what [`GlobalAMG`](@ref) /
[`SmoothedAggregationPreconditioner`](@ref) read when the operator is a
`Projected` (`P.right`, unless `nullspace=` is passed explicitly).
"""
struct Projected{TA,TL,TR}
    A::TA
    left::TL
    right::TR
end

function Projected(A, N::Union{DVector,DMatrix}; orthonormalize::Bool=true)
    return Projected(A, N, N; orthonormalize)
end

function Projected(A, left::Union{DVector,DMatrix}, right::Union{DVector,DMatrix};
                   orthonormalize::Bool=true)
    L = orthonormalize ? _orthonormalize_basis(left) : left
    R = if right === left
        L
    elseif orthonormalize
        _orthonormalize_basis(right)
    else
        right
    end
    _check_nullspace_dim(A, L, :left)
    _check_nullspace_dim(A, R, :right)
    return Projected{typeof(A),typeof(L),typeof(R)}(A, L, R)
end

function _nullspace_rows(N::DVector)
    return length(N)
end
function _nullspace_rows(N::DMatrix)
    return size(N, 1)
end

function _check_nullspace_dim(A, N, side::Symbol)
    nA = size(A, side === :left ? 1 : 2)
    nN = _nullspace_rows(N)
    nA == nN || throw(DimensionMismatch(
        "$side nullspace has $nN rows but the operator is $(size(A, 1))×$(size(A, 2))"))
    return nothing
end

Base.size(A::Projected) = size(A.A)
Base.size(A::Projected, d::Integer) = size(A.A, d)
Base.eltype(A::Projected) = eltype(A.A)
Base.adjoint(A::Projected) = Adjoint(A)
Base.transpose(A::Projected) = Transpose(A)

# --- Vector projection ----------------------------------------------------
# `x ← x - N(N'x)` for orthonormal columns. A `DVector` is the rank-1 (Poisson
# constant) case; a `DMatrix` uses one distributed `gemv` pair so a handful of
# columns stay one product, not a Julia loop over `N[:, j]`.

"""
    project!(x::DVector, N) -> x

Remove the columns of `N` from `x` in place: `x ← x - N(N'x)` for
orthonormal columns. `N` is a `DVector` or a `DMatrix`. Used internally by
[`Projected`](@ref); callers of Krylov do not need it.
"""
function project! end

function project!(x::DVector, N::DVector)
    size(x) == size(N) || throw(DimensionMismatch(
        "project!: vector has size $(size(x)) but the basis has size $(size(N))"))
    α = LinearAlgebra.dot(N, x)
    LinearAlgebra.axpy!(-α, N, x)
    return x
end

function project!(x::DVector, N::DMatrix)
    size(x, 1) == size(N, 1) || throw(DimensionMismatch(
        "project!: vector length $(length(x)) incompatible with basis $(size(N))"))
    T = eltype(x)
    k = size(N, 2)
    # Tiny coefficient vector; one tile is enough (k is the nullspace
    # dimension, typically 1).
    c = zeros(Blocks(k), T, k)
    LinearAlgebra.mul!(c, N', x)
    LinearAlgebra.mul!(x, N, c, -one(T), one(T))
    return x
end

function _orthonormalize_basis(N::DVector)
    Q = copy(N)
    nrm = LinearAlgebra.norm2(Q)
    iszero(nrm) && throw(ArgumentError("nullspace basis is the zero vector"))
    LinearAlgebra.rmul!(Q, inv(nrm))
    return Q
end

function _orthonormalize_basis(N::DMatrix)
    # Tall-skinny (n × k, k typically 1–6): gather, MGS on the host, redistribute.
    # Column `getindex` of a DMatrix is an n×1 DArray whose tiles are themselves
    # DArrays; `rmul!` then asks Datadeps to alias a `DArray` and throws
    # `ConcurrencyViolationError`. Construction is not a hot path.
    Qh = Matrix(collect(N))
    k = size(Qh, 2)
    for j in 1:k
        qj = view(Qh, :, j)
        for i in 1:j-1
            qi = view(Qh, :, i)
            LinearAlgebra.axpy!(-LinearAlgebra.dot(qi, qj), qi, qj)
        end
        nrm = LinearAlgebra.norm(qj)
        iszero(nrm) && throw(ArgumentError(
            "nullspace column $j is linearly dependent (or zero)"))
        LinearAlgebra.rmul!(qj, inv(nrm))
    end
    return distribute(Qh, N.partitioning)
end

# Copy-and-project the input so `mul!(y, A, x)` never mutates `x`. The extra
# vector is one `similar` per product; the alternative (project `x` in place
# and restore) would still need that copy.
function _projected_copy(N, x::DVector)
    xt = copy(x)
    project!(xt, N)
    return xt
end

function LinearAlgebra.mul!(y::DVector, A::Projected, x::DVector)
    xt = _projected_copy(A.right, x)
    LinearAlgebra.mul!(y, A.A, xt)
    project!(y, A.left)
    return y
end

function LinearAlgebra.mul!(y::DVector, AdjA::Adjoint{<:Any,<:Projected}, x::DVector)
    A = parent(AdjA)
    # Right nullspace of A' is the left nullspace of A, and vice versa.
    xt = _projected_copy(A.left, x)
    LinearAlgebra.mul!(y, A.A', xt)
    project!(y, A.right)
    return y
end

function LinearAlgebra.mul!(y::DVector, TrA::Transpose{<:Any,<:Projected}, x::DVector)
    A = parent(TrA)
    xt = _projected_copy(A.left, x)
    LinearAlgebra.mul!(y, transpose(A.A), xt)
    project!(y, A.right)
    return y
end

function LinearAlgebra.mul!(y::DVector, A::Projected, x::DVector, α::Number, β::Number)
    if iszero(α)
        iszero(β) && return fill!(y, zero(eltype(y)))
        isone(β) && return y
        return LinearAlgebra.rmul!(y, β)
    end
    if iszero(β) && isone(α)
        return LinearAlgebra.mul!(y, A, x)
    end
    tmp = similar(y)
    LinearAlgebra.mul!(tmp, A, x)
    return LinearAlgebra.axpby!(α, tmp, β, y)
end

Base.:*(A::Projected, x::DVector) = LinearAlgebra.mul!(similar(x), A, x)
