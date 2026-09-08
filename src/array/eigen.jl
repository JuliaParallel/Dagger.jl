# Iterative few-eigenpair solver for distributed / matrix-free operators.
#
# LinearAlgebra's generic `eigen` / `eigvals` collect an `AbstractMatrix` and
# run dense geev. That would silently densify a sparse-backed `DMatrix` (an
# out-of-memory multiplier) and cannot see a matrix-free `mul!`. This file
# intercepts those entry points for `DMatrix` (and the matrix-free wrappers)
# and computes a *few* extreme eigenpairs with LOBPCG over `DVector`s.
#
# The large work is only `mul!(y, A, x)` plus BLAS-1 (`dot` / `axpy!` /
# `rmul!` / `norm2`). Rayleigh-Ritz is a host eigenproblem of size O(nev)
# (typically 1–6). Trial vectors stay a `Vector{DVector}`: column `getindex`
# of a tall-skinny `DMatrix` is a nested `DArray` (AGENTS.md lesson 33).
#
# This is not ScaLAPACK dense geev and does not return the full spectrum.
# Default `nev=1`, `which=:SR` (smallest algebraic) — the 1-D Laplacian
# ground state. Check `‖Ax-λx‖`, not only the Ritz residual.

# ────────────────────────────────────────────────────────────────────────────
# DVector block kernels
# ────────────────────────────────────────────────────────────────────────────

function _eigen_apply!(Y::Vector{<:DVector}, A, X::Vector{<:DVector})
    length(Y) == length(X) || throw(DimensionMismatch(
        "eigen apply: $(length(Y)) outputs for $(length(X)) inputs"))
    @inbounds for j in eachindex(X)
        LinearAlgebra.mul!(Y[j], A, X[j])
    end
    return Y
end

function _eigen_gram(X::Vector{<:DVector}, AX::Vector{<:DVector})
    m = length(X)
    T = typeof(LinearAlgebra.dot(zero(eltype(X[1])), zero(eltype(AX[1]))))
    G = Matrix{T}(undef, m, m)
    @inbounds for j in 1:m, i in 1:m
        G[i, j] = LinearAlgebra.dot(X[i], AX[j])
    end
    return (G .+ G') ./ 2
end

function _eigen_combine!(Y::Vector{<:DVector}, X::Vector{<:DVector}, C::AbstractMatrix)
    size(C, 1) == length(X) || throw(DimensionMismatch(
        "eigen combine: $(length(X)) vectors vs $(size(C, 1)) combination rows"))
    size(C, 2) == length(Y) || throw(DimensionMismatch(
        "eigen combine: $(length(Y)) outputs vs $(size(C, 2)) combination columns"))
    T = eltype(Y[1])
    @inbounds for j in eachindex(Y)
        fill!(Y[j], zero(T))
        for i in eachindex(X)
            cij = T(C[i, j])
            iszero(cij) && continue
            LinearAlgebra.axpy!(cij, X[i], Y[j])
        end
    end
    return Y
end

# Modified Gram-Schmidt on copies. Drops columns whose residual norm after
# orthogonalization is below `drop_tol` (linearly dependent search directions).
function _eigen_mgs(xs::Vector{<:DVector}; drop_tol::Real)
    Q = DVector[]
    for x in xs
        q = copy(x)
        for qi in Q
            LinearAlgebra.axpy!(-LinearAlgebra.dot(qi, q), qi, q)
        end
        nrm = LinearAlgebra.norm2(q)
        if nrm > drop_tol
            LinearAlgebra.rmul!(q, inv(nrm))
            push!(Q, q)
        end
    end
    return Q
end

function _eigen_write_cols!(tile, cols...)
    for j in 1:length(cols)
        @views copyto!(tile[:, j], cols[j])
    end
    return nothing
end

# Pack a short list of `DVector`s as columns of a `DMatrix` without column
# `getindex` (lesson 33). `nev` is small, so one column-tile of width `nev`
# is the natural layout.
function _eigen_pack(X::Vector{<:DVector{T}}) where {T}
    k = length(X)
    k >= 1 || throw(ArgumentError("eigen pack: no vectors"))
    n = length(X[1])
    mb = X[1].partitioning.blocksize[1]
    V = zeros(Blocks(mb, k), T, n, k)
    mt = length(X[1].chunks)
    Dagger.spawn_datadeps() do
        for i in 1:mt
            cols = ntuple(j -> In(X[j].chunks[i]), k)
            Dagger.spawn(_eigen_write_cols!, InOut(V.chunks[i, 1]), cols...)
        end
    end
    return V
end

function _eigen_random_block(proto::DVector{T}, nev::Int) where {T}
    part = proto.partitioning
    dims = size(proto)
    return [randn(part, T, dims...) for _ in 1:nev]
end

function _eigen_initial_block(proto::DVector{T}, nev::Int, v0) where {T}
    if v0 === nothing
        return _eigen_random_block(proto, nev)
    elseif v0 isa DVector
        X = _eigen_random_block(proto, nev)
        copyto!(X[1], v0)
        return X
    elseif v0 isa AbstractVector && !isempty(v0) && first(v0) isa DVector
        length(v0) == nev || throw(ArgumentError(
            "v0 has $(length(v0)) vectors but nev=$nev"))
        return DVector[copy(v) for v in v0]
    else
        throw(ArgumentError(
            "v0 must be a DVector, a Vector of DVectors, or nothing; got $(typeof(v0))"))
    end
end

# ────────────────────────────────────────────────────────────────────────────
# Rayleigh-Ritz selection
# ────────────────────────────────────────────────────────────────────────────

function _eigen_normalize_which(which::Symbol)
    which === :SA && return :SR
    which === :smallest && return :SR
    which === :LA && return :LR
    which === :largest && return :LR
    which in (:SR, :LR, :SM, :LM) && return which
    throw(ArgumentError(
        "which=$which is not supported; use :SR / :LR (algebraic) or :SM / :LM (magnitude)"))
end

function _eigen_pick_ritz(evals::AbstractVector, evecs::AbstractMatrix, which::Symbol, nev::Int)
    nritz = length(evals)
    ntake = min(nev, nritz)
    ntake >= 1 || throw(ArgumentError("Rayleigh-Ritz produced no Ritz values"))
    idx = if which === :SR
        collect(1:ntake)                         # Hermitian eigen is ascending
    elseif which === :LR
        collect(nritz:-1:(nritz - ntake + 1))    # largest first
    elseif which === :SM
        sortperm(abs.(evals))[1:ntake]
    else # :LM
        sortperm(abs.(evals); rev=true)[1:ntake]
    end
    return real.(evals[idx]), evecs[:, idx]
end

# ────────────────────────────────────────────────────────────────────────────
# LOBPCG (Knyazev): few extreme eigenpairs of a Hermitian operator
# ────────────────────────────────────────────────────────────────────────────

function _lobpcg(A, X::Vector{<:DVector};
                 which::Symbol=:SR,
                 P=nothing,
                 tol::Union{Real,Nothing}=nothing,
                 maxiter::Integer=200)
    nev = length(X)
    nev >= 1 || throw(ArgumentError("LOBPCG needs at least one trial vector"))
    T = eltype(X[1])
    R = real(float(T))
    n = length(X[1])
    nev < n || throw(ArgumentError(
        "LOBPCG computes a few eigenpairs (nev=$nev) of an n=$n operator; \
        nev must be < n. This is not a dense full-spectrum eigensolver."))
    which = _eigen_normalize_which(which)
    rtol = tol === nothing ? 10 * sqrt(eps(R)) : R(tol)
    drop_tol = max(eps(R) * n, rtol * R(1e-8))

    X = _eigen_mgs(X; drop_tol)
    length(X) == nev || throw(ArgumentError(
        "initial trial vectors are linearly dependent (got $(length(X)) of $nev)"))

    proto = X[1]
    AX = [similar(proto) for _ in 1:nev]
    W = [similar(proto) for _ in 1:nev]
    Rvecs = [similar(proto) for _ in 1:nev]
    Pdir = DVector[]
    λ = zeros(R, nev)
    maxres = R(Inf)
    precon = !(P === nothing || P === LinearAlgebra.I)

    for _iter in 1:maxiter
        _eigen_apply!(AX, A, X)
        G = _eigen_gram(X, AX)
        evals, evecs = LinearAlgebra.eigen(LinearAlgebra.Hermitian(G))
        λ, C = _eigen_pick_ritz(evals, evecs, which, nev)
        Xnew = [similar(proto) for _ in 1:nev]
        AXnew = [similar(proto) for _ in 1:nev]
        _eigen_combine!(Xnew, X, C)
        _eigen_combine!(AXnew, AX, C)
        X, AX = Xnew, AXnew

        maxres = zero(R)
        @inbounds for j in 1:nev
            copyto!(Rvecs[j], AX[j])
            LinearAlgebra.axpy!(-T(λ[j]), X[j], Rvecs[j])
            maxres = max(maxres, R(LinearAlgebra.norm2(Rvecs[j])))
        end
        maxres <= rtol && return λ, X, maxres

        if precon
            @inbounds for j in 1:nev
                LinearAlgebra.mul!(W[j], P, Rvecs[j])
            end
        else
            @inbounds for j in 1:nev
                copyto!(W[j], Rvecs[j])
            end
        end

        S = _eigen_mgs(vcat(X, W, Pdir); drop_tol)
        length(S) >= nev || continue
        AS = [similar(S[1]) for _ in S]
        _eigen_apply!(AS, A, S)
        Gs = _eigen_gram(S, AS)
        evals, evecs = LinearAlgebra.eigen(LinearAlgebra.Hermitian(Gs))
        λ, C = _eigen_pick_ritz(evals, evecs, which, nev)
        X = [similar(S[1]) for _ in 1:nev]
        _eigen_combine!(X, S, C)

        nX = min(nev, length(S))
        if length(S) > nX
            Cwp = C[(nX + 1):end, :]
            WP = S[(nX + 1):end]
            Pdir = [similar(S[1]) for _ in 1:nev]
            _eigen_combine!(Pdir, WP, Cwp)
            Pdir = _eigen_mgs(Pdir; drop_tol)
        else
            empty!(Pdir)
        end
    end
    return λ, X, maxres
end

# ────────────────────────────────────────────────────────────────────────────
# Prototypes and LinearAlgebra entry points
# ────────────────────────────────────────────────────────────────────────────

function _eigen_prototype(A::DMatrix{T}) where {T}
    n = LinearAlgebra.checksquare(A)
    mb, nb = A.partitioning.blocksize
    return randn(Blocks(mb == nb ? mb : min(mb, nb)), T, n)
end

function _eigen_prototype(A::Projected)
    N = A.left
    if N isa DVector
        return randn(N.partitioning, eltype(N), size(N)...)
    end
    n = size(N, 1)
    mb = N.partitioning.blocksize[1]
    return randn(Blocks(mb), eltype(N), n)
end

function _eigen_prototype(A::BlockOperator, v0)
    v0 isa DVector && return v0
    throw(ArgumentError(
        "eigen(::BlockOperator) needs v0::DVector so the trial vectors match \
        the field partitioning (the operator does not store a vector Blocks)."))
end

function _eigen_from_prototype(A, proto::DVector;
                               nev::Integer=1,
                               which::Symbol=:SR,
                               v0=nothing,
                               P=nothing,
                               M=nothing,
                               tol::Union{Real,Nothing}=nothing,
                               maxiter::Integer=200,
                               sortby::Union{Function,Nothing}=nothing,
                               permute::Bool=true,
                               scale::Bool=true)
    nev >= 1 || throw(ArgumentError("nev must be ≥ 1, got $nev"))
    n = length(proto)
    size(A, 1) == n || throw(DimensionMismatch(
        "operator has $(size(A, 1)) rows but the trial vector has length $n"))
    size(A, 2) == n || throw(DimensionMismatch(
        "eigen is only implemented for square operators, got $(size(A))"))
    precon = M === nothing ? P : M
    X = _eigen_initial_block(proto, Int(nev), v0 === nothing ? proto : v0)
    λ, X, _ = _lobpcg(A, X; which, P=precon, tol, maxiter)
    if sortby !== nothing
        p = sortperm(λ; by=sortby)
        λ = λ[p]
        X = X[p]
    end
    return LinearAlgebra.Eigen(λ, _eigen_pack(X))
end

"""
    eigen(A::DMatrix; nev=1, which=:SR, v0=nothing, P=nothing, tol, maxiter) -> Eigen

A few extreme eigenpairs of a distributed matrix by LOBPCG (locally optimal
block preconditioned conjugate gradient). This is **not** dense geev: only
`nev` pairs are computed (default 1), and the operator is applied through
distributed `mul!` — a sparse-backed `DMatrix` stays sparse.

`which` selects which end of the spectrum (`:SR` / `:SA` smallest algebraic,
`:LR` / `:LA` largest; `:SM` / `:LM` by magnitude). `P` (alias `M`) is an
optional preconditioner applied as `mul!(y, P, r)` (`y ← M⁻¹ r`), the same
convention as Krylov `ldiv=false`. `v0` is an optional `DVector` (or a
vector of them) used as the initial trial block.

Returns `LinearAlgebra.Eigen` with a host `values::Vector` and distributed
`vectors::DMatrix` (`n × nev`). Check the true residual `‖Ax-λx‖`.
"""
function LinearAlgebra.eigen(A::DMatrix; kwargs...)
    return _eigen_from_prototype(A, _eigen_prototype(A); kwargs...)
end

"""
    eigvals(A::DMatrix; nev=1, which=:SR, ...) -> Vector

Eigenvalues only; see [`eigen`](@ref). Same iterative LOBPCG path — does not
form a dense Schur factorization.
"""
function LinearAlgebra.eigvals(A::DMatrix; kwargs...)
    return LinearAlgebra.eigen(A; kwargs...).values
end

# LinearAlgebra defines
#   eigen(::Union{Hermitian{T,S}, Hermitian{Complex{T},S}, Symmetric{T,S}} where {T<:Real,S})
# which is *not* more specific than `Hermitian{<:Any,<:DMatrix}` (and vice
# versa), so a `Hermitian{Float64,DMatrix}` is ambiguous. Match that union
# with `S<:DMatrix` so we win on distributed wrappers.
const _DHermOrSymReal{T} = Union{
    LinearAlgebra.Hermitian{T,<:DMatrix},
    LinearAlgebra.Hermitian{Complex{T},<:DMatrix},
    LinearAlgebra.Symmetric{T,<:DMatrix},
} where {T<:Real}

function LinearAlgebra.eigen(A::_DHermOrSymReal; kwargs...)
    return LinearAlgebra.eigen(parent(A); kwargs...)
end
function LinearAlgebra.eigvals(A::_DHermOrSymReal; kwargs...)
    return LinearAlgebra.eigvals(parent(A); kwargs...)
end

function LinearAlgebra.eigen(A::Projected; v0=nothing, kwargs...)
    proto = v0 isa DVector ? v0 : _eigen_prototype(A)
    return _eigen_from_prototype(A, proto; v0, kwargs...)
end
function LinearAlgebra.eigvals(A::Projected; kwargs...)
    return LinearAlgebra.eigen(A; kwargs...).values
end

function LinearAlgebra.eigen(A::BlockOperator; v0, kwargs...)
    proto = _eigen_prototype(A, v0)
    return _eigen_from_prototype(A, proto; v0, kwargs...)
end
function LinearAlgebra.eigvals(A::BlockOperator; v0, kwargs...)
    return LinearAlgebra.eigen(A; v0, kwargs...).values
end
