module KrylovExt

import Krylov
import Krylov: KrylovConstructor
import Dagger
import Dagger: DVector, DMatrix, Blocks
import LinearAlgebra
import Base.Broadcast

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

Dagger._sparse_iterative_lu(A::DMatrix) =
    Dagger.SparseIterativeFactorization(A, size(A, 1))

# ---------------------------------------------------------------------------
# Block Krylov (`block_gmres` / `block_minres`) on a `DMatrix` RHS
# ---------------------------------------------------------------------------
#
# Krylov's block workspaces use one storage type `SM` for *every* matrix: the
# tall n×p blocks (`X`, `V`, `W`) *and* the p×p / 2p×p Hessenberg blocks that
# are Householder-QR'd and scalar-indexed. `DMatrix{T}(undef, n, p)` cannot
# see the RHS partitioning (same reason as the vector `KrylovConstructor`
# hook), and a p×p `DMatrix` would make `H[1:p,:] .= R` / `kunmqr!` / views
# go through distributed getindex.
#
# `BlockKrylovMatrix` is that `SM`: `SM(undef, n, p)` allocates a dense
# `DMatrix` with the RHS row/column blocking; `SM(undef, p, p)` (first
# dimension ≠ n) stays a host `Matrix`. Tall QR gathers the n×p panel (O(np),
# the size of `B` itself); the operator apply is `mul!(W, A, P)` — one
# SpMM/GEMM, not a Julia loop of `A \ b`.

"""
Internal Krylov block-workspace storage. Not a solver type; not exported.
Tall blocks (`size(A,1) == n`) are dense `DMatrix`s; the Hessenberg is host.
"""
struct BlockKrylovMatrix{T,M,Bm,Bn} <: AbstractMatrix{T}
    data::AbstractMatrix{T}
end

function _block_sm_type(B::DMatrix{T}) where T
    m = size(B, 1)
    bm, bn = Int(B.partitioning.blocksize[1]), Int(B.partitioning.blocksize[2])
    return BlockKrylovMatrix{T,m,bm,bn}
end

function BlockKrylovMatrix{T,M,Bm,Bn}(::UndefInitializer, m::Integer, n::Integer) where {T,M,Bm,Bn}
    m = Int(m)
    n = Int(n)
    if m == 0 || n == 0
        return BlockKrylovMatrix{T,M,Bm,Bn}(Matrix{T}(undef, m, n))
    elseif m == M
        return BlockKrylovMatrix{T,M,Bm,Bn}(Dagger.zeros(Blocks(Bm, Bn), T, m, n))
    else
        return BlockKrylovMatrix{T,M,Bm,Bn}(Matrix{T}(undef, m, n))
    end
end

_block_result(X::BlockKrylovMatrix) = X.data
_block_result(X) = X

_bk_data(A::BlockKrylovMatrix) = A.data
_bk_data(A::LinearAlgebra.Adjoint{<:Any,<:BlockKrylovMatrix}) = adjoint(parent(A).data)
_bk_data(A::LinearAlgebra.Transpose{<:Any,<:BlockKrylovMatrix}) = transpose(parent(A).data)
_bk_data(A) = A

Base.size(A::BlockKrylovMatrix) = size(A.data)
Base.isempty(A::BlockKrylovMatrix) = isempty(A.data)
Base.IndexStyle(::Type{<:BlockKrylovMatrix}) = IndexCartesian()
Base.getindex(A::BlockKrylovMatrix, i::Integer...) = getindex(A.data, i...)
function Base.setindex!(A::BlockKrylovMatrix, v, i::Integer...)
    setindex!(A.data, v isa BlockKrylovMatrix ? v.data : v, i...)
    return A
end
function Base.setindex!(A::BlockKrylovMatrix, v, I...)
    setindex!(A.data, v isa BlockKrylovMatrix ? v.data : v, I...)
    return A
end
Base.view(A::BlockKrylovMatrix, I...) = view(A.data, I...)
Base.broadcastable(A::BlockKrylovMatrix) = A.data
Base.parent(A::BlockKrylovMatrix) = A.data

function Base.fill!(A::BlockKrylovMatrix, x)
    fill!(A.data, x)
    return A
end

function Base.copyto!(dest::BlockKrylovMatrix, src::BlockKrylovMatrix)
    copyto!(dest.data, src.data)
    return dest
end
function Base.copyto!(dest::BlockKrylovMatrix, src::AbstractArray)
    copyto!(dest.data, src)
    return dest
end
function Base.copyto!(dest::Dagger.DArray, src::BlockKrylovMatrix)
    copyto!(dest, src.data)
    return dest
end
function Base.copyto!(dest::AbstractArray, src::BlockKrylovMatrix)
    copyto!(dest, src.data)
    return dest
end
function Base.copyto!(dest::BlockKrylovMatrix, bc::Broadcast.Broadcasted)
    copyto!(dest.data, bc)
    return dest
end

function Base.convert(::Type{D}, A::BlockKrylovMatrix) where {D<:Dagger.DArray}
    A.data isa D && return A.data
    return convert(D, A.data)
end

LinearAlgebra.norm(A::BlockKrylovMatrix) =
    A.data isa Dagger.DArray ? LinearAlgebra.norm2(A.data) : LinearAlgebra.norm(A.data)
# `block_minres` uses `R₀ = B` (the caller's `DMatrix`) on a cold start.
LinearAlgebra.norm(A::DMatrix) = LinearAlgebra.norm2(A)

function LinearAlgebra.mul!(C::BlockKrylovMatrix, A, B)
    LinearAlgebra.mul!(_bk_data(C), _bk_data(A), _bk_data(B))
    return C
end
function LinearAlgebra.mul!(C::BlockKrylovMatrix, A, B, α::Number, β::Number)
    LinearAlgebra.mul!(_bk_data(C), _bk_data(A), _bk_data(B), α, β)
    return C
end
# More specific than `mul!(::BlockKrylovMatrix, A, B)` ×
# `mul!(::AbstractMatrix, ::AdjOrTrans{<:BlockKrylovMatrix}, ::BlockKrylovMatrix)`.
function LinearAlgebra.mul!(
        C::BlockKrylovMatrix,
        A::LinearAlgebra.AdjOrTrans{<:Any,<:BlockKrylovMatrix},
        B::BlockKrylovMatrix,
    )
    LinearAlgebra.mul!(_bk_data(C), _bk_data(A), _bk_data(B))
    return C
end
function LinearAlgebra.mul!(
        C::BlockKrylovMatrix,
        A::LinearAlgebra.AdjOrTrans{<:Any,<:BlockKrylovMatrix},
        B::BlockKrylovMatrix,
        α::Number,
        β::Number,
    )
    LinearAlgebra.mul!(_bk_data(C), _bk_data(A), _bk_data(B), α, β)
    return C
end
function LinearAlgebra.mul!(C::AbstractMatrix, A::LinearAlgebra.AdjOrTrans{<:Any,<:BlockKrylovMatrix}, B::BlockKrylovMatrix)
    LinearAlgebra.mul!(C, _bk_data(A), _bk_data(B))
    return C
end
function LinearAlgebra.mul!(C::AbstractMatrix, A::LinearAlgebra.AdjOrTrans{<:Any,<:BlockKrylovMatrix}, B::BlockKrylovMatrix, α::Number, β::Number)
    LinearAlgebra.mul!(C, _bk_data(A), _bk_data(B), α, β)
    return C
end

function LinearAlgebra.ldiv!(A::LinearAlgebra.UpperTriangular{<:Any,<:BlockKrylovMatrix}, B::BlockKrylovMatrix)
    LinearAlgebra.ldiv!(LinearAlgebra.UpperTriangular(_bk_data(parent(A))), _bk_data(B))
    return B
end
function LinearAlgebra.rdiv!(A::BlockKrylovMatrix, B::LinearAlgebra.UpperTriangular)
    LinearAlgebra.rdiv!(_bk_data(A), LinearAlgebra.UpperTriangular(_bk_data(parent(B))))
    return A
end
function LinearAlgebra.rdiv!(A::DMatrix, B::LinearAlgebra.UpperTriangular{<:Any,<:AbstractMatrix})
    Ah = collect(A)
    LinearAlgebra.rdiv!(Ah, B)
    copyto!(A, Ah)
    return A
end

function _householder_dmatrix!(Q::DMatrix, R::AbstractMatrix, τ::AbstractVector, buffer; compact::Bool=false)
    Qh = collect(Q)
    Rh = R isa Matrix ? R : collect(R)
    if buffer === nothing
        Krylov.householder!(Qh, Rh, τ; compact)
    else
        Krylov.householder!(Qh, Rh, τ, buffer; compact)
    end
    copyto!(Q, Qh)
    R === Rh || copyto!(R, Rh)
    return Q, R
end

function Krylov.householder!(Q::DMatrix, R::AbstractMatrix, τ::AbstractVector; compact::Bool=false)
    return _householder_dmatrix!(Q, R, τ, nothing; compact)
end
function Krylov.householder!(Q::DMatrix, R::AbstractMatrix, τ::AbstractVector, buffer::AbstractVector; compact::Bool=false)
    return _householder_dmatrix!(Q, R, τ, buffer; compact)
end
function Krylov.householder!(Q::BlockKrylovMatrix, R::AbstractMatrix, τ::AbstractVector; compact::Bool=false)
    return Krylov.householder!(Q.data, R isa BlockKrylovMatrix ? R.data : R, τ; compact)
end
function Krylov.householder!(Q::BlockKrylovMatrix, R::AbstractMatrix, τ::AbstractVector, buffer::AbstractVector; compact::Bool=false)
    return Krylov.householder!(Q.data, R isa BlockKrylovMatrix ? R.data : R, τ, buffer; compact)
end

function Krylov.kgeqrf_buffer!(A::Union{DMatrix,BlockKrylovMatrix}, τ::AbstractVector)
    return 0
end
function Krylov.kungqr_buffer!(A::Union{DMatrix,BlockKrylovMatrix}, τ::AbstractVector)
    return 0
end
function Krylov.kunmqr_buffer!(side::Char, trans::Char, A::BlockKrylovMatrix, τ::AbstractVector, C::AbstractMatrix)
    return Krylov.kunmqr_buffer!(side, trans, A.data, τ, C isa BlockKrylovMatrix ? C.data : C)
end
function Krylov.kunmqr!(side::Char, trans::Char, A::BlockKrylovMatrix, τ::AbstractVector, C::BlockKrylovMatrix)
    Krylov.kunmqr!(side, trans, A.data, τ, C.data)
    return C
end
function Krylov.kunmqr!(side::Char, trans::Char, A::BlockKrylovMatrix, τ::AbstractVector, C::BlockKrylovMatrix, buffer::AbstractVector)
    Krylov.kunmqr!(side, trans, A.data, τ, C.data, buffer)
    return C
end

Krylov.ktypeof(B::DMatrix) = _block_sm_type(B)
Krylov.matrix_to_vector(::Type{<:BlockKrylovMatrix{T}}) where {T} = Vector{T}

function _block_qr_buffer(::Type{FC}, p::Int) where FC
    Hprobe = Matrix{FC}(undef, 2p, p)
    τprobe = Vector{FC}(undef, p)
    Dprobe = Matrix{FC}(undef, 2p, p)
    trans = FC <: AbstractFloat ? 'T' : 'C'
    return max(
        Int(Krylov.kgeqrf_buffer!(Hprobe, τprobe)),
        Int(Krylov.kungqr_buffer!(Hprobe, τprobe)),
        Int(Krylov.kunmqr_buffer!('L', trans, Hprobe, τprobe, Dprobe)),
        1,
    )
end

function _block_memory(n::Int, p::Int, memory::Int)
    p > 0 || throw(ArgumentError("block Krylov RHS must have at least one column"))
    return max(1, min(max(div(n, p), 1), memory))
end

function Krylov.BlockGmresWorkspace(A, B::DMatrix{FC}; memory::Int=5) where {FC}
    start_allocation_time = time_ns()
    mA, nA = size(A)
    n, p = size(B)
    memory = _block_memory(n, p, memory)
    T = real(FC)
    SM = _block_sm_type(B)
    SV = Vector{FC}
    ΔX = SM(undef, 0, 0)
    X = SM(undef, n, p)
    W = SM(undef, n, p)
    P = SM(undef, 0, 0)
    Q = SM(undef, 0, 0)
    C = SM(undef, p, p)
    D = SM(undef, 2p, p)
    V = SM[SM(undef, n, p) for _ in 1:memory]
    Z = SM[SM(undef, p, p) for _ in 1:memory]
    R = SM[SM(undef, p, p) for _ in 1:div(memory * (memory + 1), 2)]
    H = SM[SM(undef, 2p, p) for _ in 1:memory]
    τ = SV[SV(undef, p) for _ in 1:memory]
    buffer = Vector{FC}(undef, _block_qr_buffer(FC, p))
    stats = Krylov.SimpleStats(0, false, false, false, 0, T[], T[], T[], 0.0, 0.0, "unknown")
    workspace = Krylov.BlockGmresWorkspace{T,FC,SV,SM}(
        mA, nA, p, ΔX, X, W, P, Q, C, D, V, Z, R, H, τ, buffer, false, stats)
    workspace.stats.allocation_timer = start_allocation_time |> Krylov.ktimer
    return workspace
end

function Krylov.BlockMinresWorkspace(A, B::DMatrix{FC}) where {FC}
    start_allocation_time = time_ns()
    mA, nA = size(A)
    n, p = size(B)
    T = real(FC)
    SM = _block_sm_type(B)
    SV = Vector{FC}
    ΔX = SM(undef, 0, 0)
    X = SM(undef, n, p)
    P = SM(undef, 0, 0)
    Q = SM(undef, n, p)
    C = SM(undef, p, p)
    D = SM(undef, 2p, p)
    Φ = SM(undef, p, p)
    Ψₖ = SM(undef, p, p)
    Ωₖ = SM(undef, p, p)
    Ψₖ₊₁ = SM(undef, p, p)
    Πₖ₋₂ = SM(undef, p, p)
    Γbarₖ₋₁ = SM(undef, p, p)
    Γₖ₋₁ = SM(undef, p, p)
    Λbarₖ = SM(undef, p, p)
    Λₖ = SM(undef, p, p)
    Vₖ₋₁ = SM(undef, n, p)
    Vₖ = SM(undef, n, p)
    wₖ₋₂ = SM(undef, n, p)
    wₖ₋₁ = SM(undef, n, p)
    wₖ = SM(undef, n, p)
    Hₖ₋₂ = SM(undef, 2p, p)
    Hₖ₋₁ = SM(undef, 2p, p)
    τₖ₋₂ = SV(undef, p)
    τₖ₋₁ = SV(undef, p)
    buffer = Vector{FC}(undef, _block_qr_buffer(FC, p))
    stats = Krylov.SimpleStats(0, false, false, false, 0, T[], T[], T[], 0.0, 0.0, "unknown")
    workspace = Krylov.BlockMinresWorkspace{T,FC,SV,SM}(
        mA, nA, p, ΔX, X, P, Q, C, D, Φ, Ψₖ, Ωₖ, Ψₖ₊₁, Πₖ₋₂, Γbarₖ₋₁, Γₖ₋₁,
        Λbarₖ, Λₖ, Vₖ₋₁, Vₖ, wₖ₋₂, wₖ₋₁, wₖ, Hₖ₋₂, Hₖ₋₁, τₖ₋₂, τₖ₋₁,
        buffer, false, stats)
    workspace.stats.allocation_timer = start_allocation_time |> Krylov.ktimer
    return workspace
end

function Krylov.solution(workspace::Krylov.BlockGmresWorkspace{T,FC,SV,SM}) where {T,FC,SV,SM<:BlockKrylovMatrix}
    return _block_result(workspace.X)
end
function Krylov.solution(workspace::Krylov.BlockMinresWorkspace{T,FC,SV,SM}) where {T,FC,SV,SM<:BlockKrylovMatrix}
    return _block_result(workspace.X)
end
function Krylov.results(workspace::Krylov.BlockGmresWorkspace{T,FC,SV,SM}) where {T,FC,SV,SM<:BlockKrylovMatrix}
    return (_block_result(workspace.X), workspace.stats)
end
function Krylov.results(workspace::Krylov.BlockMinresWorkspace{T,FC,SV,SM}) where {T,FC,SV,SM<:BlockKrylovMatrix}
    return (_block_result(workspace.X), workspace.stats)
end

Dagger.krylov_solve(method::Symbol, A, B::DMatrix; kwargs...) =
    Krylov.krylov_solve(Val(method), A, B; kwargs...)

Dagger.block_gmres(A, B::DMatrix; kwargs...)  = Krylov.block_gmres(A, B; kwargs...)
Dagger.block_minres(A, B::DMatrix; kwargs...) = Krylov.block_minres(A, B; kwargs...)

end # module KrylovExt
