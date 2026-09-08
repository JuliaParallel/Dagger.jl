# Nested block operator and field-split (block-Jacobi-on-fields) PC.
#
# `[A11 A12; A21 A22]` of `DMatrix`s already means *concatenate the tiles*
# (`Base.cat` on `ArrayOp`). That is an assembled nest, not PETSc `MatNest`.
# BlockArrays.jl `mortar` is the ecosystem API for block arrays of arrays, but
# it is not a Dagger dependency and does not know about matrix-free blocks, so
# the operator is a small dedicated type. `mul!` applies the blocks; the
# matching PC is field-split Jacobi (`BlockDiagonalPC`), i.e. PCFIELDSPLIT
# with the additive composition. A true Schur complement is a follow-up.

"""
    BlockOperator(A11, A12, A21, A22)
    BlockOperator((A11, A12, A21, A22))
    BlockOperator(blocks::AbstractMatrix; row_sizes, col_sizes)

Nested block operator `A = [A₁₁ A₁₂; A₂₁ A₂₂]` whose blocks are `DMatrix`s
or any matrix-free object with `mul!(y, Aᵢⱼ, x)` over `DVector`s. This is
the distributed analogue of PETSc `MatNest`.

`[A11 A12; A21 A22]` of `DMatrix`s already *concatenates* tiles into one
bigger `DMatrix` via `cat`, so a nested operator cannot reuse `hvcat`.
Pass the four blocks (or a `Matrix{Any}` built without `hvcat`) instead.

Zero blocks are `nothing` (or a zero `UniformScaling` / `0`).
`UniformScaling` (`I`, `λ*I`) scales the matching field. Field sizes are
inferred from the blocks that have `size`; pass `row_sizes` / `col_sizes`
when every block in a field is matrix-free without `size`.

```julia
A = Dagger.BlockOperator(A11, A12, A21, A22)
x, stats = Krylov.gmres(A, b; M = Dagger.BlockDiagonalPC((P1, P2)))
```
"""
struct BlockOperator{B}
    blocks::B
    row_sizes::Vector{Int}
    col_sizes::Vector{Int}
end

function BlockOperator(A11, A12, A21, A22; row_sizes=nothing, col_sizes=nothing)
    # Do *not* write `Any[A11 A12; A21 A22]`: DMatrix <: AbstractArray, so
    # hvcat concatenates tiles into one DMatrix{Any} (see AGENTS.md lesson 28).
    blocks = Matrix{Any}(undef, 2, 2)
    blocks[1, 1] = A11
    blocks[1, 2] = A12
    blocks[2, 1] = A21
    blocks[2, 2] = A22
    return BlockOperator(blocks; row_sizes, col_sizes)
end

function BlockOperator(blocks::NTuple{4,Any}; row_sizes=nothing, col_sizes=nothing)
    return BlockOperator(blocks...; row_sizes, col_sizes)
end

function BlockOperator(blocks::AbstractMatrix; row_sizes=nothing, col_sizes=nothing)
    nr, nc = size(blocks)
    nr >= 1 && nc >= 1 || throw(ArgumentError("BlockOperator needs a non-empty block grid"))
    rs = row_sizes === nothing ? [_infer_field_dim(blocks, i, :row) for i in 1:nr] :
                                collect(Int, row_sizes)
    cs = col_sizes === nothing ? [_infer_field_dim(blocks, j, :col) for j in 1:nc] :
                                collect(Int, col_sizes)
    length(rs) == nr || throw(ArgumentError("row_sizes has $(length(rs)) entries for $nr block rows"))
    length(cs) == nc || throw(ArgumentError("col_sizes has $(length(cs)) entries for $nc block columns"))
    _validate_block_sizes(blocks, rs, cs)
    return BlockOperator{typeof(blocks)}(blocks, rs, cs)
end

_is_zero_block(::Nothing) = true
_is_zero_block(::Missing) = true
_is_zero_block(A::LinearAlgebra.UniformScaling) = iszero(A.λ)
_is_zero_block(x::Number) = iszero(x)
_is_zero_block(_) = false

function _try_block_size(A, dim::Int)
    _is_zero_block(A) && return nothing
    A isa LinearAlgebra.UniformScaling && return nothing
    A isa Number && return nothing
    try
        return Int(size(A, dim))
    catch
        return nothing
    end
end

function _infer_field_dim(blocks, idx::Int, which::Symbol)
    if which === :row
        for j in axes(blocks, 2)
            s = _try_block_size(blocks[idx, j], 1)
            s !== nothing && return s
        end
        throw(ArgumentError("cannot infer row size of field $idx (no block with size)"))
    else
        for i in axes(blocks, 1)
            s = _try_block_size(blocks[i, idx], 2)
            s !== nothing && return s
        end
        throw(ArgumentError("cannot infer column size of field $idx (no block with size)"))
    end
end

function _validate_block_sizes(blocks, row_sizes, col_sizes)
    for i in axes(blocks, 1), j in axes(blocks, 2)
        B = blocks[i, j]
        s1 = _try_block_size(B, 1)
        s2 = _try_block_size(B, 2)
        if s1 !== nothing && s1 != row_sizes[i]
            throw(DimensionMismatch(
                "block ($i,$j) has $(s1) rows but field row size is $(row_sizes[i])"))
        end
        if s2 !== nothing && s2 != col_sizes[j]
            throw(DimensionMismatch(
                "block ($i,$j) has $(s2) columns but field column size is $(col_sizes[j])"))
        end
    end
    return nothing
end

Base.size(A::BlockOperator) = (sum(A.row_sizes), sum(A.col_sizes))
Base.size(A::BlockOperator, d::Integer) =
    d == 1 ? sum(A.row_sizes) : d == 2 ? sum(A.col_sizes) : 1

function Base.eltype(A::BlockOperator)
    T = Union{}
    for B in A.blocks
        _is_zero_block(B) && continue
        T = promote_type(T, _block_eltype(B))
    end
    return T === Union{} ? Float64 : T
end
_block_eltype(B) = eltype(B)
_block_eltype(::LinearAlgebra.UniformScaling{T}) where {T} = T
_block_eltype(::T) where {T<:Number} = T

Base.adjoint(A::BlockOperator) = Adjoint(A)
Base.transpose(A::BlockOperator) = Transpose(A)

function _field_span(sizes, i::Int)
    start = 1
    @inbounds for k in 1:i-1
        start += sizes[k]
    end
    return start:(start + sizes[i] - 1)
end

# Range getindex / setindex already copy through a view; field sizes need not
# be tile-aligned. The extra copy is one per field per product, which is the
# price of not assembling the nest.
_field_vector(x::DVector, sizes, i::Int) = x[_field_span(sizes, i)]

function _scatter_field!(y::DVector, yi, sizes, i::Int)
    y[_field_span(sizes, i)] = yi
    return y
end

function _apply_block!(y::DVector, A, x::DVector)
    return LinearAlgebra.mul!(y, A, x)
end
function _apply_block!(y::DVector, A::LinearAlgebra.UniformScaling, x::DVector)
    length(y) == length(x) || throw(DimensionMismatch(
        "UniformScaling block needs matching field lengths, got $(length(y)) and $(length(x))"))
    copyto!(y, x)
    isone(A.λ) || LinearAlgebra.rmul!(y, A.λ)
    return y
end
_apply_block!(y::DVector, λ::Number, x::DVector) = _apply_block!(y, λ * I, x)

function _apply_block_adj!(y::DVector, A, x::DVector)
    return LinearAlgebra.mul!(y, A', x)
end
_apply_block_adj!(y::DVector, A::LinearAlgebra.UniformScaling, x::DVector) =
    _apply_block!(y, A', x)
_apply_block_adj!(y::DVector, λ::Number, x::DVector) =
    _apply_block!(y, conj(λ) * I, x)

function _apply_block_transpose!(y::DVector, A, x::DVector)
    return LinearAlgebra.mul!(y, transpose(A), x)
end
_apply_block_transpose!(y::DVector, A::LinearAlgebra.UniformScaling, x::DVector) =
    _apply_block!(y, transpose(A), x)
_apply_block_transpose!(y::DVector, λ::Number, x::DVector) =
    _apply_block!(y, λ * I, x)

function _mul_block_rows!(y::DVector, apply!, blocks, xs, row_sizes, col_idx_of_row)
    # `col_idx_of_row(i, j)` maps a (output-field, input-field) pair onto the
    # stored block. Forward: (i, j) → (i, j). Adjoint: (i, j) → (j, i).
    for i in 1:length(row_sizes)
        yi = similar(y, eltype(y), (row_sizes[i],))
        first = true
        for j in 1:length(xs)
            Bij = blocks[col_idx_of_row(i, j)...]
            _is_zero_block(Bij) && continue
            if first
                apply!(yi, Bij, xs[j])
                first = false
            else
                tmp = similar(yi)
                apply!(tmp, Bij, xs[j])
                LinearAlgebra.axpy!(one(eltype(yi)), tmp, yi)
            end
        end
        first && fill!(yi, zero(eltype(yi)))
        _scatter_field!(y, yi, row_sizes, i)
    end
    return y
end

function LinearAlgebra.mul!(y::DVector, A::BlockOperator, x::DVector)
    n, m = size(A)
    length(y) == n || throw(DimensionMismatch(
        "BlockOperator is $(n)×$(m) but y has length $(length(y))"))
    length(x) == m || throw(DimensionMismatch(
        "BlockOperator is $(n)×$(m) but x has length $(length(x))"))
    xs = [_field_vector(x, A.col_sizes, j) for j in 1:length(A.col_sizes)]
    return _mul_block_rows!(y, _apply_block!, A.blocks, xs, A.row_sizes, (i, j) -> (i, j))
end

function LinearAlgebra.mul!(y::DVector, AdjA::Adjoint{<:Any,<:BlockOperator}, x::DVector)
    A = parent(AdjA)
    n, m = size(AdjA)
    length(y) == n || throw(DimensionMismatch(
        "Adjoint(BlockOperator) is $(n)×$(m) but y has length $(length(y))"))
    length(x) == m || throw(DimensionMismatch(
        "Adjoint(BlockOperator) is $(n)×$(m) but x has length $(length(x))"))
    xs = [_field_vector(x, A.row_sizes, j) for j in 1:length(A.row_sizes)]
    return _mul_block_rows!(y, _apply_block_adj!, A.blocks, xs, A.col_sizes, (i, j) -> (j, i))
end

function LinearAlgebra.mul!(y::DVector, TrA::Transpose{<:Any,<:BlockOperator}, x::DVector)
    A = parent(TrA)
    n, m = size(TrA)
    length(y) == n || throw(DimensionMismatch(
        "Transpose(BlockOperator) is $(n)×$(m) but y has length $(length(y))"))
    length(x) == m || throw(DimensionMismatch(
        "Transpose(BlockOperator) is $(n)×$(m) but x has length $(length(x))"))
    xs = [_field_vector(x, A.row_sizes, j) for j in 1:length(A.row_sizes)]
    return _mul_block_rows!(y, _apply_block_transpose!, A.blocks, xs, A.col_sizes, (i, j) -> (j, i))
end

function LinearAlgebra.mul!(y::DVector, A::BlockOperator, x::DVector, α::Number, β::Number)
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

Base.:*(A::BlockOperator, x::DVector) = LinearAlgebra.mul!(similar(x), A, x)

"""
    BlockDiagonalPC(pcs; sizes=nothing)

Field-split preconditioner: `M⁻¹ = blockdiag(P₁, …, Pₖ)`, each `Pᵢ` applied
to one field of a [`BlockOperator`](@ref) unknown. This is PETSc
`PCFIELDSPLIT` with the additive / Jacobi composition (a Schur complement
is a follow-up). Each `Pᵢ` is any object with `mul!(y, Pᵢ, x)` over
`DVector`s — typically a [`JacobiPreconditioner`](@ref) or
[`BlockJacobiPreconditioner`](@ref) built from the matching diagonal block.

```julia
P = Dagger.BlockDiagonalPC((
    Dagger.JacobiPreconditioner(A11),
    Dagger.JacobiPreconditioner(A22),
))
x, stats = Krylov.gmres(A, b; M = P)
```

Distinct from [`BlockJacobiPreconditioner`](@ref), which splits a *single*
operator by its tile grid rather than by named fields. Field lengths are
inferred from the `Pᵢ` when they expose `size` (or `.n` / `.dinv`); pass
`sizes` otherwise.

`BlockDiagonalPC` follows Krylov's `ldiv=false` convention: `mul!(y, P, x)`
computes `y = M⁻¹ x`.
"""
struct BlockDiagonalPC{P} <: AbstractDaggerPreconditioner
    pcs::P
    sizes::Vector{Int}
end

function BlockDiagonalPC(pcs::Tuple; sizes=nothing)
    isempty(pcs) && throw(ArgumentError("BlockDiagonalPC needs at least one field"))
    sz = sizes === nothing ? [_infer_pc_size(p) for p in pcs] : collect(Int, sizes)
    length(sz) == length(pcs) || throw(ArgumentError(
        "sizes has $(length(sz)) entries for $(length(pcs)) fields"))
    return BlockDiagonalPC{typeof(pcs)}(pcs, sz)
end
BlockDiagonalPC(pcs::AbstractVector; kwargs...) = BlockDiagonalPC(Tuple(pcs); kwargs...)

_infer_pc_size(P::JacobiPreconditioner) = length(P.dinv)
_infer_pc_size(P::AbstractBlockPreconditioner) = P.n
function _infer_pc_size(P)
    try
        return Int(size(P, 1))
    catch
        throw(ArgumentError(
            "cannot infer field size from a $(typeof(P)); pass sizes=(n1, n2, …) to BlockDiagonalPC"))
    end
end

function LinearAlgebra.mul!(y::DVector, P::BlockDiagonalPC, x::DVector)
    n = sum(P.sizes)
    length(x) == n || throw(DimensionMismatch(
        "BlockDiagonalPC acts on length $n but x has length $(length(x))"))
    length(y) == n || throw(DimensionMismatch(
        "BlockDiagonalPC acts on length $n but y has length $(length(y))"))
    for i in 1:length(P.pcs)
        xi = _field_vector(x, P.sizes, i)
        yi = similar(xi)
        LinearAlgebra.mul!(yi, P.pcs[i], xi)
        _scatter_field!(y, yi, P.sizes, i)
    end
    return y
end
