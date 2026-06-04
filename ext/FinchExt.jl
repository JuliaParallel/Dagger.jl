module FinchExt

import Finch
import LinearAlgebra
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseMatrix

Dagger.sparse_mode(::Finch.Tensor) = :finch
Dagger._sparse_alloc(::Val{:finch}, T::Type, dims::Dims) =
    Finch.fspzeros(T, dims...)
Dagger._sparse_collect(A::Finch.Tensor) = Array(A)
Dagger.maybe_wrap_tile(x::Finch.Tensor) = DSparseMatrix{eltype(x)}(x)

function Finch.fspzeros(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseMatrix{T}(Finch.fspzeros(T, _dims...)), false, d, Dagger.partition(p, d), p, assignment)
    return Dagger._to_darray(a)
end
Finch.fspzeros(p::BlocksOrAuto, T::Type, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    Finch.fspzeros(p, T, dims; assignment)
Finch.fspzeros(p::BlocksOrAuto, dims::Integer...; assignment::AssignmentType = :arbitrary) =
    Finch.fspzeros(p, Float64, dims; assignment)
Finch.fspzeros(p::BlocksOrAuto, dims::Dims; assignment::AssignmentType = :arbitrary) =
    Finch.fspzeros(p, Float64, dims; assignment)
Finch.fspzeros(::AutoBlocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    Finch.fspzeros(Dagger.auto_blocks(dims), T, dims; assignment)

function Finch.fsprand(p::Blocks, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary)
    d = Dagger.ArrayDomain(map(x->1:x, dims))
    a = Dagger.AllocateArray(T, (T, _dims) -> DSparseMatrix{T}(Finch.fsprand(T, _dims..., sparsity)), false, d, Dagger.partition(p, d), p, assignment)
    return Dagger._to_darray(a)
end
Finch.fsprand(p::BlocksOrAuto, T::Type, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    Finch.fsprand(p, T, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
Finch.fsprand(p::BlocksOrAuto, dims_and_sparsity::Real...; assignment::AssignmentType = :arbitrary) =
    Finch.fsprand(p, Float64, dims_and_sparsity[1:end-1], dims_and_sparsity[end]; assignment)
Finch.fsprand(p::BlocksOrAuto, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    Finch.fsprand(p, Float64, dims, sparsity; assignment)
Finch.fsprand(::AutoBlocks, T::Type, dims::Dims, sparsity::AbstractFloat; assignment::AssignmentType = :arbitrary) =
    Finch.fsprand(Dagger.auto_blocks(dims), T, dims, sparsity; assignment)

# Materialize a (possibly lazy/`SwizzleArray`) result into a concrete sparse
# `Tensor`. `@einsum` returns a lazy `SwizzleArray` for transposed-output
# patterns, which is not itself a `Finch.Tensor` and corrupts subsequent
# accumulation steps; copying through `@finch` produces a clean `SparseCOO`.
function _finch_materialize(X, ::Type{T}, dims) where {T}
    out = Finch.fspzeros(T, dims...)
    Finch.@finch begin
        out .= 0
        for j = _, i = _
            if X[i, j] != 0
                out[i, j] = X[i, j]
            end
        end
    end
    return out
end

# Finch tensors do not define `Base.copy`; build a fresh equivalent tensor.
Dagger._sparse_copy(mat::Finch.Tensor) = _finch_materialize(mat, eltype(mat), size(mat))

# Finch tensors do not support `setindex!`, so copy-buffering writeback into a
# Finch tile densifies, applies the (partial-range) copy, then re-sparsifies.
function Dagger._sparse_copyto_view!(mat::Finch.Tensor, Brange, src)
    dense = Array(mat)
    copyto!(view(dense, Brange), src)
    return _finch_materialize(dense, eltype(mat), size(mat))
end

# Compute `C_out += alpha * op(A) * op(B)` via Finch's `@einsum`, choosing the
# right index pattern for each transpose option. We deliberately use *explicit*
# index patterns (`A[k,i]`) rather than `Finch.swizzle`, because feeding a
# swizzled tensor into `@einsum` makes it return a lazy `SwizzleArray` (rather
# than a materialized `Tensor`), which breaks subsequent accumulation steps.
# Adjoint ('C') additionally applies `conj` inside the einsum.
function _finch_einsum_matmul!(C_out, A, B, transA::Char, transB::Char, alpha)
    aT = transA == 'T' || transA == 'C'
    aC = transA == 'C'
    bT = transB == 'T' || transB == 'C'
    bC = transB == 'C'
    if !aT && !bT
        bC ? Finch.@einsum(C_out[i,j] += alpha * (A[i,k] * conj(B[k,j]))) :
             Finch.@einsum(C_out[i,j] += alpha * (A[i,k] * B[k,j]))
    elseif !aT && bT
        bC ? Finch.@einsum(C_out[i,j] += alpha * (A[i,k] * conj(B[j,k]))) :
             Finch.@einsum(C_out[i,j] += alpha * (A[i,k] * B[j,k]))
    elseif aT && !bT
        aC ? Finch.@einsum(C_out[i,j] += alpha * (conj(A[k,i]) * B[k,j])) :
             Finch.@einsum(C_out[i,j] += alpha * (A[k,i] * B[k,j]))
    else # aT && bT
        if aC && bC
            Finch.@einsum C_out[i,j] += alpha * (conj(A[k,i]) * conj(B[j,k]))
        elseif aC
            Finch.@einsum C_out[i,j] += alpha * (conj(A[k,i]) * B[j,k])
        elseif bC
            Finch.@einsum C_out[i,j] += alpha * (A[k,i] * conj(B[j,k]))
        else
            Finch.@einsum C_out[i,j] += alpha * (A[k,i] * B[j,k])
        end
    end
    return C_out
end

function Dagger.matmatmul!(
    C::DSparseMatrix,
    transA::Char,
    transB::Char,
    A::Finch.Tensor,
    B::Finch.Tensor,
    alpha,
    beta
)
    if !isa(C.mat, Finch.Tensor)
        # Not supported here, forward to generic matmatmul!
        return Dagger.matmatmul!(C.mat, transA, transB, A, B, alpha, beta)
    end
    transA in ('N', 'T', 'C') || throw(ArgumentError("Invalid transA: $transA"))
    transB in ('N', 'T', 'C') || throw(ArgumentError("Invalid transB: $transB"))

    # `@einsum` cannot write into an existing sparse tensor in place, so we build
    # a fresh output tensor and (re)assign it to the wrapper. `DSparseMatrix`
    # hides this reallocation from Datadeps.
    #
    # N.B. BEWARE: `@einsum Cm[i,j] = ...` (assignment into an existing tensor)
    # does not work; use a fresh `C_out` with `+=`.
    Cm = C.mat
    C_out = Finch.fspzeros(eltype(Cm), size(Cm)...)
    C_out = _finch_einsum_matmul!(C_out, A, B, transA, transB, alpha)
    # `@einsum` may return a lazy `SwizzleArray`; materialize to a concrete
    # sparse `Tensor` so accumulation and later iterations stay well-typed.
    C_out = _finch_materialize(C_out, eltype(Cm), size(Cm))
    if beta != 0
        C_out = C_out + beta * Cm
    end
    C.mat = C_out

    return C
end

# Tile transpose/symmetrization used by `copytri!`. Finch tensors don't support
# in-place `setindex!`, so we densify, build the result, and re-sparsify via
# `_finch_materialize`.
# - `uplo === nothing`: off-diagonal tile, return the conjugate transpose.
# - `uplo == 'U'`/`'L'`: diagonal tile, build the full Hermitian tile from the
#   given triangle (matching the dense `copydiagtile!` semantics).
function Dagger.transpose_tile(B::Finch.Tensor, uplo=nothing)
    _B = Array(B)
    if uplo === nothing
        C = adjoint(_B)
        return _finch_materialize(C, eltype(_B), size(C))
    end
    n = LinearAlgebra.checksquare(_B)
    C = zeros(eltype(_B), n, n)
    if uplo == 'U'
        for j in 1:n, i in 1:n
            C[i, j] = i <= j ? _B[i, j] : conj(_B[j, i])
        end
    elseif uplo == 'L'
        for j in 1:n, i in 1:n
            C[i, j] = i >= j ? _B[i, j] : conj(_B[j, i])
        end
    else
        throw(ArgumentError("uplo must be 'U' or 'L', got $uplo"))
    end
    return _finch_materialize(C, eltype(_B), size(C))
end

end # module FinchExt