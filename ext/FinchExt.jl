module FinchExt

import Finch
import Dagger
import Dagger: Blocks, AutoBlocks, BlocksOrAuto, AssignmentType, DSparseMatrix

Dagger.sparse_mode(::Finch.Tensor) = :finch
Dagger._sparse_alloc(::Val{:finch}, T::Type, dims::Dims) =
    Finch.fspzeros(T, dims...)
Dagger._sparse_collect(A::Finch.Tensor) = Array(A)

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

    # Use optimized Finch operations
    Cm = C.mat
    C_out = Finch.fspzeros(eltype(Cm), size(Cm)...)
    # N.B. BEWARE: @einsum doesn't work correctly with Cm[i,j] = ... assignments.
    #Finch.@einsum Cm[i,j] = alpha * (A[i,k] * B[k,j]) + beta * Cm[i,j]
    Finch.@einsum C_out[i,j] += alpha * (A[i,k] * B[k,j])
    if beta != 0
        C_out += beta * Cm
    end
    # TODO: Remove this once we're sure @einsum works correctly
    @assert Array(C_out) ≈ (alpha * Array(A) * Array(B)) .+ (beta * Array(Cm))
    C.mat = C_out

    return C
end

function Dagger.transpose_tile(B::Finch.Tensor, uplo=nothing)
    if uplo == 'U'
        Finch.@finch begin
            C .= 0
            for i in _, j in _
                if i <= j
                    C[i, j] = B[j, i]
                end
            end
            return C
        end
        return C
    elseif uplo == 'L'
        Finch.@finch begin
            C .= 0
            for i in _, j in _
                if i >= j
                    C[i, j] = B[j, i]
                end
            end
            return C
        end
        return C
    else
        Finch.@einsum C[i,j] = B[j,i]
        return C
    end
end

end # module FinchExt