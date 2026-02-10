function LinearAlgebra.norm2(A::DArray{T,N}) where {T,N}
    Ac = A.chunks
    norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac]::Array{DTask,N}
    zeroRT = zero(real(T))
    return sqrt(sum(map(norm->fetch(norm)::real(T), norms); init=zeroRT))
end
function LinearAlgebra.norm2(A::UpperTriangular{T,<:DArray{T,2}}) where T
    Ac = parent(A).chunks
    Ac_upper = []
    for i in 1:size(Ac, 1)
        append!(Ac_upper, Ac[i, (i+1):end])
    end
    upper_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_upper]
    Ac_diag = [Dagger.spawn(UpperTriangular, Ac[i,i]) for i in 1:size(Ac, 1)]
    diag_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_diag]
    upper_norms_values = map(fetch, upper_norms)
    diag_norms_values = map(fetch, diag_norms)
    zeroRT = zero(real(T))
    return sqrt(sum(upper_norms_values; init=zeroRT) + sum(diag_norms_values; init=zeroRT))
end
function LinearAlgebra.norm2(A::LowerTriangular{T,<:DArray{T,2}}) where T
    Ac = parent(A).chunks
    Ac_lower = []
    for i in 1:size(Ac, 1)
        append!(Ac_lower, Ac[(i+1):end, i])
    end
    lower_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_lower]
    Ac_diag = [Dagger.spawn(LowerTriangular, Ac[i,i]) for i in 1:size(Ac, 1)]
    diag_norms = [Dagger.@spawn mapreduce(LinearAlgebra.norm_sqr, +, chunk) for chunk in Ac_diag]
    lower_norms_values = map(fetch, lower_norms)
    diag_norms_values = map(fetch, diag_norms)
    zeroRT = zero(real(T))
    return sqrt(sum(lower_norms_values; init=zeroRT) + sum(diag_norms_values; init=zeroRT))
end

is_cross_symmetric(A1, A2) = A1 == A2'
function LinearAlgebra.issymmetric(A::DArray{T,2}) where T
    Ac = A.chunks
    if size(Ac, 1) != size(Ac, 2)
        return false
    end

    to_check = [Dagger.@spawn issymmetric(Ac[i, i]) for i in 1:size(Ac, 1)]
    for i in 2:(size(Ac, 1)-1)
        j_pre_diag = i - 1
        for j in 1:j_pre_diag
            push!(to_check, Dagger.@spawn is_cross_symmetric(Ac[i, j], Ac[j, i]))
        end
    end

    return all(fetch, to_check)
end
function LinearAlgebra.ishermitian(A::DArray{T,2}) where T
    Ac = A.chunks
    if size(Ac, 1) != size(Ac, 2)
        return false
    end

    to_check = [Dagger.@spawn ishermitian(Ac[i, i]) for i in 1:size(Ac, 1)]
    for i in 2:(size(Ac, 1)-1)
        j_pre_diag = i - 1
        for j in 1:j_pre_diag
            push!(to_check, Dagger.@spawn is_cross_symmetric(Ac[i, j], Ac[j, i]))
        end
    end

    return all(fetch, to_check)
end

function LinearAlgebra.LAPACK.chkfinite(A::DArray)
    Ac = A.chunks
    chunk_finite = [Ref(true) for _ in Ac]
    chkfinite!(finite, A) = finite[] = LinearAlgebra.LAPACK.chkfinite(A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(Ac)
            Dagger.@spawn chkfinite!(Out(chunk_finite[idx]), In(Ac[idx]))
        end
    end
    return all(getindex, chunk_finite)
end

DMatrix{T}(::LinearAlgebra.UniformScaling, m::Int, n::Int, IBlocks::Blocks) where T = DMatrix(Matrix{T}(I, m, n), IBlocks)

DMatrix{T}(::LinearAlgebra.UniformScaling, size::Tuple, IBlocks::Blocks) where T = DMatrix(Matrix{T}(I, size), IBlocks)

function LinearAlgebra.inv(F::LU{T,<:DMatrix}) where T 
    n = size(F, 1)
    dest = DMatrix{T}(I, n, n, F.factors.partitioning)
    LinearAlgebra.ldiv!(F, dest)
    return dest
end

function LinearAlgebra.inv(A::LowerTriangular{T,<:DMatrix}) where T
    S = typeof(LinearAlgebra.inv(oneunit(T)))
    dest = DMatrix{S}(I, size(A), A.data.partitioning)
    LinearAlgebra.ldiv!(convert(AbstractArray{S}, A), dest)
    dest = LowerTriangular(dest)
    return dest
end

function LinearAlgebra.inv(A::UpperTriangular{T,<:DMatrix}) where T
    S = typeof(LinearAlgebra.inv(oneunit(T)))
    dest = DMatrix{S}(I, size(A), A.data.partitioning)
    LinearAlgebra.ldiv!(convert(AbstractArray{S}, A), dest)
    dest = UpperTriangular(dest)
    return dest
end

function LinearAlgebra.inv(A::UnitLowerTriangular{T,<:DMatrix}) where T
    S = typeof(LinearAlgebra.inv(oneunit(T)))
    dest = DMatrix{S}(I, size(A), A.data.partitioning)
    LinearAlgebra.ldiv!(convert(AbstractArray{S}, A), dest)
    dest = UnitLowerTriangular(dest)
    return dest
end

function LinearAlgebra.inv(A::UnitUpperTriangular{T,<:DMatrix}) where T
    S = typeof(LinearAlgebra.inv(oneunit(T)))
    dest = DMatrix{S}(I, size(A), A.data.partitioning)
    LinearAlgebra.ldiv!(convert(AbstractArray{S}, A), dest)
    dest = UnitUpperTriangular(dest)
    return dest
end

function LinearAlgebra.inv(A::DMatrix{T}) where T
    n = LinearAlgebra.checksquare(A)
    S = typeof(zero(T)/one(T))      # dimensionful
    S0 = typeof(zero(T)/oneunit(T)) # dimensionless
    dest = DMatrix{S0}(I, n, n, A.partitioning)
    F = factorize(convert(AbstractMatrix{S}, A))
    LinearAlgebra.ldiv!(F, dest)
    return dest
end


function LinearAlgebra.ldiv!(A::LU{<:Any,<:DMatrix}, B::AbstractVecOrMat)
    allowscalar(true) do
        LinearAlgebra._apply_ipiv_rows!(A, B)
    end
    LinearAlgebra.ldiv!(UnitLowerTriangular(A.factors), B)
    LinearAlgebra.ldiv!(UpperTriangular(A.factors), B)
end

function LinearAlgebra.ldiv!(A::Union{LowerTriangular{<:Any,<:DMatrix},UnitLowerTriangular{<:Any,<:DMatrix},UpperTriangular{<:Any,<:DMatrix},UnitUpperTriangular{<:Any,<:DMatrix}}, B::AbstractVecOrMat)
    alpha = one(eltype(A))
    trans = 'N'
    diag = isa(A, UnitUpperTriangular) || isa(A, UnitLowerTriangular) ? 'U' : 'N'

    if isa(A, UpperTriangular) || isa(A, UnitUpperTriangular)
        uplo = 'U'
    elseif isa(A, LowerTriangular) || isa(A, UnitLowerTriangular)
        uplo = 'L'
    end

    dB = B isa DVecOrMat ? B : (B isa AbstractMatrix ? view(B, A.data.partitioning) : view(B, AutoBlocks()))

    parent_A = parent(A)
    if isa(B, AbstractVector)
        min_bsa = min(min(parent_A.partitioning.blocksize...), dB.partitioning.blocksize[1])
        Dagger.maybe_copy_buffered(parent_A => Blocks(min_bsa, min_bsa), dB=>Blocks(min_bsa)) do parent_A, dB
            Dagger.trsv!(uplo, trans, diag, alpha, parent_A, dB)
        end
    elseif isa(B, AbstractMatrix)
        min_bsa = min(parent_A.partitioning.blocksize...)
        Dagger.maybe_copy_buffered(parent_A => Blocks(min_bsa, min_bsa), dB=>Blocks(min_bsa, min_bsa)) do parent_A, dB
            Dagger.trsm!('L', uplo, trans, diag, alpha, parent_A, dB)
        end
    end
end

function LinearAlgebra.ldiv!(Y::DArray, A::DMatrix, B::DArray)
    LinearAlgebra.ldiv!(A, copyto!(Y, B))
end

function LinearAlgebra.ldiv!(A::DMatrix, B::DArray)
    LinearAlgebra.ldiv!(LinearAlgebra.lu(A), B)
end

function LinearAlgebra.ldiv!(C::DVecOrMat, A::Union{LowerTriangular{<:Any,<:DMatrix},UnitLowerTriangular{<:Any,<:DMatrix},UpperTriangular{<:Any,<:DMatrix},UnitUpperTriangular{<:Any,<:DMatrix}}, B::DVecOrMat)
    LinearAlgebra.ldiv!(A, copyto!(C, B))
end

function LinearAlgebra.ldiv!(C::Cholesky{T,<:DMatrix}, B::DVecOrMat) where T
    # Solve directly with C.factors and the trans parameter to avoid
    # C.L / C.U which use copy(adjoint(factors)) — that creates a DMatrix
    # with inconsistent block metadata vs chunk layout, breaking darray_copyto!.
    factors = C.factors
    alpha = one(T)
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'  # conjugate transpose for complex, plain transpose for real

    parent_A = factors
    dB = B isa DVecOrMat ? B : (B isa AbstractMatrix ? view(B, factors.partitioning) : view(B, AutoBlocks()))
    min_bsa = min(parent_A.partitioning.blocksize...)

    if C.uplo == 'U'
        # A = U'U → solve U'y = B, then Ux = y
        maybe_copy_buffered(parent_A => Blocks(min_bsa, min_bsa), dB => Blocks(min_bsa, min_bsa)) do pA, pB
            Dagger.trsm!('L', 'U', trans, 'N', alpha, pA, pB)
        end
        maybe_copy_buffered(parent_A => Blocks(min_bsa, min_bsa), dB => Blocks(min_bsa, min_bsa)) do pA, pB
            Dagger.trsm!('L', 'U', 'N', 'N', alpha, pA, pB)
        end
    else
        # A = LL' → solve Ly = B, then L'x = y
        maybe_copy_buffered(parent_A => Blocks(min_bsa, min_bsa), dB => Blocks(min_bsa, min_bsa)) do pA, pB
            Dagger.trsm!('L', 'L', 'N', 'N', alpha, pA, pB)
        end
        maybe_copy_buffered(parent_A => Blocks(min_bsa, min_bsa), dB => Blocks(min_bsa, min_bsa)) do pA, pB
            Dagger.trsm!('L', 'L', trans, 'N', alpha, pA, pB)
        end
    end

    return B
end