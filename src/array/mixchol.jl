function mixedtrsm!(side, uplo, trans, diag, alpha, A, B, StoragePrecision)
    T = StoragePrecision
    if typeof(B) != Matrix{T}
        println("B is not of type $T but of type $(typeof(B))")
        if typeof(A) != Matrix{T}
            Acopy = convert(Matrix{T}, A)
        else
            Acopy = A
        end
        Bcopy = convert(Matrix{T}, B)
        BLAS.trsm!(side, uplo, trans, diag, T(alpha), Acopy, Bcopy)
    end
    BLAS.trsm!(side, uplo, trans, diag, alpha, A, B)
end
function mixedgemm!(transa, transb, alpha, A, B, beta, C, StoragePrecision)
    T = StoragePrecision
    if typeof(C) != Matrix{T}
        if typeof(A) != Matrix{T}
            Acopy = convert(Matrix{T}, A)
        else
            Acopy = A
        end
        if typeof(B) != Matrix{T}
            Bcopy = convert(Matrix{T}, B)
        else
            Bcopy = B
        end
        Ccopy = convert(Matrix{T}, C)
        BLAS.gemm!(transa, transb, T(alpha), Acopy, Bcopy, T(beta), Ccopy)
    end
    BLAS.gemm!(transa, transb, alpha, A, B, beta, C)
end
function mixedsyrk!(uplo, trans, alpha, A, beta, C, StoragePrecision)
    T = StoragePrecision
    if typeof(C) != Matrix{T}
        if typeof(A) != Matrix{T}
            Acopy = convert(Matrix{T}, A)
        else
            Acopy = A
        end
        Ccopy = convert(Matrix{T}, C)
        BLAS.syrk!(uplo, trans, T(alpha), Acopy, T(beta), Ccopy)
    end
    BLAS.syrk!(uplo, trans, alpha, A, beta, C)
end
function mixedherk!(uplo, trans, alpha, A, beta, C, StoragePrecision)
    T = StoragePrecision
    if typeof(C) != Matrix{T}
        if typeof(A) != Matrix{T}
            Acopy = convert(Matrix{T}, A)
        else
            Acopy = A
        end
        Ccopy = convert(Matrix{T}, C)
        BLAS.herk!(uplo, trans, T(alpha), Acopy, T(beta), Ccopy)
    end
    BLAS.herk!(uplo, trans, alpha, A, beta, C)
end
function MixedPrecisionChol!(A::DArray{T,2}, ::Type{LowerTriangular}, MP::Matrix{DataType}) where T
    LinearAlgebra.checksquare(A)

    zone = one(T)
    mzone = -one(T)
    rzone = one(real(T))
    rmzone = -one(real(T))
    uplo = 'L'
    Ac = A.chunks
    mt, nt = size(Ac)
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'


    info = [convert(LinearAlgebra.BlasInt, 0)]
    try
        Dagger.spawn_datadeps() do
            for k in range(1, mt)
                Dagger.@spawn potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))
                for m in range(k+1, mt)
                    Dagger.@spawn mixedtrsm!('R', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[m, k]), MP[m,k])
                end
                for n in range(k+1, nt)
                    if iscomplex
                        Dagger.@spawn mixedherk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]), MP[n,n])
                    else
                        Dagger.@spawn mixedsyrk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]), MP[n,n])
                    end
                    for m in range(n+1, mt)
                        Dagger.@spawn mixedgemm!('N', trans, mzone, In(Ac[m, k]), In(Ac[n, k]), zone, InOut(Ac[m, n]), MP[m,n])
                    end
                end
            end
        end
    catch err
        err isa ThunkFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    return LowerTriangular(A), info[1]
end

function MixedPrecisionChol!(A::DArray{T,2}, ::Type{UpperTriangular}, MP::Matrix{DataType}) where T
    LinearAlgebra.checksquare(A)

    zone = one(T)
    mzone = -one(T)
    rzone = one(real(T))
    rmzone = -one(real(T))
    uplo = 'U'
    Ac = A.chunks
    mt, nt = size(Ac)
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'

    info = [convert(LinearAlgebra.BlasInt, 0)]
    try
        Dagger.spawn_datadeps() do
            for k in range(1, mt)
                Dagger.@spawn potrf_checked!(uplo, InOut(Ac[k, k]), Out(info))
                for n in range(k+1, nt)
                    Dagger.@spawn mixedtrsm!('L', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[k, n]), MP[k,n])
                end
                for m in range(k+1, mt)
                    if iscomplex
                        Dagger.@spawn mixedherk!(uplo, 'C', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                    else
                        Dagger.@spawn mixedherk!(uplo, 'T', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                    end
                    for n in range(m+1, nt)
                        Dagger.@spawn mixedgemm!(trans, 'N', mzone, In(Ac[k, m]), In(Ac[k, n]), zone, InOut(Ac[m, n]))
                    end
                end
            end
        end
    catch err
        err isa ThunkFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    return UpperTriangular(A), info[1]
end