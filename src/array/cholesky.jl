LinearAlgebra.cholcopy(A::DArray{T,2}) where T = copy(A)
function potrf_checked!(uplo, A, info_arr)
    _A, info = move(task_processor(), LAPACK.potrf!)(uplo, A)
    if info != 0
        fill!(info_arr, info)
        throw(PosDefException(info))
    end
    return _A, info
end
function LinearAlgebra._chol!(A::DArray{T,2}, ::Type{UpperTriangular}) where T
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
                    Dagger.@spawn BLAS.trsm!('L', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[k, n]))
                end
                for m in range(k+1, mt)
                    if iscomplex
                        Dagger.@spawn BLAS.herk!(uplo, 'C', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                    else
                        Dagger.@spawn BLAS.syrk!(uplo, 'T', rmzone, In(Ac[k, m]), rzone, InOut(Ac[m, m]))
                    end
                    for n in range(m+1, nt)
                        Dagger.@spawn BLAS.gemm!(trans, 'N', mzone, In(Ac[k, m]), In(Ac[k, n]), zone, InOut(Ac[m, n]))
                    end
                end
            end
        end
    catch err
        err isa DTaskFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    return UpperTriangular(A), info[1]
end
function LinearAlgebra._chol!(A::DArray{T,2}, ::Type{LowerTriangular}) where T
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
                    Dagger.@spawn BLAS.trsm!('R', uplo, trans, 'N', zone, In(Ac[k, k]), InOut(Ac[m, k]))
                end
                for n in range(k+1, nt)
                    if iscomplex
                        Dagger.@spawn BLAS.herk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]))
                    else
                        Dagger.@spawn BLAS.syrk!(uplo, 'N', rmzone, In(Ac[n, k]), rzone, InOut(Ac[n, n]))
                    end
                    for m in range(n+1, mt)
                        Dagger.@spawn BLAS.gemm!('N', trans, mzone, In(Ac[m, k]), In(Ac[n, k]), zone, InOut(Ac[m, n]))
                    end
                end
            end
        end
    catch err
        err isa DTaskFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    return LowerTriangular(A), info[1]
end
