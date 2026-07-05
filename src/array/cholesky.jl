LinearAlgebra.cholcopy(A::DArray{T,2}) where T = copy(A)

# The direct (non-autotuned) implementation, reached via Base's generic
# `cholesky!(A::AbstractMatrix, ::NoPivot; check)` -> `cholesky!(Hermitian(A), ...)`
# -> `_chol!` machinery. `invoke`-ing straight into the `AbstractMatrix` method
# skips our own (more specific) guarded method below, so this is safe to call
# from Autotune's raw-impl hook (`:dagger_cholesky`) without recursing.
_dagger_cholesky!(A::DArray{T,2}; check::Bool=true) where T =
    invoke(LinearAlgebra.cholesky!, Tuple{AbstractMatrix,LinearAlgebra.NoPivot}, A,
           LinearAlgebra.NoPivot(); check)

function LinearAlgebra.cholesky!(A::DArray{T,2}, ::LinearAlgebra.NoPivot=LinearAlgebra.NoPivot(); check::Bool=true) where T
    if Autotune.enabled()
        return Autotune.invoke_best(:cholesky!, A)
    end
    return _dagger_cholesky!(A; check)
end

function LinearAlgebra.cholesky(A::DArray{T,2}, ::LinearAlgebra.NoPivot=LinearAlgebra.NoPivot(); check::Bool=true) where T
    if Autotune.enabled()
        return Autotune.invoke_best(:cholesky, A)
    end
    return invoke(LinearAlgebra.cholesky, Tuple{AbstractMatrix,LinearAlgebra.NoPivot}, A,
                  LinearAlgebra.NoPivot(); check)
end

function potrf_checked!(uplo, A, info_arr)
    result = move(task_processor(), LAPACK.potrf!)(uplo, A)
    # Most LAPACK backends (OpenBLAS, CUBLAS, rocBLAS, ...) return an
    # `(A, info)` tuple, but some (e.g. oneMKL via oneAPI) return only the
    # factorized matrix and signal a non-positive-definite matrix by throwing
    # instead of returning a non-zero `info`. Handle both conventions here.
    if result isa Tuple
        _A, info = result
    else
        _A, info = result, 0
    end
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
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'

    mb, nb = A.partitioning.blocksize
    min_bs = min(mb, nb)
    info = [convert(LinearAlgebra.BlasInt, 0)]
    try
        maybe_copy_buffered(A => Blocks(min_bs, min_bs)) do A
            Ac = A.chunks
            mt, nt = size(Ac)
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
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'

    mb, nb = A.partitioning.blocksize
    min_bs = min(mb, nb)
    info = [convert(LinearAlgebra.BlasInt, 0)]
    try
        maybe_copy_buffered(A => Blocks(min_bs, min_bs)) do A
            Ac = A.chunks
            mt, nt = size(Ac)
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
        end
    catch err
        err isa DTaskFailedException || rethrow()
        err = Dagger.Sch.unwrap_nested_exception(err.ex)
        err isa PosDefException || rethrow()
    end

    return LowerTriangular(A), info[1]
end
