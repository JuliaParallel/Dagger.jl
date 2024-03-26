function LinearAlgebra.generic_matmatmul!(
    C::DMatrix{T},
    transA::Char,
    transB::Char,
    A::DMatrix{T},
    B::DMatrix{T},
    _add::LinearAlgebra.MulAddMul,
) where {T}
    if all(in(('N', 'T', 'C')), (transA, transB))
        if (transA == 'T' || transA == 'C') && transB == 'N' && A === B
            return syrk_dagger!(C, transA, A, _add)
        elseif transA == 'N' && (transB == 'T' || transB == 'C') && A === B
            return syrk_dagger!(C, transA, A, _add)
        else
            return gemm_dagger!(C, transA, transB, A, B, _add)
        end
    end

    # FIXME Add symm and hemm implementation (please note hemm will be inside symm as the case for syrk)

    return gemm_dagger!(C, transA, transB, A, B, _add)
end

"""
Performs one of the matrix-matrix operations

C = alpha [op( A ) * op( B )] + beta C,

where op( X ) is one of

op( X ) = X  or op( X ) = X' or op( X ) = g( X' )

alpha and beta are scalars, and A, B and C  are matrices, with op( A )
an m by k matrix, op( B ) a k by n matrix and C an m by n matrix.
"""
function gemm_dagger!(
    C::DMatrix{T},
    transA::Char,
    transB::Char,
    A::DMatrix{T},
    B::DMatrix{T},
    _add::LinearAlgebra.MulAddMul,
) where {T}
    Ac = A.chunks
    Bc = B.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Bmt, Bnt = size(Bc)
    Cmt, Cnt = size(Cc)

    alpha = _add.alpha
    beta = _add.beta
    #=
    if Ant != Bmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt,$Bnt)"))
    end
    =#

    Dagger.spawn_datadeps() do
        for m in range(1, Cmt)
            for n in range(1, Cnt)
                if transA == 'N'
                    if transB == 'N'
                        # A: NoTrans / B: NoTrans
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn BLAS.gemm!(
                                transA,
                                transB,
                                alpha,
                                In(Ac[m, k]),
                                In(Bc[k, n]),
                                mzone,
                                InOut(Cc[m, n]),
                            )
                        end
                    else
                        # A: NoTrans / B: [Conj]Trans
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn BLAS.gemm!(
                                transA,
                                transB,
                                alpha,
                                In(Ac[m, k]),
                                In(Bc[n, k]),
                                mzone,
                                InOut(Cc[m, n]),
                            )
                        end
                    end
                else
                    if transB == 'N'
                        # A: [Conj]Trans / B: NoTrans
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn BLAS.gemm!(
                                transA,
                                transB,
                                alpha,
                                In(Ac[k, m]),
                                In(Bc[k, n]),
                                mzone,
                                InOut(Cc[m, n]),
                            )
                        end
                    else
                        # A: [Conj]Trans / B: [Conj]Trans
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn BLAS.gemm!(
                                transA,
                                transB,
                                alpha,
                                In(Ac[k, m]),
                                In(Bc[n, k]),
                                mzone,
                                InOut(Cc[m, n]),
                            )
                        end
                    end
                end
            end
        end
    end

    return C
end

"""
Performs one of the symmetric/hermitian rank k operations

 C = alpha [ op( A ) * g( op( A )' )] + beta C,

where op( X ) is one of

 op( X ) = X  or op( X ) = g( X' )

where alpha and beta are real scalars, C is an n-by-n symmetric/hermitian
matrix and A is an n-by-k matrix in the first case and a k-by-n
matrix in the second case.
"""
function syrk_dagger!(
    C::DMatrix{T},
    trans::Char,
    A::DMatrix{T},
    _add::LinearAlgebra.MulAddMul,
) where {T}

    Ac = A.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Cmt, Cnt = size(Cc)

    alpha = _add.alpha
    beta = _add.beta

    uplo = 'L'
    #=
    if Ant != Bmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt,$Bnt)"))
    end
    =#

    iscomplex = T <: Complex
    transs = iscomplex ? 'C' : 'T'

    Dagger.spawn_datadeps() do
        for n in range(1, Cnt)
            if trans == 'N'
                # NoTrans
                for k in range(1, Ant)
                    mzone = k == 1 ? real(beta) : one(real(T))
                    if iscomplex
                        Dagger.@spawn BLAS.herk!(
                            uplo,
                            trans,
                            real(alpha),
                            In(Ac[n, k]),
                            mzone,
                            InOut(Cc[n, n]),
                        )
                    else
                        Dagger.@spawn BLAS.syrk!(
                            uplo,
                            trans,
                            alpha,
                            In(Ac[n, k]),
                            mzone,
                            InOut(Cc[n, n]),
                        )
                    end
                end
                if uplo == 'L'
                    # NoTrans / Lower
                    for m in range(n + 1, Cmt)
                        for k in range(1, Ant)
                            mzone = k == 1 ? beta : one(T)
                            Dagger.@spawn BLAS.gemm!(
                                trans,
                                transs,
                                alpha,
                                In(Ac[m, k]),
                                In(Ac[n, k]),
                                mzone,
                                InOut(Cc[m, n]),
                            )
                        end
                    end
                else
                    # NoTrans / Upper
                    for m in range(n + 1, Cmt)
                        for k in range(1, Ant)
                            mzone = k == 1 ? beta : one(T)
                            Dagger.@spawn BLAS.gemm!(
                                trans,
                                transs,
                                alpha,
                                In(Ac[n, k]),
                                In(Ac[m, k]),
                                mzone,
                                InOut(Cc[n, m]),
                            )
                        end
                    end
                end
            else
                # [Conj]Trans
                for k in range(1, Amt)
                    mzone = k == 1 ? real(beta) : one(real(T))
                    if iscomplex
                        Dagger.@spawn BLAS.herk!(
                            uplo,
                            transs,
                            real(alpha),
                            In(Ac[k, n]),
                            mzone,
                            InOut(Cc[n, n]),
                        )
                    else
                        Dagger.@spawn BLAS.syrk!(
                            uplo,
                            trans,
                            alpha,
                            In(Ac[k, n]),
                            mzone,
                            InOut(Cc[n, n]),
                        )
                    end
                end
                if uplo == 'L'
                    # [Conj]Trans / Lower
                    for m in range(n + 1, Cmt)
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : one(T)
                            Dagger.@spawn BLAS.gemm!(
                                transs,
                                'N',
                                alpha,
                                In(Ac[k, m]),
                                In(Ac[k, n]),
                                mzone,
                                InOut(Cc[m, n]),
                            )
                        end
                    end
                else
                    # [Conj]Trans / Upper
                    for m in range(n + 1, Cmt)
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : one(T)
                            Dagger.@spawn BLAS.gemm!(
                                transs,
                                'N',
                                alpha,
                                In(Ac[k, n]),
                                In(Ac[k, m]),
                                mzone,
                                InOut(Cc[n, m]),
                            )
                        end
                    end
                end
            end
        end
    end

    C = copytri!(C, 'L')
    return C
end


# copy transposed(adjoint) of upper(lower) side-diagonals.
@inline function copytri!(A::DArray{T,2}, uplo::AbstractChar) where {T}
    #n = checksquare(A) FIXME find replacement in DArray

    Ac = A.chunks
    Amt, Ant = size(Ac)

    Dagger.spawn_datadeps() do
        if uplo == 'U'
            for i = 1:Amt, j = (i):Amt
                if (i == j)
                    Dagger.@spawn copydiagtile!(Out(Ac[j, i]), In(Ac[i, j]), uplo)
                else
                    Dagger.@spawn copytile!(Out(Ac[j, i]), In(Ac[i, j]))
                end
            end
        elseif uplo == 'L'
            for i = 1:Amt, j = (i):Amt
                if (i == j)
                    Dagger.@spawn copydiagtile!(Out(Ac[i, j]), In(Ac[j, i]), uplo)
                else
                    Dagger.@spawn copytile!(Out(Ac[i, j]), In(Ac[j, i]))
                end

            end
        else
            throw(ArgumentError(lazy"uplo argument must be 'U' (upper) or 'L' (lower), got $uplo"))
        end
    end

    return A
end

@inline function copytile!(A, B)
    m, n = size(A)

    for i = 1:m, j = 1:n
        A[j, i] = B[i, j]
    end
end

@inline function copydiagtile!(A, B, uplo)
    m, n = size(A)

    if uplo == 'U'
        for i = 1:m, j = 1:n
            if j >= i
                A[j, i] = B[i, j]
            end
        end
    elseif uplo == 'L'
        for i = 1:m, j = 1:n
            if j <= i
                A[j, i] = B[i, j]
            end
        end
    end
end
