"""
    matmatmul!(C, transA::Char, transB::Char, A, B, alpha, beta)

A general-purpose matrix-matrix multiply, like `LinearAlgebra.generic_matmatmul!`,
but with extra functionality. May internally convert `A` and `B` to a type that
better matches `C` and provides optimal portability and, when possible,
better performance. The actual matrix multiply operation should happen in
`LinearAlgebra.generic_matmatmul!` or an equivalent call.

The following automatic conversions are performed:
- If no `LinearAlgebra.generic_matmatmul!` method is available, convert `A` and `B` to dense Array-like
- If `C` is a `DSparseMatrix`, perform the operation out-of-place and then update `C` in-place
"""
function matmatmul!(
    C,
    transA::Char,
    transB::Char,
    A,
    B,
    alpha,
    beta
)
    EC = eltype(C)
    EA = eltype(A)
    EB = eltype(B)

    TC = typeof(C)
    TA = typeof(A)
    TB = typeof(B)

    mam = LinearAlgebra.MulAddMul(alpha, beta)

    # Check if C doesn't support in-place operations (e.g. DSparseMatrix)
    # We'll get here if A and B don't have equivalent types
    if isa(C, DSparseMatrix)
        C.mat = alpha * A * B + beta * C.mat
        return C
    end

    # Check if the call will fail due to MethodError
    sig = Tuple{TC, Char, Char, TA, TB, typeof(mam)}
    if !hasmethod(LinearAlgebra.generic_matmatmul!, sig)
        # Convert to Array-like
        # FIXME: GPU support
        C_new = C
        A_new = collect(A)
        B_new = collect(B)
        alpha_new = alpha
        beta_new = beta
        # FIXME: Re-check hasmethod, and if no method, then convert and bounce C
        @goto ready
    end

    C_new = C
    A_new = A
    B_new = B
    alpha_new = alpha
    beta_new = beta

    @label ready
    mam_new = LinearAlgebra.MulAddMul(alpha_new, beta_new)
    return LinearAlgebra.generic_matmatmul!(
        C_new,
        transA,
        transB,
        A_new,
        B_new,
        mam_new
    )
end

function LinearAlgebra.generic_matmatmul!(
    C::DMatrix{T},
    transA::Char,
    transB::Char,
    A::DMatrix{T},
    B::DMatrix{T},
    _add::LinearAlgebra.MulAddMul,
) where {T}
    partC, partA, partB = _repartition_matmatmul(C, A, B, transA, transB)

    if all(in(('N', 'T', 'C')), (transA, transB))
        if (transA == 'T' || transA == 'C') && transB == 'N' && A === B
            return maybe_copy_buffered(C=>partC, A=>partA) do C, A
                return syrk_dagger!(C, transA, A, _add)
            end
        elseif transA == 'N' && (transB == 'T' || transB == 'C') && A === B
            return maybe_copy_buffered(C=>partC, A=>partA) do C, A
                return syrk_dagger!(C, transA, A, _add)
            end
        else
            return maybe_copy_buffered(C=>partC, A=>partA, B=>partB) do C, A, B
                return gemm_dagger!(C, transA, transB, A, B, _add)
            end
        end
    end

    # FIXME Add symm and hemm implementation (please note hemm will be inside symm as the case for syrk)

    return maybe_copy_buffered(C=>partC, A=>partA, B=>partB) do C, A, B
        return gemm_dagger!(C, transA, transB, A, B, _add)
    end
end
# FIXME: Mixed-precision methods
function _repartition_matmatmul(C, A, B, transA::Char, transB::Char)
    partA = A.partitioning.blocksize
    partB = B.partitioning.blocksize
    istransA = transA == 'T' || transA == 'C'
    istransB = transB == 'T' || transB == 'C'
    dimA = !istransA ? partA[1] : partA[2]
    dimB = !istransB ? partB[2] : partB[1]
    dimA_other = !istransA ? partA[2] : partA[1]
    dimB_other = !istransB ? partB[1] : partB[2]

    # If A and B rows/cols don't match, fix them
    # Uses the smallest blocking of all dimensions
    sz = minimum((partA[1], partA[2], partB[1], partB[2]))
    if dimA != dimB
        dimA = dimB = sz
        if !istransA
            partA = (sz, partA[2])
        else
            partA = (partA[1], sz)
        end
        if !istransB
            partB = (partB[1], sz)
        else
            partB = (sz, partB[2])
        end
    end
    if dimA_other != dimB_other
        dimA_other = dimB_other = sz
        if !istransA
            partA = (partA[1], sz)
        else
            partA = (sz, partA[2])
        end
        if !istransB
            partB = (sz, partB[2])
        else
            partB = (partB[1], sz)
        end
    end

    if A === B && ((!istransA && istransB) || (istransA && !istransB))
        # syrk requires A to be square blocks
        partA = (sz, sz)
        dimA = dimB = sz
    end

    # Ensure C partitioning matches A * B
    partC = (dimA, dimB)

    return Blocks(partC...), Blocks(partA...), Blocks(partB...)
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

    alpha = T(_add.alpha)
    beta = T(_add.beta)

    if Ant != Bmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt,$Bnt)"))
    end

    Dagger.spawn_datadeps() do
        for m in range(1, Cmt)
            for n in range(1, Cnt)
                if transA == 'N'
                    if transB == 'N'
                        # A: NoTrans / B: NoTrans
                        for k in range(1, Ant)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn matmatmul!(
                                InOut(Cc[m, n]),
                                transA,
                                transB,
                                In(Ac[m, k]),
                                In(Bc[k, n]),
                                alpha,
                                mzone,
                            )
                        end
                    else
                        # A: NoTrans / B: [Conj]Trans
                        for k in range(1, Ant)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn matmatmul!(
                                InOut(Cc[m, n]),
                                transA,
                                transB,
                                In(Ac[m, k]),
                                In(Bc[n, k]),
                                alpha,
                                mzone,
                            )
                        end
                    end
                else
                    if transB == 'N'
                        # A: [Conj]Trans / B: NoTrans
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn matmatmul!(
                                InOut(Cc[m, n]),
                                transA,
                                transB,
                                In(Ac[k, m]),
                                In(Bc[k, n]),
                                alpha,
                                mzone,
                            )
                        end
                    else
                        # A: [Conj]Trans / B: [Conj]Trans
                        for k in range(1, Amt)
                            mzone = k == 1 ? beta : T(1.0)
                            Dagger.@spawn matmatmul!(
                                InOut(Cc[m, n]),
                                transA,
                                transB,
                                In(Ac[k, m]),
                                In(Bc[n, k]),
                                alpha,
                                mzone,
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

    alpha = T(_add.alpha)
    beta = T(_add.beta)

    uplo = 'U'
    if Ant != Cmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but C has number of blocks ($Cmt,$Cnt)"))
    end

    iscomplex = T <: Complex
    transs = iscomplex ? 'C' : 'T'
    anti_transs = trans == 'N' ? transs : 'N'

    Dagger.spawn_datadeps() do
        for n in range(1, Cnt)
            if trans == 'N'
                # NoTrans
                for k in range(1, Ant)
                    mzone = k == 1 ? real(beta) : one(real(T))
                    _alpha = iscomplex ? real(alpha) : alpha
                    Dagger.@spawn matmatmul!(
                        InOut(Cc[n, n]),
                        trans,
                        anti_transs,
                        In(Ac[n, k]),
                        In(Ac[n, k]),
                        _alpha,
                        mzone,
                    )
                end
                # NoTrans / Upper
                for m in range(n + 1, Cmt)
                    for k in range(1, Ant)
                        mzone = k == 1 ? beta : one(T)
                        Dagger.@spawn matmatmul!(
                            InOut(Cc[n, m]),
                            trans,
                            transs,
                            In(Ac[n, k]),
                            In(Ac[m, k]),
                            alpha,
                            mzone,
                        )
                    end
                end
            else
                # [Conj]Trans
                for k in range(1, Amt)
                    mzone = k == 1 ? real(beta) : one(real(T))
                    _alpha = iscomplex ? real(alpha) : alpha
                    _trans = iscomplex ? transs : trans
                    Dagger.@spawn matmatmul!(
                        InOut(Cc[n, n]),
                        _trans,
                        anti_transs,
                        In(Ac[k, n]),
                        In(Ac[k, n]),
                        _alpha,
                        mzone,
                    )
                end
                # [Conj]Trans / Upper
                for m in range(n + 1, Cmt)
                    for k in range(1, Amt)
                        mzone = k == 1 ? beta : one(T)
                        Dagger.@spawn matmatmul!(
                            InOut(Cc[n, m]),
                            transs,
                            'N',
                            In(Ac[k, n]),
                            In(Ac[k, m]),
                            alpha,
                            mzone,
                        )
                    end
                end
            end
        end
    end
    C = copytri!(C, uplo)
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
                    Dagger.@spawn copydiagtile!(InOut(Ac[i, j]), uplo)
                else
                    Dagger.@spawn copytile!(Out(Ac[j, i]), In(Ac[i, j]))
                end
            end
        elseif uplo == 'L'
            for i = 1:Amt, j = (i):Amt
                if (i == j)
                    Dagger.@spawn copydiagtile!(InOut(Ac[i, j]), uplo)
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

function copytile!(A, B)
    m, n = size(A)
    C = B'

    for i = 1:m, j = 1:n
        A[i, j] = C[i, j]
    end
end

function copydiagtile!(A, uplo)
    m, n = size(A)

    if uplo == 'U'
        C = UpperTriangular(A)' + UpperTriangular(A)
        C[diagind(C)] .= A[diagind(A)]
    elseif uplo == 'L'
        C = LowerTriangular(A)' + A - UpperTriangular(A)
        C[diagind(C)] .= A[diagind(A)]
    end

    for i = 1:m, j = 1:n
        A[i, j] = C[i, j]
    end
end
function LinearAlgebra.generic_matvecmul!(
    C::DVector{T},
    transA::Char,
    A::DMatrix{T},
    B::DVector{T},
    _add::LinearAlgebra.MulAddMul,
) where {T}
    partC, partA, partB = _repartition_matvecmul(C, A, B, transA)
    return maybe_copy_buffered(C=>partC, A=>partA, B=>partB) do C, A, B
        return gemv_dagger!(C, transA, A, B, _add)
    end
end
function _repartition_matvecmul(C, A, B, transA::Char)
    partA = A.partitioning.blocksize
    partB = B.partitioning.blocksize
    istransA = transA == 'T' || transA == 'C'
    dimA = !istransA ? partA[1] : partA[2]
    dimA_other = !istransA ? partA[2] : partA[1]
    dimB = partB[1]

    # If A and B rows/cols don't match, fix them
    # Uses the smallest blocking of all dimensions
    sz = minimum((partA[1], partA[2], partB[1]))
    if dimA_other != dimB
        dimA_other = dimB = sz
        if !istransA
            partA = (partA[1], sz)
        else
            partA = (sz, partA[2])
        end
    end
    partC = (dimA,)
    return Blocks(partC...), Blocks(partA...), Blocks(partB...)
end
function gemv_dagger!(
    C::DVector{T},
    transA::Char,
    A::DMatrix{T},
    B::DVector{T},
    _add::LinearAlgebra.MulAddMul,
) where {T}
    Ac = A.chunks
    Bc = B.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Bmt = size(Bc)[1]
    Cmt = size(Cc)[1]

    alpha = T(_add.alpha)
    beta = T(_add.beta)

    if Ant != Bmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt)"))
    end
    if Amt != Cmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but C has number of blocks ($Cmt)"))
    end

    Dagger.spawn_datadeps() do
        for m in range(1, Cmt)
            if transA == 'N'
                # A: NoTrans
                for k in range(1, Ant)
                    mzone = k == 1 ? beta : T(1.0)
                    Dagger.@spawn BLAS.gemv!(
                        transA,
                        alpha,
                        In(Ac[m, k]),
                        In(Bc[k]),
                        mzone,
                        InOut(Cc[m]),
                    )
                end
            else
                # A: [Conj]Trans
                for k in range(1, Amt)
                    mzone = k == 1 ? beta : T(1.0)
                    Dagger.@spawn BLAS.gemv!(
                        transA,
                        alpha,
                        In(Ac[k, m]),
                        In(Bc[k]),
                        mzone,
                        InOut(Cc[m]),
                    )
                end
            end
        end
    end

    return C
end
