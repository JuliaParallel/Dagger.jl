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
# CPU dense tiles: same `BLAS.gemm!` kernel `gemm_dagger!` used before sparse
# support. The generic method below is unspecialized (and probes `hasmethod`).
# Do not widen this to `StridedMatrix` — `CuArray` is strided, and stealing
# those tiles would send GPU GEMM through host BLAS.
function matmatmul!(
    C::Matrix{T},
    transA::Char,
    transB::Char,
    A::Matrix{T},
    B::Matrix{T},
    alpha::Number,
    beta::Number,
) where T
    BLAS.gemm!(transA, transB, convert(T, alpha), A, B, convert(T, beta), C)
    return C
end

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

# Eltypes need not match: LinearAlgebra's `mul!` already promotes
# (`DMatrix{Float32} * DVector{Float64} → DVector{Float64}`). Same-type tiles
# still hit the `Matrix{T}` `BLAS.gemm!` method above; mixed tiles fall through
# to `LinearAlgebra.generic_matmatmul!` the same way host `mul!` does.
function LinearAlgebra.generic_matmatmul!(
    C::DMatrix,
    transA::Char,
    transB::Char,
    A::DMatrix,
    B::DMatrix,
    _add::LinearAlgebra.MulAddMul,
)
    return LinearAlgebra.generic_matmatmul!(C, transA, transB, A, B, _add.alpha, _add.beta)
end
function LinearAlgebra.generic_matmatmul!(
    C::DMatrix,
    transA::Char,
    transB::Char,
    A::DMatrix,
    B::DMatrix,
    alpha::Number,
    beta::Number,
)
    partC, partA, partB = _repartition_matmatmul(C, A, B, transA, transB)

    if all(in(('N', 'T', 'C')), (transA, transB))
        if (transA == 'T' || transA == 'C') && transB == 'N' && A === B
            return maybe_copy_buffered(C=>partC, A=>partA) do C, A
                return syrk_dagger!(C, transA, A, alpha, beta)
            end
        elseif transA == 'N' && (transB == 'T' || transB == 'C') && A === B
            return maybe_copy_buffered(C=>partC, A=>partA) do C, A
                return syrk_dagger!(C, transA, A, alpha, beta)
            end
        else
            return maybe_copy_buffered(C=>partC, A=>partA, B=>partB) do C, A, B
                return gemm_dagger!(C, transA, transB, A, B, alpha, beta)
            end
        end
    end

    return maybe_copy_buffered(C=>partC, A=>partA, B=>partB) do C, A, B
        return gemm_dagger!(C, transA, transB, A, B, alpha, beta)
    end
end

# Host `Matrix` / `SubArray` tiles in GEMM, matching the GEMV mix below.
# Block Krylov needs `V' * Q → p×p` (host dest) and `V * Y → n×p` (host
# `p×p` RHS) without collecting the tall `DMatrix`. Wrap the host side as a
# single-tile `DArray` view and reuse the path above. Skip the all-`DMatrix`
# method (already defined) and the all-host method (piracy).
_host_as_dmatrix(A::DArray) = A
_host_as_dmatrix(A::Matrix) = view(A, Blocks(size(A)...))
_host_as_dmatrix(A::SubArray{<:Any,2,<:Array}) = wrap_as_darray(A)

for CT in (DMatrix, Matrix, SubArray{<:Any,2,<:Array}),
    AT in (DMatrix, Matrix, SubArray{<:Any,2,<:Array}),
    BT in (DMatrix, Matrix, SubArray{<:Any,2,<:Array})
    n_d = (CT === DMatrix) + (AT === DMatrix) + (BT === DMatrix)
    (n_d == 0 || n_d == 3) && continue
    @eval function LinearAlgebra.generic_matmatmul!(
        C::$(CT),
        transA::Char,
        transB::Char,
        A::$(AT),
        B::$(BT),
        _add::LinearAlgebra.MulAddMul,
    )
        LinearAlgebra.generic_matmatmul!(
            _host_as_dmatrix(C), transA, transB,
            _host_as_dmatrix(A), _host_as_dmatrix(B),
            _add,
        )
        return C
    end
end

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
    C::DMatrix,
    transA::Char,
    transB::Char,
    A::DMatrix,
    B::DMatrix,
    _alpha,
    _beta,
)
    T = eltype(C)
    Ac = A.chunks
    Bc = B.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Bmt, Bnt = size(Bc)
    Cmt, Cnt = size(Cc)

    alpha = T(_alpha)
    beta = T(_beta)

    # The contracted ("inner") block dimension depends on whether each operand is
    # transposed: for `op(A)*op(B)`, A contributes its column-blocks when not
    # transposed and its row-blocks when transposed (and vice versa for B).
    A_inner = transA == 'N' ? Ant : Amt
    B_inner = transB == 'N' ? Bmt : Bnt
    if A_inner != B_inner
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
    C::DMatrix,
    trans::Char,
    A::DMatrix,
    _alpha,
    _beta,
)
    T = eltype(C)
    Ac = A.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Cmt, Cnt = size(Cc)

    alpha = T(_alpha)
    beta = T(_beta)

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

# Tile-local product with the stored triangle of a diagonal block. Host
# `Symmetric` / `Hermitian` `mul!` uses BLAS.symm!/hemm! on `Matrix` tiles.
function _symm_diag_mul!(C, uplo::AbstractChar, herm::Bool, side::AbstractChar, A, B, α, β)
    up = (uplo == 'U' || uplo == 'u') ? :U : :L
    AW = herm ? LinearAlgebra.Hermitian(A, up) : LinearAlgebra.Symmetric(A, up)
    if side == 'L' || side == 'l'
        LinearAlgebra.mul!(C, AW, B, α, β)
    else
        LinearAlgebra.mul!(C, B, AW, α, β)
    end
    return C
end

function _symm_diag_mv!(y, uplo::AbstractChar, herm::Bool, A, x, α, β)
    up = (uplo == 'U' || uplo == 'u') ? :U : :L
    AW = herm ? LinearAlgebra.Hermitian(A, up) : LinearAlgebra.Symmetric(A, up)
    LinearAlgebra.mul!(y, AW, x, α, β)
    return y
end

function _uplo_char(uplo)
    uplo isa AbstractChar && return Char(uppercase(uplo))
    uplo === :U && return 'U'
    uplo === :L && return 'L'
    throw(ArgumentError("uplo must be 'U'/'L' or :U/:L, got $uplo"))
end

function _side_char(side)
    side isa AbstractChar && return Char(uppercase(side))
    side === :L && return 'L'
    side === :R && return 'R'
    throw(ArgumentError("side must be 'L'/'R' or :L/:R, got $side"))
end

"""
Tiled SYMM / HEMM: `C = α * A_sym * B + β * C` (`side='L'`) or
`C = α * B * A_sym + β * C` (`side='R'`). Only the stored triangle of `A`
is read (`uplo`). Off-diagonal tiles use `matmatmul!`; the diagonal uses
`Symmetric`/`Hermitian` tile `mul!`.
"""
function _symm_dagger!(C::DMatrix, A::DMatrix, B::DMatrix, _alpha, _beta,
                       side::Char, uplo::Char, herm::Bool)
    T = eltype(C)
    Ac = A.chunks
    Bc = B.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Bmt, Bnt = size(Bc)
    Cmt, Cnt = size(Cc)
    alpha = T(_alpha)
    beta = T(_beta)
    trans = herm ? 'C' : 'T'

    Amt == Ant || throw(DimensionMismatch(
        "Symmetric/Hermitian DMatrix must have a square tile grid, got ($Amt,$Ant)"))

    if side == 'L'
        Ant == Bmt || throw(DimensionMismatch(
            "A has $(Ant) column tiles but B has $(Bmt) row tiles"))
        Amt == Cmt || throw(DimensionMismatch(
            "A has $(Amt) row tiles but C has $(Cmt) row tiles"))
        Bnt == Cnt || throw(DimensionMismatch(
            "B has $(Bnt) column tiles but C has $(Cnt) column tiles"))
        Dagger.spawn_datadeps() do
            for i in 1:Cmt, j in 1:Cnt
                for k in 1:Ant
                    βk = k == 1 ? beta : one(T)
                    if k == i
                        Dagger.@spawn _symm_diag_mul!(InOut(Cc[i, j]), uplo, herm, 'L',
                                                      In(Ac[i, i]), In(Bc[i, j]), alpha, βk)
                    elseif (uplo == 'U' && k > i) || (uplo == 'L' && k < i)
                        Dagger.@spawn matmatmul!(InOut(Cc[i, j]), 'N', 'N',
                                                 In(Ac[i, k]), In(Bc[k, j]), alpha, βk)
                    else
                        Dagger.@spawn matmatmul!(InOut(Cc[i, j]), trans, 'N',
                                                 In(Ac[k, i]), In(Bc[k, j]), alpha, βk)
                    end
                end
            end
        end
    else
        Bnt == Amt || throw(DimensionMismatch(
            "B has $(Bnt) column tiles but A has $(Amt) row tiles"))
        Bmt == Cmt || throw(DimensionMismatch(
            "B has $(Bmt) row tiles but C has $(Cmt) row tiles"))
        Ant == Cnt || throw(DimensionMismatch(
            "A has $(Ant) column tiles but C has $(Cnt) column tiles"))
        Dagger.spawn_datadeps() do
            for i in 1:Cmt, j in 1:Cnt
                for k in 1:Amt
                    βk = k == 1 ? beta : one(T)
                    if k == j
                        Dagger.@spawn _symm_diag_mul!(InOut(Cc[i, j]), uplo, herm, 'R',
                                                      In(Ac[j, j]), In(Bc[i, j]), alpha, βk)
                    elseif (uplo == 'U' && k < j) || (uplo == 'L' && k > j)
                        Dagger.@spawn matmatmul!(InOut(Cc[i, j]), 'N', 'N',
                                                 In(Bc[i, k]), In(Ac[k, j]), alpha, βk)
                    else
                        Dagger.@spawn matmatmul!(InOut(Cc[i, j]), 'N', trans,
                                                 In(Bc[i, k]), In(Ac[j, k]), alpha, βk)
                    end
                end
            end
        end
    end
    return C
end

function _symv_dagger!(y::DVector, A::DMatrix, x::DVector, _alpha, _beta,
                       uplo::Char, herm::Bool)
    T = eltype(y)
    Ac = A.chunks
    xc = x.chunks
    yc = y.chunks
    Amt, Ant = size(Ac)
    alpha = T(_alpha)
    beta = T(_beta)
    trans = herm ? 'C' : 'T'

    Amt == Ant || throw(DimensionMismatch(
        "Symmetric/Hermitian DMatrix must have a square tile grid, got ($Amt,$Ant)"))
    Ant == length(xc) || throw(DimensionMismatch(
        "A has $(Ant) column tiles but x has $(length(xc)) tiles"))
    Amt == length(yc) || throw(DimensionMismatch(
        "A has $(Amt) row tiles but y has $(length(yc)) tiles"))

    Dagger.spawn_datadeps() do
        for i in 1:Amt
            for k in 1:Ant
                βk = k == 1 ? beta : one(T)
                if k == i
                    Dagger.@spawn _symm_diag_mv!(InOut(yc[i]), uplo, herm,
                                                 In(Ac[i, i]), In(xc[i]), alpha, βk)
                elseif (uplo == 'U' && k > i) || (uplo == 'L' && k < i)
                    Dagger.@spawn matvecmul!(InOut(yc[i]), 'N',
                                             In(Ac[i, k]), In(xc[k]), alpha, βk)
                else
                    Dagger.@spawn matvecmul!(InOut(yc[i]), trans,
                                             In(Ac[k, i]), In(xc[k]), alpha, βk)
                end
            end
        end
    end
    return y
end

function _symm_mul!(C::DMatrix, A::DMatrix, B::DMatrix, α, β,
                    side::AbstractChar, uplo::AbstractChar, herm::Bool)
    sideC = _side_char(side)
    uploC = _uplo_char(uplo)
    if sideC == 'L'
        partC, partA, partB = _repartition_matmatmul(C, A, B, 'N', 'N')
        return maybe_copy_buffered(C=>partC, A=>partA, B=>partB) do C, A, B
            return _symm_dagger!(C, A, B, α, β, 'L', uploC, herm)
        end
    else
        partC, partB, partA = _repartition_matmatmul(C, B, A, 'N', 'N')
        return maybe_copy_buffered(C=>partC, B=>partB, A=>partA) do C, B, A
            return _symm_dagger!(C, A, B, α, β, 'R', uploC, herm)
        end
    end
end

function _symv_mul!(y::DVector, A::DMatrix, x::DVector, α, β,
                    uplo::AbstractChar, herm::Bool)
    uploC = _uplo_char(uplo)
    partC, partA, partB = _repartition_matvecmul(y, A, x, 'N')
    return maybe_copy_buffered(y=>partC, A=>partA, x=>partB) do y, A, x
        return _symv_dagger!(y, A, x, α, β, uploC, herm)
    end
end

# BLAS.symm! already has `AbstractMatrix{Float64}` / `Float32` methods. A
# `Number` + `DMatrix` method is ambiguous with those (more specific on the
# arrays, less specific on α/β). Match the stdlib α/β union.
for T in (Float32, Float64)
    @eval function LinearAlgebra.BLAS.symm!(side::AbstractChar, uplo::AbstractChar,
                                            α::Union{Bool,$T}, A::DMatrix{$T},
                                            B::DMatrix{$T}, β::Union{Bool,$T},
                                            C::DMatrix{$T})
        return _symm_mul!(C, A, B, α, β, side, uplo, false)
    end
end
for T in (ComplexF32, ComplexF64)
    @eval function LinearAlgebra.BLAS.hemm!(side::AbstractChar, uplo::AbstractChar,
                                            α::Union{Bool,$T}, A::DMatrix{$T},
                                            B::DMatrix{$T}, β::Union{Bool,$T},
                                            C::DMatrix{$T})
        return _symm_mul!(C, A, B, α, β, side, uplo, true)
    end
end
# `1+0im` is Complex{Int64}. A lone `Number`+`DMatrix` method is ambiguous
# with the stdlib `AbstractMatrix{T}` methods when α is already `T`; the
# typed methods above win in that case, and these only fire for other
# Numbers (then convert).
function LinearAlgebra.BLAS.symm!(side::AbstractChar, uplo::AbstractChar,
                                  α::Number, A::DMatrix{T}, B::DMatrix{T},
                                  β::Number, C::DMatrix{T}) where T<:LinearAlgebra.BlasReal
    return LinearAlgebra.BLAS.symm!(side, uplo, convert(T, α), A, B, convert(T, β), C)
end
function LinearAlgebra.BLAS.hemm!(side::AbstractChar, uplo::AbstractChar,
                                  α::Number, A::DMatrix{T}, B::DMatrix{T},
                                  β::Number, C::DMatrix{T}) where T<:LinearAlgebra.BlasComplex
    return LinearAlgebra.BLAS.hemm!(side, uplo, convert(T, α), A, B, convert(T, β), C)
end

function LinearAlgebra.mul!(C::DMatrix, A::LinearAlgebra.Symmetric{<:Any,<:DMatrix},
                            B::DMatrix, α::Number, β::Number)
    return _symm_mul!(C, A.data, B, α, β, 'L', A.uplo, false)
end
function LinearAlgebra.mul!(C::DMatrix, A::DMatrix,
                            B::LinearAlgebra.Symmetric{<:Any,<:DMatrix},
                            α::Number, β::Number)
    return _symm_mul!(C, B.data, A, α, β, 'R', B.uplo, false)
end
function LinearAlgebra.mul!(C::DMatrix, A::LinearAlgebra.Hermitian{<:Any,<:DMatrix},
                            B::DMatrix, α::Number, β::Number)
    return _symm_mul!(C, A.data, B, α, β, 'L', A.uplo, true)
end
function LinearAlgebra.mul!(C::DMatrix, A::DMatrix,
                            B::LinearAlgebra.Hermitian{<:Any,<:DMatrix},
                            α::Number, β::Number)
    return _symm_mul!(C, B.data, A, α, β, 'R', B.uplo, true)
end

function LinearAlgebra.mul!(y::DVector, A::LinearAlgebra.Symmetric{<:Any,<:DMatrix},
                            x::DVector, α::Number, β::Number)
    return _symv_mul!(y, A.data, x, α, β, A.uplo, false)
end
function LinearAlgebra.mul!(y::DVector, A::LinearAlgebra.Hermitian{<:Any,<:DMatrix},
                            x::DVector, α::Number, β::Number)
    return _symv_mul!(y, A.data, x, α, β, A.uplo, true)
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
    return nothing
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
    return nothing
end

function LinearAlgebra.generic_matvecmul!(
    C::DVector,
    transA::Char,
    A::DMatrix,
    B::DVector,
    _add::LinearAlgebra.MulAddMul,
)
    return LinearAlgebra.generic_matvecmul!(C, transA, A, B, _add.alpha, _add.beta)
end
function LinearAlgebra.generic_matvecmul!(
    C::DVector,
    transA::Char,
    A::DMatrix,
    B::DVector,
    _alpha::Number,
    _beta::Number,
)
    partC, partA, partB = _repartition_matvecmul(C, A, B, transA)
    return maybe_copy_buffered(C=>partC, A=>partA, B=>partB) do C, A, B
        return gemv_dagger!(C, transA, A, B, _alpha, _beta)
    end
end
function _repartition_matvecmul(C, A, B, transA::Char)::Tuple{Blocks{1}, Blocks{2}, Blocks{1}}
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
            partB = (sz,)
        else
            partA = (sz, partA[2])
            partB = (sz,)
        end
    end
    partC = (dimA,)
    return Blocks(partC...), Blocks(partA...), Blocks(partB...)
end
"""
    matvecmul!(C, transA::Char, A, B, alpha, beta)

Tile-level matrix-vector multiply computing `C = alpha*op(A)*B + beta*C` in
place on the (dense) output vector `C`, where `op` is determined by `transA`
(`'N'`, `'T'`, `'C'`). Dispatches on the tile types: dense tiles use BLAS, while
sparse tiles (e.g. `DSparseArray`) provide their own method (in a package
extension) using a sparse matrix-vector product. This is the matvec analogue of
[`matmatmul!`](@ref).
"""
function matvecmul!(C, transA::Char, A, B, alpha, beta)
    # Same-eltype tiles keep `BLAS.gemv!` (GPU backends overload it). Mixed
    # eltypes use LinearAlgebra's mixed `generic_matvecmul!`, matching host
    # `mul!`. Sparse / Finch / vendor-GPU methods are more specific than this.
    TC = eltype(C)
    if TC === eltype(A) === eltype(B)
        BLAS.gemv!(transA, convert(TC, alpha), A, B, convert(TC, beta), C)
    else
        LinearAlgebra.generic_matvecmul!(C, transA, A, B, alpha, beta)
    end
    return C
end

function gemv_dagger!(
    C::DVector,
    transA::Char,
    A::DMatrix,
    B::DVector,
    _alpha,
    _beta,
)
    T = eltype(C)
    Ac = A.chunks
    Bc = B.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Bmt = size(Bc)[1]
    Cmt = size(Cc)[1]

    alpha = T(_alpha)
    beta = T(_beta)

    # For op(A)*x: when A is not transposed, x matches A's column-blocks and
    # C matches A's row-blocks; when A is [conj-]transposed the roles swap.
    if transA == 'N'
        if Ant != Bmt
            throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt)"))
        end
        if Amt != Cmt
            throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but C has number of blocks ($Cmt)"))
        end
    else
        if Amt != Bmt
            throw(DimensionMismatch(lazy"A' has number of blocks ($Ant,$Amt) but B has number of blocks ($Bmt)"))
        end
        if Ant != Cmt
            throw(DimensionMismatch(lazy"A' has number of blocks ($Ant,$Amt) but C has number of blocks ($Cmt)"))
        end
    end

    Dagger.spawn_datadeps() do
        for m in range(1, Cmt)
            if transA == 'N'
                # A: NoTrans
                for k in range(1, Ant)
                    mzone = k == 1 ? beta : T(1.0)
                    Dagger.@spawn matvecmul!(
                        InOut(Cc[m]),
                        transA,
                        In(Ac[m, k]),
                        In(Bc[k]),
                        alpha,
                        mzone,
                    )
                end
            else
                # A: [Conj]Trans — C's blocks index A's column-blocks
                for k in range(1, Amt)
                    mzone = k == 1 ? beta : T(1.0)
                    Dagger.@spawn matvecmul!(
                        InOut(Cc[m]),
                        transA,
                        In(Ac[k, m]),
                        In(Bc[k]),
                        alpha,
                        mzone,
                    )
                end
            end
        end
    end

    return C
end

wrap_as_darray(A::DArray) = A
wrap_as_darray(A::Array) = view(A, AutoBlocks())
function wrap_as_darray(A::SubArray{T,Nv,Array{T,Na}}) where {T,Nv,Na}
    Ap = parent(A)
    part = auto_blocks(map(last, parentindices(Ap)))
    partsize = part.blocksize
    inds = parentindices(A)
    inds_ranges_parent = ntuple(i->to_range(inds[i]), Val(Na))
    inds_ranges_view = ntuple(i->to_range(inds[i]), Val(Nv))
    subdomains = partition(part, ArrayDomain(inds_ranges_parent))
    nparts = size(subdomains)
    chunks = Array{Any,Na}(undef, nparts...)
    for idx in CartesianIndices(nparts)
        subdomain_view = subdomains[idx]
        subdomain_parent = ArrayDomain(ntuple(i->Nv >= i ? subdomain_view.indexes[i] : inds_ranges_parent[i], Val(Na)))
        subinds = ntuple(i->subdomain_parent.indexes[i], Val(Na))
        subA = view(Ap, subinds...)
        chunks[idx] = tochunk(subA)
    end
    return DArray(T, ArrayDomain(inds_ranges_parent), subdomains, chunks, part)
end

# Generate generic_matvecmul! methods for all combinations of DArray, Array, and SubArray
for CT in (DVector, Vector, SubArray{<:Any,1,<:Array}),
    AT in (DMatrix, Matrix, SubArray{<:Any,2,<:Array}),
    BT in (DVector, Vector, SubArray{<:Any,1,<:Array})

    # Don't commit type piracy
    CT isa DArray || AT isa DArray || BT isa DArray || continue

    @eval function LinearAlgebra.generic_matvecmul!(
        C::$(CT),
        transA::Char,
        A::$(AT),
        B::$(BT),
        _add::LinearAlgebra.MulAddMul,
    )
        new_C = wrap_as_darray(C)
        new_A = wrap_as_darray(A)
        new_B = wrap_as_darray(B)
        return LinearAlgebra.generic_matvecmul!(
            new_C,
            transA,
            new_A,
            new_B,
            _add,
        )
    end
end