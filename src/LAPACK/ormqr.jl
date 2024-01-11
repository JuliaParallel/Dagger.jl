using LinearAlgebra
using LinearAlgebra: BlasInt, require_one_based_indexing
using LinearAlgebra.LAPACK: liblapack, chkstride1, chklapackerror, chktrans, chkside
using LinearAlgebra.BLAS: @blasfunc


"""
Applies a complex orthogonal matrix Q.

 The matrix Q is obtained from a "triangular-pentagonal" complex block reflector H 
 to a general complex matrix C, which consists of two blocks A and B.
"""

function ormqr!(::Type{T}, side::Char, trans::Char, A::AbstractMatrix{T}, 
    Tau::AbstractMatrix{T}, C::AbstractMatrix{T}) where {T<: Number}
    require_one_based_indexing(A, Tau, C)
    chktrans(trans)
    chkside(side)
    chkstride1(A, C, Tau)
    mA, nA=size(A)
    m,n = ndims(C) == 2 ? size(C) : (size(C, 1), 1)
    ib, nb = size(Tau)
    k = mA
    if side == 'L' && m != mA
        throw(DimensionMismatch("for a left-sided multiplication, the first dimension of C, $m, must equal the second dimension of A, $mA"))
    end
    if side == 'R' && n != mA
        throw(DimensionMismatch("for a right-sided multiplication, the second dimension of C, $m, must equal the second dimension of A, $mA"))
    end
    if side == 'L' && k > m
        throw(DimensionMismatch("invalid number of reflectors: k = $k should be <= m = $m"))
    end
    if side == 'R' && k > n
        throw(DimensionMismatch("invalid number of reflectors: k = $k should be <= n = $n"))
    end

    lda = max(1, stride(A,2))
    ldc = max(1, stride(C,2))
    work = Vector{T}(undef, 1)
    lwork = BlasInt(-1)
    info = Ref{BlasInt}()

    #tau2= vec(Tau) 
    for i in 1:2  # first call returns lwork as work[1]
        display(A)
        display(Tau)
        display(C)
        display(work)
        ccall((@blasfunc(dormqr_), libblastrampoline), Cvoid,
            (Ref{UInt8}, Ref{UInt8}, Ref{BlasInt}, Ref{BlasInt}, 
            Ref{BlasInt}, 
            Ptr{T}, Ref{BlasInt}, Ptr{T},
            Ptr{T}, Ref{BlasInt}, Ptr{T}, Ref{BlasInt}, Ptr{BlasInt}),
            side, trans, m, n, k, A, lda, Tau, 
            C, ldc, work, lwork, info)
            println(info[])
            display(A)
            display(Tau)
            display(C)
            display(work)
            if i == 1
                lwork = BlasInt(real(work[1]))
                resize!(work, lwork)
            end
        chklapackerror(info[])
    end
    C
end

