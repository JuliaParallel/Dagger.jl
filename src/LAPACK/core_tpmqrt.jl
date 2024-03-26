using LinearAlgebra
using LinearAlgebra: BlasInt, require_one_based_indexing
using LinearAlgebra.LAPACK: liblapack, chkstride1, chklapackerror
using LinearAlgebra.BLAS: @blasfunc


"""
Applies a complex orthogonal matrix Q.

 The matrix Q is obtained from a "triangular-pentagonal" complex block reflector H 
 to a general complex matrix C, which consists of two blocks A and B.
"""

function core_tpmqrt!(::Type{T}, side::Char, trans::Char, l::Int64, V::AbstractMatrix{T}, 
    Tau::AbstractMatrix{T}, A::AbstractMatrix{T}, B::AbstractMatrix{T}) where {T<: Number}
    require_one_based_indexing(A, Tau)
    chkstride1(A)
    m, n = size(B)
    ib, nb = size(Tau)
    minmn = min(m, n)
    k=nb
    if nb > minmn
        throw(ArgumentError("block size $nb > $minmn too large"))
    end

    ldv = max(1, stride(V,2))
    ldt = max(1, stride(Tau,2))
    lda = max(1, stride(A,2))
    ldb = max(1, stride(B,2))
    work = Vector{T}(undef, (ib+1)*n)
  
    if n > 0
        info = Ref{BlasInt}()
        
        ccall((@blasfunc(dtpmqrt_), libblastrampoline), Cvoid,
            (Ref{UInt8}, Ref{UInt8}, Ref{BlasInt}, Ref{BlasInt}, 
            Ref{BlasInt}, Ref{BlasInt},  Ref{BlasInt},
            Ptr{T}, Ref{BlasInt}, Ptr{T}, Ref{BlasInt}, 
            Ptr{T}, Ref{BlasInt}, Ptr{T}, Ref{BlasInt}, 
            Ptr{T}, Ptr{BlasInt}),
            side, trans, m, n, k, l, ib, V, ldv, Tau, ldt, A, lda,
            B, ldb, work, info)
    
        chklapackerror(info[])
       
    end
end

