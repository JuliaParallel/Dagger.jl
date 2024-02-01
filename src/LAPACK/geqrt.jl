using LinearAlgebra
using LinearAlgebra: BlasInt, require_one_based_indexing
using LinearAlgebra.LAPACK: liblapack, chkstride1, chklapackerror
using LinearAlgebra.BLAS: @blasfunc

function geqrt!(::Type{T}, A::AbstractMatrix{T}, Tau::AbstractMatrix{T}) where {T<: Number}
    require_one_based_indexing(A, Tau)
    chkstride1(A)
    m, n = size(A)
    minmn = min(m, n)
    nb = size(Tau, 1)
    if nb > minmn
        throw(ArgumentError("block size $nb > $minmn too large"))
    end
    lda = max(1, stride(A,2))
    work = Vector{T}(undef, nb*n)
    if n > 0
        info = Ref{BlasInt}()
        ccall((@blasfunc(dgeqrt_), liblapack), Cvoid,
            (Ref{BlasInt}, Ref{BlasInt}, Ref{BlasInt}, Ptr{T},
                Ref{BlasInt}, Ptr{T}, Ref{BlasInt}, Ptr{T},
                Ptr{BlasInt}),
                m, n, nb, A,
                lda, Tau, max(1,stride(Tau,2)), work,
                info)
        chklapackerror(info[])
    end
end
