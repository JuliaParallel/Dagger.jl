using LinearAlgebra
using LinearAlgebra: BlasInt, require_one_based_indexing
using LinearAlgebra.LAPACK: liblapack, chkstride1, chklapackerror
using LinearAlgebra.BLAS: @blasfunc

function geqrt!(A::AbstractMatrix{Float64}, T::AbstractMatrix{Float64})
    require_one_based_indexing(A, T)
    chkstride1(A)
    m, n = size(A)
    minmn = min(m, n)
    nb = size(T, 1)
    if nb > minmn
        throw(ArgumentError("block size $nb > $minmn too large"))
    end
    lda = max(1, stride(A,2))
    work = Vector{Float64}(undef, nb*n)
    if n > 0
        info = Ref{BlasInt}()
        ccall((@blasfunc(dgeqrt_), liblapack), Cvoid,
            (Ref{BlasInt}, Ref{BlasInt}, Ref{BlasInt}, Ptr{Float64},
                Ref{BlasInt}, Ptr{Float64}, Ref{BlasInt}, Ptr{Float64},
                Ptr{BlasInt}),
                m, n, nb, A,
                lda, T, max(1,stride(T,2)), work,
                info)
        chklapackerror(info[])
    end
    A, T
end
