using LinearAlgebra
using LinearAlgebra: libblastrampoline, BlasInt, require_one_based_indexing
using LinearAlgebra.LAPACK: liblapack, chkstride1, chklapackerror
using LinearAlgebra.BLAS: @blasfunc


"""
Computes a blocked QR factorization of a real or complex "triangular-pentagonal" matrix, which is 
composed of a triangular block and a pentagonal block, using the compact WY representation for Q.
"""
##TODO this for Float64 you have to put it in for loop to generate for all types see: vim ~/julia-1.9.3/share/julia/stdlib/v1.9/LinearAlgebra/src/lapack.jl
function core_tpqrt!(::Type{T}, l::Int64, A::AbstractMatrix{T}, B::AbstractMatrix{T}, 
    Tau::AbstractMatrix{T}) where {T<: Number}
    require_one_based_indexing(A, Tau)
    chkstride1(A)
    m, n = size(B)
    ib, nb = size(Tau)
    minmn = min(m, n)
    
    if nb > minmn
        throw(ArgumentError("block size $nb > $minmn too large"))
    end
    lda = max(1, stride(A,2))
    ldb = max(1, stride(B,2))
    work = Vector{T}(undef, (ib+1)*n)
    if n > 0
        info = Ref{BlasInt}()
        ccall((@blasfunc(dtpqrt_), libblastrampoline), Cvoid,
            (Ref{BlasInt}, Ref{BlasInt}, Ref{BlasInt}, Ref{BlasInt}, 
            Ptr{T}, Ref{BlasInt}, Ptr{T}, Ref{BlasInt}, 
            Ptr{T}, Ref{BlasInt}, Ptr{T}, Ptr{BlasInt}),
            m, n, l, ib, A, lda, B, ldb, Tau, max(1,stride(Tau,2)), 
            work, info)
        chklapackerror(info[])
    end
end


#https://gist.github.com/jiahao/ad788965335d25972e2d
