for (geqrt, T) in 
    ((:coreblas_dgeqrt, Float64),
    (:coreblas_sgeqrt, Float32),
    (:coreblas_cgeqrt, ComplexF32),
    (:coreblas_zgeqrt, ComplexF64))
    @eval begin
        function coreblas_geqrt!(A::AbstractMatrix{$T},
                Tau::AbstractMatrix{$T}) 
            require_one_based_indexing(A, Tau)
            chkstride1(A)
            m, n = size(A)
            ib, nb = size(Tau)
            lda = max(1, stride(A,2))
            ldt = max(1, stride(Tau,2))
            work = Vector{$T}(undef, (ib)*n)
            ttau = Vector{$T}(undef, n)

            err = ccall(($(QuoteNode(geqrt)), :libcoreblas), Int64,
                (Int64, Int64, Int64,
                Ptr{$T}, Int64, Ptr{$T}, Int64, 
                Ptr{$T}, Ptr{$T}),
                m, n, ib, 
                A, lda, 
                Tau, ldt, 
                ttau, work)
            if err != 0
                throw(ArgumentError("coreblas_geqrt failed. Error number: $err"))
            end 
        end
    end
end

