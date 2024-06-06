
for (gettqrt, T) in 
    ((:coreblas_dttqrt, Float64), 
     (:coreblas_sttqrt, Float32),
     (:coreblas_cttqrt, ComplexF32),
     (:coreblas_zttqrt, ComplexF64))
    @eval begin
        function coreblas_ttqrt!(A1::AbstractMatrix{$T}, 
            A2::AbstractMatrix{$T}, triT::AbstractMatrix{$T})
            m1, n1 = size(A1)
            m2, n2 = size(A2)
            ib, nb = size(triT)

            lwork = nb + ib*nb
            tau = Vector{$T}(undef, nb)
            work = Vector{$T}(undef, (ib+1)*nb)
            lda1 = max(1, stride(A1, 2))
            lda2 = max(1, stride(A2, 2))
            ldt = max(1, stride(triT, 2))


            err = ccall(($(QuoteNode(gettqrt)), libcoreblas), Int64,
                    (Int64, Int64, Int64, Ptr{$T}, Int64, Ptr{$T}, Int64,
                    Ptr{$T}, Int64, Ptr{$T}, Ptr{$T}),
                    m1, n1, ib, A1, lda1, A2, lda2, triT, ldt, tau, work)
            
            if err != 0
                throw(ArgumentError("coreblas_ttqrt failed. Error number: $err"))
            end    
        end
    end
end
