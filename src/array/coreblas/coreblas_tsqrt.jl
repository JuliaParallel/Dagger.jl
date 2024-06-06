
for (getsqrt,T) in 
    ((:coreblas_dtsqrt, Float64), 
    (:coreblas_stsqrt, Float32), 
    (:coreblas_ctsqrt, ComplexF32), 
    (:coreblas_ztsqrt, ComplexF64))
    @eval begin
        function coreblas_tsqrt!(A1::AbstractMatrix{$T}, A2::AbstractMatrix{$T}, 
                Tau::AbstractMatrix{$T})
            m = size(A2)[1]
            n = size(A1)[2]
            ib, nb = size(Tau)
            lda1 = max(1, stride(A1,2))
            lda2 = max(1, stride(A2,2))
            ldt = max(1, stride(Tau,2))
            work = Vector{$T}(undef, (ib)*n)
            ttau = Vector{$T}(undef, n)

            err = ccall(($(QuoteNode(getsqrt)), libcoreblas), Int64,
                (Int64, Int64, Int64,
                Ptr{$T}, Int64, Ptr{$T}, Int64, 
                Ptr{$T}, Int64, Ptr{$T}, Ptr{$T}),
                m, n, ib, 
                A1, lda1, 
                A2, lda2, 
                Tau, ldt, 
                ttau, work)
            if err != 0
                throw(ArgumentError("coreblas_tsqrt failed. Error number: $err"))
            end 
        end
    end
end


