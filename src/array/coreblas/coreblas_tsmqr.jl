for (getsmqr, T) in 
    ((:coreblas_dtsmqr, Float64),
     (:coreblas_ctsmqr, ComplexF32),
     (:coreblas_ztsmqr, ComplexF64),
     (:coreblas_stsmqr, Float32))
    @eval begin
        function coreblas_tsmqr!(side::Char, trans::Char, A1::AbstractMatrix{$T}, 
                A2::AbstractMatrix{$T}, V::AbstractMatrix{$T}, Tau::AbstractMatrix{$T}) 
            m1, n1 = size(A1)
            m2, n2 = size(A2)
            ib, nb = size(Tau)
            k = nb
            
            if $T <: Complex
                transnum = trans == 'N' ? 111 : 113
            else
                transnum = trans == 'N' ? 111 : 112
            end

            sidenum = side == 'L' ? 141 : 142

            lda1 = max(1, stride(A1,2))
            lda2 = max(1, stride(A2,2))
            ldv = max(1, stride(V,2))
            ldt = max(1, stride(Tau,2))
            ldwork = side == 'L' ? ib : m1
            work = Vector{$T}(undef, ib*nb)

                
            err = ccall(($(QuoteNode(getsmqr)), :libcoreblas), Int64,
                (Int64, Int64, Int64, Int64, 
                Int64, Int64, Int64, Int64,
                Ptr{$T}, Int64, Ptr{$T}, Int64, 
                Ptr{$T}, Int64, Ptr{$T}, Int64, Ptr{$T}, Int64),
                sidenum, transnum, 
                m1, n1,
                m2, n2,
                k, ib, 
                A1, lda1, 
                A2, lda2,
                V, ldv,
                Tau, ldt, 
                work, ldwork)
            if err != 0
                throw(ArgumentError("coreblas_tsmqr failed. Error number: $err"))
            end 
        end
    end
end
