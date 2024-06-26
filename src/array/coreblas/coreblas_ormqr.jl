for (geormqr, T) in
    ((:coreblas_dormqr, Float64), 
    (:coreblas_sormqr, Float32),
    (:coreblas_zunmqr, ComplexF64),
    (:coreblas_cunmqr, ComplexF32))
    @eval begin
        function coreblas_ormqr!(side::Char, trans::Char, A::AbstractMatrix{$T}, 
             Tau::AbstractMatrix{$T}, C::AbstractMatrix{$T})

            m, n = size(C)
            ib, nb = size(Tau)
            k = nb
            if $T <: Complex
                transnum = trans == 'N' ? 111 : 113
            else
                transnum = trans == 'N' ? 111 : 112
            end
            sidenum = side == 'L' ? 141 : 142

            lda = max(1, stride(A,2))
            ldt = max(1, stride(Tau,2))
            ldc = max(1, stride(C,2))
            ldwork = side == 'L' ? n : m
            work = Vector{$T}(undef, ib*nb)

                
            err = ccall(($(QuoteNode(geormqr)), :libcoreblas), Int64,
                (Int64, Int64, Int64, Int64, 
                Int64, Int64, 
                Ptr{$T}, Int64, Ptr{$T}, Int64, 
                Ptr{$T}, Int64, Ptr{$T}, Int64),
                sidenum, transnum, 
                m, n, 
                k, ib, 
                A, lda, 
                Tau, ldt, 
                C, ldc,
                work, ldwork)
            if err != 0
                throw(ArgumentError("coreblas_ormqr failed. Error number: $err"))
            end 
        end
    end
end

