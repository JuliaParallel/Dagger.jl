using libblastrampoline_jll
using LinearAlgebra
using libcoreblas_jll

for (gemm, T) in
    ((:coreblas_dgemm, Float64), 
     (:coreblas_sgemm, Float32),
     (:coreblas_cgemm, ComplexF32),
     (:coreblas_zgemm, ComplexF64))
    @eval begin
        function coreblas_gemm!(transa::Int64,  transb::Int64, 
            alpha::$T, A::AbstractMatrix{$T}, B::AbstractMatrix{$T}, beta::$T, C::AbstractMatrix{$T})
            m, k = size(A)
            k, n = size(B)
            ccall(($gemm, "libcoreblas.so"), Cvoid,
                    (Int64, Int64, Int64, Int64, Int64, $T, Ptr{$T}, Int64, Ptr{$T}, Int64, 
                    $T,  Ptr{$T}, Int64),
                    transa, transb, m, n, k, alpha, A, m, B, k, beta, C, m)
        end
    end
end
