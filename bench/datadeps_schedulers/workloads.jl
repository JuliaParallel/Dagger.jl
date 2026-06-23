using LinearAlgebra
using Dagger
using Dagger: In, Out, InOut

function tiled_cholesky!(M::Matrix{<:Matrix})
    mt = size(M, 1)
    for k in 1:mt
        Dagger.@spawn LinearAlgebra.LAPACK.potrf!('L', InOut(M[k, k]))
        for m in (k+1):mt
            Dagger.@spawn LinearAlgebra.BLAS.trsm!('R', 'L', 'T', 'N',
                                                   1.0, In(M[k, k]),
                                                   InOut(M[m, k]))
        end
        for n in (k+1):mt
            Dagger.@spawn LinearAlgebra.BLAS.syrk!('L', 'N', -1.0,
                                                   In(M[n, k]), 1.0,
                                                   InOut(M[n, n]))
            for m in (n+1):mt
                Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'T', -1.0,
                                                       In(M[m, k]),
                                                       In(M[n, k]), 1.0,
                                                       InOut(M[m, n]))
            end
        end
    end
    return M
end

function make_spd_tiles(sz::Int, nb::Int)
    @assert sz % nb == 0
    A = rand(sz, sz)
    A = A * A'
    A[diagind(A)] .+= sz
    return [A[i:(i+nb-1), j:(j+nb-1)] for i in 1:nb:sz, j in 1:nb:sz]
end

function tiled_matmul!(C::Matrix{<:Matrix}, A::Matrix{<:Matrix}, B::Matrix{<:Matrix})
    nt = size(C, 1)
    @assert size(A) == size(B) == size(C) == (nt, nt)
    for i in 1:nt, j in 1:nt
        for k in 1:nt
            Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                   In(A[i, k]),
                                                   In(B[k, j]), 1.0,
                                                   InOut(C[i, j]))
        end
    end
    return C
end

function make_matmul_tiles(sz::Int, nb::Int)
    @assert sz % nb == 0
    nt = sz ÷ nb
    A = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
    B = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
    C = [zeros(nb, nb) for _ in 1:nt, _ in 1:nt]
    return A, B, C
end
