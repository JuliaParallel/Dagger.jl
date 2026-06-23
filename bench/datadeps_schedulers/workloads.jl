using LinearAlgebra
using Dagger
using Dagger: In, Out, InOut

# Tiled blocked Cholesky factorization of an `nb`-tile × `nb`-tile SPD matrix.
# Block size is `bs`. The same task structure as the in-tree test
# `test/datadeps/scheduling.jl::mock_cholesky!`, kept here as a standalone
# function so the bench harness does not depend on test files.
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

# Build an SPD block matrix as a tile-grid of `Matrix`es. `sz` is total side
# length; `nb` is tile side length. `sz` must be a multiple of `nb`.
function make_spd_tiles(sz::Int, nb::Int)
    @assert sz % nb == 0
    A = rand(sz, sz)
    A = A * A'
    A[diagind(A)] .+= sz
    return [A[i:(i+nb-1), j:(j+nb-1)] for i in 1:nb:sz, j in 1:nb:sz]
end

# Tiled matrix multiply C = A * B where A, B, C are nt×nt tile grids.
# Each tile is `bs`×`bs`. Performs C[i,j] += A[i,k] * B[k,j] for all i,j,k.
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
