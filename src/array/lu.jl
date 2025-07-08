function LinearAlgebra.lu(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool=true) where T
    A_copy = LinearAlgebra._lucopy(A, LinearAlgebra.lutype(T))
    return LinearAlgebra.lu!(A_copy, LinearAlgebra.NoPivot(); check=check)
end
function LinearAlgebra.lu!(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool=true) where T
    zone = one(T)
    mzone = -one(T)
    Ac = A.chunks
    mt, nt = size(Ac)
    iscomplex = T <: Complex
    trans = iscomplex ? 'C' : 'T'

    Dagger.spawn_datadeps() do
        for k in range(1, min(mt, nt))
            Dagger.@spawn LinearAlgebra.generic_lufact!(InOut(Ac[k, k]), LinearAlgebra.NoPivot(); check)
            for m in range(k+1, mt)
                Dagger.@spawn BLAS.trsm!('R', 'U', 'N', 'N', zone, In(Ac[k, k]), InOut(Ac[m, k]))
            end
            for n in range(k+1, nt)
                Dagger.@spawn BLAS.trsm!('L', 'L', 'N', 'U', zone, In(Ac[k, k]), InOut(Ac[k, n]))
                for m in range(k+1, mt)
                    Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(Ac[m, k]), In(Ac[k, n]), zone, InOut(Ac[m, n]))
                end
            end
        end
    end

    ipiv = DVector([i for i in 1:min(size(A)...)])

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, 0)
end
