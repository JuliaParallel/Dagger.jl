
import Dagger.DArray

WrappedDArray{T,N} = Union{<:DArray{T,N}, Transpose{<:DArray{T,N}}, Adjoint{<:DArray{T,N}}}
WrappedDMatrix{T} = WrappedDArray{T,2}
WrappedDVector{T} = WrappedDArray{T,1}
DMatrix{T} = DArray{T,2}
DVector{T} = DArray{T,1}

"""
function LinearAlgebra.mul!(C::DMatrix{T}, A::WrappedDMatrix{T}, B::WrappedDMatrix{T}, alpha::Number, beta::Number) where T

    mul!(C, A, B, T(alpha), T(beta))

end


function LinearAlgebra.mul!(C::DMatrix{T}, A::WrappedDMatrix{T}, B::WrappedDMatrix{T}, alpha::T, beta::T) where T


    transA = LinearAlgebra.wrapper_char(A)
    transB = LinearAlgebra.wrapper_char(B)

    A = LinearAlgebra._unwrap(A)
    B = LinearAlgebra._unwrap(B)
    
    LinearAlgebra.generic_matmatmul!(C, transA, transB, A, B, LinearAlgebra.MulAddMul(alpha, beta))
end

"""

function LinearAlgebra.generic_matmatmul!(C::DMatrix{T}, transA::Char, transB::Char, A::DMatrix{T}, B::DMatrix{T}, _add::LinearAlgebra.MulAddMul) where T


    Ac = A.chunks
    Bc = B.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Bmt, Bnt = size(Bc)
    Cmt, Cnt = size(Cc)

    alpha = _add.alpha
    beta = _add.beta
    #=
    if Ant != Bmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt,$Bnt)"))
    end
    =#
    @show transA, transB, alpha, beta

    Dagger.spawn_datadeps() do
        for m in range(1, Cmt)
            for n in range(1, Cnt)

                #  A: NoTrans / B: NoTrans
                if (transA == 'N')
                    if (transB == 'N')
                        for k in range(1, Amt)
                            Dagger.@spawn BLAS.gemm!(transA, transB, alpha, In(Ac[m, k]), In(Bc[k, n]), beta, InOut(Cc[m, n]))
                         end
                        # A: NoTrans / B: [Conj]Trans
                    else
                        for k in range(1, Amt)
                            Dagger.@spawn BLAS.gemm!(transA, transB, alpha, In(Ac[m, k]), In(Bc[n, k]), beta, InOut(Cc[m, n]))
                         end
                    end
                else 
                # A: [Conj]Trans / B: NoTrans
                    if (transB == 'N')
                        for k in range(1, Amt)
                            Dagger.@spawn BLAS.gemm!(transA, transB, alpha, In(Ac[k, m]), In(Bc[k, n]), beta, InOut(Cc[m, n]))
                         end
                        # A: [Conj]Trans / B: [Conj]Trans
                    else
                        for k in range(1, Amt)
                            Dagger.@spawn BLAS.gemm!(transA, transB, alpha, In(Ac[k, m]), In(Bc[n, k]), beta, InOut(Cc[m, n]))
                         end
                    end
                end

            end
        end
    end

end
