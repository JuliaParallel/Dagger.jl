function mul!(A::Dagger.DArray{T,2}, B::Dagger.DArray{T,2}, C::Dagger.DArray{T,2}, 
    transA::Char, transB::Char, alpha::T, beta::T) where T

    Ac = A.chunks
    Bc = B.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Bmt, Bnt = size(Bc)
    Cmt, Cnt = size(Cc)

    #=
    if Ant != Bmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt,$Bnt)"))
    end
    =#

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