
function syrk!(A::Dagger.DArray{T,2}, C::Dagger.DArray{T,2}, uplo::Char, trans::Char, alpha::T, beta::T) where T

    Ac = A.chunks
    Cc = C.chunks
    Amt, Ant = size(Ac)
    Cmt, Cnt = size(Cc)
    

    #=
    if Ant != Bmt
        throw(DimensionMismatch(lazy"A has number of blocks ($Amt,$Ant) but B has number of blocks ($Bmt,$Bnt)"))
    end
    =#

    iscomplex = T <: Complex
    transs = iscomplex ? 'C' : 'T'

    Dagger.spawn_datadeps() do
        for n in range(1, Cnt)

            #  NoTrans
            if (trans == 'N')
                for k in range(1, Ant)
                    mzone = k == 1 ? real(beta) : one(real(T))
                    if iscomplex
                        Dagger.@spawn BLAS.herk!(uplo, trans, real(alpha), In(Ac[n, k]), mzone, InOut(Cc[n, n]))
                    else
                        Dagger.@spawn BLAS.syrk!(uplo, trans, alpha, In(Ac[n, k]), mzone, InOut(Cc[n, n]))
                    end
                end
                    # NoTrans / Lower
                    if (uplo == 'L')

                        for m in range(n+1, Cmt)
                            for k in range(1, Ant)
                                mzone = k == 1 ? beta : one(T)
                                Dagger.@spawn BLAS.gemm!(trans, transs, alpha, In(Ac[m, k]), In(Ac[n, k]), mzone, InOut(Cc[m, n]))
                            end
                        end

                         # NoTrans / Upper
                    else
                        for m in range(n+1, Cmt)
                            for k in range(1, Ant)
                                mzone = k == 1 ? beta : one(T)
                                Dagger.@spawn BLAS.gemm!(trans, transs, alpha, In(Ac[n, k]), In(Ac[m, k]), mzone, InOut(Cc[n, m]))
                            end
                        end

                    end

                #[Conj]Trans
            else
                for k in range(1, Amt)
                    mzone = k == 1 ? real(beta) : one(real(T))
                    if iscomplex
                        Dagger.@spawn BLAS.herk!(uplo, transs, real(alpha), In(Ac[k, n]), mzone, InOut(Cc[n, n]))
                    else
                        Dagger.@spawn BLAS.syrk!(uplo, trans, alpha, In(Ac[k, n]), mzone, InOut(Cc[n, n]))
                    end
                end
                    # [Conj]Trans / Lower
                    if (uplo == 'L')

                        for m in range(n+1, Cmt)
                            for k in range(1, Amt)
                                mzone = k == 1 ? beta : one(T)
                                Dagger.@spawn BLAS.gemm!(transs, 'N', alpha, In(Ac[k, m]), In(Ac[k, n]), mzone, InOut(Cc[m, n]))
                            end
                        end

                         # [Conj]Trans / Upper
                    else
                        for m in range(n+1, Cmt)
                            for k in range(1, Amt)
                                mzone = k == 1 ? beta : one(T)
                                Dagger.@spawn BLAS.gemm!(transs, 'N', alpha, In(Ac[k, n]), In(Ac[k, m]), mzone, InOut(Cc[n, m]))
                            end
                        end

                    end                
            end
        
        end

    end


end 