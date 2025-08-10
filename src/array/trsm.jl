function trsv!(uplo::Char, trans::Char, diag::Char, alpha::T, A::DArray{T,2}, B::AbstractArray{T,1}) where T

    zone = one(T)
    mzone = -one(T)

    mba, nba = A.partitioning.blocksize

    # is_dB = isa(B, DArray)
    # B = is_dB ? B : view(B, Blocks(mba))

    nbb = B.partitioning.blocksize

    Ac = A.chunks
    Bc = B.chunks
    Bnt = length(Bc)

    Dagger.spawn_datadeps() do
        if uplo == 'U'
            if trans == 'N'
                for k in reverse(1:Bnt)
                    lalpha = (k == Bnt) ? alpha : zone
                    Dagger.@spawn BLAS.trsv!('U', 'N', diag, In(Ac[k, k]), InOut(Bc[k]))
                    for i in 1:k-1
                        Dagger.@spawn BLAS.gemv!('N', mzone, In(Ac[i, k]), In(Bc[k]), lalpha, InOut(Bc[i]))
                    end
                end
            elseif trans == 'T'
                for k in 1:Bnt
                    lalpha = (k == 1) ? alpha : zone
                    Dagger.@spawn BLAS.trsv!('U', 'T', diag, In(Ac[k, k]), InOut(Bc[k]))
                    for i in k+1:Bnt
                        Dagger.@spawn BLAS.gemv!('T', mzone, In(Ac[k, i]), In(Bc[i]), lalpha, InOut(Bc[k]))
                    end
                end
            end
        elseif uplo == 'L'
            if trans == 'N'
                for k in 1:Bnt
                    lalpha = (k == 1) ? alpha : zone
                    Dagger.@spawn BLAS.trsv!('L', 'N', diag, In(Ac[k, k]), InOut(Bc[k]))
                    for i in k+1:Bnt
                        Dagger.@spawn BLAS.gemv!('N', mzone, In(Ac[i, k]), In(Bc[k]), lalpha, InOut(Bc[i]))
                    end
                end
            elseif trans == 'T'
                for k in reverse(1:Bnt)
                    lalpha = (k == Bnt) ? alpha : zone
                    Dagger.@spawn BLAS.trsv!('L', 'T', diag, In(Ac[k, k]), InOut(Bc[k]))
                    for i in 1:k-1
                        Dagger.@spawn BLAS.gemv!('T', mzone, In(Ac[k, i]), In(Bc[i]), lalpha, InOut(Bc[k]))
                    end
                end
            end
        end
    end


end

function trsm!(side::Char, uplo::Char, trans::Char, diag::Char, alpha::T, A::DArray{T,2}, B::DArray{T,2}) where T
   
    zone = one(T)
    mzone = -one(T)

    mba, nba = A.partitioning.blocksize

    # is_dB = isa(B, DMatrix)
    # B = is_dB ? B : view(B, Blocks(mba, nba))

    mbb, nbb = B.partitioning.blocksize

    # if mba != nba
    #     mba = nba = min(mba, nba)
    #     A = maybe_copy_buffered(A => Blocks(nba, nba)) do A
    #         A
    #     end
    # end

    # if mbb != mba || nbb != mba
    #     mbb = nbb = mba
    #     B = maybe_copy_buffered(B => Blocks(mbb, nbb)) do B
    #         B
    #     end
    # end

    Ac = A.chunks
    Bc = B.chunks
    Bmt, Bnt = size(Bc)

    Dagger.spawn_datadeps() do
        if side == 'L'
            if uplo == 'U'
                if trans == 'N'
                    for k in range(1, Bmt)
                        lalpha = k == 1 ? alpha : zone;
                        for n in range(1, Bnt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, lalpha, In(Ac[(Bmt-k)+1, (Bmt-k)+1]), InOut(Bc[(Bmt-k)+1, n]))
                        end
                        for m in range(k+1, Bmt)
                            for n in range(1, Bnt)
                                Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(Ac[(Bmt-m)+1, (Bmt-k)+1]), In(Bc[(Bmt-k)+1, n]), lalpha, InOut(Bc[(Bmt-m)+1, n]))
                            end
                        end
                    end
                elseif trans == 'T'
                    for k in range(1, Bmt)
                        lalpha = k == 1 ? alpha : zone;
                        for n in range(1, Bnt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, lalpha, In(Ac[k, k]), InOut(Bc[k, n]))
                        end
                        for m in range(k+1, Bmt)
                            for n in range(1, Bnt)
                                Dagger.@spawn BLAS.gemm!('T', 'N', mzone, In(Ac[k, m]), In(Bc[k, n]), lalpha, InOut(Bc[m, n]))
                            end
                        end
                    end
                end
            elseif uplo == 'L'
                if trans == 'N'
                    for k in range(1, Bmt)
                        lalpha = k == 1 ? alpha : zone;
                        for n in range(1, Bnt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, lalpha, In(Ac[k, k]), InOut(Bc[k, n]))
                        end
                        for m in range(k+1, Bmt)
                            for n in range(1, Bnt)
                                Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(Ac[m, k]), In(Bc[k, n]), lalpha, InOut(Bc[m, n]))
                            end
                        end
                    end
                elseif trans == 'T'
                    for k in range(1, Bmt)
                        lalpha = k == 1 ? alpha : zone;
                        for n in range(1, Bnt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, lalpha, In(Ac[(Bmt-k)+1, (Bmt-k)+1]), InOut(Bc[(Bmt-k)+1, n]))
                        end
                        for m in range(k+1, Bmt)
                            for n in range(1, Bnt)
                                Dagger.@spawn BLAS.gemm!('T', 'N', mzone, In(Ac[(Bmt-k)+1, (Bmt-m)+1]), In(Bc[(Bmt-k)+1, n]), lalpha, InOut(Bc[(Bmt-m)+1, n]))
                            end
                        end
                    end
                end
            end
        elseif side == 'R'
            if uplo == 'U'
                if trans == 'N'
                    for k in range(1, Bnt)
                        lalpha = k == 1 ? alpha : zone;
                        for m in range(1, Bmt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, lalpha, In(Ac[k, k]), InOut(Bc[m, k]))
                        end
                        for m in range(1, Bmt)
                            for n in range(k+1, Bnt)
                                Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(Bc[m, k]), In(Ac[k, n]), lalpha, InOut(Bc[m, n]))
                            end
                        end
                    end
                elseif trans == 'T'
                    for k in range(1, Bnt)
                        for m in range(1, Bmt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, alpha, In(Ac[(Bnt-k)+1, (Bnt-k)+1]), InOut(Bc[m, (Bnt-k)+1]))
                            for n in range(k+1, Bnt)
                                Dagger.@spawn BLAS.gemm!('N', 'T', minvalpha, In(B[m, (Bnt-k)+1]), In(Ac[(Bnt-n)+1, (Bnt-k)+1]), zone, InOut(Bc[m, (Bnt-n)+1]))
                            end
                        end
                    end
                end
            elseif uplo == 'L'
                if trans == 'N'
                    for k in range(1, Bnt)
                        lalpha = k == 1 ? alpha : zone;
                        for m in range(1, Bmt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, lalpha, In(Ac[(Bnt-k)+1, (Bnt-k)+1]), InOut(Bc[m, (Bnt-k)+1]))
                            for n in range(k+1, Bnt)
                                Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(Bc[m, (Bnt-k)+1]), In(Ac[(Bnt-k)+1, (Bnt-n)+1]), lalpha, InOut(Bc[m, (Bnt-n)+1]))
                            end
                        end
                    end
                elseif trans == 'T'
                    for k in range(1, Bnt)
                        for m in range(1, Bmt)
                            Dagger.@spawn BLAS.trsm!(side, uplo, trans, diag, alpha, In(Ac[k, k]), InOut(Bc[m, k]))
                            for n in range(k+1, Bnt)
                                Dagger.@spawn BLAS.gemm!('N', 'T', minvalpha, In(Bc[m, k]), In(Ac[n, k]), zone, InOut(Bc[m, n]))
                            end
                        end
                    end
                end
            end
        end
    end

end