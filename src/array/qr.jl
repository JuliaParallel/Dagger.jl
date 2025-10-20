export geqrf!, porgqr!, pormqr!, cageqrf!
import LinearAlgebra: QRCompactWY, AdjointQ, BlasFloat, QRCompactWYQ, AbstractQ, StridedVecOrMat, I
import Base.:*

(*)(Q::QRCompactWYQ{T, M}, b::Number) where {T<:Number, M<:DMatrix{T}} = DMatrix(Q) * b
(*)(b::Number, Q::QRCompactWYQ{T, M}) where {T<:Number, M<:DMatrix{T}} = DMatrix(Q) * b

(*)(Q::AdjointQ{T, QRCompactWYQ{T, M, C}}, b::Number) where {T<:Number, M<:DMatrix{T}, C<:M} = DMatrix(Q) * b
(*)(b::Number, Q::AdjointQ{T, QRCompactWYQ{T, M, C}}) where {T<:Number, M<:DMatrix{T}, C<:M} = DMatrix(Q) * b

LinearAlgebra.lmul!(B::QRCompactWYQ{T, <:DMatrix{T}}, A::DMatrix{T}) where {T} = pormqr!('L', 'N', B.factors, B.T, A)
function LinearAlgebra.lmul!(B::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}, A::Dagger.DMatrix{T}) where {T}
    trans = T <: Complex ? 'C' : 'T'
    pormqr!('L', trans, B.Q.factors, B.Q.T, A)
end

LinearAlgebra.rmul!(A::Dagger.DMatrix{T}, B::QRCompactWYQ{T, <:Dagger.DMatrix{T}}) where {T} = pormqr!('R', 'N', B.factors, B.T, A)
function LinearAlgebra.rmul!(A::Dagger.DMatrix{T}, B::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}) where {T} 
    trans = T <: Complex ? 'C' : 'T'
    pormqr!('R', trans, B.Q.factors, B.Q.T, A)
end

function Dagger.DMatrix(Q::QRCompactWYQ{T, <:Dagger.DMatrix{T}}) where {T}
    DQ = DMatrix(Q.factors.partitioning, I*one(T), size(Q))
    porgqr!('N', Q.factors, Q.T, DQ)
    return DQ
end

function Dagger.DMatrix(AQ::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}) where {T}
    DQ = DMatrix(AQ.Q.factors.partitioning, I*one(T), size(AQ))
    trans = T <: Complex ? 'C' : 'T'
    porgqr!(trans, AQ.Q.factors, AQ.Q.T, DQ)
    return DQ
end

Base.collect(Q::QRCompactWYQ{T, <:Dagger.DMatrix{T}}) where {T} = collect(Dagger.DMatrix(Q))
Base.collect(AQ::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}) where {T} = collect(Dagger.DMatrix(AQ))

function _repartition_pormqr(A, Tm, C, side::Char, trans::Char)
    partA = A.partitioning.blocksize
    partTm = Tm.partitioning.blocksize
    partC = C.partitioning.blocksize

    # The pormqr! kernels assume that the number of row tiles (index k)
    # matches between the reflector matrix A and the target matrix C.
    # Adjust C's block size accordingly but avoid reshaping A or Tm,
    # as their chunking encodes the factorisation structure.
    partC_new = partC
    if side == 'L'
        # Q * C or Q' * C: C's row blocking must match A's row blocking.
        partC_new = (partA[1], partC[2])
    else
        # C * Q or C * Q': C's column blocking must match A's row blocking
        # because the kernels iterate over the k index along columns.
        partC_new = (partC[1], partA[1])
    end

    return Blocks(partA...), Blocks(partTm...), Blocks(partC_new...)
end

function pormqr!(side::Char, trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, C::Dagger.DMatrix{T}) where {T<:Number}
    partA, partTm, partC = _repartition_pormqr(A, Tm, C, side, trans)
    
    return maybe_copy_buffered(A=>partA, Tm=>partTm, C=>partC) do A, Tm, C
        return _pormqr_impl!(side, trans, A, Tm, C)
    end
end

function _pormqr_impl!(side::Char, trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, C::Dagger.DMatrix{T}) where {T<:Number}
    m, n = size(C)
    Ac = A.chunks
    Tc = Tm.chunks
    Cc = C.chunks

    Amt, Ant = size(Ac)
    Tmt, Tnt = size(Tc)
    Cmt, Cnt = size(Cc)
    minMT = min(Amt, Ant)

    Dagger.spawn_datadeps() do
        if side == 'L'
            if (trans == 'T' || trans == 'C')
                for k in 1:minMT
                    for n in 1:Cnt
                        Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[k,n]))   
                    end
                    for m in k+1:Cmt, n in 1:Cnt
                        Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[k, n]), InOut(Cc[m, n]), In(Ac[m, k]), In(Tc[m, k]))  
                    end
                end
            end
            if trans == 'N'
                for k in minMT:-1:1
                    for m in Cmt:-1:k+1, n in 1:Cnt
                        Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[k, n]), InOut(Cc[m, n]),  In(Ac[m, k]), In(Tc[m, k]))
                    end
                    for n in 1:Cnt
                        Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[k, n]))
                    end
                end
            end
        else 
            if side == 'R'
                if trans == 'T' || trans == 'C'
                    for k in minMT:-1:1
                        for n in Cmt:-1:k+1, m in 1:Cmt
                            Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[m, k]), InOut(Cc[m, n]), In(Ac[n, k]), In(Tc[n, k]))
                        end
                        for m in 1:Cmt
                            Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[m, k]))
                        end
                    end
                end
                if trans == 'N'
                    for k in 1:minMT
                        for m in 1:Cmt
                            Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[m, k]))
                        end
                        for n in k+1:Cmt, m in 1:Cmt
                            Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[m, k]), InOut(Cc[m, n]), In(Ac[n, k]), In(Tc[n, k]))
                        end
                    end
                end
            end
        end
    end
    return C
end

function cageqrf!(A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder, p::Int64=1) where {T<: Number}
    if p == 1 
        return geqrf!(A, Tm; static, traversal)
    end
    Ac = A.chunks
    mt, nt = size(Ac)
    @assert mt % p == 0 "Number of tiles must be divisible by the number of domains"
    mtd = Int64(mt/p)
    Tc = Tm.chunks
    proot = 1
    nxtmt = mtd
    trans = T <: Complex ? 'C' : 'T'
    Dagger.spawn_datadeps(;static, traversal) do
        for k in 1:min(mt, nt)
            if k > nxtmt
                proot += 1
                nxtmt += mtd
            end
            for pt in proot:p
                ibeg = 1 + (pt-1) * mtd
                if pt == proot
                    ibeg = k 
                end
                Dagger.@spawn NextLA.geqrt!(InOut(Ac[ibeg, k]), Out(Tc[ibeg,k]))
                for n in k+1:nt
                    Dagger.@spawn NextLA.unmqr!('L', trans, Deps(Ac[ibeg, k], In(LowerTriangular)), In(Tc[ibeg,k]), InOut(Ac[ibeg, n]))
                end
                for m in ibeg+1:(pt * mtd)
                    Dagger.@spawn NextLA.tsqrt!(Deps(Ac[ibeg, k], InOut(UpperTriangular)), InOut(Ac[m, k]), Out(Tc[m,k]))
                    for n in k+1:nt
                         Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Ac[ibeg, n]), InOut(Ac[m, n]), In(Ac[m, k]), In(Tc[m,k]))
                    end
                end
            end
            for m in 1:ceil(Int64, log2(p-proot+1))
                p1 = proot
                p2 = p1 + 2^(m-1)
                while p2 ≤ p
                    i1 = 1 + (p1-1) * mtd
                    i2 = 1 + (p2-1) * mtd
                    if p1 == proot
                        i1 = k
                    end
                    Dagger.@spawn NextLA.ttqrt!(Deps(Ac[i1, k], InOut(UpperTriangular)), Deps(Ac[i2, k], InOut(UpperTriangular)), Out(Tc[i2, k]))
                    for n in k+1:nt
                        Dagger.@spawn NextLA.ttmqr!('L', trans, InOut(Ac[i1, n]), InOut(Ac[i2, n]), Deps(Ac[i2, k], In(UpperTriangular)), In(Tc[i2, k]))
                    end
                    p1 += 2^m
                    p2 += 2^m
                end
            end
        end
    end
end

function geqrf!(A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder) where {T<: Number}
    Ac = A.chunks
    mt, nt = size(Ac)
    Tc = Tm.chunks
    trans = T <: Complex ? 'C' : 'T'

    Dagger.spawn_datadeps(;static, traversal) do
        for k in 1:min(mt, nt) 
            Dagger.@spawn NextLA.geqrt!(InOut(Ac[k, k]), Out(Tc[k,k]))
            for n in k+1:nt
                Dagger.@spawn NextLA.unmqr!('L', trans, Deps(Ac[k,k], In(LowerTriangular)), In(Tc[k,k]), InOut(Ac[k, n]))
            end
            for m in k+1:mt
                Dagger.@spawn NextLA.tsqrt!(Deps(Ac[k, k], InOut(UpperTriangular)), InOut(Ac[m, k]), Out(Tc[m,k])) 
                for n in k+1:nt
                    Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Ac[k, n]), InOut(Ac[m, n]), In(Ac[m, k]), In(Tc[m,k]))
                end
            end
        end
    end
end

function porgqr!(trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, Q::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder) where {T<:Number} 
    Ac = A.chunks
    Tc = Tm.chunks
    Qc = Q.chunks
    mt, nt = size(Ac)
    qmt, qnt = size(Qc)
    
    Dagger.spawn_datadeps(;static, traversal) do
        if trans == 'N'
            for k in min(mt, nt):-1:1
                for m in qmt:-1:k + 1, n in k:qnt
                        Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Qc[k, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                end
                for n in k:qnt
                    Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[k, k]), 
                    In(Tc[k, k]), InOut(Qc[k, n]))
                end
            end
        else
            for k in 1:min(mt, nt)
                for n in 1:k
                    Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[k, k]), 
                    In(Tc[k, k]), InOut(Qc[k, n]))
                end
                for m in k+1:qmt, n in 1:qnt
                        Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Qc[k, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                end
            end
        end
    end
end

function meas_ws(A::Dagger.DMatrix{T}, ib::Int64) where {T<: Number}
    mb, nb = A.partitioning.blocksize
    m, n = size(A)
    MT = (mod(m,nb)==0) ? floor(Int64, (m / mb)) : floor(Int64, (m / mb) + 1) 
    NT = (mod(n,nb)==0) ? floor(Int64,(n / nb)) : floor(Int64, (n / nb) + 1) * 2 
    lm = ib * MT;
    ln = nb * NT;
    lm, ln
end

function LinearAlgebra.qr!(A::Dagger.DMatrix{T}; ib::Int64=1, p::Int64=1) where {T<:Number}   
    lm, ln = meas_ws(A, ib)
    nb = A.partitioning.blocksize[2]
    Tm = DArray{T}(Blocks(ib, nb), undef, (lm, ln))
    cageqrf!(A, Tm; p=p)
    return QRCompactWY(A, Tm);
end


