export geqrf!, porgqr!, pormqr!, cageqrf!
import LinearAlgebra: QRCompactWY, AdjointQ, BlasFloat, QRCompactWYQ, AbstractQ, StridedVecOrMat, I
import Base.:*
include("coreblas/coreblas_ormqr.jl")
include("coreblas/coreblas_ttqrt.jl")
include("coreblas/coreblas_ttmqr.jl")
include("coreblas/coreblas_geqrt.jl")
include("coreblas/coreblas_tsqrt.jl")
include("coreblas/coreblas_tsmqr.jl")

(*)(Q::QRCompactWYQ{T, M}, b::Number) where {T<:Number, M<:DMatrix{T}} = DMatrix(Q) * b
(*)(b::Number, Q::QRCompactWYQ{T, M}) where {T<:Number, M<:DMatrix{T}} = DMatrix(Q) * b

(*)(Q::AdjointQ{T, QRCompactWYQ{T, M, C}}, b::Number) where {T<:Number, M<:DMatrix{T}, C<:LowerTrapezoidal{T, M}} = DMatrix(Q) * b
(*)(b::Number, Q::AdjointQ{T, QRCompactWYQ{T, M, C}}) where {T<:Number, M<:DMatrix{T}, C<:LowerTrapezoidal{T, M}} = DMatrix(Q) * b

LinearAlgebra.lmul!(B::QRCompactWYQ{T, M}, A::M) where {T, M<:DMatrix{T}} = pormqr!('L', 'N', B.factors, B.T, A)
function LinearAlgebra.lmul!(B::AdjointQ{T, <:QRCompactWYQ{T, M}}, A::M) where {T, M<:Dagger.DMatrix{T}}
    trans = T <: Complex ? 'C' : 'T'
    pormqr!('L', trans, B.Q.factors, B.Q.T, A)
end

LinearAlgebra.rmul!(A::Dagger.DMatrix{T}, B::QRCompactWYQ{T, M}) where {T, M<:Dagger.DMatrix{T}} = pormqr!('R', 'N', B.factors, B.T, A)
function LinearAlgebra.rmul!(A::Dagger.DArray{T,2}, B::AdjointQ{T, <:QRCompactWYQ{T, M}}) where {T, M<:Dagger.DMatrix{T}} 
    trans = T <: Complex ? 'C' : 'T'
    pormqr!('R', trans, B.Q.factors, B.Q.T, A)
end

function Dagger.DMatrix(Q::QRCompactWYQ{T, <:Dagger.DArray{T, 2}}) where {T}
    DQ = distribute(Matrix(I*one(T), size(Q.factors)[1], size(Q.factors)[1]), Q.factors.partitioning)
    porgqr!('N', Q.factors, Q.T, DQ)
    return DQ
end

function Dagger.DMatrix(AQ::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DArray{T, 2}}}) where {T}
    DQ = distribute(Matrix(I*one(T), size(AQ.Q.factors)[1], size(AQ.Q.factors)[1]), AQ.Q.factors.partitioning)
    trans = T <: Complex ? 'C' : 'T'
    porgqr!(trans, AQ.Q.factors, AQ.Q.T, DQ)
    return DQ
end

Base.collect(Q::QRCompactWYQ{T, <:Dagger.DArray{T, 2}}) where {T} = collect(Dagger.DMatrix(Q))
Base.collect(AQ::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DArray{T, 2}}}) where {T} = collect(Dagger.DMatrix(AQ))

function pormqr!(side::Char, trans::Char, A::Dagger.DArray{T, 2}, Tm::LowerTrapezoidal{T, <:Dagger.DMatrix{T}}, C::Dagger.DArray{T, 2}) where {T<:Number}
    m, n = size(C)
    Ac = A.chunks
    Tc = Tm.data.chunks
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
                        Dagger.@spawn coreblas_ormqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[k,n]))   
                    end
                    for m in k+1:Cmt, n in 1:Cnt
                        Dagger.@spawn coreblas_tsmqr!(side, trans, InOut(Cc[k, n]), InOut(Cc[m, n]), In(Ac[m, k]), In(Tc[m, k]))  
                    end
                end
            end
            if trans == 'N'
                for k in minMT:-1:1
                    for m in Cmt:-1:k+1, n in 1:Cnt
                        Dagger.@spawn coreblas_tsmqr!(side, trans, InOut(Cc[k, n]), InOut(Cc[m, n]),  In(Ac[m, k]), In(Tc[m, k]))
                    end
                    for n in 1:Cnt
                        Dagger.@spawn coreblas_ormqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[k, n]))
                    end
                end
            end
        else 
            if side == 'R'
                if trans == 'T' || trans == 'C'
                    for k in minMT:-1:1
                        for n in Cmt:-1:k+1, m in 1:Cmt
                            Dagger.@spawn coreblas_tsmqr!(side, trans, InOut(Cc[m, k]), InOut(Cc[m, n]), In(Ac[n, k]), In(Tc[n, k]))
                        end
                        for m in 1:Cmt
                            Dagger.@spawn coreblas_ormqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[m, k]))
                        end
                    end
                end
                if trans == 'N'
                    for k in 1:minMT
                        for m in 1:Cmt
                            Dagger.@spawn coreblas_ormqr!(side, trans, In(Ac[k, k]), In(Tc[k, k]), InOut(Cc[m, k]))
                        end
                        for n in k+1:Cmt, m in 1:Cmt
                            Dagger.@spawn coreblas_tsmqr!(side, trans, InOut(Cc[m, k]), InOut(Cc[m, n]), In(Ac[n, k]), In(Tc[n, k]))
                        end
                    end
                end
            end
        end
    end
    return C
end

function cageqrf!(A::Dagger.DArray{T, 2}, Tm::LowerTrapezoidal{T, <:Dagger.DMatrix{T}}; static::Bool=true, traversal::Symbol=:inorder, p::Int64=1) where {T<: Number}
    if p == 1 
        return geqrf!(A, Tm; static, traversal)
    end
    Ac = A.chunks
    mt, nt = size(Ac)
    @assert mt % p == 0 "Number of tiles must be divisible by the number of domains"
    mtd = Int64(mt/p)
    Tc = Tm.data.chunks
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
                Dagger.@spawn coreblas_geqrt!(InOut(Ac[ibeg, k]), Out(Tc[ibeg,k]))
                for n in k+1:nt
                    Dagger.@spawn coreblas_ormqr!('L', trans, In(Ac[ibeg, k]), In(Tc[ibeg,k]), InOut(Ac[ibeg, n]))
                end
                for m in ibeg+1:(pt * mtd)
                    Dagger.@spawn coreblas_tsqrt!(InOut(Ac[ibeg, k]), InOut(Ac[m, k]), Out(Tc[m,k]))
                    for n in k+1:nt
                         Dagger.@spawn coreblas_tsmqr!('L', trans, InOut(Ac[ibeg, n]), InOut(Ac[m, n]), In(Ac[m, k]), In(Tc[m,k]))
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
                    Dagger.@spawn coreblas_ttqrt!(InOut(Ac[i1, k]), InOut(Ac[i2, k]), Out(Tc[i2, k]))
                    for n in k+1:nt
                        Dagger.@spawn coreblas_ttmqr!('L', trans, InOut(Ac[i1, n]), InOut(Ac[i2, n]), In(Ac[i2, k]), In(Tc[i2, k]))
                    end
                    p1 += 2^m
                    p2 += 2^m
                end
            end
        end
    end
end

function geqrf!(A::Dagger.DArray{T, 2}, Tm::LowerTrapezoidal{T, <:Dagger.DMatrix{T}}; static::Bool=true, traversal::Symbol=:inorder) where {T<: Number}
    Ac = A.chunks
    mt, nt = size(Ac)
    Tc = Tm.data.chunks
    trans = T <: Complex ? 'C' : 'T'

    Ccopy = Dagger.DArray{T}(undef, A.partitioning, A.partitioning.blocksize[1], min(mt, nt) * A.partitioning.blocksize[2])
    Cc = Ccopy.chunks
    Dagger.spawn_datadeps(;static, traversal) do
        for k in 1:min(mt, nt) 
            Dagger.@spawn coreblas_geqrt!(InOut(Ac[k, k]), Out(Tc[k,k]))
            # FIXME: This is a hack to avoid aliasing
            Dagger.@spawn copyto!(InOut(Cc[1,k]), In(Ac[k, k]))
            for n in k+1:nt
                #FIXME: Change Cc[1,k] to upper triangular of Ac[k,k]
                Dagger.@spawn coreblas_ormqr!('L', trans, In(Cc[1, k]), In(Tc[k,k]), InOut(Ac[k, n]))
            end
            for m in k+1:mt
                Dagger.@spawn coreblas_tsqrt!(InOut(Ac[k, k]), InOut(Ac[m, k]), Out(Tc[m,k])) 
                for n in k+1:nt
                    Dagger.@spawn coreblas_tsmqr!('L', trans, InOut(Ac[k, n]), InOut(Ac[m, n]), In(Ac[m, k]), In(Tc[m,k]))
                end
            end
        end
    end
end

function porgqr!(trans::Char, A::Dagger.DArray{T, 2}, Tm::LowerTrapezoidal{T, <:Dagger.DMatrix{T}}, Q::Dagger.DArray{T, 2}; static::Bool=true, traversal::Symbol=:inorder) where {T<:Number} 
    Ac = A.chunks
    Tc = Tm.data.chunks
    Qc = Q.chunks
    mt, nt = size(Ac)
    qmt, qnt = size(Qc)
    
    Dagger.spawn_datadeps(;static, traversal) do
        if trans == 'N'
            for k in min(mt, nt):-1:1
                for m in qmt:-1:k + 1, n in k:qnt
                        Dagger.@spawn coreblas_tsmqr!('L', trans, InOut(Qc[k, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                end
                for n in k:qnt
                    Dagger.@spawn coreblas_ormqr!('L', trans, In(Ac[k, k]), 
                    In(Tc[k, k]), InOut(Qc[k, n]))
                end
            end
        else
            for k in 1:min(mt, nt)
                for n in 1:k
                    Dagger.@spawn coreblas_ormqr!('L', trans, In(Ac[k, k]), 
                    In(Tc[k, k]), InOut(Qc[k, n]))
                end
                for m in k+1:qmt, n in 1:qnt
                        Dagger.@spawn coreblas_tsmqr!('L', trans, InOut(Qc[k, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                end
            end
        end
    end
end

function meas_ws(A::Dagger.DArray{T, 2}, ib::Int64) where {T<: Number}
    mb, nb = A.partitioning.blocksize
    m, n = size(A)
    MT = (mod(m,nb)==0) ? floor(Int64, (m / mb)) : floor(Int64, (m / mb) + 1) 
    NT = (mod(n,nb)==0) ? floor(Int64,(n / nb)) : floor(Int64, (n / nb) + 1) * 2 
    lm = ib * MT;
    ln = nb * NT;
    lm, ln
end

function LinearAlgebra.qr!(A::Dagger.DArray{T, 2}; ib::Int64=1, p::Int64=1) where {T<:Number}   
    lm, ln = meas_ws(A, ib)
    Ac = A.chunks
    nb = A.partitioning.blocksize[2]
    mt, nt = size(Ac)
    st = nb * (nt - 1)
    Tm = LowerTrapezoidal(zeros, Blocks(ib, nb), T, st, lm, ln)
    geqrf!(A, Tm)
    return QRCompactWY(A, Tm);
end


