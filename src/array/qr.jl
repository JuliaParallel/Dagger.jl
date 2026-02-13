export geqrf!, porgqr!, pormqr!, cageqrf!
import LinearAlgebra: QRCompactWY, AdjointQ, BlasFloat, QRCompactWYQ, AbstractQ, StridedVecOrMat, I
import Base.:*

# Maps Tm DArray → p (CAQR domain count) so that porgqr!/pormqr! can
# reconstruct the butterfly tree without requiring p as an argument.
const _CAQR_P_MAP = WeakKeyDict{DArray, Int}()

(*)(Q::QRCompactWYQ{T, M}, b::Number) where {T<:Number, M<:DMatrix{T}} = DMatrix(Q) * b
(*)(b::Number, Q::QRCompactWYQ{T, M}) where {T<:Number, M<:DMatrix{T}} = DMatrix(Q) * b

(*)(Q::AdjointQ{T, QRCompactWYQ{T, M, C}}, b::Number) where {T<:Number, M<:DMatrix{T}, C<:M} = DMatrix(Q) * b
(*)(b::Number, Q::AdjointQ{T, QRCompactWYQ{T, M, C}}) where {T<:Number, M<:DMatrix{T}, C<:M} = DMatrix(Q) * b

LinearAlgebra.lmul!(B::QRCompactWYQ{T, <:DMatrix{T}}, A::DMatrix{T}) where {T} = pormqr!('L', 'N', B.factors, B.T, A)
function LinearAlgebra.lmul!(B::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}, A::Dagger.DMatrix{T}) where {T}
    trans = T <: Complex ? 'C' : 'T'
    pormqr!('L', trans, B.Q.factors, B.Q.T, A)
end
function _apply_dense_qr!(side::Char, trans::Char, Q::QRCompactWYQ{T, <:Dagger.DMatrix{T}}, M::AbstractMatrix{T}) where {T}
    # Distribute M with block sizes compatible with the Q factorization.
    # _repartition_pormqr will further adjust, but we need a sane starting point.
    bs = Q.factors.partitioning.blocksize  # (mb, nb)
    if side == 'L'
        Cd = distribute(M, Blocks(bs[1], bs[1]))
    else
        Cd = distribute(M, Blocks(bs[2], bs[2]))
    end
    pormqr!(side, trans, Q.factors, Q.T, Cd)
    copyto!(M, collect(Cd))
    return M
end
LinearAlgebra.lmul!(B::QRCompactWYQ{T, <:DMatrix{T}}, A::StridedVecOrMat{T}) where {T} = _apply_dense_qr!('L', 'N', B, A)
function LinearAlgebra.lmul!(B::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}, A::StridedVecOrMat{T}) where {T}
    trans = T <: Complex ? 'C' : 'T'
    return _apply_dense_qr!('L', trans, B.Q, A)
end

LinearAlgebra.rmul!(A::Dagger.DMatrix{T}, B::QRCompactWYQ{T, <:Dagger.DMatrix{T}}) where {T} = pormqr!('R', 'N', B.factors, B.T, A)
function LinearAlgebra.rmul!(A::Dagger.DMatrix{T}, B::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}) where {T} 
    trans = T <: Complex ? 'C' : 'T'
    pormqr!('R', trans, B.Q.factors, B.Q.T, A)
end
LinearAlgebra.rmul!(A::StridedVecOrMat{T}, B::QRCompactWYQ{T, <:Dagger.DMatrix{T}}) where {T} = _apply_dense_qr!('R', 'N', B, A)
function LinearAlgebra.rmul!(A::StridedVecOrMat{T}, B::AdjointQ{<:Any, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}, <:Dagger.DMatrix{T}}}) where {T<:Union{Float32,Float64}}
    trans = 'T'
    return _apply_dense_qr!('R', trans, B.Q, A)
end
function LinearAlgebra.rmul!(A::StridedVecOrMat{T}, B::AdjointQ{<:Any, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}, <:Dagger.DMatrix{T}}}) where {T<:Union{ComplexF32,ComplexF64}}
    trans = T <: Complex ? 'C' : 'T'
    return _apply_dense_qr!('R', trans, B.Q, A)
end

"""
    _infer_caqr_p(A, Tm) -> Int

Infer the CAQR domain count `p` from the reflector matrix `A` and the
T-factor matrix `Tm`.  First checks the `_CAQR_P_MAP` cache; falls back
to heuristic detection from Tm column count.
"""
function _infer_caqr_p(A::Dagger.DMatrix, Tm::Dagger.DMatrix)
    # Check the explicit cache first
    haskey(_CAQR_P_MAP, Tm) && return _CAQR_P_MAP[Tm]
    Ant = size(A.chunks, 2)
    Tnt = size(Tm.chunks, 2)
    Tnt == Ant && return 1           # no tree T → flat QR
    return 1  # fallback — should not reach here if qr! populated the cache
end

@inline function _tile_len(cum::AbstractVector{<:Integer}, idx::Int)
    prev = idx == 1 ? 0 : cum[idx-1]
    return cum[idx] - prev
end

function _tile_lens(cum::AbstractVector{<:Integer})
    out = Vector{Int}(undef, length(cum))
    prev = 0
    for i in eachindex(cum)
        out[i] = cum[i] - prev
        prev = cum[i]
    end
    return out
end

@inline function _locate_cut(cum::AbstractVector{<:Integer}, pos::Int)
    idx = searchsortedfirst(cum, pos)
    prev = idx == 1 ? 0 : cum[idx-1]
    return idx, pos - prev
end

function _is_uniform_square_tiling(A::Dagger.DMatrix)
    rcum = A.subdomains.cumlength[1]
    ccum = A.subdomains.cumlength[2]
    (isempty(rcum) || isempty(ccum)) && return false
    r = _tile_lens(rcum)
    c = _tile_lens(ccum)
    return all(==(r[1]), r) && all(==(c[1]), c) && r[1] == c[1]
end

@inline _use_irregular_qr_tiling(A::Dagger.DMatrix) = !_is_uniform_square_tiling(A)

function _largest_square_redistribution_block(A::Dagger.DMatrix)
    m, n = size(A)
    mb, nb = A.partitioning.blocksize
    lim = min(m, n, mb, nb)
    lim <= 1 && return nothing

    g = gcd(m, n)
    b = min(g, lim)
    while b > 1
        if g % b == 0
            return b
        end
        b -= 1
    end
    return nothing
end

function _panel_steps(A::Dagger.DMatrix)
    rcum = A.subdomains.cumlength[1]
    ccum = A.subdomains.cumlength[2]
    m, n = size(A)

    steps = Vector{NTuple{7, Int}}()
    dpos = 1
    cpos = 1
    while cpos <= n && dpos <= m
        rd, lr = _locate_cut(rcum, dpos)
        kc, lc = _locate_cut(ccum, cpos)
        row_rem = rcum[rd] - dpos + 1
        col_rem = ccum[kc] - cpos + 1
        b = min(row_rem, col_rem, m - dpos + 1)
        b > 0 || break
        rend = _tile_len(rcum, rd)
        cend = lc + b - 1
        push!(steps, (rd, kc, lr, rend, lc, cend, b))
        dpos += b
        cpos += b
    end
    return steps, rcum, ccum
end

function _geqrf_irregular!(A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder) where {T<:Number}
    Ac = A.chunks
    Tc = Tm.chunks
    mt, nt = size(Ac)
    trans = T <: Complex ? 'C' : 'T'
    steps, _, ccum = _panel_steps(A)

    Dagger.spawn_datadeps(; static, traversal) do
        for (rd, kc, lr, rend, lc, cend, b) in steps
            Av = view(Ac[rd, kc], lr:rend, lc:cend)
            Tv = view(Tc[rd, kc], :, lc:cend)
            Dagger.@spawn NextLA.geqrt!(InOut(Av), InOut(Tv))

            for n in kc:nt
                tc1 = n == kc ? cend + 1 : 1
                tc2 = _tile_len(ccum, n)
                tc1 <= tc2 || continue
                Av = view(Ac[rd, kc], lr:rend, lc:cend)
                Tv = view(Tc[rd, kc], :, lc:cend)
                Cv = view(Ac[rd, n], lr:rend, tc1:tc2)
                Dagger.@spawn NextLA.unmqr!('L', trans, In(Av), In(Tv), InOut(Cv))
            end

            for m in rd+1:mt
                A1 = view(Ac[rd, kc], lr:lr+b-1, lc:cend)
                A2 = view(Ac[m, kc], :, lc:cend)
                Tv = view(Tc[m, kc], :, lc:cend)
                Dagger.@spawn NextLA.tsqrt!(InOut(A1), InOut(A2), InOut(Tv))
                for n in kc:nt
                    tc1 = n == kc ? cend + 1 : 1
                    tc2 = _tile_len(ccum, n)
                    tc1 <= tc2 || continue
                    C1 = view(Ac[rd, n], lr:lr+b-1, tc1:tc2)
                    C2 = view(Ac[m, n], :, tc1:tc2)
                    V  = view(Ac[m, kc], :, lc:cend)
                    Tv = view(Tc[m, kc], :, lc:cend)
                    Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(C1), InOut(C2), In(V), In(Tv))
                end
            end
        end
    end
    return A
end

function _porgqr_irregular!(trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, Q::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder) where {T<:Number}
    Ac = A.chunks
    Tc = Tm.chunks
    Qc = Q.chunks
    mt, _ = size(Ac)
    qmt, qnt = size(Qc)
    qmt == mt || throw(ArgumentError("Q row tiling must match A row tiling for irregular QR"))
    steps, _, _ = _panel_steps(A)

    Dagger.spawn_datadeps(; static, traversal) do
        if trans == 'N'
            for (rd, kc, lr, rend, lc, cend, b) in Iterators.reverse(steps)
                for m in qmt:-1:rd+1, n in 1:qnt
                    C1 = view(Qc[rd, n], lr:lr+b-1, :)
                    C2 = view(Qc[m, n], :, :)
                    V  = view(Ac[m, kc], :, lc:cend)
                    Tv = view(Tc[m, kc], :, lc:cend)
                    Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(C1), InOut(C2), In(V), In(Tv))
                end
                for n in 1:qnt
                    Av = view(Ac[rd, kc], lr:rend, lc:cend)
                    Tv = view(Tc[rd, kc], :, lc:cend)
                    Cv = view(Qc[rd, n], lr:rend, :)
                    Dagger.@spawn NextLA.unmqr!('L', trans, In(Av), In(Tv), InOut(Cv))
                end
            end
        else
            for (rd, kc, lr, rend, lc, cend, b) in steps
                for n in 1:qnt
                    Av = view(Ac[rd, kc], lr:rend, lc:cend)
                    Tv = view(Tc[rd, kc], :, lc:cend)
                    Cv = view(Qc[rd, n], lr:rend, :)
                    Dagger.@spawn NextLA.unmqr!('L', trans, In(Av), In(Tv), InOut(Cv))
                end
                for m in rd+1:qmt, n in 1:qnt
                    C1 = view(Qc[rd, n], lr:lr+b-1, :)
                    C2 = view(Qc[m, n], :, :)
                    V  = view(Ac[m, kc], :, lc:cend)
                    Tv = view(Tc[m, kc], :, lc:cend)
                    Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(C1), InOut(C2), In(V), In(Tv))
                end
            end
        end
    end
    return Q
end

function _pormqr_irregular!(side::Char, trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, C::Dagger.DMatrix{T}) where {T<:Number}
    Q = Dagger.DMatrix(QRCompactWYQ(A, Tm))
    Qop =
        trans == 'N' ? Q :
        trans == 'T' ? transpose(Q) :
        trans == 'C' ? adjoint(Q) :
        throw(ArgumentError("trans must be 'N', 'T', or 'C', got '$trans'"))

    updated = side == 'L' ? Qop * C :
              side == 'R' ? C * Qop :
              throw(ArgumentError("side must be 'L' or 'R', got '$side'"))
    copyto!(C, updated)
    return C
end

function Dagger.DMatrix(Q::QRCompactWYQ{T, <:Dagger.DMatrix{T}}) where {T}
    DQ = DMatrix(Q.factors.partitioning, I*one(T), size(Q))
    p = _infer_caqr_p(Q.factors, Q.T)
    porgqr!('N', Q.factors, Q.T, DQ; p=p)
    return DQ
end

function Dagger.DMatrix(AQ::AdjointQ{T, <:QRCompactWYQ{T, <:Dagger.DMatrix{T}}}) where {T}
    DQ = DMatrix(AQ.Q.factors.partitioning, I*one(T), size(AQ))
    trans = T <: Complex ? 'C' : 'T'
    p = _infer_caqr_p(AQ.Q.factors, AQ.Q.T)
    porgqr!(trans, AQ.Q.factors, AQ.Q.T, DQ; p=p)
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
        # C * Q or C * Q': Align C's column blocking with A's column blocking
        # so that the k index traverses compatible tile widths.
        partC_new = (partC[1], partA[2])
    end

    return Blocks(partA...), Blocks(partTm...), Blocks(partC_new...)
end

function pormqr!(side::Char, trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, C::Dagger.DMatrix{T}; p::Int=_infer_caqr_p(A, Tm)) where {T<:Number}
    if _use_irregular_qr_tiling(A)
        return _pormqr_irregular!(side, trans, A, Tm, C)
    end

    partA, partTm, partC = _repartition_pormqr(A, Tm, C, side, trans)
    
    return maybe_copy_buffered(A=>partA, Tm=>partTm, C=>partC) do A, Tm, C
        return _pormqr_impl!(side, trans, A, Tm, C; p=p)
    end
end

function _pormqr_impl!(side::Char, trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, C::Dagger.DMatrix{T}; p::Int=1) where {T<:Number}
    m, n = size(C)
    Ac = A.chunks
    Tc = Tm.chunks
    Cc = C.chunks

    Amt, Ant = size(Ac)
    Tmt, Tnt = size(Tc)
    Cmt, Cnt = size(Cc)
    minMT = min(Amt, Ant)
    has_tree = Tnt > Ant   # CAQR tree T present
    Tc2_offset = has_tree ? Tnt ÷ 2 : 0
    mtd = p > 1 ? Amt ÷ p : Amt   # tiles per domain

    # ordering rule:
    # tree_first = (side == Left) == (trans == NoTrans)
    # When tree_first, apply ttmqr (descend: root→leaves) BEFORE local ops;
    # otherwise, apply local ops first, then ttmqr (ascend: leaves→root).

    Dagger.spawn_datadeps() do
        if side == 'L'
            if (trans == 'T' || trans == 'C')
                # Left, ConjTrans: unmqr first, then ttmqr ascending.  k forward.
                for k in 1:minMT
                    proot = p > 1 ? ((k - 1) ÷ mtd) + 1 : 1

                    # Domain-local reflectors: unmqr then tsmqr within each domain
                    for pt in proot:p
                        ibeg = pt == proot ? k : 1 + (pt - 1) * mtd
                        iend = pt * mtd
                        for n in 1:Cnt
                            Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[ibeg, k]), In(Tc[ibeg, k]), InOut(Cc[ibeg,n]))   
                        end
                        for m in ibeg+1:iend, n in 1:Cnt
                            Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[ibeg, n]), InOut(Cc[m, n]), In(Ac[m, k]), In(Tc[m, k]))  
                        end
                    end

                    # Tree TT reduction (ascend: leaves→root)
                    if has_tree && p > 1
                        max_level = ceil(Int, log2(p - proot + 1))
                        for m_level in 1:max_level
                            p1 = proot
                            p2 = p1 + 2^(m_level - 1)
                            while p2 ≤ p
                                i1 = 1 + (p1 - 1) * mtd
                                i2 = 1 + (p2 - 1) * mtd
                                if p1 == proot
                                    i1 = k
                                end
                                for n in 1:Cnt
                                    Dagger.@spawn NextLA.ttmqr!(side, trans, InOut(Cc[i1, n]), InOut(Cc[i2, n]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                                end
                                p1 += 2^m_level
                                p2 += 2^m_level
                            end
                        end
                    end
                end
            end
            if trans == 'N'
                # Left, NoTrans: ttmqr descending first, then tsmqr+unmqr.  k reverse.
                for k in minMT:-1:1
                    proot = p > 1 ? ((k - 1) ÷ mtd) + 1 : 1

                    # Tree TT reduction (descend: root→leaves)
                    if has_tree && p > 1
                        max_level = ceil(Int, log2(p - proot + 1))
                        for m_level in max_level:-1:1
                            p1 = proot
                            p2 = p1 + 2^(m_level - 1)
                            while p2 ≤ p
                                i1 = 1 + (p1 - 1) * mtd
                                i2 = 1 + (p2 - 1) * mtd
                                if p1 == proot
                                    i1 = k
                                end
                                for n in 1:Cnt
                                    Dagger.@spawn NextLA.ttmqr!(side, trans, InOut(Cc[i1, n]), InOut(Cc[i2, n]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                                end
                                p1 += 2^m_level
                                p2 += 2^m_level
                            end
                        end
                    end

                    # Domain-local reflectors: tsmqr then unmqr within each domain (reverse)
                    for pt in p:-1:proot
                        ibeg = pt == proot ? k : 1 + (pt - 1) * mtd
                        iend = pt * mtd
                        for m in iend:-1:ibeg+1, n in 1:Cnt
                            Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[ibeg, n]), InOut(Cc[m, n]),  In(Ac[m, k]), In(Tc[m, k]))
                        end
                        for n in 1:Cnt
                            Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[ibeg, k]), In(Tc[ibeg, k]), InOut(Cc[ibeg, n]))
                        end
                    end
                end
            end
        else
            if side == 'R'
                nmax = min(Cnt, Ant)
                if trans == 'T' || trans == 'C'
                    # Right, ConjTrans: ttmqr descending first, then tsmqr+unmqr.  k reverse.
                    for k in minMT:-1:1
                        proot = p > 1 ? ((k - 1) ÷ mtd) + 1 : 1

                        # Tree TT reduction (descend: root→leaves)
                        if has_tree && p > 1
                            max_level = ceil(Int, log2(p - proot + 1))
                            for m_level in max_level:-1:1
                                p1 = proot
                                p2 = p1 + 2^(m_level - 1)
                                while p2 ≤ p
                                    i1 = 1 + (p1 - 1) * mtd
                                    i2 = 1 + (p2 - 1) * mtd
                                    if p1 == proot
                                        i1 = k
                                    end
                                    for m in 1:Cmt
                                        Dagger.@spawn NextLA.ttmqr!(side, trans, InOut(Cc[m, i1]), InOut(Cc[m, i2]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                                    end
                                    p1 += 2^m_level
                                    p2 += 2^m_level
                                end
                            end
                        end

                        # Domain-local reflectors: tsmqr then unmqr within each domain (reverse)
                        for pt in p:-1:proot
                            ibeg = pt == proot ? k : 1 + (pt - 1) * mtd
                            iend = pt * mtd
                            for n in iend:-1:ibeg+1
                                for m in 1:Cmt
                                    Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[m, ibeg]), InOut(Cc[m, n]), In(Ac[n, k]), In(Tc[n, k]))
                                end
                            end
                            for m in 1:Cmt
                                Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[ibeg, k]), In(Tc[ibeg, k]), InOut(Cc[m, ibeg]))
                            end
                        end
                    end
                end
                if trans == 'N'
                    # Right, NoTrans: unmqr+tsmqr first, then ttmqr ascending.  k forward.
                    for k in 1:minMT
                        proot = p > 1 ? ((k - 1) ÷ mtd) + 1 : 1

                        # Domain-local reflectors: unmqr then tsmqr within each domain
                        for pt in proot:p
                            ibeg = pt == proot ? k : 1 + (pt - 1) * mtd
                            iend = pt * mtd
                            for m in 1:Cmt
                                Dagger.@spawn NextLA.unmqr!(side, trans, In(Ac[ibeg, k]), In(Tc[ibeg, k]), InOut(Cc[m, ibeg]))
                            end
                            for n in ibeg+1:iend
                                for m in 1:Cmt
                                    Dagger.@spawn NextLA.tsmqr!(side, trans, InOut(Cc[m, ibeg]), InOut(Cc[m, n]), In(Ac[n, k]), In(Tc[n, k]))
                                end
                            end
                        end

                        # Tree TT reduction (ascend: leaves→root)
                        if has_tree && p > 1
                            max_level = ceil(Int, log2(p - proot + 1))
                            for m_level in 1:max_level
                                p1 = proot
                                p2 = p1 + 2^(m_level - 1)
                                while p2 ≤ p
                                    i1 = 1 + (p1 - 1) * mtd
                                    i2 = 1 + (p2 - 1) * mtd
                                    if p1 == proot
                                        i1 = k
                                    end
                                    for m in 1:Cmt
                                        Dagger.@spawn NextLA.ttmqr!(side, trans, InOut(Cc[m, i1]), InOut(Cc[m, i2]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                                    end
                                    p1 += 2^m_level
                                    p2 += 2^m_level
                                end
                            end
                        end
                    end
                end
            end
        end
    end
    return C
end

function cageqrf!(A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder, p::Int=1) where {T<: Number}
    if p == 1 
        return geqrf!(A, Tm; static, traversal)
    end

    _use_irregular_qr_tiling(A) && throw(ArgumentError("p > 1 requires uniform square tiling"))

    Ac = A.chunks
    mt, nt = size(Ac)
    @assert mt % p == 0 "Number of tiles must be divisible by the number of domains"
    mtd = Int(mt/p)
    Tc = Tm.chunks
    # Tree reduction T matrices are stored in the second half of Tm's
    # columns (columns nt+1 : 2*nt),
    Tc2_offset = size(Tc, 2) ÷ 2   # == nt
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
                    Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[ibeg, k]), In(Tc[ibeg,k]), InOut(Ac[ibeg, n]))
                end
                for m in ibeg+1:(pt * mtd)
                    Dagger.@spawn NextLA.tsqrt!(InOut(Ac[ibeg, k]), InOut(Ac[m, k]), Out(Tc[m,k]))
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
                    Dagger.@spawn NextLA.ttqrt!(InOut(Ac[i1, k]), InOut(Ac[i2, k]), Out(Tc[i2, k + Tc2_offset]))
                    for n in k+1:nt
                        Dagger.@spawn NextLA.ttmqr!('L', trans, InOut(Ac[i1, n]), InOut(Ac[i2, n]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                    end
                    p1 += 2^m
                    p2 += 2^m
                end
            end
        end
    end
end

function geqrf!(A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder) where {T<: Number}
    if _use_irregular_qr_tiling(A)
        return _geqrf_irregular!(A, Tm; static, traversal)
    end

    Ac = A.chunks
    mt, nt = size(Ac)
    Tc = Tm.chunks
    trans = T <: Complex ? 'C' : 'T'

    Dagger.spawn_datadeps(;static, traversal) do
        for k in 1:min(mt, nt) 
            Dagger.@spawn NextLA.geqrt!(InOut(Ac[k, k]), Out(Tc[k,k]))
            for n in k+1:nt
                Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[k,k]), In(Tc[k,k]), InOut(Ac[k, n]))
            end
            for m in k+1:mt
                Dagger.@spawn NextLA.tsqrt!(InOut(Ac[k, k]), InOut(Ac[m, k]), Out(Tc[m,k])) 
                for n in k+1:nt
                    Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Ac[k, n]), InOut(Ac[m, n]), In(Ac[m, k]), In(Tc[m,k]))
                end
            end
        end
    end
end

function porgqr!(trans::Char, A::Dagger.DMatrix{T}, Tm::Dagger.DMatrix{T}, Q::Dagger.DMatrix{T}; static::Bool=true, traversal::Symbol=:inorder, p::Int=_infer_caqr_p(A, Tm)) where {T<:Number} 
    if _use_irregular_qr_tiling(A)
        return _porgqr_irregular!(trans, A, Tm, Q; static, traversal)
    end

    Ac = A.chunks
    Tc = Tm.chunks
    Qc = Q.chunks
    mt, nt = size(Ac)
    qmt, qnt = size(Qc)
    has_tree = size(Tc, 2) > nt   # CAQR tree T present
    Tc2_offset = has_tree ? size(Tc, 2) ÷ 2 : 0
    mtd = p > 1 ? mt ÷ p : mt    # tiles per domain
    
    Dagger.spawn_datadeps(;static, traversal) do
        if has_tree && p > 1
            if trans == 'N'
                # Build Q with CAQR tree:
                # 1) apply tree nodes (root->leaves), 2) apply per-domain local blocks.
                for k in min(mt, nt):-1:1
                    proot = ((k - 1) ÷ mtd) + 1

                    max_level = ceil(Int, log2(p - proot + 1))
                    for m_level in max_level:-1:1
                        p1 = proot
                        p2 = p1 + 2^(m_level - 1)
                        while p2 ≤ p
                            i1 = 1 + (p1 - 1) * mtd
                            i2 = 1 + (p2 - 1) * mtd
                            if p1 == proot
                                i1 = k
                            end
                            for n in k:qnt
                                Dagger.@spawn NextLA.ttmqr!('L', trans, InOut(Qc[i1, n]), InOut(Qc[i2, n]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                            end
                            p1 += 2^m_level
                            p2 += 2^m_level
                        end
                    end

                    # Local reflectors are domain-local (not global k+1:mt).
                    # Column range must start at k (not ibeg) because the
                    # preceding tree ttmqr step already coupled rows from
                    # different domains, making Q[ibeg, k:ibeg-1] nonzero.
                    for pt in p:-1:proot
                        ibeg = pt == proot ? k : 1 + (pt - 1) * mtd
                        iend = pt * mtd
                        for m in iend:-1:ibeg+1, n in k:qnt
                            Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Qc[ibeg, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                        end
                        for n in k:qnt
                            Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[ibeg, k]), In(Tc[ibeg, k]), InOut(Qc[ibeg, n]))
                        end
                    end
                end
            else
                # Build Q' with CAQR tree:
                # 1) apply per-domain local blocks, 2) apply tree nodes (leaves->root).
                for k in 1:min(mt, nt)
                    proot = ((k - 1) ÷ mtd) + 1

                    for pt in proot:p
                        ibeg = pt == proot ? k : 1 + (pt - 1) * mtd
                        iend = pt * mtd
                        for n in 1:qnt
                            Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[ibeg, k]), In(Tc[ibeg, k]), InOut(Qc[ibeg, n]))
                        end
                        for m in ibeg+1:iend, n in 1:qnt
                            Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Qc[ibeg, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                        end
                    end

                    max_level = ceil(Int, log2(p - proot + 1))
                    for m_level in 1:max_level
                        p1 = proot
                        p2 = p1 + 2^(m_level - 1)
                        while p2 ≤ p
                            i1 = 1 + (p1 - 1) * mtd
                            i2 = 1 + (p2 - 1) * mtd
                            if p1 == proot
                                i1 = k
                            end
                            for n in 1:qnt
                                Dagger.@spawn NextLA.ttmqr!('L', trans, InOut(Qc[i1, n]), InOut(Qc[i2, n]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                            end
                            p1 += 2^m_level
                            p2 += 2^m_level
                        end
                    end
                end
            end
            return
        end

        if trans == 'N'
            # Building Q: Left/NoTrans. ordering: ttmqr first (descend),
            # then local tsmqr + unmqr.  Sweep k in reverse.
            for k in min(mt, nt):-1:1
                proot = p > 1 ? ((k - 1) ÷ mtd) + 1 : 1

                # --- Tree TT reduction (descend: root→leaves) ---
                if has_tree && p > 1
                    max_level = ceil(Int, log2(p - proot + 1))
                    for m_level in max_level:-1:1
                        p1 = proot
                        p2 = p1 + 2^(m_level - 1)
                        while p2 ≤ p
                            i1 = 1 + (p1 - 1) * mtd
                            i2 = 1 + (p2 - 1) * mtd
                            if p1 == proot
                                i1 = k
                            end
                            for n in k:qnt
                                Dagger.@spawn NextLA.ttmqr!('L', trans, InOut(Qc[i1, n]), InOut(Qc[i2, n]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                            end
                            p1 += 2^m_level
                            p2 += 2^m_level
                        end
                    end
                end

                # --- Local: tsmqr then unmqr (per domain, reverse) ---
                for m in qmt:-1:k + 1, n in k:qnt
                    Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Qc[k, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                end
                for n in k:qnt
                    Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[k, k]),
                    In(Tc[k, k]), InOut(Qc[k, n]))
                end
            end
        else
            # Building Q': Left/ConjTrans.  ordering: local unmqr + tsmqr
            # first, then ttmqr (ascend: leaves→root).  Sweep k forward.
            for k in 1:min(mt, nt)
                proot = p > 1 ? ((k - 1) ÷ mtd) + 1 : 1

                # --- Local: unmqr then tsmqr ---
                for n in 1:qnt
                    Dagger.@spawn NextLA.unmqr!('L', trans, In(Ac[k, k]),
                    In(Tc[k, k]), InOut(Qc[k, n]))
                end
                for m in k+1:qmt, n in 1:qnt
                    Dagger.@spawn NextLA.tsmqr!('L', trans, InOut(Qc[k, n]), InOut(Qc[m, n]), In(Ac[m, k]), In(Tc[m, k]))
                end

                # --- Tree TT reduction (ascend: leaves→root) ---
                if has_tree && p > 1
                    max_level = ceil(Int, log2(p - proot + 1))
                    for m_level in 1:max_level
                        p1 = proot
                        p2 = p1 + 2^(m_level - 1)
                        while p2 ≤ p
                            i1 = 1 + (p1 - 1) * mtd
                            i2 = 1 + (p2 - 1) * mtd
                            if p1 == proot
                                i1 = k
                            end
                            for n in 1:qnt
                                Dagger.@spawn NextLA.ttmqr!('L', trans, InOut(Qc[i1, n]), InOut(Qc[i2, n]), In(Ac[i2, k]), In(Tc[i2, k + Tc2_offset]))
                            end
                            p1 += 2^m_level
                            p2 += 2^m_level
                        end
                    end
                end
            end
        end
    end
end

function meas_ws(A::Dagger.DMatrix{T}, ib::Int) where {T<: Number}
    mb, nb = A.partitioning.blocksize
    m, n = size(A)
    MT = cld(m, mb)
    NT = cld(n, nb)
    lm = ib * MT
    ln = nb * NT
    lm, ln
end

function _qr_impl!(A::Dagger.DMatrix{T}; ib::Int=1, p::Int=1) where {T<:Number}
    p >= 1 || throw(ArgumentError("p must be >= 1, got $p"))
    lm, ln = meas_ws(A, ib)
    nb = A.partitioning.blocksize[2]
    irregular = _use_irregular_qr_tiling(A)

    Tm_cols = p > 1 ? 2 * ln : ln
    Tm = zeros(Blocks(ib, nb), T, (lm, Tm_cols))

    if irregular
        # Hierarchical irregular path (no CAQR tree metadata).
        _geqrf_irregular!(A, Tm)
    elseif p == 1
        geqrf!(A, Tm)
    else
        _CAQR_P_MAP[Tm] = p
        cageqrf!(A, Tm; p=p)
    end
    return QRCompactWY(A, Tm)
end

function LinearAlgebra.qr!(A::Dagger.DMatrix{T}; ib::Int=1, p::Int=1) where {T<:Number}
    p >= 1 || throw(ArgumentError("p must be >= 1, got $p"))

    if !_use_irregular_qr_tiling(A)
        return _qr_impl!(A; ib=ib, p=p)
    end

    # Non-square or non-uniform tiling: try to repartition to uniform square
    # tiles first. If impossible, run the irregular hierarchical path.
    b = _largest_square_redistribution_block(A)
    if b !== nothing
        part = Blocks(b, b)
        return maybe_copy_buffered(A=>part) do Abuf
            _qr_impl!(Abuf; ib=ib, p=p)
        end
    end

    return _qr_impl!(A; ib=ib, p=1)
end
