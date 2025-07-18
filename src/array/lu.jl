LinearAlgebra.lu(A::DMatrix{T}, pivot::Union{LinearAlgebra.RowMaximum,LinearAlgebra.NoPivot} = LinearAlgebra.RowMaximum(); check::Bool=true) where {T<:LinearAlgebra.BlasFloat} = LinearAlgebra.lu(A, pivot; check=check)

LinearAlgebra.lu!(A::DMatrix{T}, pivot::Union{LinearAlgebra.RowMaximum,LinearAlgebra.NoPivot} = LinearAlgebra.RowMaximum(); check::Bool=true) where {T<:LinearAlgebra.BlasFloat} = LinearAlgebra.lu(A, pivot; check=check)

function LinearAlgebra.lu(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool = true) where {T<:LinearAlgebra.BlasFloat}
    A_copy = LinearAlgebra._lucopy(A, LinearAlgebra.lutype(T))
    return LinearAlgebra.lu!(A_copy, LinearAlgebra.NoPivot(); check=check)
end
function LinearAlgebra.lu!(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool = true) where {T<:LinearAlgebra.BlasFloat}
  
    check && LinearAlgebra.LAPACK.chkfinite(A)

    zone = one(T)
    mzone = -one(T)

    mb, nb = A.partitioning.blocksize

    if mb != nb 
        mb = nb = min(mb, nb)
        A = maybe_copy_buffered(A => Blocks(nb, nb)) do A
            A
        end
    end

    Ac = A.chunks
    mt, nt = size(Ac)

    info = 0

    Dagger.spawn_datadeps() do
        for k in range(1, min(mt, nt))
            Dagger.@spawn LinearAlgebra.generic_lufact!(InOut(Ac[k, k]), LinearAlgebra.NoPivot(); check=check, allowsingular=allowsingular)
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

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, info)
end


function searchmax_panel!(piv_idx::AbstractVector{Int}, piv_val::AbstractVector{T}, A::AbstractMatrix{T}, p::Int, offset::Int=1) where T
    max_idx = LinearAlgebra.BLAS.iamax(view(A, offset:size(A,1),p))
    piv_idx[1] = offset-1+max_idx
    piv_val[1] = A[offset-1+max_idx,p]
end

function update_ipiv!(ipivl::AbstractVector{Int}, info::Ref{Int}, piv_idx::AbstractVector{Int}, piv_val::AbstractVector{T}, p::Int, k::Int, nb::Int) where T
    max_piv_idx = LinearAlgebra.BLAS.iamax(piv_val[k:end])
    max_piv_val = piv_val[k-1 + max_piv_idx]
    abs_max_piv_val = max_piv_val isa Real ? abs(max_piv_val) : abs(real(max_piv_val)) + abs(imag(max_piv_val))    
    if isapprox(abs_max_piv_val, zero(T); atol=eps(real(T)))
        info[] = k
    end
    ipivl[p] = (max_piv_idx+k-2)*nb + piv_idx[k-1 + max_piv_idx]
end

function swaprows_panel!(A::AbstractMatrix{T}, M::AbstractMatrix{T}, ipivl::AbstractVector{Int}, m::Int, p::Int, nb::Int) where T
    q = div(ipivl[p]-1,nb) + 1
    r = (ipivl[p]-1)%nb+1
    if m == q
        A[p,:], M[r,:] = M[r,:], A[p,:]
    end
end

function update_panel!(M::AbstractMatrix{T}, A::AbstractMatrix{T}, p::Int, offset::Int=1) where T
    Acinv = one(T) / A[p,p]
    LinearAlgebra.BLAS.scal!(Acinv, view(M,offset:size(M,1),p))
    LinearAlgebra.BLAS.ger!(-one(T), view(M,offset:size(M,1),p), conj.(view(A,p,p+1:size(A,2))), view(M,offset:size(M,1),p+1:size(M,2)))
end

function swaprows_trail!(A::AbstractMatrix{T}, M::AbstractMatrix{T}, ipiv::AbstractVector{Int}, m::Int, nb::Int) where T
    for p in eachindex(ipiv)
        q = div(ipiv[p]-1,nb) + 1
        r = (ipiv[p]-1)%nb+1
        if m == q
            A[p,:], M[r,:] = M[r,:], A[p,:]
        end
    end
end


function LinearAlgebra.lu(A::DMatrix{T}, ::LinearAlgebra.RowMaximum; check::Bool = true) where {T<:LinearAlgebra.BlasFloat}
    A_copy = LinearAlgebra._lucopy(A, LinearAlgebra.lutype(T))
    return LinearAlgebra.lu!(A_copy, LinearAlgebra.RowMaximum(); check=check)
end
function LinearAlgebra.lu!(A::DMatrix{T}, ::LinearAlgebra.RowMaximum; check::Bool = true) where {T<:LinearAlgebra.BlasFloat}
    
    check && LinearAlgebra.LAPACK.chkfinite(A)

    zone = one(T)
    mzone = -one(T)

    mb, nb = A.partitioning.blocksize

    if mb != nb 
        mb = nb = min(mb, nb)
        A = maybe_copy_buffered(A => Blocks(nb, nb)) do A
            A
        end
    end

    Ac = A.chunks
    mt, nt = size(Ac)
    m,  n  = size(A)

    ipiv = DVector(collect(1:min(m, n)), Blocks(nb))
    ipivc = ipiv.chunks

    info = Ref(0)

    max_piv_idx = zeros(Int,mt)
    max_piv_val = zeros(T, mt)

    Dagger.spawn_datadeps() do
        for k in 1:min(mt, nt)
            for p in 1:min(nb, m-(k-1)*nb, n-(k-1)*nb)
                Dagger.@spawn searchmax_panel!(Out(view(max_piv_idx,k:k)), Out(view(max_piv_val,k:k)), In(Ac[k,k]), p, p)
                for i in k+1:mt
                    Dagger.@spawn searchmax_panel!(Out(view(max_piv_idx,i:i)), Out(view(max_piv_val,i:i)), In(Ac[i,k]), p)
                end
                Dagger.@spawn update_ipiv!(InOut(ipivc[k]), InOut(info), In(max_piv_idx), In(max_piv_val), p, k, nb)
                for i in k:mt
                    Dagger.@spawn swaprows_panel!(InOut(Ac[k, k]), InOut(Ac[i, k]), In(ipivc[k]), i, p, nb)
                end
                Dagger.@spawn update_panel!(InOut(Ac[k,k]), In(Ac[k,k]), p, p+1)
                for i in k+1:mt
                    Dagger.@spawn update_panel!(InOut(Ac[i, k]), In(Ac[k,k]), p)
                end
            end
            for j in Iterators.flatten((1:k-1, k+1:nt))
                for i in k:mt
                    Dagger.@spawn swaprows_trail!(InOut(Ac[k, j]), InOut(Ac[i, j]), In(ipivc[k]), i, mb)
                end
            end
            for j in k+1:nt
                Dagger.@spawn BLAS.trsm!('L', 'L', 'N', 'U', zone, In(Ac[k, k]), InOut(Ac[k, j]))
                for i in k+1:mt
                    Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(Ac[i, k]), In(Ac[k, j]), zone, InOut(Ac[i, j]))
                end
            end
        end
    end

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, info[])
end