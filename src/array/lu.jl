LinearAlgebra.lu!(A::DMatrix{T}, pivot::Union{LinearAlgebra.RowMaximum,LinearAlgebra.NoPivot} = LinearAlgebra.RowMaximum(); check::Bool=true, allowsingular::Bool=false) where {T<:LinearAlgebra.BlasFloat} = LinearAlgebra.lu(A, pivot; check=check, allowsingular=allowsingular)

function LinearAlgebra.lu(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
    A_copy = LinearAlgebra._lucopy(A, LinearAlgebra.lutype(T))
    return LinearAlgebra.lu!(A_copy, LinearAlgebra.NoPivot(); check=check)
end
function LinearAlgebra.lu!(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
  
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

    check && LinearAlgebra._check_lu_success(info, allowsingular)

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, info)
end


function LinearAlgebra.LAPACK.chkfinite(A::DMatrix)
    Ac = A.chunks
    to_check = [Dagger.@spawn LinearAlgebra.LAPACK.chkfinite(Ac[i, j]) for i in 1:size(Ac, 1), j in 1:size(Ac,2)]
    return all(fetch, to_check)
end

function searchmax_pivot!(piv_idx::AbstractVector{Int}, piv_val::AbstractVector{T}, A::AbstractMatrix{T}, m::Int) where T
    max_idx = LinearAlgebra.BLAS.iamax(A[:,1])
    piv_idx[m] = max_idx
    piv_val[m] = A[max_idx,1]
end

function update_ipiv!(ipivl::AbstractVector{Int}, info::Ref{Int}, piv_idx::AbstractVector{Int}, piv_val::AbstractVector{T}, k::Int, nb::Int, p::Int) where T
    max_piv_idx = LinearAlgebra.BLAS.iamax(piv_val[k:end])
    max_piv_val = piv_val[max_piv_idx]
    abs_max_piv_val = max_piv_val isa Real ? abs(max_piv_val) : abs(real(max_piv_val)) + abs(imag(max_piv_val))    
    if isapprox(abs_max_piv_val, zero(T); atol=eps(real(T)))
        info[] = k
    end
    ipivl[p] = (max_piv_idx+k-2)*nb + piv_idx[k-1+max_piv_idx]
end

function swaprows_panel!(A::AbstractMatrix{T}, ipivl::AbstractVector{Int}, m::Int, p::Int, nb::Int; M::AbstractMatrix{T}=A) where T
    q = div(ipivl[p]-1,nb) + 1
    r = (ipivl[p]-1)%nb+1
    if m == q
        A[p,:], M[r,:] = M[r,:], A[p,:]
    end
end

function update_pivot_and_searchmax_pivot!(A::AbstractMatrix{T}, piv_idx::AbstractVector{Int}, piv_val::AbstractVector{T}, k::Int, p::Int) where T
    LinearAlgebra.BLAS.scal!(one(T) / A[p,p], view(A, p+1:size(A,1), p:p))
    LinearAlgebra.BLAS.ger!(-one(T), view(A, p+1:size(A,1), p), conj.(view(A, p, p+1:size(A,2))), view(A, p+1:size(A,1), p+1:size(A,2)))

    if  size(A,2) > p
        max_idx = LinearAlgebra.BLAS.iamax(A[p+1:end,p+1])
        piv_idx[k] = max_idx+p
        piv_val[k] = A[max_idx+p,p+1]  
    end
end

function update_panel_and_searchmax_panel!(M::AbstractMatrix{T}, A::AbstractMatrix{T}, piv_idx::AbstractVector{Int}, piv_val::AbstractVector{T}, k::Int, p::Int) where T
    LinearAlgebra.BLAS.scal!(one(T) / A[p,p], view(M, :, p:p))
    LinearAlgebra.BLAS.ger!(-one(T), view(M, :, p), conj.(view(A, p, p+1:size(A,2))), view(M, :, p+1:size(M,2)))

    if  size(M,2) > p
        max_idx = LinearAlgebra.BLAS.iamax(M[:,p+1])
        piv_idx[k] = max_idx
        piv_val[k] = M[max_idx,p+1]  
    end
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


function LinearAlgebra.lu(A::DMatrix{T}, ::LinearAlgebra.RowMaximum; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
    A_copy = LinearAlgebra._lucopy(A, LinearAlgebra.lutype(T))
    return LinearAlgebra.lu!(A_copy, LinearAlgebra.RowMaximum(); check=check, allowsingular=allowsingular)
end
function LinearAlgebra.lu!(A::DMatrix{T}, ::LinearAlgebra.RowMaximum; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
    
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
            for i in k:mt
                Dagger.@spawn searchmax_pivot!(Out(max_piv_idx), Out(max_piv_val), In(Ac[i,k]), i)
            end
            for p in 1:min(nb, m-(k-1)*nb, n-(k-1)*nb) 
                Dagger.@spawn update_ipiv!(InOut(ipivc[k]), InOut(info), In(max_piv_idx), In(max_piv_val), k, nb, p)
                Dagger.@spawn swaprows_panel!(InOut(Ac[k, k]), In(ipivc[k]), k, p, nb)
                for i in k+1:mt
                    Dagger.@spawn swaprows_panel!(InOut(Ac[k, k]), In(ipivc[k]), i, p, nb; M=InOut(Ac[i, k]))
                end
                Dagger.@spawn update_pivot_and_searchmax_pivot!(InOut(Ac[k,k]), Out(max_piv_idx), Out(max_piv_val), k, p)
                for i in k+1:mt
                    Dagger.@spawn update_panel_and_searchmax_panel!(InOut(Ac[i,k]), In(Ac[k,k]), Out(max_piv_idx), Out(max_piv_val), i, p)
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

    check && LinearAlgebra._check_lu_success(info[], allowsingular)

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, info[])
end