LinearAlgebra.lu(A::DMatrix{T}, pivot::Union{LinearAlgebra.RowMaximum,LinearAlgebra.NoPivot} = LinearAlgebra.RowMaximum(); check::Bool=true, allowsingular::Bool=false) where {T<:LinearAlgebra.BlasFloat} = LinearAlgebra.lu(A, pivot; check=check, allowsingular=allowsingular)

LinearAlgebra.lu!(A::DMatrix{T}, pivot::Union{LinearAlgebra.RowMaximum,LinearAlgebra.NoPivot} = LinearAlgebra.RowMaximum(); check::Bool=true, allowsingular::Bool=false) where {T<:LinearAlgebra.BlasFloat} = LinearAlgebra.lu(A, pivot; check=check, allowsingular=allowsingular)

function LinearAlgebra.lu(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
    A_copy = LinearAlgebra._lucopy(A, LinearAlgebra.lutype(T))
    return LinearAlgebra.lu!(A_copy, LinearAlgebra.NoPivot(); check)
end
function LinearAlgebra.lu!(A::DMatrix{T}, ::LinearAlgebra.NoPivot; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
    check && LinearAlgebra.LAPACK.chkfinite(A)

    zone = one(T)
    mzone = -one(T)

    mb, nb = A.partitioning.blocksize

    min_mb_nb = min(mb, nb)
    maybe_copy_buffered(A => Blocks(min_mb_nb, min_mb_nb)) do A
        Ac = A.chunks
        mt, nt = size(Ac)

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

        if check
            @static if VERSION >= v"1.11-"
                LinearAlgebra._check_lu_success(0, allowsingular)
            else
                LinearAlgebra.checknonsingular(0)
            end
        end
    end

    ipiv = DVector([i for i in 1:min(size(A)...)])

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, 0)
end

# N.B. Task functions receive full Chunk data and create views internally.
# The full-Chunk approach is used here for simplicity,
# but ChunkViews could be used for finer-grained dependencies if needed.

# Column pivot metric matches BLAS iamax: max abs (real) or max |Re|+|Im| (complex).
function _lu_pivot_col_metric(col::AbstractVector{T}) where T
    if T <: Real
        findmax(abs.(col))
    else
        findmax(@. abs(real(col)) + abs(imag(col)))
    end
end

# Combined search+reduce: pivot via findmax (GPU-friendly broadcast+reduction).
function search_and_update_ipiv!(ipiv_chunk::AbstractVector{Int}, info::Ref{Int},
                                  diag_block::AbstractMatrix{T},
                                  k::Int, p::Int, mb::Int, m::Int,
                                  n_offdiag::Int,
                                  offdiag_blocks::Vararg{AbstractMatrix{T}}) where T
    diag_col = view(diag_block, p:min(mb, m-(k-1)*mb), p:p)
    isempty(diag_col) && return
    diag_vec = vec(diag_col)
    best_mag, rel_idx = _lu_pivot_col_metric(diag_vec)
    best_piv_idx = (p - 1) + rel_idx
    best_block = 1

    for (bi, blk) in enumerate(offdiag_blocks)
        col = view(blk, :, p:p)
        colv = vec(col)
        isempty(colv) && continue
        mag_max, idx = _lu_pivot_col_metric(colv)
        if mag_max > best_mag
            best_mag = mag_max
            best_piv_idx = idx
            best_block = bi + 1
        end
    end

    Tf = real(float(T))
    if info[] == 0 && best_mag <= eps(Tf)
        info[] = (k-1)*mb + p
    end

    # Scalar write; wrap in allowscalar when ipiv_chunk is a GPU array (e.g. ROCArray).
    GPUArraysCore.allowscalar() do
        ipiv_chunk[p] = (best_block + k - 2) * mb + best_piv_idx
    end
end

# Swap rows in the panel column. Receives full Chunks.
# Uses allowscalar for ipiv read and row swap when chunks are GPU arrays.
function swaprows_panel!(A::AbstractMatrix{T}, M::AbstractMatrix{T}, ipiv_chunk::AbstractVector{Int}, m::Int, p::Int, mb::Int) where T
    GPUArraysCore.allowscalar() do
        q = div(ipiv_chunk[p]-1,mb) + 1
        r = (ipiv_chunk[p]-1)%mb+1
        if m == q
            A[p,:], M[r,:] = M[r,:], A[p,:]
        end
    end
end

@kernel function _geru_kernel!(alpha, x, y, A)
    i, j = @index(Global, NTuple)
    @inbounds A[i, j] = A[i, j] + alpha * x[i] * y[j]
end

function geru!(α::T, x::AbstractVector{T}, y::AbstractVector{T}, A::AbstractMatrix{T}) where T
    isempty(A) && return A
    Kernel(_geru_kernel!)(α, x, y, A; ndrange=size(A))
    return A
end

function _lu_inv_diag_el(A::AbstractMatrix{T}, p::Int) where T
    if A isa GPUArraysCore.AbstractGPUArray
        return one(T) / Array(@view A[p:p, p:p])[1]
    end
    return one(T) / A[p, p]
end

# Update panel on the diagonal block (rows p+1:end). Receives the full block.
function update_panel_diag!(A::AbstractMatrix{T}, p::Int, row_end::Int) where T
    M = view(A, p+1:row_end, :)
    Acinv = _lu_inv_diag_el(A, p)
    view(M, :, p) .= Acinv .* view(M, :, p)
    geru!(-one(T), view(M, :, p), view(A, p, p+1:size(A,2)), view(M, :, p+1:size(M,2)))
    return A
end

# Update panel on an off-diagonal block. Receives full Chunks.
function update_panel_offdiag!(M::AbstractMatrix{T}, A::AbstractMatrix{T}, p::Int) where T
    Acinv = _lu_inv_diag_el(A, p)
    view(M, :, p) .= Acinv .* view(M, :, p)
    geru!(-one(T), view(M, :, p), view(A, p, p+1:size(A,2)), view(M, :, p+1:size(M,2)))
    return M
end

# Swap rows in trailing columns. Receives full Chunks.
# Uses allowscalar for ipiv reads and row swaps when chunks are GPU arrays.
function swaprows_trail!(A::AbstractMatrix{T}, M::AbstractMatrix{T}, ipiv::AbstractVector{Int}, m::Int, mb::Int) where T
    GPUArraysCore.allowscalar() do
        for p in eachindex(ipiv)
            q = div(ipiv[p]-1,mb) + 1
            r = (ipiv[p]-1)%mb+1
            if m == q
                A[p,:], M[r,:] = M[r,:], A[p,:]
            end
        end
    end
end

# Implementation of https://inria.hal.science/hal-04984070v1/file/ipdps_paper.pdf
function LinearAlgebra.lu(A::DMatrix{T}, ::LinearAlgebra.RowMaximum; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
    A_copy = LinearAlgebra._lucopy(A, LinearAlgebra.lutype(T))
    return LinearAlgebra.lu!(A_copy, LinearAlgebra.RowMaximum(); check, allowsingular)
end
function LinearAlgebra.lu!(A::DMatrix{T}, ::LinearAlgebra.RowMaximum; check::Bool = true, allowsingular::Bool = false) where {T<:LinearAlgebra.BlasFloat}
    check && LinearAlgebra.LAPACK.chkfinite(A)

    zone = one(T)
    mzone = -one(T)

    info = Ref(0)

    mb, nb = A.partitioning.blocksize
    min_mb_nb = min(mb, nb)
    local ipiv
    maybe_copy_buffered(A => Blocks(min_mb_nb, min_mb_nb)) do A
        Ac = A.chunks
        mb, nb = A.partitioning.blocksize
        mt, nt = size(Ac)
        m,  n  = size(A)

        ipiv = DVector(collect(1:min(m, n)), Blocks(nb))
        ipivc = ipiv.chunks

        # Using full Chunks in annotations for simplicity
        # ChunkViews work correctly and could be used here if needed.
        Dagger.spawn_datadeps() do
            for k in 1:min(mt, nt)
                for p in 1:min(mb, nb, m-(k-1)*mb, n-(k-1)*nb)
                    # Search all blocks in column k for pivot, then update ipiv.
                    # Uses Dagger.spawn for variable number of In arguments.
                    n_offdiag = mt - k
                    spawn_args = Any[
                        search_and_update_ipiv!,
                        InOut(ipivc[k]),
                        InOut(info),
                        In(Ac[k,k]),
                        k, p, mb, m, n_offdiag,
                    ]
                    for i in k+1:mt
                        push!(spawn_args, In(Ac[i,k]))
                    end
                    Dagger.spawn(spawn_args...)

                    # Swap rows in the panel column
                    for i in k:mt
                        Dagger.@spawn swaprows_panel!(InOut(Ac[k, k]), InOut(Ac[i, k]), In(ipivc[k]), i, p, mb)
                    end

                    # Update panel: scale and rank-1 update on diagonal block
                    if length(p+1:min(mb,m-(k-1)*mb)) > 0
                        Dagger.@spawn update_panel_diag!(InOut(Ac[k,k]), p, min(mb, m-(k-1)*mb))
                    end

                    # Update panel: off-diagonal blocks
                    for i in k+1:mt
                        Dagger.@spawn update_panel_offdiag!(InOut(Ac[i, k]), In(Ac[k,k]), p)
                    end
                end

                # Trailing submatrix row swaps
                for j in Iterators.flatten((1:k-1, k+1:nt))
                    for i in k:mt
                        Dagger.@spawn swaprows_trail!(InOut(Ac[k, j]), InOut(Ac[i, j]), In(ipivc[k]), i, mb)
                    end
                end

                # TRSM and GEMM
                for j in k+1:nt
                    Dagger.@spawn BLAS.trsm!('L', 'L', 'N', 'U', zone, In(Ac[k, k]), InOut(Ac[k, j]))
                    for i in k+1:mt
                        Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(Ac[i, k]), In(Ac[k, j]), zone, InOut(Ac[i, j]))
                    end
                end
            end
        end

        if check
            @static if VERSION >= v"1.11-"
                LinearAlgebra._check_lu_success(info[], allowsingular)
            else
                LinearAlgebra.checknonsingular(info[])
            end
        end
    end

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, info[])
end
