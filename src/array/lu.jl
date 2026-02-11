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

        check && LinearAlgebra._check_lu_success(0, allowsingular)
    end

    ipiv = DVector([i for i in 1:min(size(A)...)])

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, 0)
end

# N.B. Task functions receive full Chunk data and create views internally.
# The full-Chunk approach is used here for simplicity,
# but ChunkViews could be used for finer-grained dependencies if needed.

# Combined search+reduce: searches all block columns for the pivot and
# updates ipiv. Receives full Chunks for the diagonal block and off-diagonal
# blocks; creates column views internally.
function search_and_update_ipiv!(ipiv_chunk::AbstractVector{Int}, info::Ref{Int},
                                  diag_block::AbstractMatrix{T},
                                  k::Int, p::Int, mb::Int, m::Int,
                                  n_offdiag::Int,
                                  offdiag_blocks::Vararg{AbstractMatrix{T}}) where T
    # Search diagonal block column p (rows p:end)
    diag_col = view(diag_block, p:min(mb, m-(k-1)*mb), p:p)
    max_idx = LinearAlgebra.BLAS.iamax(diag_col[:])
    best_piv_idx = (p - 1) + max_idx
    best_piv_val = diag_col[max_idx]
    best_block = 1

    # Search off-diagonal block columns
    for (bi, blk) in enumerate(offdiag_blocks)
        col = view(blk, :, p:p)
        idx = LinearAlgebra.BLAS.iamax(col[:])
        val = col[idx]
        abs_best = best_piv_val isa Real ? abs(best_piv_val) : abs(real(best_piv_val)) + abs(imag(best_piv_val))
        abs_val = val isa Real ? abs(val) : abs(real(val)) + abs(imag(val))
        if abs_val > abs_best
            best_piv_idx = idx
            best_piv_val = val
            best_block = bi + 1
        end
    end

    # Singularity detection
    abs_best = best_piv_val isa Real ? abs(best_piv_val) : abs(real(best_piv_val)) + abs(imag(best_piv_val))
    if info[] == 0 && isapprox(abs_best, zero(T); atol=eps(real(T)))
        info[] = (k-1)*mb + p
    end

    # Update ipiv
    ipiv_chunk[p] = (best_block+k-2)*mb + best_piv_idx
end

# Swap rows in the panel column. Receives full Chunks.
function swaprows_panel!(A::AbstractMatrix{T}, M::AbstractMatrix{T}, ipiv_chunk::AbstractVector{Int}, m::Int, p::Int, mb::Int) where T
    q = div(ipiv_chunk[p]-1,mb) + 1
    r = (ipiv_chunk[p]-1)%mb+1
    if m == q
        A[p,:], M[r,:] = M[r,:], A[p,:]
    end
end

# Update panel on the diagonal block (rows p+1:end). Receives the full block.
function update_panel_diag!(A::AbstractMatrix{T}, p::Int, row_end::Int) where T
    M = view(A, p+1:row_end, :)
    Acinv = one(T) / A[p,p]
    LinearAlgebra.BLAS.scal!(Acinv, view(M, :, p))
    LinearAlgebra.BLAS.geru!(-one(T), view(M, :, p), view(A, p, p+1:size(A,2)), view(M, :, p+1:size(M,2)))
end

# Update panel on an off-diagonal block. Receives full Chunks.
function update_panel_offdiag!(M::AbstractMatrix{T}, A::AbstractMatrix{T}, p::Int) where T
    Acinv = one(T) / A[p,p]
    LinearAlgebra.BLAS.scal!(Acinv, view(M, :, p))
    LinearAlgebra.BLAS.geru!(-one(T), view(M, :, p), view(A, p, p+1:size(A,2)), view(M, :, p+1:size(M,2)))
end

# Swap rows in trailing columns. Receives full Chunks.
function swaprows_trail!(A::AbstractMatrix{T}, M::AbstractMatrix{T}, ipiv::AbstractVector{Int}, m::Int, mb::Int) where T
    for p in eachindex(ipiv)
        q = div(ipiv[p]-1,mb) + 1
        r = (ipiv[p]-1)%mb+1
        if m == q
            A[p,:], M[r,:] = M[r,:], A[p,:]
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

        check && LinearAlgebra._check_lu_success(info[], allowsingular)
    end

    return LinearAlgebra.LU{T,DMatrix{T},DVector{Int}}(A, ipiv, info[])
end
