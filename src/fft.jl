using AbstractFFTs
using LinearAlgebra
using Dagger: DArray, @spawn, InOut, In
using MPI

struct FFT end
struct RFFT end
struct IRFFT end
struct IFFT end
struct FFT! end
struct RFFT! end
struct IRFFT! end
struct IFFT! end

export FFT, RFFT, IRFFT, IFFT, FFT!, RFFT!, IRFFT!, IFFT!, fft, ifft
abstract type Decomposition end
struct Pencil <: Decomposition end
struct Slab <: Decomposition end

function plan_transform(transform, A, dims; kwargs...)  
        if transform isa RFFT
            return plan_rfft(A, dims; kwargs...)
        elseif transform isa FFT
            return plan_fft(A, dims; kwargs...)
        elseif transform isa IRFFT
            return plan_irfft(A, dims; kwargs...)
        elseif transform isa IFFT
            return plan_ifft(A, dims; kwargs...)
        elseif transform isa FFT!
            return plan_fft!(A, dims; kwargs...)
        elseif transform isa IFFT!
            return plan_ifft!(A, dims; kwargs...)
        else
            throw(ArgumentError("Unknown transform type"))
        end

end


function apply_fft!(out_part, in_part, transform, dim)
    plan = plan_transform(transform, in_part, dim)
    mul!(out_part, plan, in_part)
    return
end
apply_fft!(inout_part, transform, dim) = apply_fft!(inout_part, inout_part, transform, dim)


"""
@kernel function redistribute_kernel!(dest, src, 
    dest_starts, src_starts,
    overlap_sizes)
    idx = @index(Global, Linear)
    if idx <= prod(overlap_sizes)
    #linear index to 3D
        iz = (idx - 1) ÷ (overlap_sizes[1] * overlap_sizes[2]) + 1
        temp = (idx - 1) % (overlap_sizes[1] * overlap_sizes[2])
        iy = temp ÷ overlap_sizes[1] + 1
        ix = temp % overlap_sizes[1] + 1

        # Calculate actual indices for source and dest
        src_idx_x = src_starts[1] + ix - 1
        src_idx_y = src_starts[2] + iy - 1
        src_idx_z = src_starts[3] + iz - 1

        dest_idx_x = dest_starts[1] + ix - 1
        dest_idx_y = dest_starts[2] + iy - 1
        dest_idx_z = dest_starts[3] + iz - 1

        dest[dest_idx_x, dest_idx_y, dest_idx_z] = src[src_idx_x, src_idx_y, src_idx_z]
    end
end

function redistribute_x_to_y!(dest::DArray{T,3}, src::DArray{T,3}) where T
    comm = MPI.COMM_WORLD
    rank = MPI.Comm_rank(comm)


    for (src_idx, src_chunk) in enumerate(src.chunks)
        if src_chunk.handle.rank == rank
        src_data = fetch(src_chunk)  # Already on GPU
        backend = KernelAbstractions.get_backend(src_data)
        src_domain = src.subdomains[src_idx]

    for (dst_idx, dst_chunk) in enumerate(dest.chunks)
    if dst_chunk.handle.rank == rank
    dst_data = fetch(dst_chunk)  # Already on GPU
    dst_domain = dest.subdomains[dst_idx]

    overlap_x = intersect(src_domain.indexes[1], dst_domain.indexes[1])
    overlap_y = intersect(src_domain.indexes[2], dst_domain.indexes[2])
    overlap_z = intersect(src_domain.indexes[3], dst_domain.indexes[3])

    if !isempty(overlap_x) && !isempty(overlap_y) && !isempty(overlap_z)
        overlap_sizes = (length(overlap_x), length(overlap_y), length(overlap_z))

        src_starts = (
            first(overlap_x) - first(src_domain.indexes[1]) + 1,
            first(overlap_y) - first(src_domain.indexes[2]) + 1,
            first(overlap_z) - first(src_domain.indexes[3]) + 1
        )

        dst_starts = (
            first(overlap_x) - first(dst_domain.indexes[1]) + 1,
            first(overlap_y) - first(dst_domain.indexes[2]) + 1,
            first(overlap_z) - first(dst_domain.indexes[3]) + 1
        )

        kernel! = redistribute_kernel!(backend)
        kernel!(dst_data, src_data,
                            dst_starts, src_starts, overlap_sizes,
                            ndrange=prod(overlap_sizes))
        KernelAbstractions.synchronize(backend)
        end
    end
end
end
end

MPI.Barrier(comm)
end

function redistribute_y_to_z!(dest::DArray{T,3}, src::DArray{T,3}) where T
comm = MPI.COMM_WORLD
rank = MPI.Comm_rank(comm)


for (src_idx, src_chunk) in enumerate(src.chunks)
    if src_chunk.handle.rank == rank
        src_data = fetch(src_chunk)  # Already on GPU
        backend = KernelAbstractions.get_backend(src_data)
        src_domain = src.subdomains[src_idx]

for (dst_idx, dst_chunk) in enumerate(dest.chunks)
    if dst_chunk.handle.rank == rank
        dst_data = fetch(dst_chunk)  # Already on GPU
        dst_domain = dest.subdomains[dst_idx]

        overlap_x = intersect(src_domain.indexes[1], dst_domain.indexes[1])
        overlap_y = intersect(src_domain.indexes[2], dst_domain.indexes[2])
        overlap_z = intersect(src_domain.indexes[3], dst_domain.indexes[3])

    if !isempty(overlap_x) && !isempty(overlap_y) && !isempty(overlap_z)
        overlap_sizes = (length(overlap_x), length(overlap_y), length(overlap_z))

        src_starts = (
        first(overlap_x) - first(src_domain.indexes[1]) + 1,
        first(overlap_y) - first(src_domain.indexes[2]) + 1,
        first(overlap_z) - first(src_domain.indexes[3]) + 1
        )

        dst_starts = (
        first(overlap_x) - first(dst_domain.indexes[1]) + 1,
        first(overlap_y) - first(dst_domain.indexes[2]) + 1,
        first(overlap_z) - first(dst_domain.indexes[3]) + 1
        )

        kernel! = redistribute_kernel!(backend)
        kernel!(dst_data, src_data,
        dst_starts, src_starts, overlap_sizes,
        ndrange=prod(overlap_sizes))
        KernelAbstractions.synchronize(backend)
        end
    end
end
end
end

MPI.Barrier(comm)
end
"""

function redistribute_x_to_y!(dest::DArray{T,3}, src::DArray{T,3}) where T
    comm = MPI.COMM_WORLD
    rank = MPI.Comm_rank(comm)
    nranks = MPI.Comm_size(comm)
    reqs = Vector{MPI.Request}() 
    
    # First pass: Post receives
    for (dst_idx, dst_chunk) in enumerate(dest.chunks)
        if dst_chunk.handle.rank == rank
            dst_domain = dest.subdomains[dst_idx]
            
            for (src_idx, src_chunk) in enumerate(src.chunks)
                if src_chunk.handle.rank != rank
                    src_domain = src.subdomains[src_idx]
                    
                    overlap_x = intersect(src_domain.indexes[1], dst_domain.indexes[1])
                    overlap_y = intersect(src_domain.indexes[2], dst_domain.indexes[2])
                    overlap_z = intersect(src_domain.indexes[3], dst_domain.indexes[3])
                    
                    if !isempty(overlap_x) && !isempty(overlap_y) && !isempty(overlap_z)
                        recv_buf = view(fetch(dst_chunk),
                            (overlap_x .- first(dst_domain.indexes[1]) .+ 1),
                            (overlap_y .- first(dst_domain.indexes[2]) .+ 1),
                            (overlap_z .- first(dst_domain.indexes[3]) .+ 1))
                        
                        tag = src_idx * nranks + dst_idx
                        req = MPI.Irecv!(recv_buf, src_chunk.handle.rank, tag, comm)
                        push!(reqs, req)
                    end
                end
            end
        end
    end
    
    # Second pass: Process local data and initiate sends
    for (src_idx, src_chunk) in enumerate(src.chunks)
        if src_chunk.handle.rank == rank
            src_data = fetch(src_chunk)
            src_domain = src.subdomains[src_idx]
            
            for (dst_idx, dst_chunk) in enumerate(dest.chunks)
                dst_domain = dest.subdomains[dst_idx]
                
                overlap_x = intersect(src_domain.indexes[1], dst_domain.indexes[1])
                overlap_y = intersect(src_domain.indexes[2], dst_domain.indexes[2])
                overlap_z = intersect(src_domain.indexes[3], dst_domain.indexes[3])
                
                if !isempty(overlap_x) && !isempty(overlap_y) && !isempty(overlap_z)
                    if dst_chunk.handle.rank == rank
                        # Local copy
                        dst_data = fetch(dst_chunk)
                        src_indices = (
                            overlap_x .- first(src_domain.indexes[1]) .+ 1,
                            overlap_y .- first(src_domain.indexes[2]) .+ 1,
                            overlap_z .- first(src_domain.indexes[3]) .+ 1
                        )
                        dst_indices = (
                            overlap_x .- first(dst_domain.indexes[1]) .+ 1,
                            overlap_y .- first(dst_domain.indexes[2]) .+ 1,
                            overlap_z .- first(dst_domain.indexes[3]) .+ 1
                        )
                        dst_data[dst_indices...] .= view(src_data, src_indices...)
                    else
                        # Remote send using views
                        src_indices = (
                            overlap_x .- first(src_domain.indexes[1]) .+ 1,
                            overlap_y .- first(src_domain.indexes[2]) .+ 1,
                            overlap_z .- first(src_domain.indexes[3]) .+ 1
                        )
                        send_data = view(src_data, src_indices...)
                        
                        tag = src_idx * nranks + dst_idx
                        req = MPI.Isend(send_data, dst_chunk.handle.rank, tag, comm)
                        push!(reqs, req)
                    end
                end
            end
        end
    end
    
    if !isempty(reqs)
        MPI.Waitall(reqs)
    end
    
    MPI.Barrier(comm)
end

#apply better indexing.
function redistribute_y_to_z!(dest::DArray{T,3}, src::DArray{T,3}) where T
    comm = MPI.COMM_WORLD
    rank = MPI.Comm_rank(comm)
    nranks = MPI.Comm_size(comm)
    
    reqs = Vector{MPI.Request}()
    
    # First pass: Post receives
    for (dst_idx, dst_chunk) in enumerate(dest.chunks)
        if dst_chunk.handle.rank == rank
            dst_domain = dest.subdomains[dst_idx]
            
            for (src_idx, src_chunk) in enumerate(src.chunks)
                if src_chunk.handle.rank != rank
                    src_domain = src.subdomains[src_idx]
                    
                    overlap_x = intersect(src_domain.indexes[1], dst_domain.indexes[1])
                    overlap_y = intersect(src_domain.indexes[2], dst_domain.indexes[2])
                    overlap_z = intersect(src_domain.indexes[3], dst_domain.indexes[3])
                    
                    if !isempty(overlap_x) && !isempty(overlap_y) && !isempty(overlap_z)
                        recv_buf = view(fetch(dst_chunk),
                            (overlap_x .- first(dst_domain.indexes[1]) .+ 1),
                            (overlap_y .- first(dst_domain.indexes[2]) .+ 1),
                            (overlap_z .- first(dst_domain.indexes[3]) .+ 1))
                        
                        tag = src_idx * nranks + dst_idx
                        req = MPI.Irecv!(recv_buf, src_chunk.handle.rank, tag, comm)
                        push!(reqs, req)
                    end
                end
            end
        end
    end
    
    # Second pass: Process local data and initiate sends
    for (src_idx, src_chunk) in enumerate(src.chunks)
        if src_chunk.handle.rank == rank
            src_data = fetch(src_chunk)
            src_domain = src.subdomains[src_idx]
            
            for (dst_idx, dst_chunk) in enumerate(dest.chunks)
                dst_domain = dest.subdomains[dst_idx]
                
                overlap_x = intersect(src_domain.indexes[1], dst_domain.indexes[1])
                overlap_y = intersect(src_domain.indexes[2], dst_domain.indexes[2])
                overlap_z = intersect(src_domain.indexes[3], dst_domain.indexes[3])
                
                if !isempty(overlap_x) && !isempty(overlap_y) && !isempty(overlap_z)
                    if dst_chunk.handle.rank == rank
                        # Local copy
                        dst_data = fetch(dst_chunk)
                        src_indices = (
                            overlap_x .- first(src_domain.indexes[1]) .+ 1,
                            overlap_y .- first(src_domain.indexes[2]) .+ 1,
                            overlap_z .- first(src_domain.indexes[3]) .+ 1
                        )
                        dst_indices = (
                            overlap_x .- first(dst_domain.indexes[1]) .+ 1,
                            overlap_y .- first(dst_domain.indexes[2]) .+ 1,
                            overlap_z .- first(dst_domain.indexes[3]) .+ 1
                        )
                        dst_data[dst_indices...] .= view(src_data, src_indices...)
                    else
                        # Remote send using views!!!
                        src_indices = (
                            overlap_x .- first(src_domain.indexes[1]) .+ 1,
                            overlap_y .- first(src_domain.indexes[2]) .+ 1,
                            overlap_z .- first(src_domain.indexes[3]) .+ 1
                        )
                        send_data = view(src_data, src_indices...)
                        
                        tag = src_idx * nranks + dst_idx
                        req = MPI.Isend(send_data, dst_chunk.handle.rank, tag, comm)
                        push!(reqs, req)
                    end
                end
            end
        end
    end
    
    if !isempty(reqs)
        MPI.Waitall(reqs)
    end
    
    MPI.Barrier(comm)
end


function fft(A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}, transforms, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks
    C_parts = C.chunks
    
    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[1]), In(dims[1]))
        end
    end

    redistribute_x_to_y!(B, A)

    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[2]), In(dims[2]))
        end
    end

    redistribute_y_to_z!(C, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(C_parts)
            Dagger.@spawn name="apply_fft!(dim 3)[$idx]" apply_fft!(InOut(C_parts[idx]), In(transforms[3]), In(dims[3]))
        end
    end

    return C
end

function fft(A::DArray{T,3}, B::DArray{T,3}, transforms, dims, decomp::Decomposition = Slab()) where T 
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
             Dagger.@spawn apply_fft!(InOut(A_parts[idx]), In(transforms[1]), (dims[1], dims[2]))
        end
    end
        
    redistribute_x_to_y!(B, A)
 
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn apply_fft!(InOut(B_parts[idx]), In(transforms[3]), (dims[3]))
        end
    end
end

function redistribute!(B::DArray{T,2}, A::DArray{T,2}) where T
    comm = MPI.COMM_WORLD
    rank = MPI.Comm_rank(comm)
    nranks = MPI.Comm_size(comm)
    
    x, y = size(A)
    A_chunks = A.chunks
    B_chunks = B.chunks
    
    A_owners = [c.handle.rank for c in A_chunks]
    B_owners = [c.handle.rank for c in B_chunks]
    
    send_reqs = MPI.Request[]
    recv_reqs = MPI.Request[]
    recv_data = []
    send_bufs = [] 
    
    for (i, chunk) in enumerate(A_chunks)
        if A_owners[i] == rank 
            src_data = fetch(chunk)
            
            src_domain = A.subdomains[i]
            src_ranges = src_domain.indexes
            
            for (j, dst_domain) in enumerate(B.subdomains)
                dst_ranges = dst_domain.indexes
                
                overlap_x = intersect(src_ranges[1], dst_ranges[1])
                overlap_y = intersect(src_ranges[2], dst_ranges[2])
                
                if !isempty(overlap_x) && !isempty(overlap_y)
                    src_x = overlap_x .- first(src_ranges[1]) .+ 1
                    src_y = overlap_y .- first(src_ranges[2]) .+ 1
                    overlap_data = view(src_data, src_x, src_y)
                    
                    if B_owners[j] != rank
                        send_buf = Array(overlap_data) 
                        push!(send_bufs, send_buf) 
                        req = MPI.Isend(send_buf, comm; dest=B_owners[j], tag=i*nranks + j)
                        push!(send_reqs, req)
                    else
                        dst_chunk = fetch(B_chunks[j])
                        dst_x = overlap_x .- first(dst_ranges[1]) .+ 1
                        dst_y = overlap_y .- first(dst_ranges[2]) .+ 1
                        dst_chunk[dst_x, dst_y] .= overlap_data
                    end
                end
            end
        end
        
        for (j, dst_chunk) in enumerate(B_chunks)
            if B_owners[j] == rank
                dst_domain = B.subdomains[j]
                dst_ranges = dst_domain.indexes
                src_domain = A.subdomains[i]
                src_ranges = src_domain.indexes
                
                overlap_x = intersect(src_ranges[1], dst_ranges[1])
                overlap_y = intersect(src_ranges[2], dst_ranges[2])
                
                if !isempty(overlap_x) && !isempty(overlap_y) && A_owners[i] != rank
                    overlap_size = (length(overlap_x), length(overlap_y))
                    recv_buf = Array{T}(undef, overlap_size...)
                    
                    req = MPI.Irecv!(recv_buf, comm; source=A_owners[i], tag=i*nranks + j)
                    push!(recv_reqs, req)
                    push!(recv_data, (recv_buf, j, overlap_x, overlap_y))
                end
            end
        end
    end
    
    if !isempty(recv_reqs)
        statuses = MPI.Waitall(recv_reqs)
        
        for (idx, (recv_buf, chunk_idx, overlap_x, overlap_y)) in enumerate(recv_data)
            dst_chunk = fetch(B_chunks[chunk_idx])
            dst_ranges = B.subdomains[chunk_idx].indexes
            dst_x = overlap_x .- first(dst_ranges[1]) .+ 1
            dst_y = overlap_y .- first(dst_ranges[2]) .+ 1
            
            dst_chunk[dst_x, dst_y] .= recv_buf
        end
    end
    
    if !isempty(send_reqs)
        MPI.Waitall(send_reqs)
    end
    
    MPI.Barrier(comm)
end

function fft(A::DArray{T,2}, B::DArray{T,2}, transforms, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[1]), In(dims[1]))
        end
    end

    redistribute!(B, A)
    #copyto!(B, A)
    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_fft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[2]), In(dims[2]))
        end
    end

end


function ifft(A::DArray{T,3}, B::DArray{T,3}, C::DArray{T,3}, transforms, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks
    C_parts = C.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[3]), In(dims[3]))
        end
    end

    redistribute_x_to_y!(B, A)

    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), In(transforms[2]), In(dims[2]))
        end
    end

    redistribute_y_to_z!(C, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(C_parts)
            Dagger.@spawn name="apply_ifft!(dim 1)[$idx]" apply_fft!(InOut(C_parts[idx]), In(transforms[1]), In(dims[1]))
        end
    end

    return C
end


function ifft(A::DArray{T,3}, B::DArray{T,3}, transforms, dims, decomp::Decomposition = Slab()) where T 
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 3)[$idx]" apply_fft!(InOut(B_parts[idx]), transforms[3], dims[3])
        end
    end
        
    redistribute_x_to_y!(A, B)
 
    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_ifft!(dim 1&2)[$idx]" apply_fft!(InOut(A_parts[idx]), In(transforms[1]), (dims[1], dims[2]))
        end
    end
end

function ifft(A::DArray{T,2}, B::DArray{T,2}, transforms, dims) where T
    A_parts = A.chunks
    B_parts = B.chunks

    Dagger.spawn_datadeps() do
        for idx in eachindex(B_parts)
            Dagger.@spawn name="apply_ifft!(dim 2)[$idx]" apply_fft!(InOut(B_parts[idx]), transforms[2], dims[2])
        end
    end

    redistribute!(A, B)
    Dagger.spawn_datadeps() do
        for idx in eachindex(A_parts)
            Dagger.@spawn name="apply_fft!(dim 1)[$idx]" apply_fft!(InOut(A_parts[idx]), transforms[1], dims[1])
        end
    end

end