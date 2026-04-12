function Base.permutedims(A::DArray{T,N}, perm) where {T,N}
    dc = domainchunks(A)
    new_dc = DomainBlocks(
        ntuple(i -> dc.start[perm[i]], N),
        ntuple(i -> dc.cumlength[perm[i]], N),
    )

    dom = domain(A)
    new_domain = ArrayDomain(ntuple(i -> dom.indexes[perm[i]], N))

    new_partitioning = Blocks(ntuple(i -> A.partitioning.blocksize[perm[i]], N))

    old_chunk_size = size(A.chunks)
    new_chunk_size = ntuple(i -> old_chunk_size[perm[i]], N)
    new_chunks = Array{Any,N}(undef, new_chunk_size)

    Dagger.spawn_datadeps() do
        for idx in CartesianIndices(A.chunks)
            new_idx = CartesianIndex(ntuple(i -> idx.I[perm[i]], N))
            new_chunks[new_idx] = Dagger.@spawn permutedims(In(A.chunks[idx]), perm)
        end
    end

    return DArray(T, new_domain, new_dc, new_chunks, new_partitioning, A.concat)
end
