# Copy Buffering

function maybe_copy_buffered(f, args...)
    @assert all(arg->arg isa Pair{<:DArray,<:Blocks}, args) "maybe_copy_buffered only supports `DArray`=>`Blocks`"
    if any(arg_part->arg_part[1].partitioning != arg_part[2], args)
        return copy_buffered(f, args...)
    else
        return f(map(first, args)...)
    end
end
function copy_buffered(f, args...)
    real_args = map(arg_part->arg_part[1], args)
    buffered_args = map(arg_part->allocate_copy_buffer(arg_part[2], arg_part[1]), args)
    for (buf_arg, arg) in zip(buffered_args, real_args)
        copyto!(buf_arg, arg)
    end
    result = f(buffered_args...)
    for (buf_arg, arg) in zip(buffered_args, real_args)
        copyto!(arg, buf_arg)
    end
    return result
end
function allocate_copy_buffer(part::Blocks{N}, A::DArray{T,N}) where {T,N}
    # FIXME: undef initializer
    return zeros(part, T, size(A))
end
function Base.copyto!(B::DArray{T,N}, A::DArray{T,N}) where {T,N}
    if size(B) != size(A)
        throw(DimensionMismatch("Cannot copy from array of size $(size(A)) to array of size $(size(B))"))
    end

    Bc = B.chunks
    Ac = A.chunks
    Asd_all = A.subdomains::DomainBlocks{N}

    Dagger.spawn_datadeps() do
        for Bidx in CartesianIndices(Bc)
            Bpart = Bc[Bidx]
            Bsd = B.subdomains[Bidx]

            # Find the first overlapping subdomain of A
            if A.partitioning isa Blocks
                Aidx = CartesianIndex(ntuple(i->fld1(Bsd.indexes[i].start, A.partitioning.blocksize[i]), N))
            else
                # Fallback just in case of non-dense partitioning
                Aidx = first(CartesianIndices(Ac))
                Asd = first(Asd_all)
                for dim in 1:N
                    while Asd.indexes[dim].stop < Bsd.indexes[dim].start
                        Aidx += CartesianIndex(ntuple(i->i==dim, N))
                        Asd = Asd_all[Aidx]
                    end
                end
            end
            Aidx_start = Aidx

            # Find the last overlapping subdomain of A
            for dim in 1:N
                while true
                    Aidx_next = Aidx + CartesianIndex(ntuple(i->i==dim, N))
                    if !(Aidx_next in CartesianIndices(Ac))
                        break
                    end
                    Asd_next = Asd_all[Aidx_next]
                    if Asd_next.indexes[dim].start <= Bsd.indexes[dim].stop
                        Aidx = Aidx_next
                    else
                        break
                    end
                end
            end
            Aidx_end = Aidx

            # Find the span and set of subdomains of A overlapping Bpart
            Aidx_span = Aidx_start:Aidx_end
            Asd_view = view(A.subdomains, Aidx_span)

            # Copy all overlapping subdomains of A
            for Aidx in Aidx_span
                Asd = Asd_all[Aidx]
                Apart = Ac[Aidx]

                # Compute the true range
                range_start = CartesianIndex(ntuple(i->max(Bsd.indexes[i].start, Asd.indexes[i].start), N))
                range_end = CartesianIndex(ntuple(i->min(Bsd.indexes[i].stop, Asd.indexes[i].stop), N))
                range_diff = range_end - range_start

                # Compute the offset range into Apart
                Asd_start = ntuple(i->Asd.indexes[i].start, N)
                Asd_end = ntuple(i->Asd.indexes[i].stop, N)
                Arange = range(range_start - CartesianIndex(Asd_start) + CartesianIndex{N}(1),
                               range_start - CartesianIndex(Asd_start) + CartesianIndex{N}(1) + range_diff)

                # Compute the offset range into Bpart
                Bsd_start = ntuple(i->Bsd.indexes[i].start, N)
                Bsd_end = ntuple(i->Bsd.indexes[i].stop, N)
                Brange = range(range_start - CartesianIndex(Bsd_start) + CartesianIndex{N}(1),
                               range_start - CartesianIndex(Bsd_start) + CartesianIndex{N}(1) + range_diff)

                # Perform view copy
                Dagger.@spawn copyto_view!(Out(Bpart), Brange, In(Apart), Arange)
            end
        end
    end

    return B
end
function copyto_view!(Bpart, Brange, Apart, Arange)
    copyto!(view(Bpart, Brange), view(Apart, Arange))
    return
end
