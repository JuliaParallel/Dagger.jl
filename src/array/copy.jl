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
function darray_copyto!(B::DArray{TB,NB}, A::DArray{TA,NA}, Binds=parentindices(B), Ainds=parentindices(A)) where {TB,NB,TA,NA}
    Nmax = max(NA, NB)
    pad1(x, i) = length(x) < i ? 1 : x[i]
    pad1range(x, i) = length(x) < i ? (1:1) : x[i]

    if !all(ntuple(i->length(pad1range(Binds, i)) == length(pad1range(Ainds, i)), Nmax))
        throw(DimensionMismatch("Cannot copy from array of size $(size(A)) (indices $Ainds) to array of size $(size(B)) (indices $Binds)"))
    end
    @show Binds Ainds

    to_range(x::UnitRange) = x
    to_range(x::Integer) = 1:x
    to_range(x::Base.OneTo{Int}) = UnitRange(x)
    to_range(x::Base.Slice{Base.OneTo{Int}}) = UnitRange(x)
    # View ranges
    Binds_range = ntuple(i->to_range(pad1range(Binds, i)), Nmax)
    Ainds_range = ntuple(i->to_range(pad1range(Ainds, i)), Nmax)
    # Global offsets
    Binds_offset = ntuple(i->Binds_range[i].start-1, Nmax)
    Ainds_offset = ntuple(i->Ainds_range[i].start-1, Nmax)
    @show Binds_range Ainds_range Binds_offset Ainds_offset

    Bc = B.chunks
    Ac = A.chunks
    Bci = CartesianIndices(reshape(Bc, ntuple(i->pad1(size(Bc), 1), Nmax)))
    Aci = CartesianIndices(reshape(Ac, ntuple(i->pad1(size(Ac), 1), Nmax)))
    Bsd = B.subdomains::DomainBlocks{NB}
    Asd = A.subdomains::DomainBlocks{NA}
    Bsd_all = collect(reshape(Bsd, ntuple(i->pad1(size(Bsd), 1), Nmax)))
    Asd_all = collect(reshape(Asd, ntuple(i->pad1(size(Asd), 1), Nmax)))

    sub_range(x::UnitRange, y::UnitRange) = ntuple(i->(x[i].start-y[i].start+1):(x[i].stop-y[i].stop+1), Nmax)
    shift_shrink_range(x::ArrayDomain, range::NTuple{N,UnitRange}) where N=
        ArrayDomain(ntuple(i->x.indexes[i][range[i]], Nmax))

    Dagger.spawn_datadeps() do
        for Bidx in Bci
            Bpart = Bc[Bidx]
            Bsd = sub_range(Bsd_all[Bidx], Binds_range)

            # Find the first overlapping subdomain of A
            if A.partitioning isa Blocks
                Aidx = CartesianIndex(ntuple(i->fld1(pad1range(Bsd.indexes, i).start, pad1(A.partitioning.blocksize, i)), Nmax))
            else
                # Fallback just in case of non-dense partitioning
                Aidx = first(Aci)
                Asd = first(Asd_all)
                for dim in 1:Nmax
                    while pad1range(Asd.indexes, dim).stop < pad1range(Bsd.indexes, dim).start
                        Aidx += CartesianIndex(ntuple(i->i==dim, Nmax))
                        Asd = Asd_all[Aidx]
                    end
                end
            end
            Aidx_start = Aidx

            # Find the last overlapping subdomain of A
            for dim in 1:Nmax
                while true
                    Aidx_next = Aidx + CartesianIndex(ntuple(i->i==dim, Nmax))
                    if !(Aidx_next in Aci)
                        break
                    end
                    Asd_next = Asd_all[Aidx_next]
                    if pad1range(Asd_next.indexes, dim).start <= pad1range(Bsd.indexes, dim).stop
                        Aidx = Aidx_next
                    else
                        break
                    end
                end
            end
            Aidx_end = Aidx

            # Find the span and set of subdomains of A overlapping Bpart
            Aidx_span = Aidx_start:Aidx_end
            Asd_view = view(Asd_all, Aidx_span)

            # Copy all overlapping subdomains of A
            for Aidx in Aidx_span
                Apart = Ac[Aidx]
                Asd = sub_range(Asd_all[Aidx], Ainds_range)

                # Compute the true range
                range_start = CartesianIndex(ntuple(i->max(pad1range(Bsd.indexes, i).start, pad1range(Asd.indexes, i).start), Nmax))
                range_end = CartesianIndex(ntuple(i->min(pad1range(Bsd.indexes, i).stop, pad1range(Asd.indexes, i).stop), Nmax))
                range_diff = range_end - range_start

                # Compute the offset range into Apart
                Asd_start = ntuple(i->pad1range(Asd.indexes, i).start, Nmax)
                Arange = range(range_start - CartesianIndex(Asd_start) + CartesianIndex{Nmax}(1),
                               range_start - CartesianIndex(Asd_start) + CartesianIndex{Nmax}(1) + range_diff)

                # Compute the offset range into Bpart
                Bsd_start = ntuple(i->pad1range(Bsd.indexes, i).start, Nmax)
                Brange = range(range_start - CartesianIndex(Bsd_start) + CartesianIndex{Nmax}(1),
                               range_start - CartesianIndex(Bsd_start) + CartesianIndex{Nmax}(1) + range_diff)

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

Base.copyto!(B::DArray{T,N}, A::DArray{T,N}) where {T,N} =
    darray_copyto!(B, A)
Base.copyto!(B::DArray{T,N}, A::Array{T,N}) where {T,N} =
    darray_copyto!(B, view(A, B.partitioning))
Base.copyto!(B::Array{T,N}, A::DArray{T,N}) where {T,N} =
    darray_copyto!(view(B, A.partitioning), A)

StridedDArray{T,N} = Union{<:DArray{T,N}, SubArray{T,N,<:DArray{T,NP}} where NP}

Base.copyto!(B::StridedDArray, A::StridedDArray) =
    darray_copyto!(parent(B), parent(A), parentindices(B), parentindices(A))