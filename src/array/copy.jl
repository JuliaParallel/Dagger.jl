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

    # Free the buffers
    foreach(unsafe_free!, buffered_args)

    # If the result is one of the buffered args, return the corresponding
    # original arg instead (since we've already copied data back to it,
    # and the buffer has been freed)
    result_idx = findfirst(buf_arg -> buf_arg === result, buffered_args)
    if result_idx !== nothing
        return real_args[result_idx]
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
    pad1range(x::ArrayDomain, i) = length(x.indexes) < i ? (1:1) : x.indexes[i]
    padNmax(x) = ntuple(i->pad1range(x, i), Nmax)
    padNmax(x::ArrayDomain) = padNmax(x.indexes)

    to_range(x::UnitRange) = x
    to_range(x::Integer) = x:x
    to_range(x::Base.OneTo{Int}) = UnitRange(x)
    to_range(x::Base.Slice{Base.OneTo{Int}}) = UnitRange(x)
    to_range(::StepRange) = throw(ArgumentError("Non-continuous ranges are not yet supported for DArray copy"))
    to_range(x) = throw(ArgumentError("Unsupported range type for DArray copy: $(typeof(x))"))

    if any(x->x isa Vector, Binds) || any(x->x isa Vector, Ainds)
        # Split the copy into multiple copies
        dims_with_vector = findall(x->x[1] isa Vector || x[2] isa Vector, collect(zip(Binds, Ainds)))
        Binds_set = Iterators.product(ntuple(i->i in dims_with_vector ? pad1range(Binds, i) : Ref(pad1range(Binds, i)), Nmax)...)
        Ainds_set = Iterators.product(ntuple(i->i in dims_with_vector ? pad1range(Ainds, i) : Ref(pad1range(Ainds, i)), Nmax)...)
        for (Binds_inner, Ainds_inner) in zip(Binds_set, Ainds_set)
            darray_copyto!(B, A, Binds_inner, Ainds_inner)
        end
        return
    end

    if !all(ntuple(i->length(pad1range(Binds, i)) == length(pad1range(Ainds, i)), Nmax))
        throw(DimensionMismatch("Cannot copy from array of size $(size(A)) (indices $Ainds) to array of size $(size(B)) (indices $Binds)"))
    end

    # Global element ranges
    Binds_range = ntuple(i->to_range(pad1range(Binds, i)), Nmax)
    Ainds_range = ntuple(i->to_range(pad1range(Ainds, i)), Nmax)

    # Global element offsets
    Binds_offset = ntuple(i->Binds_range[i].start-1, Nmax)
    Ainds_offset = ntuple(i->Ainds_range[i].start-1, Nmax)

    # Limited chunk ranges
    Bblocksize = ntuple(i->pad1(B.partitioning.blocksize, i), Nmax)
    Ablocksize = ntuple(i->pad1(A.partitioning.blocksize, i), Nmax)
    Bidx_range = ntuple(i->UnitRange(fld1(Binds_range[i].start, Bblocksize[i]), fld1(Binds_range[i].stop, Bblocksize[i])), Nmax)
    Aidx_range = ntuple(i->UnitRange(fld1(Ainds_range[i].start, Ablocksize[i]), fld1(Ainds_range[i].stop, Ablocksize[i])), Nmax)

    # Limited chunk indices
    Bci = CartesianIndices(Bidx_range)
    Aci = CartesianIndices(Aidx_range)

    # Per-chunk ranges
    Bsd = B.subdomains::DomainBlocks{NB}
    Asd = A.subdomains::DomainBlocks{NA}
    Bsd_all = collect(reshape(Bsd, ntuple(i->pad1(size(Bsd), i), Nmax)))
    Asd_all = collect(reshape(Asd, ntuple(i->pad1(size(Asd), i), Nmax)))

    shift_ranges(x::NTuple{N1,UnitRange}, offset::NTuple{N2,Int}) where {N1,N2} =
        ntuple(i->UnitRange(x[i].start-offset[i], x[i].stop-offset[i]), Nmax)

    Dagger.spawn_datadeps() do
        for Bidx in Bci
            Bpart = B.chunks[Bidx]
            Bsd_global_raw = padNmax(Bsd_all[Bidx])
            Bsd_global_shifted = shift_ranges(Bsd_global_raw, Binds_offset)

            ## Find the overlapping subdomains of A
            # Calculate start indices based on overlap with Bsd
            Asd_global_target = shift_ranges(Bsd_global_shifted, map(-, Ainds_offset))
            Aidx_start_vals = ntuple(i->clamp(fld1(Asd_global_target[i].start, Ablocksize[i]), Aidx_range[i].start, Aidx_range[i].stop), Nmax)
            Aidx_start = CartesianIndex(Aidx_start_vals)
            # Calculate end indices based on overlap with Bsd
            Aidx_end_vals = ntuple(i->clamp(fld1(Asd_global_target[i].stop, Ablocksize[i]), Aidx_range[i].start, Aidx_range[i].stop), Nmax)
            Aidx_end = CartesianIndex(Aidx_end_vals)

            # Copy all overlapping subdomains of A
            for Aidx in Aidx_start:Aidx_end
                Apart = A.chunks[Aidx]
                Asd_global_raw = padNmax(Asd_all[Aidx])
                Asd_global_shifted = shift_ranges(Asd_global_raw, Ainds_offset)

                # Compute the global ranges
                range_overlap = intersect(CartesianIndices(Bsd_global_shifted), CartesianIndices(Asd_global_shifted))
                Brange_start = ntuple(i->Bsd_global_raw[i].start, Nmax)
                Arange_start = ntuple(i->Asd_global_raw[i].start, Nmax)
                Brange_global = range_overlap .+ CartesianIndex(Binds_offset)
                Arange_global = range_overlap .+ CartesianIndex(Ainds_offset)

                # Clamp to the selected indices
                Brange_global_clamped = intersect(Brange_global, CartesianIndices(Binds_range))
                Arange_global_clamped = intersect(Arange_global, CartesianIndices(Ainds_range))

                # Compute the local ranges
                Brange_local = Brange_global_clamped .- CartesianIndex(Brange_start) .+ CartesianIndex{Nmax}(1)
                Arange_local = Arange_global_clamped .- CartesianIndex(Arange_start) .+ CartesianIndex{Nmax}(1)

                # Perform local view copy
                Dagger.@spawn copyto_view!(Out(Bpart), Brange_local, In(Apart), Arange_local)
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