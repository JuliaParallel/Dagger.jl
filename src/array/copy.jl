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

crop_range(x::UnitRange, y::UnitRange) = max(x.start, y.start):min(x.stop, y.stop)

function darray_copyto!(B::DArray{TB,NB}, A::DArray{TA,NA},  Binds=parentindices(B), Ainds=parentindices(A)) where {TB,NB,TA,NA}
    Nmax = max(NA, NB)

    pad1(x, i) = length(x) < i ? 1 : x[i]
    pad1range(x, i) = length(x) < i ? (1:1) : x[i]

    to_range(x::UnitRange) = x
    to_range(x::Integer) = x:x
    to_range(x::Base.OneTo{Int}) = UnitRange(x)
    to_range(x::Base.Slice{Base.OneTo{Int}}) = UnitRange(x)

    Bsd = B.subdomains::DomainBlocks{NB}
    Asd = A.subdomains::DomainBlocks{NA}

    Bsd_all = collect(reshape(Bsd, ntuple(i->pad1(size(Bsd), i), Nmax)))
    Asd_all = collect(reshape(Asd, ntuple(i->pad1(size(Asd), i), Nmax)))

    bszA = A.partitioning.blocksize
    bszB = B.partitioning.blocksize

    Bidx_real = ntuple(i->to_range(pad1(Binds, i)), Nmax)
    Aidx_real = ntuple(i->to_range(pad1(Ainds, i)), Nmax)

    Bc = B.chunks
    Ac = A.chunks

    #Define which chunks of B we will be copying into
    Bidx_start = ntuple(i->fld1(pad1range(Bidx_real, i).start, pad1(bszB, i)), Nmax)
    Bidx_end = ntuple(i->fld1(pad1range(Bidx_real, i).stop, pad1(bszB, i)), Nmax)
    spanBidx = CartesianIndices(ntuple(i->Bidx_start[i]:Bidx_end[i], Nmax))

    #Define which chunks of A we will be copying from
    Aidx_start = ntuple(i->fld1(pad1range(Aidx_real, i).start, pad1(bszA, i)), Nmax)
    #Aidx_end = ntuple(i->fld1(pad1range(Aidx_real, i).stop, pad1(bszA, i)), Nmax)
    #spanAidx = CartesianIndices(ntuple(i->Aidx_start[i]:Aidx_end[i], Nmax))

    #Adjust the subdomain indexes to be relative to the real index range
    Bsd_all[first(spanBidx)] = ArrayDomain(ntuple(i->crop_range(pad1range(Bsd_all[first(spanBidx)].indexes, i), Bidx_real[i]), Nmax))
    Bsd_all[last(spanBidx)] = ArrayDomain(ntuple(i->crop_range(pad1range(Bsd_all[last(spanBidx)].indexes, i), Bidx_real[i]), Nmax))

    prev_Aidx = CartesianIndex(ntuple(i->Aidx_start[i], Nmax))
    Acc_len = ntuple(zero, Nmax)

    prev_Bsd = Bsd_all[first(spanBidx)].indexes
    if first(spanBidx) == last(spanBidx)
        Aidx_end = ntuple(i -> i > length(bszA) ? 1 : Aidx_start[i] + fld(length(prev_Bsd[i]), bszA[i]), Nmax)
    else
        Aidx_end = ntuple(i -> i > length(bszA) ? 1 : Aidx_start[i] + fld(Bsd_all[spanBidx[2]].indexes[i].start - prev_Bsd[i].start, bszA[i]), Nmax)
    end

    Dagger.spawn_datadeps() do
        for Bidx in spanBidx
            Bpart = Bc[Bidx]
            Bsdcur = ArrayDomain(ntuple(i->crop_range(pad1range(Bsd_all[Bidx].indexes, i), Bidx_real[i]), Nmax))

            Aidx_start = ntuple(i->i > length(bszA) ? 1 : fld1(Bsdcur.indexes[i].start - prev_Bsd[i].start + Asd_all[prev_Aidx].indexes[i].start, bszA[i]), Nmax)
            Aidx_end = ntuple(i->i > length(bszA) ? 1 : fld1(Bsdcur.indexes[i].start - prev_Bsd[i].start + Asd_all[prev_Aidx].indexes[i].stop, bszA[i]), Nmax)
            Aidx_span = CartesianIndices(ntuple(i->Aidx_start[i]:Aidx_end[i], Nmax))
            prev_Bsd = Bsdcur.indexes

            # Copy all overlapping subdomains of A into Bpart
            Bacc_len = ntuple(zero, Nmax)
            for Aidx in Aidx_span
                Apart = Ac[Aidx]
                Asdcur =  ArrayDomain(ntuple(i->crop_range(pad1range(Asd_all[Aidx].indexes, i), Aidx_real[i]), NA))
                
                if prev_Aidx != Aidx
                    Acc_len = ntuple(zero, Nmax)
                end
                prev_Aidx = Aidx

                tempBstart = ntuple(i->pad1range(Bsdcur.indexes, i).start + Bacc_len[i], Nmax)
                tempAstart = ntuple(i->pad1range(Asdcur.indexes, i).start + Acc_len[i], Nmax)
                Cp_len = ntuple(i->min(pad1range(Asdcur.indexes, i).stop - tempAstart[i] + 1, pad1range(Bsdcur.indexes, i).stop - tempBstart[i] + 1), Nmax)

                # Compute the offset range into Apart
                Aintra = ntuple(i->tempAstart[i] - pad1range(Asd[Aidx].indexes, i).start + 1, Nmax)
                Arange = ntuple(i->(Aintra[i]):(Aintra[i] + Cp_len[i] - 1), Nmax)
                Acc_len = ntuple(i->Cp_len[i] == 1 ? Acc_len[i] : Acc_len[i] + length(Arange[i]), Nmax)

                # Compute the offset range into Bpart
                Bintra = ntuple(i->tempBstart[i] - pad1range(Bsd[Bidx].indexes, i).start + 1, Nmax)
                Brange = ntuple(i->(Bintra[i]):(Bintra[i] + Cp_len[i] - 1), Nmax)
                Bacc_len = ntuple(i -> Cp_len[i] == 1 ?  Bacc_len[i] : Bacc_len[i] + length(Brange[i]), Nmax)

                Dagger.@spawn copyto_view!(Out(Bpart), Brange, In(Apart), Arange)
            end
        end
    end
    return B
end
function copyto_view!(Bpart, Brange, Apart, Arange)
    copyto!(view(Bpart, Brange...), view(Apart, Arange...))
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