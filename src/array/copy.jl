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
"""
    allocate_tiled(::Type{TT}, ::Type{T}, part::Blocks{N}, dims::Dims{N}) -> DArray

Allocate a `part`-partitioned `DArray{T,N}` of `dims` whose tiles have the same
*backend* as tiles of type `TT`. Dispatching on the tile type is what keeps a
re-tiling of a sparse array sparse: allocating dense tiles for it would turn a
repartitioning copy into a densification, which for a large sparse operator is
an out-of-memory multiplier rather than a slowdown.

`array/sparse.jl` adds the [`DSparseArray`](@ref) method.
"""
allocate_tiled(::Type{TT}, ::Type{T}, part::Blocks{N}, dims::Dims{N}) where {TT,T,N} =
    DArray{T}(undef, part, dims)

# The tile type of `A`, for `allocate_tiled`. A `DArray`'s own type parameters do
# not record it, so it comes off a chunk.
#
# A `DTask`'s `chunktype` is only its declared or inferred `return_type`, which
# is not always the type it will actually produce: `distribute` spawns
# `maybe_wrap_tile` per tile, whose return type is the *union* of the wrapped and
# unwrapped tile types, so a sparse array's tiles advertise something that is not
# `<:DSparseArray`. Dispatching on that gives dense tiles and silently densifies
# the array -- numerically correct, so tests pass, while memory use explodes. Ask
# the chunk itself in that case; `raw=true` keeps it a `Chunk`, so this waits for
# the tile's task but moves no data, and every caller is about to copy the whole
# array anyway.
function darray_tiletype(A::DArray)
    isempty(A.chunks) && return Any
    c = first(A.chunks)
    TT = chunktype(c)
    isconcretetype(TT) && return TT
    return chunktype(_resolved_chunk(c))
end
_resolved_chunk(c::Chunk) = c
_resolved_chunk(c) = fetch(c; raw=true)

allocate_copy_buffer(part::Blocks{N}, A::DArray{T,N}) where {T,N} =
    allocate_tiled(darray_tiletype(A), T, part, size(A))

"""
    repartition(A::DArray, part::Blocks) -> DArray

A copy of `A` re-tiled to `part`, preserving the tile backend (sparse tiles stay
sparse). Returns `A` itself if it is already partitioned that way.

Unlike `maybe_copy_buffered`, the result is an ordinary array whose
lifetime is not tied to a call: that function frees its buffers as soon as its
body returns, which is wrong whenever the re-tiled *tiles* outlive the call —
e.g. a block preconditioner, whose per-tile operators are built from them by
tasks it does not await.
"""
function repartition(A::DArray{T,N}, part::Blocks{N}) where {T,N}
    A.partitioning == part && return A
    B = allocate_tiled(darray_tiletype(A), T, part, size(A))
    copyto!(B, A)
    return B
end

to_range(x::UnitRange) = x
to_range(x::Integer) = x:x
to_range(x::Base.OneTo{Int}) = UnitRange(x)
to_range(x::Base.Slice) = Int(first(x)):Int(last(x))
to_range(::StepRange) = throw(ArgumentError("Cannot convert StepRange to UnitRange"))
to_range(x) = throw(ArgumentError("Cannot convert $(typeof(x)) to UnitRange"))

# Normalize a `parentindices` entry for a copy. Integers become length-1
# ranges (dropped dimensions / scalar parent indices). StepRange is left as
# an error: `copyto!` of a non-contiguous `view` is an intentional throw
# (`test/array/copyto.jl`). `getindex` converts StepRange to `Vector{Int}`
# before it reaches here.
_copy_index(x::UnitRange{<:Integer}) = UnitRange{Int}(x)
_copy_index(x::Integer) = Int(x):Int(x)
_copy_index(x::Base.OneTo) = UnitRange{Int}(x)
_copy_index(x::Base.Slice) = Int(first(x)):Int(last(x))
_copy_index(::StepRange) = throw(ArgumentError("Cannot convert StepRange to UnitRange"))
# `[]` is `Vector{Any}`; `to_indices` leaves it alone. Empty and mixed
# integer vectors still copy (the scattered path no-ops on a zero selection).
_copy_index(x::AbstractVector) = x isa Vector{Int} ? x : collect(Int, x)
_copy_index(x::Base.LogicalIndex) = collect(Int, x)
_copy_index(x) = throw(ArgumentError("Cannot convert $(typeof(x)) to a copy index"))

_is_scattered_copy_index(x::AbstractRange) = false
_is_scattered_copy_index(x::AbstractVector) = true
_is_scattered_copy_index(x) = false

# Linear `parentindices` of an N>1 array (`view(A, 9:12)` / `A[9:12]`).
_is_linear_parentinds(::DArray{<:Any,N}, inds) where N = N > 1 && length(inds) == 1

function _chunk_and_local(A::DArray{T,N}, dim::Int, p::Int) where {T,N}
    dim > N && return (1, 1)
    sd = A.subdomains
    start = sd.start[dim]
    cum = sd.cumlength[dim]
    rel = p - start + 1
    ci = searchsortedfirst(cum, rel)
    (1 <= ci <= length(cum)) || throw(BoundsError(A, ntuple(d -> d == dim ? p : 1, N)))
    prev = ci == 1 ? 0 : cum[ci-1]
    tile_start = prev + start
    return (ci, p - tile_start + 1)
end

_index_at(ind::AbstractRange, k::Int) = Int(ind[k])
_index_at(ind::AbstractVector, k::Int) = Int(ind[k])

# Length before `_copy_index`: a StepRange view into a differently-sized dest
# is `DimensionMismatch` (`test/array/copyto.jl`). Same-length StepRange still
# throws `ArgumentError` at conversion — that is the intentional contract.
_copy_index_length(x::Integer) = 1
_copy_index_length(x) = length(x)
function _copy_inds_lengths_match(Binds, Ainds)
    n = max(length(Binds), length(Ainds))
    for i in 1:n
        bl = i <= length(Binds) ? _copy_index_length(Binds[i]) : 1
        al = i <= length(Ainds) ? _copy_index_length(Ainds[i]) : 1
        bl == al || return false
    end
    return true
end

function darray_copyto!(B::DArray{TB,NB}, A::DArray{TA,NA}, Binds=parentindices(B), Ainds=parentindices(A)) where {TB,NB,TA,NA}
    if _is_linear_parentinds(B, Binds) || _is_linear_parentinds(A, Ainds)
        return _darray_copyto_linear!(B, A, Binds, Ainds)
    end

    _copy_inds_lengths_match(Binds, Ainds) || throw(DimensionMismatch(
        "Cannot copy from array of size $(size(A)) (indices $Ainds) to array of size $(size(B)) (indices $Binds)"))

    Binds_n = ntuple(i -> _copy_index(Binds[i]), length(Binds))
    Ainds_n = ntuple(i -> _copy_index(Ainds[i]), length(Ainds))
    if any(_is_scattered_copy_index, Binds_n) || any(_is_scattered_copy_index, Ainds_n)
        return _darray_copyto_scattered!(B, A, Binds_n, Ainds_n)
    end

    return _darray_copyto_contiguous!(B, A, Binds_n, Ainds_n)
end

function _darray_copyto_contiguous!(B::DArray{TB,NB}, A::DArray{TA,NA}, Binds, Ainds) where {TB,NB,TA,NA}
    Nmax = max(NA, NB, length(Binds), length(Ainds))

    pad1(x, i) = length(x) < i ? 1 : x[i]
    pad1range(x, i) = length(x) < i ? (1:1) : x[i]
    pad1range(x::ArrayDomain, i) = length(x.indexes) < i ? (1:1) : x.indexes[i]
    padNmax(x) = ntuple(i->pad1range(x, i), Nmax)
    padNmax(x::ArrayDomain) = padNmax(x.indexes)

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

# Non-contiguous (Vector) indices: one Datadeps task per overlapping tile
# pair, not one `spawn_datadeps` per selected element.
function _darray_copyto_scattered!(B::DArray{TB,NB}, A::DArray{TA,NA}, Binds, Ainds) where {TB,NB,TA,NA}
    Nmax = max(NA, NB, length(Binds), length(Ainds))
    Binds_p = ntuple(i -> i <= length(Binds) ? Binds[i] : (1:1), Nmax)
    Ainds_p = ntuple(i -> i <= length(Ainds) ? Ainds[i] : (1:1), Nmax)
    nsel = ntuple(i -> length(Binds_p[i]), Nmax)
    for i in 1:Nmax
        length(Binds_p[i]) == length(Ainds_p[i]) || throw(DimensionMismatch(
            "Cannot copy from array of size $(size(A)) (indices $Ainds) to array of size $(size(B)) (indices $Binds)"))
    end
    isempty(CartesianIndices(nsel)) && return B

    buckets = Dict{Tuple{CartesianIndex{Nmax},CartesianIndex{Nmax}},
                   Vector{Tuple{CartesianIndex{Nmax},CartesianIndex{Nmax}}}}()
    for k in CartesianIndices(nsel)
        Bchunk_t = ntuple(d -> _chunk_and_local(B, d, _index_at(Binds_p[d], k[d]))[1], Nmax)
        Bloc_t = ntuple(d -> _chunk_and_local(B, d, _index_at(Binds_p[d], k[d]))[2], Nmax)
        Achunk_t = ntuple(d -> _chunk_and_local(A, d, _index_at(Ainds_p[d], k[d]))[1], Nmax)
        Aloc_t = ntuple(d -> _chunk_and_local(A, d, _index_at(Ainds_p[d], k[d]))[2], Nmax)
        key = (CartesianIndex(Bchunk_t), CartesianIndex(Achunk_t))
        pair = (CartesianIndex(Bloc_t), CartesianIndex(Aloc_t))
        dests = get!(buckets, key) do
            Vector{Tuple{CartesianIndex{Nmax},CartesianIndex{Nmax}}}()
        end
        push!(dests, pair)
    end

    Dagger.spawn_datadeps() do
        for ((Bidx, Aidx), pairs) in buckets
            dests = [p[1] for p in pairs]
            srcs = [p[2] for p in pairs]
            Dagger.@spawn copyto_scattered!(Out(B.chunks[Bidx]), dests, In(A.chunks[Aidx]), srcs)
        end
    end
    return B
end

function _linear_index_vec(inds)
    length(inds) == 1 || throw(ArgumentError("linear copy needs a single index vector, got $(inds)"))
    return _copy_index(inds[1])
end

function _darray_copyto_linear!(B::DArray{TB,NB}, A::DArray{TA,NA}, Binds, Ainds) where {TB,NB,TA,NA}
    Blin = _is_linear_parentinds(B, Binds) ? _linear_index_vec(Binds) :
           (length(Binds) == 1 ? _copy_index(Binds[1]) : throw(DimensionMismatch(
            "Cannot copy from array of size $(size(A)) (indices $Ainds) to array of size $(size(B)) (indices $Binds)")))
    Alin = _is_linear_parentinds(A, Ainds) ? _linear_index_vec(Ainds) :
           (length(Ainds) == 1 ? _copy_index(Ainds[1]) : throw(DimensionMismatch(
            "Cannot copy from array of size $(size(A)) (indices $Ainds) to array of size $(size(B)) (indices $Binds)")))
    length(Blin) == length(Alin) || throw(DimensionMismatch(
        "Cannot copy from array of size $(size(A)) (indices $Ainds) to array of size $(size(B)) (indices $Binds)"))
    n = length(Blin)
    n == 0 && return B

    Bcart = CartesianIndices(size(B))
    Acart = CartesianIndices(size(A))
    NB_ = ndims(B)
    NA_ = ndims(A)
    buckets = Dict{Tuple{CartesianIndex{NB_},CartesianIndex{NA_}},
                   Vector{Tuple{CartesianIndex{NB_},CartesianIndex{NA_}}}}()
    for k in 1:n
        BI = Bcart[Blin[k]]
        AI = Acart[Alin[k]]
        Bchunk_t = ntuple(d -> _chunk_and_local(B, d, BI[d])[1], NB_)
        Bloc_t = ntuple(d -> _chunk_and_local(B, d, BI[d])[2], NB_)
        Achunk_t = ntuple(d -> _chunk_and_local(A, d, AI[d])[1], NA_)
        Aloc_t = ntuple(d -> _chunk_and_local(A, d, AI[d])[2], NA_)
        key = (CartesianIndex(Bchunk_t), CartesianIndex(Achunk_t))
        pair = (CartesianIndex(Bloc_t), CartesianIndex(Aloc_t))
        dests = get!(buckets, key) do
            Vector{Tuple{CartesianIndex{NB_},CartesianIndex{NA_}}}()
        end
        push!(dests, pair)
    end

    Dagger.spawn_datadeps() do
        for ((Bidx, Aidx), pairs) in buckets
            dests = [p[1] for p in pairs]
            srcs = [p[2] for p in pairs]
            Dagger.@spawn copyto_scattered!(Out(B.chunks[Bidx]), dests, In(A.chunks[Aidx]), srcs)
        end
    end
    return B
end

function copyto_view!(Bpart, Brange, Apart, Arange)
    copyto!(view(Bpart, Brange), view(Apart, Arange))
    return
end

# Named so MPI ArgumentWrapper hashes stay rank-uniform (not a closure).
function copyto_scattered!(Bpart, dests, Apart, srcs)
    @inbounds for k in eachindex(dests)
        Bpart[dests[k]] = Apart[srcs[k]]
    end
    return
end

Base.copyto!(B::DArray{T,N}, A::DArray{T,N}) where {T,N} =
    darray_copyto!(B, A)
Base.copyto!(B::DArray{T,N}, A::Array{T,N}) where {T,N} =
    darray_copyto!(B, view(A, B.partitioning))
Base.copyto!(B::Array{T,N}, A::DArray{T,N}) where {T,N} =
    darray_copyto!(view(B, A.partitioning), A)
Base.copyto!(B::DArray, A::SubArray{T,N,<:Array}) where {T,N} =
    darray_copyto!(B, view(A, B.partitioning))

StridedDArray{T,N} = Union{<:DArray{T,N}, SubArray{T,N,<:DArray{T,NP}} where NP}

# `view(A::DArray{T,N}, i)` for N>1 is a linear SubArray of a ReshapedArray,
# not a SubArray of the DArray. Base copyto! then scalar-indexes.
const LinearDArrayView{T} = SubArray{T,1,<:Base.ReshapedArray{T,1,<:DArray}}
_linear_view_parent(A::LinearDArrayView) = parent(parent(A))

Base.copyto!(B::StridedDArray, A::StridedDArray) =
    darray_copyto!(parent(B), parent(A), parentindices(B), parentindices(A))
function Base.copyto!(B::Array, A::StridedDArray)
    DB = view(B, AutoBlocks())
    darray_copyto!(DB, parent(A), parentindices(DB), parentindices(A))
    return B
end
function Base.copyto!(B::StridedDArray, A::LinearDArrayView)
    darray_copyto!(parent(B), _linear_view_parent(A), parentindices(B), parentindices(A))
    return B
end
function Base.copyto!(B::LinearDArrayView, A::StridedDArray)
    darray_copyto!(_linear_view_parent(B), parent(A), parentindices(B), parentindices(A))
    return B
end
function Base.copyto!(B::LinearDArrayView, A::LinearDArrayView)
    darray_copyto!(_linear_view_parent(B), _linear_view_parent(A), parentindices(B), parentindices(A))
    return B
end
function Base.copyto!(B::Array, A::LinearDArrayView)
    DB = view(B, AutoBlocks())
    darray_copyto!(DB, _linear_view_parent(A), parentindices(DB), parentindices(A))
    return B
end
function Base.copyto!(B::LinearDArrayView, A::AbstractArray)
    DA = view(A, AutoBlocks())
    darray_copyto!(_linear_view_parent(B), DA, parentindices(B), axes(DA))
    return B
end
# `view(::DArray, I, J)` is a Base `SubArray`. Sending that parent through
# `view(::AbstractArray, ::Blocks)` would wrap it as a DArray of SubArrays of
# a DArray, and the copy would scalar-index (one task per element).
function Base.copyto!(B::SubArray, A::StridedDArray)
    Ap = parent(A)
    Ainds = parentindices(A)
    if parent(B) isa DArray
        darray_copyto!(parent(B), Ap, parentindices(B), Ainds)
    else
        DB = view(parent(B), AutoBlocks())
        darray_copyto!(DB, Ap, parentindices(B), Ainds)
    end
    return B
end

function _copyto_darray_view_from_local!(B::SubArray{<:Any,<:Any,<:DArray}, A::AbstractArray)
    size(B) == size(A) || throw(DimensionMismatch(
        "Cannot copy from array of size $(size(A)) to view of size $(size(B))"))
    DA = view(A, AutoBlocks())
    darray_copyto!(parent(B), DA, parentindices(B), axes(DA))
    return B
end
Base.copyto!(B::SubArray{T,N,<:DArray}, A::Array) where {T,N} =
    _copyto_darray_view_from_local!(B, A)
Base.copyto!(B::SubArray{T,N,<:DArray}, A::SubArray{S,M,<:Array}) where {T,N,S,M} =
    _copyto_darray_view_from_local!(B, A)
