import Base.Sort: Forward, Ordering, lt
import SharedArrays: SharedArray

import StatsBase: fit!, sample


function getmedians(x, n)
    q,r = divrem(length(x), n+1)

    if q == 0
        return x
    end
    buckets = [q for _ in 1:n+1]
    for i in 1:r
        buckets[i] += 1
    end
    pop!(buckets)
    x[cumsum(buckets)]
end

function sortandsample_array(ord, xs, nsamples, presorted=false)
    r = sample(1:length(xs), min(length(xs), nsamples),
               replace=false, ordered=true)
    if !presorted
        sorted = sort(xs, order=ord)
        chunk = tochunk(sorted)
        samples = sorted[r]
    else
        chunk = nothing # avoid communicating metadata if already sorted
        samples = chunk[r]
    end

    (chunk, samples)
end

function batchedsplitmerge(chunks, splitters, batchsize, start_proc=1;
                           merge=merge_sorted,
                           by=identity,
                           sub=getindex,
                           order=default_ord)

    if batchsize >= length(chunks)
        if isempty(splitters)
            # this means we need to combine all chunks
            # into a single chunk.
            return [collect_merge(merge, chunks)]
        else
            # split each chunk by the splitters, then merge
            # corresponding portions from all the chunks
            return splitmerge(chunks, splitters, merge, by, sub, order)
        end
    end

    # group chunks into batches:
    # e.g. if length(chunks) = 128, and batchsize = 8
    # then we will have q = 16, r=0 and length(b) = 16
    q, r = divrem(length(chunks), batchsize)
    b = [batchsize for _ in 1:q]
    r != 0 && push!(b, r) # add the remaining as a batch
    # create ranges that can extract a batch each
    batch_ranges = map(UnitRange, cumsum(vcat(1, b[1:end-1])), cumsum(b))
    # make the batches of chunks
    batches = map(range->chunks[range], batch_ranges)

    # splitmerge each batch
    topsplits, lowersplits = recursive_splitters(order, splitters, length(chunks), batchsize)

    sorted_batches = map(batches) do b
        isempty(topsplits) ?
            [collect_merge(merge, b)] :
            splitmerge(b, topsplits, merge, by, sub, order)
    end

    range_groups = transpose_vecvec(sorted_batches)

    chunks = []
    p = start_proc
    for i = 1:length(range_groups)
        s = lowersplits[i]
        group = range_groups[i]
        if !isempty(s)
            # recursively call batchedsplitmerge with a smaller batch
            cs = batchedsplitmerge(group, s, batchsize; merge = merge, by=by, sub=sub, order=order)
            append!(chunks, cs)
        else
            push!(chunks, collect_merge(merge, group))
        end
    end
    return chunks
end

function collect_merge(merge, group)
    #delayed((xs...) -> treereduce(merge, Any[xs...]))(group...)
    t = treereduce((args...) -> (Dagger.@spawn merge(args...)), group)
end

# Given sorted chunks, splits each chunk according to splitters
# then merges corresponding splits together to form length(splitters) + 1 sorted chunks
# these chunks will be in turn sorted
function splitmerge(chunks, splitters, merge, by, sub, ord)
    c1 = map(c->splitchunk(c, splitters, by, sub, ord), chunks)
    map(cs->collect_merge(merge, cs), transpose_vecvec(c1))
end

function splitchunk(c, splitters, by, sub, ord)
    function getbetween(xs, lo, hi, ord)
        i = searchsortedlast(xs, lo, order=ord)
        j = searchsortedlast(xs, hi, order=ord)
        (i+1):j
    end

    function getgt(xs, lo, ord)
        i = searchsortedlast(xs, lo, order=ord)
        (i+1):length(xs)
    end

    function getlt(xs, hi, ord)
        j = searchsortedlast(xs, hi, order=ord)
        1:j
    end

    between = map((hi, lo) -> Dagger.spawn(x->sub(x, getbetween(by(x), hi, lo, ord)), c),
                  splitters[1:end-1], splitters[2:end])
    hi = splitters[1]
    lo = splitters[end]

    a = Dagger.spawn(x->sub(x, getlt(by(x), hi, ord)), c)
    b = between
    c = Dagger.spawn(x->sub(x, getgt(by(x), lo, ord)), c)
    return map(fetch, vcat(a, b, c))

   #[delayed(c->sub(c, getlt(by(c), hi, ord)))(c);
   # between; delayed(c->sub(c, getgt(by(c), lo, ord)))(c)]
end

# transpose a vector of vectors
function transpose_vecvec(xs)
    map(1:length(xs[1])) do i
        map(x->x[i], xs)
    end
end

const use_shared_array = Ref(true)
function _promote_array(x::AbstractArray{T}, y::AbstractArray{S}) where {T,S}
    Q = promote_type(T,S)
    ok = (isa(x, Array) || isa(x, SharedArray)) && (isa(y, Array) || isa(y, SharedArray))
    if ok && isbitstype(Q) && use_shared_array[] && Distributed.check_same_host([workers()..., 1])
        return SharedArray{Q}(length(x)+length(y), pids=Distributed.procs())
    else
        return similar(x, Q, length(x)+length(y))
    end
end

function merge_sorted(ord::Ordering, x::AbstractArray, y::AbstractArray)
    z = _promote_array(x, y)
    _merge_sorted(ord, z, x, y)
end

function _merge_sorted(ord::Ordering, z::AbstractArray{T}, x::AbstractArray{T}, y::AbstractArray{S}) where {T, S}
    n = length(x) + length(y)
    i = 1; j = 1; k = 1
    len_x = length(x)
    len_y = length(y)
    @inbounds while i <= len_x && j <= len_y
        if lt(ord, x[i], y[j])
            z[k] = x[i]
            i += 1
        else
            z[k] = y[j]
            j += 1
        end
        k += 1
    end
    remaining, m = i <= len_x ? (x, i) : (y, j)
    @inbounds while k <= n
        z[k] = remaining[m]
        k += 1
        m += 1
    end
    z
end

"""
    recursive_splitters(ord, splitters, nchunks, batchsize) -> Tuple{Vector, Vector{Vector}}

Split the splitters themselves into batches.

# Arguments
- `ord`: `Sorting.Ordering` object
- `splitters`: the `nchunks-1` splitters
- `batchsize`: batch size

# Returns
A `Tuple{Vector, Vector{Vector}}` -- the coarse splitters which
create `batchsize` splits, finer splitters within those batches
which create a total of `nchunks` splits.

# Example
```julia-repl
julia> Dagger.recursive_splitters(Dagger.default_ord,
    [10,20,30,40,50,60], 5,3)
([30], Any[[10, 20], [40, 50, 60]])
```

The first value `[30]` represents a coarse split that cuts the dataset
from -Inf-30, and 30-Inf. Each part is further recursively split using
the next set of splitters
"""
function recursive_splitters(ord, splitters, nchunks, batchsize)

    # final number of chunks
    noutchunks = length(splitters) + 1
    # chunks per batch
    perbatch = ceil(Int, nchunks / batchsize)
    root = getmedians(splitters, perbatch-1)

    subsplits = []
    i = 1
    for c in root
        j = findlast(x->lt(ord, x, c), splitters)
        if j===nothing
            j = length(splitters)
        end
        push!(subsplits, splitters[i:j])
        i = j+2
    end
    push!(subsplits, splitters[i:end])
    root, subsplits
end

arrayorvcat(x::AbstractArray,y::AbstractArray) = vcat(x,y)
arrayorvcat(x,y) = [x,y]

const default_ord = Base.Sort.ord(isless, identity, false, Forward)

"""
    dsort_chunks(cs, [nchunks, nsamples]; options...)

Sort contents of chunks (`cs`) and return a new set of chunks
such that the chunks when concatenated return a sorted collection.
Each chunk in turn is sorted.

# Arguments
- `nchunks`: the number of chunks to produce, regardless of how many chunks were given as input
- `nsamples`: the number of elements to sample from each chunk to guess the splitters (`nchunks-1` splitters) each chunk will be delimited by the splitter.
- `merge`: a function to merge two sorted collections.
- `sub`: a function to get a subset of the collection takes (collection, range) (defaults to `getindex`)
- `order`: `Base.Sort.Ordering` to be used for sorting
- `batchsize`: number of chunks to split and merge at a time (e.g. if there are 128 input chunks and 128 output chunks, and batchsize is 8, then we first sort among batches of 8 chunks -- giving 16 batches. Then we sort among the first chunk of the first 8 batches (all elements less than the first splitter), then go on to the first 8 chunks of the second 8 batches, and so on...
- `chunks_presorted`: is each chunk in the input already sorted?
- `sortandsample`: a function to sort a chunk, then sample N elements to infer the splitters. It takes 3 arguments: (collection, N, presorted). presorted is a boolean which is true if the chunk is already sorted.
- `affinities`: a list of processes where the output chunks should go. If the length is not equal to `nchunks` then affinities array is cycled through.

# Returns
A tuple of `(chunk, samples)` where `chunk` is the `Dagger.Chunk` object. `chunk` can be `nothing` if no change to the initial array was made (e.g. it was already sorted)
"""
function dsort_chunks(cs, nchunks=length(cs), nsamples=2000;
                      merge = merge_sorted,
                      by=identity,
                      sub=getindex,
                      order=default_ord,
                      batchsize=max(2, nworkers()),
                      splitters=nothing,
                      chunks_presorted=false,
                      sortandsample = (x,ns, presorted)->sortandsample_array(order, x,ns, presorted),
                      affinities=workers(),
                     )
    if splitters != nothing
        # this means splitters are given beforehand
        nsamples = 0 # no samples needed
    end

    # first sort each chunk and sample nsamples elements from each chunk
    cs1 = map(c->Dagger.spawn(sortandsample, c, nsamples, chunks_presorted), cs)
    batchsize = max(2, batchsize)

    xs = treereduce((cs...)->Dagger.spawn(vcat, cs...), cs1)
    xs = collect(fetch(xs))
    if length(cs1) == 1
        xs = [xs]
    end

    # get nchunks-1 splitters that equally split the samples into nchunks pieces
    if splitters === nothing
        samples = reduce((a,b)->merge_sorted(order, a, b), map(x->x[2], xs))
        splitters = getmedians(samples, nchunks-1)
    end
    # if first(x) === nothing this means that
    # the chunk was already sorted, so just
    # use the input chunk as-is
    cs2 = map((x,c) -> x === nothing ? c : x, map(first, xs), cs)

    # main sort routine. At this point:
    # we know the splitters we want to use,
    # and each chunk is already sorted
    batchedsplitmerge(cs2,
                      splitters,
                      batchsize;
        merge=merge, by=by, sub=sub, order=order)
    #=
    for (w, c) in zip(Iterators.cycle(affinities), cs)
        propagate_affinity!(c, Dagger.OSProc(w) => 1)
    end
    =#
end

function propagate_affinity!(c, aff)
    if !isa(c, DTask)
        return
    end
    if c.affinity !== nothing
        push!(c.affinity, aff)
    else
        c.affinity = [aff]
    end

    for t in c.inputs
        propagate_affinity!(t, aff)
    end
end

function Base.sort(v::ArrayOp;
               lt=Base.isless,
               by=identity,
               nchunks=nothing,
               rev::Bool=false,
               batchsize=max(2, nworkers()),
               nsamples=2000,
               order::Ordering=default_ord)
    v1 = fetch(v)
    ord = Base.Sort.ord(lt,by,rev,order)
    nchunks = nchunks === nothing ? length(v1.chunks) : nchunks
    cs = dsort_chunks(v1.chunks, nchunks, nsamples,
                      order=ord, merge=(x,y)->merge_sorted(ord, x,y))
    t = Dagger.spawn((xs...)->[xs...], cs...)
    chunks = fetch(t)
    dmn = ArrayDomain((1:sum(length(domain(c)) for c in chunks),))
    DArray(eltype(v1), dmn, map(domain, chunks), chunks,
           v.partitioning, v.concat)
end
