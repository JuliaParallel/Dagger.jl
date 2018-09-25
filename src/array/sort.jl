import Base.Sort: Forward, Ordering, Algorithm, lt
using Distributed

using StatsBase

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

function batchedsplitmerge(chunks, splitters, batchsize, start_proc=1; merge=merge_sorted, by=identity, sub=getindex, order=default_ord)
    if batchsize >= length(chunks)
        if isempty(splitters)
            return [collect_merge(merge, chunks)]
        else
            return splitmerge(chunks, splitters, merge, by, sub, order)
        end
    end

    # group chunks into batches:
    q, r = divrem(length(chunks), batchsize)
    b = [batchsize for _ in 1:q]
    r != 0 && push!(b, r)
    batch_ranges = map(UnitRange, cumsum(vcat(1, b[1:end-1])), cumsum(b))
    batches = map(x->chunks[x], batch_ranges)

    # splitmerge each batch
    topsplits, lowersplits = splitter_levels(order, splitters, length(chunks), batchsize)

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
    t = treereduce(delayed(merge), group)
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

    between = map((hi, lo) -> delayed(c->sub(c, getbetween(by(c), hi, lo, ord)))(c),
                  splitters[1:end-1], splitters[2:end])
    hi = splitters[1]
    lo = splitters[end]
    [delayed(c->sub(c, getlt(by(c), hi, ord)))(c);
     between; delayed(c->sub(c, getgt(by(c), lo, ord)))(c)]
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

function splitter_levels(ord, splitters, nchunks, batchsize)
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
    if splitters !== nothing
        nsamples = 0 # no samples needed
    end

    cs1 = map(c->delayed(sortandsample)(c, nsamples, chunks_presorted), cs)
    xs = collect(treereduce(delayed(vcat), cs1))
    if length(cs1) == 1
        xs = [xs]
    end

    if splitters === nothing
        samples = reduce((a,b)->merge_sorted(order, a, b), map(x->x[2], xs))
        splitters = getmedians(samples, nchunks-1)
    end

    cs = batchedsplitmerge(map((x,c) -> first(x) === nothing ? c : first(x), xs, cs), splitters, batchsize; merge=merge, by=by, sub=sub, order=order)
    #=
    for (w, c) in zip(Iterators.cycle(affinities), cs)
        propagate_affinity!(c, Dagger.OSProc(w) => 1)
    end
    =#
    cs
end

function propagate_affinity!(c, aff)
    if !isa(c, Thunk)
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
    v1 = compute(v)
    ord = Base.Sort.ord(lt,by,rev,order)
    nchunks = nchunks === nothing ? length(v1.chunks) : nchunks
    cs = dsort_chunks(v1.chunks, nchunks, nsamples,
                      order=ord, merge=(x,y)->merge_sorted(ord, x,y))
    map(persist!, cs)
    t=delayed((xs...)->[xs...]; meta=true)(cs...)
    chunks = compute(t)
    dmn = ArrayDomain((1:sum(length(domain(c)) for c in chunks),))
    DArray(eltype(v1), dmn, map(domain, chunks), chunks)
end
