import Base.Sort: Forward, Ordering, Algorithm, lt

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
    if presorted
        perm = Base.OneTo(length(xs))
    else
        perm = sortperm(xs, order=ord)
    end
    samples = xs[perm[r]]
    (tochunk(perm), samples)
end

function batchedsplitmerge(chunks, splitters, batchsize, start_proc=1; merge=merge_sorted, sub=getindex, key=identity, order=default_ord)
    if batchsize >= length(chunks)
        if isempty(splitters)
            return [collect_merge(merge, chunks)]
        else
            return splitmerge(chunks, splitters, merge, sub, key, order)
        end
    end

    # group chunks into batches:
    q, r = divrem(length(chunks), batchsize)
    b = [batchsize for _ in 1:q]
    r != 0 && push!(b, r)
    batch_ranges = map(UnitRange, cumsum([1, b[1:end-1];]), cumsum(b))
    batches = map(x->chunks[x], batch_ranges)

    # splitmerge each batch
    topsplits, lowersplits = splitter_levels(order, splitters, length(chunks), batchsize)

    sorted_batches = map(batches) do b
        isempty(topsplits) ?
            [collect_merge(merge, b)] :
            splitmerge(b, topsplits, merge, sub, key, order)
    end

    range_groups = transpose_vecvec(sorted_batches)

    chunks = []
    p = start_proc
    for i = 1:length(range_groups)
        s = lowersplits[i]
        group = range_groups[i]
        if !isempty(s)
            cs = batchedsplitmerge(group, s, batchsize; merge = merge, sub=sub, key=key, order=order)
            append!(chunks, cs)
        else
            push!(chunks, collect_merge(merge, group))
        end
    end
    return chunks
end

function collect_merge(merge, group)
    #delayed((xs...) -> treereduce(merge, Any[xs...]))(group...)
    t = treereduce(delayed((x,y)->merge(x, y)), group)
end

# Given sorted chunks, splits each chunk according to splitters
# then merges corresponding splits together to form length(splitters) + 1 sorted chunks
# these chunks will be in turn sorted
function splitmerge(chunks, splitters, merge, sub, key, ord)
    c1 = map(c->splitchunk(c, splitters, sub, key, ord), chunks)
    map(cs->collect_merge(merge, cs), transpose_vecvec(c1))
end

import Base: lt,Ordering
function searchsortedlastperm(v::AbstractVector, x, perm, o::Ordering=Base.Forward, lo::Int=1, hi::Int=length(v))
    lo = lo-1
    hi = hi+1
    while lo < hi-1
        m = (lo+hi)>>>1
        if lt(o, x, v[perm[m]])
            hi = m
        else
            lo = m
        end
    end
    return lo
end

function splitchunk(c, splitters, sub, key, ord)
    function getbetween(data, lo, hi, ord)
        xs, perm = data
        i = searchsortedlastperm(key(xs), lo, perm, ord)
        j = searchsortedlastperm(key(xs), hi, perm, ord)
        perm[(i+1):j]
    end

    function getgt(data, lo, ord)
        xs, perm = data
        i = searchsortedlastperm(key(xs), lo, perm, ord)
        perm[(i+1):length(xs)]
    end

    function getlt(data, hi, ord)
        xs, perm = data
        j = searchsortedlastperm(key(xs), hi, perm, ord)
        perm[1:j]
    end

    function getsubpair(xs, idxs)
        sub(xs[1], idxs) => Base.OneTo(length(idxs))
    end

    between = map((hi, lo) -> delayed(c->getsubpair(c, getbetween(c, hi, lo, ord)))(c),
                  splitters[1:end-1], splitters[2:end])
    hi = splitters[1]
    lo = splitters[end]
    [delayed(c->getsubpair(c, getlt(c, hi, ord)))(c);
     between; delayed(c->getsubpair(c, getgt(c, lo, ord)))(c)]
end

# transpose a vector of vectors
function transpose_vecvec(xs)
    map(1:length(xs[1])) do i
        map(x->x[i], xs)
    end
end

function _promote_array{T,S}(x::AbstractArray{T}, y::AbstractArray{S})
    Q = promote_type(T,S)
    samehost = Distributed.check_same_host(procs())
    ok = (isa(x, Array) || isa(x, SharedArray)) && (isa(y, Array) || isa(y, SharedArray))
    if samehost && ok && isbits(Q)
        return Array{Q}(length(x)+length(y))#, pids=procs())
    else
        return similar(x, Q, length(x)+length(y))
    end
end

function merge_sorted(ord::Ordering, x::Pair, y::Pair)
    xx, xp = x
    yy, yp = y
    zz = _promote_array(xx, yy)
    _merge_sorted!(ord, zz, xx, yy, xp, yp)
    zz => Base.OneTo(length(zz))
end

function merge_sorted(ord::Ordering, x::AbstractArray, y::AbstractArray)
    merge_sorted(ord, x=>Base.OneTo(length(x)), y=>Base.OneTo(length(y)))
end
function merge_sorted(ord::Ordering, x::Pair, y::AbstractArray)
    merge_sorted(ord, x, y=>Base.OneTo(length(y)))
end
function merge_sorted(ord::Ordering, x::AbstractArray, y::Pair)
    merge_sorted(ord, x=>Base.OneTo(length(x)), y)
end

function _merge_sorted!{T, S}(ord::Ordering, z::AbstractArray{T}, x::AbstractArray{T}, y::AbstractArray{S}, xp, yp)
    n = length(x) + length(y)
    i = 1; j = 1; k = 1
    len_x = length(x)
    len_y = length(y)
    if len_x == 0
        copy!(z, y)
        return
    elseif len_y == 0
        copy!(z, x)
        return
    end
    while i <= len_x && j <= len_y
        if lt(ord, x[xp[i]], y[yp[j]])
            z[k] = x[xp[i]]
            i += 1
        else
            z[k] = y[yp[j]]
            j += 1
        end
        k += 1
    end
    if i <= len_x
        while k <= n
            z[k] = x[xp[i]]
            k += 1
            i += 1
        end
    elseif j <= len_y
        while k <= n
            z[k] = y[yp[j]]
            k += 1
            j += 1
        end
    end
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
                      sub=getindex,
                      key=identity,
                      order=default_ord,
                      merge = (x,y) -> merge_sorted(ord, x, y),
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
        samps = last.(xs)
        if length(samps) > 1
            samples = treereduce((x,y) -> merge_sorted(order, x, y), samps)[1]
        else
            samples = samps[1]
        end
        splitters = getmedians(samples, nchunks-1)
    end

    cs = batchedsplitmerge(map(delayed((x,p)->x=>p), cs, first.(xs)), splitters, batchsize; merge=merge, sub=sub, key=key, order=order)
    for (w, c) in zip(Iterators.cycle(affinities), cs)
        propagate_affinity!(c, Dagger.OSProc(w) => 1)
    end
    cs
end

function propagate_affinity!(c, aff)
    if !isa(c, Thunk)
        return
    end
    if !isnull(c.affinity)
        push!(get(c.affinity), aff)
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
    f = function (x)
        if isa(x[2], Base.OneTo)
            x[1]
        else
            x[1][x[2]]
        end
    end
    t=delayed((xs...)->[xs...]; meta=true)(map(delayed(f), cs)...)
    chunks = compute(t)
    dmn = ArrayDomain((1:sum(length(domain(c)) for c in chunks),))
    DArray(eltype(v1), dmn, map(domain, chunks), chunks)
end
