using Dagger
import Dagger: treereduce, tochunk, DArray

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

function sortchunk(xs, nsamples)
    sorted = sort(xs)
    r = randperm(length(xs))[1:min(length(xs), nsamples)]
    (tochunk(sorted), getmedians(sorted, nsamples))
end

function evaluate(x, cs, splits)
    n = length(splits)
    q, r = divrem(length(x), n+1)
    if q == 0
        return [0]
    end
    buckets = [q for _ in 1:n+1]
    for i in 1:r
        buckets[i] += 1
    end
    map(s->length(find(x->x<=s, x)), splits) .- cumsum(buckets[1:end-1])
end

function batchedsplitmerge(chunks, splitters, batchsize, start_proc=1)
    if batchsize >= length(chunks)
        return splitmerge(chunks, splitters)
    end

    # group chunks into batches:
    q, r = divrem(length(chunks), batchsize)
    b = [batchsize for _ in 1:q]
    r != 0 && push!(b, r)
    batch_ranges = map(UnitRange, cumsum([1, b[1:end-1];]), cumsum(b))
    batches = map(x->chunks[x], batch_ranges)

    # splitmerge each batch
    topsplits, lowersplits = splitter_levels(splitters, length(chunks), batchsize)

    sorted_batches = map(batches) do b
        splitmerge(b, topsplits)
    end

    range_groups = transpose_vecvec(sorted_batches)

    chunks = []
    p = start_proc
    for i = 1:length(range_groups)
        s = lowersplits[i]
        group = range_groups[i]
        if !isempty(s)
            cs = batchedsplitmerge(group, s, batchsize)
            append!(chunks, cs)
        else
            push!(chunks, @show collect_merge(group))
        end
    end
    return chunks
end

function collect_merge(group)
    #delayed((xs...) -> treereduce(merge_sorted, Any[xs...]))(group...)
    t = treereduce(delayed(merge_sorted), group)
end

# Given sorted chunks, splits each chunk according to splitters
# then merges corresponding splits together to form length(splitters) + 1 sorted chunks
# these chunks will be in turn sorted
function splitmerge(chunks, splitters)
    c1 = map(c->splitchunk(c, splitters), chunks)
    map(collect_merge, transpose_vecvec(c1))
end

function splitchunk(c, splitters)
    function getbetween(xs, lo, hi)
        i = searchsortedlast(xs, lo)+1
        j = searchsortedlast(xs, hi)
        xs[i:j]
    end

    function getgt(xs, lo)
        i = searchsortedlast(xs, lo)+1
        xs[i:end]
    end

    function getlt(xs, lo)
        j = searchsortedlast(xs, lo)
        xs[1:j]
    end

    between = map((hi, lo) -> delayed(c->getbetween(c, hi, lo))(c),
                  splitters[1:end-1], splitters[2:end])
    hi = splitters[1]
    lo = splitters[end]
    [delayed(c->getlt(c, hi))(c); between; delayed(c->getgt(c, lo))(c)]
end

# transpose a vector of vectors
function transpose_vecvec(xs)
    map(1:length(xs[1])) do i
        map(x->x[i], xs)
    end
end

function merge_sorted{T, S}(x::AbstractArray{T}, y::AbstractArray{S})
    n = length(x) + length(y)
    z = Array{promote_type(T,S)}(n)
    i = 1; j = 1; k = 1
    len_x = length(x)
    len_y = length(y)
    @inbounds while i <= len_x && j <= len_y
        if x[i]<y[j]
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

function splitter_levels(splitters, nchunks, batchsize)
    # final number of chunks
    noutchunks = length(splitters) + 1
    # chunks per batch
    perbatch = ceil(Int, nchunks / batchsize)
    root = getmedians(splitters, perbatch-1)

    subsplits = []
    i = 1
    for c in root
        j = findlast(x->x<c, splitters)
        push!(subsplits, splitters[i:j])
        i = j+2
    end
    push!(subsplits, splitters[i:end])
    root, subsplits
end

function dsort_chunks(cs, n=length(cs), nsamples=2000)
    n=n-1
    cs1 = map(c->delayed(sortchunk)(c, nsamples), cs)
    xs = collect(treereduce(delayed(vcat), cs1))
    samples = sort!(reduce(vcat, map(x->x[2], xs)))
    splitters = getmedians(samples, n)

    cs = batchedsplitmerge(map(first, xs), splitters, max(2, nworkers()))
    for (w, c) in zip(Iterators.cycle(workers()), cs)
        propagate_affinity!(c, Dagger.OSProc(w) => 1)
    end
    @show cs
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

function dsort(xs::DArray, n=length(xs.chunks), nsamples=2000)
    cs = dsort_chunks(xs.chunks, n, nsamples)
    t=delayed((xs...)->[xs...]; meta=true)(cs...)
    chunks = compute(t)
    dmn = ArrayDomain((1:sum(length(domain(c)) for c in chunks),))
    DArray(eltype(xs), dmn, map(domain, chunks), chunks)
end

#=

using Distributions

xs = rand(Gamma(9,0.01),10^6)
xs = rand(10^6)
cs = map(x->xs[x], Dagger.split_range(1:length(xs), 8))
splits = @time dsort(cs, 4)
=#
