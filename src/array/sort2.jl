using Dagger
import Dagger: treereduce, tochunk


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
    r = randperm(length(xs))[1:nsamples]
    (tochunk(sorted), getmedians(sorted, nsamples))
end

function dsort(cs, n, nsamples=2000)
    n=n-1
    cs1 = map(c->delayed(sortchunk)(c, nsamples), cs)
    xs = collect(treereduce(delayed(vcat), cs1))
    samples = sort!(reduce(vcat, map(x->x[2], xs)))
    splitters = getmedians(samples, n)

    # exchange and merge
end

using Distributions

xs = rand(Gamma(9,0.01),10^6)
xs = rand(10^6)
cs = map(x->xs[x], Dagger.split_range(1:length(xs), 8))
splits = @time dsort(cs, 4)

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

evaluate(xs, cs, splits)

using Dagger
import Dagger: split_range

function batchedsplitmerge(chunks, splitters, batchsize)
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
    for i = 1:length(range_groups)
        s = lowersplits[i]
        group = range_groups[i]
        if !isempty(s)
            cs = batchedsplitmerge(group, s, batchsize)
            append!(chunks, cs)
        else
            push!(chunks, reduce(merge_sorted, group))
        end
    end
    return chunks
end

# Given sorted chunks, splits each chunk according to splitters
# then merges corresponding splits together to form length(splitters) + 1 sorted chunks
# these chunks will be in turn sorted
function splitmerge(chunks, splitters)
    c1 = map(c->splitchunk(c, splitters), chunks)
    map(cs->reduce(merge_sorted, cs), transpose_vecvec(c1))
end

function splitchunk(c, splitters)
    pieces = typeof(c)[]
    i = 1
    for s in splitters
        j = searchsortedlast(c, s)
        push!(pieces, c[i:j])
        i=j+1
    end
    push!(pieces, c[i:length(c)])
    pieces
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
    for c = root
        j = findlast(x->x<c, splitters)
        push!(subsplits, splitters[i:j])
        i = j+2
    end
    push!(subsplits, splitters[i:end])
    root, subsplits
end

scs = map(sort, cs)
@show length(splits)
@time batchedsplitmerge(scs, splits, 3)
