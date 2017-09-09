using Compat
import Compat: view

import Base.Sort: Forward, Ordering, Algorithm, defalg, lt

struct Sort <: Computation
    input::ArrayOp
    alg::Algorithm
    order::Ordering
end

function Base.sort(v::ArrayOp;
               alg::Algorithm=defalg(v),
               lt=Base.isless,
               by=identity,
               rev::Bool=false,
               order::Ordering=Forward)
    Sort(v, alg, Base.Sort.ord(lt,by,rev,order))
end

size(x::ArrayOp) = size(x.input)
function compute(ctx, s::Sort)

    # First, we sort each chunk.
    inp = let alg=s.alg, ord = s.order
        compute(ctx, mapchunk(p->sort(p; alg=alg, order=ord), s.input))
    end

    ps = chunks(inp)

    # We need to persist! the sorted chunks so that further operations
    # will not remove it.
    foreach(persist!, chunks(inp))

    # find the ranks to split at
    ls = map(length, domainchunks(inp))
    splitter_ranks = cumsum(ls)[1:end-1]

    # parallel selection
    splitters = pselect(ctx, inp, splitter_ranks, s.order)
    DArray(compute(ctx, shuffle_merge(inp, splitter_ranks, splitters, s.order)))
end

function delayed_map_and_collect(f, ctx, Xs...)
    result_parts = map(delayed(f, get_result=true), Xs...)
    collect(ctx, delayed(tuple)(result_parts...))
end

function pselect(ctx, A, ranks, ord)
    cs = chunks(A)
    Nc = length(cs)
    Nr = length(ranks)

    ks = copy(ranks)
    lengths = map(length, domainchunks(A))

    # Initialize the ranges in which we are looking for medians
    # For each chunk it's 1:length of that chunk
    init_ranges = UnitRange[1:x for x in lengths]

    # there will be Nr active_ranges being searched for each chunk
    # We create a matrix of ranges containing as many columns as ranks
    # as many rows as chunks
    active_ranges = reducehcat([init_ranges for _ in 1:Nr], UnitRange)

    n = sum(lengths)
    Ns = Int[n for _ in 1:Nr] # Number of elements in the active range set
    iter=0                    # Iteration count
    result = Pair[]           # contains `rank => median value` pairs

    while any(x->x>0, Ns)
        # find medians
        chunk_ranges = [vec(active_ranges[i,:]) for i in 1:Nc]

        chunk_medians = delayed_map_and_collect(ctx, chunk_ranges, cs) do ranges, data
            # as many ranges as ranks to find
            map(r->submedian(data, r), ranges)
        end
        # medians: a vector Nr medians for each chunk

        tmp = reducehcat(chunk_medians, Any) # Nr x Nc
        median_matrix = permutedims(tmp, (2,1))

        ls = map(length, active_ranges)
        Ms = vec(mapslices(x->weightedmedian(x, ls, ord), median_matrix, 1))

        # scatter weighted
        LEGs = delayed_map_and_collect(ctx, cs, chunk_ranges) do chunk, ranges
            # for each median found right now, locate G,T,E vals
            map((range, m)->locate_pivot(chunk, range, m, ord), ranges, Ms)
        end

        LEG_matrix = reducehcat(LEGs, Any)
        D = reducedim((xs, x) -> map(+, xs, x), LEG_matrix, 2, (0,0,0))
        L = Int[x[1] for x in D] # length = Nr
        E = Int[x[2] for x in D]
        G = Int[x[3] for x in D]

        found = Int[]
        for i=1:length(ks)
            l = L[i]; e = E[i]; g = G[i]; k = ks[i]
            if k <= l
                # discard elements less than M
                active_ranges[:,i] = keep_lessthan(LEG_matrix[i,:], active_ranges[:,i])
                Ns[i] = l
            elseif k > l + e
                # discard elements more than M
                active_ranges[:,i] = keep_morethan(LEG_matrix[i,:], active_ranges[:,i])
                Ns[i] = g
                ks[i] = k - (l + e)
            elseif l < k && k <= l+e
                # we've found a possible splitter!
                foundat = map(active_ranges[:,i], LEG_matrix[i,:]) do rng, d
                    l,e,g=d
                    fst = first(rng)+l
                    lst = fst+e-1
                    fst:lst
                end
                push!(result, Ms[i] => foundat)
                push!(found, i)
            end
        end
        notfound_mask = ones(Bool, length(ks))
        notfound_mask[found] = false
        active_ranges = active_ranges[:, notfound_mask]
        Ns = Ns[notfound_mask]
        ks = ks[notfound_mask]
    end
    firsts = map(first, result)
    perm = sortperm(firsts, order=ord)
    return result[perm]
end

function weightedmedian(xs, weights, ord)
    perm = sortperm(xs)
    weights = weights[perm]
    xs = xs[perm]
    cutoff = sum(weights) / 2

    x = weights[1]
    i = 1
    while x <= cutoff
        if x == cutoff
            if i < length(xs)
                return xs[i]
            else
                xs[i]
            end
        end
        x += weights[i]
        x > cutoff && break
        x == cutoff && continue
        i += 1
    end
    return xs[i]
end

function sortedmedian(xs)
   l = length(xs)
   if l % 2 == 0
       i = l >> 1
       xs[i]
   else
       i = (l+1) >> 1
       xs[i]
   end
end

function submedian(xs, r)
    xs1 = view(xs, r)
    if isempty(xs1)
        idx = min(first(r), length(xs))
        return xs[idx]
    end
    sortedmedian(xs1)
end

function keep_lessthan(dists, active_ranges)
    map(dists, active_ranges) do d, r
        l = d[1]::Int
        first(r):(first(r)+l-1)
    end
end

function keep_morethan(dists, active_ranges)
    map(dists, active_ranges) do d, r
        g = (d[2]+d[1])::Int
        (first(r)+g):last(r)
    end
end

# returns number of elements less than
# equal to and greater than `s` in X within
# an index range
function locate_pivot(X, range, s, ord)
    # compute l, e, g
    X1 = view(X, range)
    output_rng = searchsorted(X1, s, ord)
    l = first(output_rng) - 1
    e = length(output_rng)
    g = length(X1) - l - e
    l,e,g
end

function reducehcat(xs,T)
    l = isempty(xs) ? 0 : length(xs[1])
    [xs[i][j] for j=1:l, i=1:length(xs)]
end

function merge_thunk(ps, starts, lasts, ord)
    ranges = map(UnitRange, starts, lasts)
    Thunk(map((p, r) -> delayed(getindex)(p, r), ps, ranges)...) do xs...
        merge_sorted(ord, xs...)
    end
end

function shuffle_merge(A, ranks, splitter_indices, ord)
    ps = chunks(A)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(ps))
    merges = [begin
        lasts = map(first, idxs).-1 # First, all elements less than that of the required rank
        i = 1
        while sum(lasts) < rank
            reqd = rank - sum(lasts)
            if i > length(idxs)
                error("Median of wrong rank found")
            end
            available = min(reqd, length(idxs[i]))
            lasts[i] += available
            i += 1
        end

        thnk = merge_thunk(ps, starts, lasts, ord)
        sz = sum(lasts.-starts.+1)
        starts = lasts.+1
        thnk,sz
        end for (rank, idxs) in zip(ranks, map(last, splitter_indices))]
    ls = map(length, domainchunks(A))
    thunks = vcat(merges, (merge_thunk(ps, starts, ls, ord), sum(ls.-starts.+1)))
    part_lengths = map(x->x[2], thunks)
    dmn = ArrayDomain(1:sum(part_lengths))
    dmnchunks = DomainBlocks((1,), (cumsum(part_lengths),))
    DArray(eltype(A), dmn, dmnchunks, map(x->x[1], thunks))
end

function merge_sorted(ord::Ordering, x::AbstractArray{T}, y::AbstractArray{S}) where {T, S}
    n = length(x) + length(y)
    z = Array{promote_type(T,S)}(n)
    i = 1; j = 1; k = 1
    len_x = length(x)
    len_y = length(y)
    while i <= len_x && j <= len_y
        @inbounds if lt(ord, x[i], y[j])
            @inbounds z[k] = x[i]
            i += 1
        else
            @inbounds z[k] = y[j]
            j += 1
        end
        k += 1
    end
    remaining, m = i <= len_x ? (x, i) : (y, j)
    while k <= n
        @inbounds z[k] = remaining[m]
        k += 1
        m += 1
    end
    z
end

merge_sorted(ord::Ordering, x::AbstractArray) = x
function merge_sorted(ord::Ordering, x::AbstractArray, y::AbstractArray, ys::AbstractArray...)
    merge_sorted(ord, merge_sorted(ord, x,y), merge_sorted(ord, ys...))
end
