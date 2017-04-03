using Compat
import Compat: view

import Base.Sort: Forward, Ordering, Algorithm, defalg, lt

immutable Sort <: Computation
    input::LazyArray
    alg::Algorithm
    order::Ordering
end

function Base.sort(v::LazyArray;
               alg::Algorithm=defalg(v),
               lt=Base.isless,
               by=identity,
               rev::Bool=false,
               order::Ordering=Forward)
    Sort(v, alg, Base.Sort.ord(lt,by,rev,order))
end

size(x::LazyArray) = size(x.input)
function compute(ctx, s::Sort)

    inp = let alg=s.alg, ord = s.order
        compute(ctx, mapchunk(p->sort(p; alg=alg, order=ord), s.input)).result
    end

    ps = chunks(inp)

    persist!(inp)

    ls = map(length, domainchunks(inp))
    splitter_ranks = cumsum(ls)[1:end-1]

    splitters = select(ctx, inp, splitter_ranks, s.order)
    ComputedArray(compute(ctx, shuffle_merge(inp, splitters, s.order)))
end

function mapchunk_eager(f, ctx, xs, T, name)
    ps = chunks(xs)
    master=OSProc(1)
    #@dbg timespan_start(ctx, name, 0, master)
    thunks = Thunk[Thunk(f(i), ps[i], get_result=true)
                 for i in 1:length(ps)]

    res = compute(ctx, Thunk((xs...)->T[xs...], thunks..., meta=true))
    #@dbg timespan_end(ctx, name, 0, master)
    res
end

function broadcast1(ctx, f, xs::Cat, m)
    ps = chunks(xs)
    @assert size(m, 1) == length(ps)
    mapchunk_eager(ctx, xs,Vector,:broadcast1) do i
        inp = vec(m[i,:])
        function (p)
            map(x->f(p, x), inp)
        end
    end |> x->matrixize(x, Any) |> x->permutedims(x, [2,1])
end

function broadcast2(ctx, f, xs::Cat, m,v)
    ps = chunks(xs)
    @assert size(m, 1) == length(ps)
    mapchunk_eager(ctx, xs,Vector,:broadcast2) do i
        inp = vec(m[i,:])
        function (p)
            map((x,y)->f(p, x, y), inp, vec(v))
        end
    end |> x->matrixize(x,Any) |> x->permutedims(x, [2,1])
end

function select(ctx, A, ranks, ord)
    ks = copy(ranks)
    lengths = map(length, domainchunks(A))
    n = sum(lengths)
    p = length(chunks(A))
    init_ranges = UnitRange[1:x for x in lengths]
    active_ranges = matrixize(Array[init_ranges for i=1:length(ks)], UnitRange)

    Ns = Int[n for _ in ks]
    iter=0
    result = Pair[]
    while any(x->x>0, Ns)
        iter+=1
        # find medians
        ms = broadcast1(ctx, submedian, A, active_ranges)
        ls = map(length, active_ranges)

        Ms = mapslices(x->weightedmedian(x, ls, ord), ms, 1)
        # scatter weighted
        dists = broadcast2(ctx, (a,r,m)->locate_pivot(a,r,m,ord), A, active_ranges, Ms)
        D = reducedim((xs, x) -> map(+, xs, x), dists, 1, (0,0,0))
        L = Int[x[1] for x in D]
        E = Int[x[2] for x in D]
        G = Int[x[3] for x in D]

        found = Int[]
        for i=1:length(ks)
            l = L[i]; e = E[i]; g = G[i]; k = ks[i]
            if k < l
                # the rank we are looking for is in the lesser-than section
                # discard elements in the more than or equal sections
                active_ranges[:,i] = keep_lessthan(dists[:,i], active_ranges[:,i])
                Ns[i] = l
            elseif k > l + e
                # the rank we are looking for is in the greater-than section
                # discard elements less than or equal to M
                active_ranges[:,i] = keep_morethan(dists[:,i], active_ranges[:,i])
                Ns[i] = g
                ks[i] = k - (l + e)
            elseif l <= k && k <= l+e
                # we have found a possible splitter!
                foundat = map(active_ranges[:,i], dists[:,i]) do rng, d
                    l,e,g=d
                    fst = first(rng)+l
                    lst = fst+e-1
                    fst:lst
                end
                push!(result, Ms[i] => foundat)
                push!(found, i)
            end
        end
        found_mask = isempty(found) ? ones(Bool, length(ks)) : Bool[!(x in found) for x in 1:length(ks)]
        active_ranges = active_ranges[:, found_mask]
        Ns = Ns[found_mask]
        ks = ks[found_mask]
    end
    return sort(result, by=x->x[1])
end

# mid for common element types
function mid(x, y)
    y
end

function mid(x::Number, y::Number)
    middle(x, y)
end

function mid(x::Tuple, y::Tuple)
    map(mid, x, y)
end


function mid{T<:Dates.TimeType}(x::T, y::T)
    x + (y-x)÷2
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
                return mid(xs[i], xs[i+1])
            else
                xs[i]
            end
        end
        x += weights[i]
        x > cutoff && break
        x == cutoff && continue
        i += 1
    end
    return mid(xs[i], xs[i])
end

function sortedmedian(xs)
   l = length(xs)
   if l % 2 == 0
       i = l >> 1
       mid(xs[i], xs[i+1])
   else
       i = (l+1) >> 1
       mid(xs[i], xs[i]) # keep type stability
   end
end

function submedian(xs, r)
    xs1 = view(xs, r)
    if isempty(xs1)
        idx = min(first(r), length(xs))
        return mid(xs[idx], xs[idx])
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

function locate_pivot(X, r, s, ord)
    # compute l, e, g
    X1 = view(X, r)
    rng = searchsorted(X1, s, ord)
    l = first(rng) - 1
    e = length(rng)
    g = length(X1) - l - e
    l,e,g
end

function matrixize(xs,T)
    l = isempty(xs) ? 0 : length(xs[1])
    [xs[i][j] for j=1:l, i=1:length(xs)]
end

function merge_thunk(ps, starts, lasts, ord)
    ranges = map(UnitRange, starts, lasts)
    Thunk(map((p, r) -> Dagger.view(p, ArrayDomain(r)), ps, ranges)...) do xs...
        merge_sorted(ord, xs...)
    end
end

function shuffle_merge(A, splitter_indices, ord)
    ps = chunks(A)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(ps))
    merges = [begin
        lasts = map(last, idxs)
        thnk = merge_thunk(ps, starts, lasts, ord)
        sz = sum(lasts.-starts.+1)
        starts = lasts.+1
        thnk,sz
        end for (val, idxs) in splitter_indices]
    ls = map(length, domainchunks(A))
    thunks = vcat(merges, (merge_thunk(ps, starts, ls, ord), sum(ls.-starts.+1)))
    part_lengths = map(x->x[2], thunks)
    dmn = ArrayDomain(1:sum(part_lengths))
    dmnchunks = DomainBlocks((1,), (cumsum(part_lengths),))
    Cat(chunktype(A), dmn, dmnchunks, map(x->x[1], thunks))
end

function merge_sorted{T, S}(ord::Ordering, x::AbstractArray{T}, y::AbstractArray{S})
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

merge_sorted(ord::Ordering, x) = x
function merge_sorted(ord::Ordering, x, y, ys...)
    merge_sorted(ord, merge_sorted(ord, x,y), merge_sorted(ord, ys...))
end
