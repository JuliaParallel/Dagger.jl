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

    ls = map(length, chunks(domain(inp)))
    splitter_ranks = cumsum(ls)[1:end-1]

    splitters = select(ctx, inp, splitter_ranks, s.order)
    ComputedArray(compute(ctx, shuffle_merge(inp, splitters, s.order)))
end

function mapchunk_eager(f, ctx, xs, T, name)
    ps = chunks(xs)
    master=OSProc(1)
    #@dbg timespan_start(ctx, name, 0, master)
    thunks = Thunk[Thunk(f(i), (ps[i],), get_result=true)
                 for i in 1:length(ps)]

    res = compute(ctx, Thunk((xs...)->T[xs...], (thunks...), meta=true))
    #@dbg timespan_end(ctx, name, 0, master)
    res
end

function broadcast1(ctx, f, xs::Cat, m,T)
    ps = chunks(xs)
    @assert size(m, 1) == length(ps)
    mapchunk_eager(ctx, xs,Vector{T},:broadcast1) do i
        inp = vec(m[i,:])
        function (p)
            map(x->f(p, x)::T, inp)
        end
    end |> x->matrixize(x,T) |> x->permutedims(x, [2,1])
end

function broadcast2(ctx, f, xs::Cat, m,v,T)
    ps = chunks(xs)
    @assert size(m, 1) == length(ps)
    mapchunk_eager(ctx, xs,Vector{T},:broadcast2) do i
        inp = vec(m[i,:])
        function (p)
            map((x,y)->f(p, x, y)::T, inp, vec(v))
        end
    end |> x->matrixize(x,T) |> x->permutedims(x, [2,1])
end

function select(ctx, A, ranks, ord)
    ks = copy(ranks)
    lengths = map(length, chunks(domain(A)))
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
        ms = broadcast1(ctx, submedian, A, active_ranges, Float64)
        ls = map(length, active_ranges)
        Ms = sum(ms .* ls, 1) ./ sum(ls, 1)
        # scatter weighted
        dists = broadcast2(ctx, (a,r,m)->locate_pivot(a,r,m,ord), A, active_ranges, Ms, Tuple{Int,Int,Int})
        D = reducedim((xs, x) -> map(+, xs, x), dists, 1, (0,0,0))
        L = Int[x[1] for x in D]
        E = Int[x[2] for x in D]
        G = Int[x[3] for x in D]

        found = Int[]
        for i=1:length(ks)
            l = L[i]; e = E[i]; g = G[i]; k = ks[i]
            if k <= l
                # discard elements less than M
                active_ranges[:,i] = keep_lessthan(dists[:,i], active_ranges[:,i])
                Ns[i] = l
            elseif k > l + e
                # discard elements more than M
                active_ranges[:,i] = keep_morethan(dists[:,i], active_ranges[:,i])
                Ns[i] = g
                ks[i] = k - (l + e)
            elseif l < k && k <= l+e
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

function sortedmedian(xs)
   l = length(xs)
   if l % 2 == 0
       i = l >> 1
       (xs[i]+xs[i+1]) / 2
   else
       i = (l+1) >> 1
       convert(Float64, xs[i])
   end
end

function submedian(xs, r)
    xs1 = view(xs, r)
    isempty(xs1) ? 0.0 : sortedmedian(xs1)
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
    T[xs[i][j] for j=1:l, i=1:length(xs)]
end

function merge_thunk(ps, starts, lasts, ord)
    ranges = map(UnitRange, starts, lasts)
    Thunk((map((p, r) -> Dagger.view(p, ArrayDomain(r)), ps, ranges)...)) do xs...
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
    ls = map(length, chunks(domain(A)))
    thunks = vcat(merges, (merge_thunk(ps, starts, ls, ord), sum(ls.-starts.+1)))
    part_lengths = map(x->x[2], thunks)
    dmn = DomainSplit(
        ArrayDomain(1:sum(part_lengths)),
        BlockedDomains((1,),
        (cumsum(part_lengths),)))
    Cat(parttype(A), dmn, map(x->x[1], thunks))
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
