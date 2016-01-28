
immutable SortNode{T<:AbstractNode} <: ComputeNode
    input::T
    options::Dict
end

Base.sort(x::AbstractNode; kwargs...) = SortNode(x, Dict(kwargs))

function find_insertion_range(xs, med, options)
    range = searchsorted(xs, med; options...)
    #@show length(range)
    if length(range) <= 0
        (first(range)-1):(first(range)-1)
    else
        (first(range)-1):last(range)
    end
end

"""
For each sorted local part,
find a vector of medians for each range in active_ranges
"""
function active_medians(sorted_parts, active_ranges)

    mappart(sorted_parts, active_ranges) do xs, ranges
        [begin
            n = length(range)
            if n == 0
                0 # doesn't matter when weighted avging
            elseif n % 2 == 0
                h = n >> 1
                (xs[range[h]] + xs[range[h+1]]) / 2
            else
                h = (n+1) >> 1
                xs[h]
            end, n
        end for range in ranges]
    end
end

absorb_range(x, y) = (first(x)+first(y)):(last(x)+last(y))

function compute(ctx, node::SortNode)

    inp = compute(ctx, node.input)
    options = node.options

    # Sort each local part
    sorted_parts = compute(ctx, mappart(x -> sort(x, options...), inp); output_layout=cutdim(1))

    # Fetch the length of the parts, make a list of p-1 ranks to search for
    # TODO: Get this from metadata when available
    part_lengths = map(x -> sum(map(length, x)), metadata(sorted_parts))
    #@show
    unfound_ranks =  cumsum(part_lengths)[1:end-1]
    rank_medians = Dict()

    num_cuts = length(unfound_ranks)

    # Start off with whole vector as active ranges
    # On each processor, There are as many active ranges as there are ranks to be found!
    active_ranges = compute(ctx, mappart(x -> UnitRange[1:length(x) for _ in 1:num_cuts], sorted_parts))

    while true

        # Compute the medians of active ranges on each processor
        medians = compute(ctx, active_medians(sorted_parts, active_ranges))
        #@show gather(ctx, medians)

        # Matrix of (median, length of active range) M[i, j]
        #     where i -> index of active range, j = index of processor
        #
        # will be assembled at all processors
        median_matrix = allgather(medians, ColumnLayout())
        #@show gather(ctx, median_matrix)

        # Now, for corresponding active ranges on all procs, we need to find the median of medians
        # weighted by their respective active range lengths
        # This calculation is exactly the same on all processors
        median_of_medians = mappart(median_matrix) do M
            rows = size(M, 1)
            [begin
                ms = M[i, :] # the (median,length) pairs from all the processes corresponding ith active ranges
                len = sum([m[2] for m in ms])
                if len == 0
                    0
                else
                    sum([m[1] * m[2] for m in ms]) / len
                end
            end for i = 1:rows]
        end |> x -> compute(ctx, x)

        # Given the median of medians of various active sets, figure out the indexes in the
        # local part at which the median of median can be inserted.
        # Note that these ranges are 0-based for correctness
        insertion_points = mappart(median_of_medians, sorted_parts) do ms, xs
            [find_insertion_range(xs, m, options) for m in ms]
        end |> x -> compute(ctx, x)

        tmp_range_matrix = gather(ctx, insertion_points, ColumnLayout())

        # Here we produce a vector of possible ranks of *the median of median of
        # the active ranges*, one such range per rank being searched
        found_ranks = reducedim(absorb_range, tmp_range_matrix, 2, 0:0)

        # Now that we have found the ranks, we need to figure out if we need to stop looking at any of the
        # Active ranges.

        found_idxs = find(map(in, unfound_ranks, found_ranks)) # These are the indexes of the ranks that have been found
        #@show ms = gather(ctx, median_of_medians)
        if !isempty(found_idxs)
            ms = gather(ctx, median_of_medians)
            for i = found_idxs
                # Store the median for the rank in the dict
                rank_medians[unfound_ranks[i]] = ms[i]
            end
        end
        #@show rank_medians

        # Active ranges to be further explored
        unfound_idxs = find(map((x, y) -> ! (x in y), unfound_ranks, found_ranks))
        if isempty(unfound_idxs)
            # We're done.
            break
        end

        # Next ranks
        unfound_ranks = unfound_ranks[unfound_idxs]
        #@show unfound_ranks
        #@show found_ranks

        active_ranges_node = halve_active_ranges(active_ranges, unfound_idxs, unfound_ranks, found_ranks[unfound_idxs], insertion_points)
        active_ranges = compute(ctx, active_ranges_node; output_layout=layout(active_ranges))
        #@show gather(ctx, active_ranges)
    end
    #@show rank_medians

    # Now calculate the splits.
    cut_points = broadcast(rank_medians)

    # All-to-all communication
    parts = compute(ctx, mappart(cut_array, sorted_parts, cut_points); output_layout=cutdim(2))
    redist = redistribute(parts, cutdim(1))
    compute(ctx, mappart(xs -> merge_sorted(xs...), redist); output_layout=cutdim(1))
    #partitions = compute(ctx, redistribute(DistData(rs, RowLayout()), ColumnLayout()))
    #ms = compute(ctx, mappart(xs -> reduce(vcat, xs), partitions))
    #DistData(refs(ms), RowLayout())
end

function cut_array(xs, medians)
    ranks = sort(collect(keys(medians)))
    a = 0
    parts = Array(Any, length(ranks)+1)
    for i in 1:length(ranks)
        r = ranks[i]
        idxs = searchsorted(xs, medians[r])
        if length(idxs) == 0
            # the median doesn't exist
            b = min(length(xs), first(idxs)-1)
        else
            # the median exists
            b = min(length(xs), Int(floor(first(idxs) + (length(idxs) / 2)))) # TODO: divide by num workers
        end
        parts[i] = xs[(a+1):b]
        a = b
    end
    parts[end] = xs[(a+1):length(xs)]
    parts
end

function halve_active_ranges(active_ranges, unfound_idxs, unfound_ranks, last_overall_ranges, part_window)
    # found_ranks[i] -> rank of the current median of median of the ith split
    # active_ranges[i] -> the indexes of the ith active range, distributed

    mappart(active_ranges, broadcast(unfound_idxs), broadcast(unfound_ranks), broadcast(last_overall_ranges), part_window) do A, idxs, rs, last_result, part_range
        next_A = A[idxs]
        next_A, last_result, part_range, rs
        map(next_range, A[idxs], last_result, part_range[idxs], rs)
    end
end

function next_range(active_range, overall_range, part_range, r)
    f = first(overall_range)
    l = last(overall_range)
    #@show active_range, r, overall_range, part_range

    if r < f
        # move search to lower indices
        first(active_range):first(part_range)
    elseif l < r
        # move search to higher indices
        last(part_range):last(active_range)
    end
end

function merge_sorted{T}(x::AbstractArray{T}, y::AbstractArray{T}; o=Base.Order.Forward)
    target = Array(T, length(x) + length(y))
    i = 1
    j = 1
    k = 1
    while k <= length(target)
        if i > length(x)
            for m=j:length(y)
                target[k] = y[m]
                k+=1
            end
        elseif j > length(y)
            for m=i:length(x)
                target[k] = x[m]
                k += 1
            end
        else
            if Base.lt(o, x[i], y[j])
                target[k] = x[i]
                i += 1
            else
                target[k] = y[j]
                j += 1
            end
            k += 1
        end
    end
    target
end

merge_sorted(x;o=Base.Order.Forward) = x
merge_sorted(xs...;o=Base.Order.Forward) = merge_sorted(merge_sorted(xs[1], xs[2]; o=o), xs[3:end]...; o=o)
