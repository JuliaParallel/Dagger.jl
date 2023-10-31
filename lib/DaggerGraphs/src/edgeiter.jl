struct DGraphEdgeIter{T} <: Graphs.AbstractEdgeIter
    graph::DGraphState{T}
end
DGraphEdgeIter(g::DGraph) = DGraphEdgeIter(fetch(g.state))
struct DGraphEdgeIterState
    adj::Bool
    part::Int
    idx::Int
    cache::Any
end
Base.length(iter::DGraphEdgeIter) = ne(iter.graph)
Base.eltype(iter::DGraphEdgeIter{T}) where T = Edge{T}
function Base.iterate(iter::DGraphEdgeIter)
    g = iter.graph
    if nv(g) == 0
        return nothing
    elseif sum(g.parts_ne; init=0) > 0
        # Start with partitions
        return iterate(iter, DGraphEdgeIterState(false, 1, 1, nothing))
    elseif sum(g.ext_adjs_ne_src; init=0) > 0
        # Start with external AdjLists
        return iterate(iter, DGraphEdgeIterState(true, 1, 1, nothing))
    else
        return nothing
    end
end
function Base.iterate(iter::DGraphEdgeIter{T}, state::DGraphEdgeIterState) where {T}
    g = iter.graph
    adj = state.adj
    part = state.part
    idx = state.idx
    cache = state.cache

    @label start
    if !adj
        if part > length(g.parts)
            # Restart with external AdjLists
            return iterate(iter, DGraphEdgeIterState(true, 1, 1, nothing))
        end
        if cache === nothing
            cache = map(Tuple, fetch(Dagger.@spawn edges(g.parts[part])))
            if !isempty(cache)
                cache::Vector{Tuple{T,T}}
                shift = g.parts_nv[part].start - 1
                for idx in 1:length(cache)
                    value = cache[idx]
                    cache[idx] = (first(value)+shift,
                                  last(value)+shift)
                end
            end
        end
    else
        if part > length(g.ext_adjs)
            # All done!
            return nothing
        end
        if cache === nothing
            cache = fetch(Dagger.@spawn edges(g.ext_adjs[part]))
        end
    end
    cache::Vector{<:Tuple}

    # Skip empty edge sets
    if isempty(cache)
        part += 1
        idx = 1
        cache = nothing
        @goto start
    end
    cache::Vector{Tuple{T,T}}

    # Get the current edge
    value = cache[idx]
    idx += 1
    cur_part = part

    # Reset if this partition/AdjList is exhausted
    if idx > length(cache)
        part += 1
        idx = 1
        cache = nothing
    end

    # Restart if this edge isn't "owned" by this AdjList
    if adj && !(value[1] in g.parts_nv[cur_part])
        @goto start
    end

    return (Edge(value),
            DGraphEdgeIterState(adj, part, idx, cache))
end
