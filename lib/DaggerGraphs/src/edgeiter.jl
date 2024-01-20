struct DGraphEdgeIter{T,M} <: Graphs.AbstractEdgeIter
    graph::DGraphState{T}
    meta_f
end
DGraphEdgeIter(g::DGraph{T}; metadata::Bool=false, meta_f=nothing) where T =
    DGraphEdgeIter{T,metadata}(fetch(g.state), meta_f)
struct DGraphEdgeIterState{T}
    adj::Bool
    part::Int
    idx::Int
    cache
    cache_meta
    seen::Union{Set{Edge{T}},Nothing}
end
Base.length(iter::DGraphEdgeIter) = ne(iter.graph)
Base.eltype(iter::DGraphEdgeIter{T,false}) where T = Edge{T}
Base.eltype(iter::DGraphEdgeIter{T,true}) where T = Tuple{Edge{T},Any}
function Base.iterate(iter::DGraphEdgeIter{T}) where T
    g = iter.graph
    if nv(g) == 0
        return nothing
    elseif sum(g.parts_ne; init=0) > 0
        # Start with partitions
        seen = is_directed(g) ? nothing : Set{Edge{T}}()
        return iterate(iter, DGraphEdgeIterState{T}(false, 1, 1, nothing, nothing, seen))
    elseif sum(g.bg_adjs_ne_src; init=0) > 0
        # Start with background AdjLists
        seen = is_directed(g) ? nothing : Set{Edge{T}}()
        return iterate(iter, DGraphEdgeIterState{T}(true, 1, 1, nothing, nothing, seen))
    else
        return nothing
    end
end
function Base.iterate(iter::DGraphEdgeIter{T,M}, state::DGraphEdgeIterState{T}) where {T,M}
    g = iter.graph
    adj = state.adj
    part = state.part
    idx = state.idx
    cache = state.cache
    cache_meta = state.cache_meta
    seen = state.seen

    edge_metadata_for(meta, edges) = map(edge->meta[edge[1],edge[2]], edges)

    @label start
    if !adj
        if part > length(g.parts)
            # Restart with background AdjLists
            return iterate(iter, DGraphEdgeIterState{T}(true, 1, 1, nothing, nothing, seen))
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
                if M
                    cache_meta = edge_metadata_for(fetch(g.parts_e_meta[part]), cache)
                end
            end
        end
    else
        if part > length(g.bg_adjs)
            # All done!
            return nothing
        end
        if cache === nothing
            cache = map(Tuple, fetch(Dagger.@spawn edges(g.bg_adjs[part])))
            if M
                cache_meta = edge_metadata_for(fetch(g.bg_adjs_e_meta[part]), cache)
            end
        end
    end
    cache::Vector{<:Tuple}
    cache_meta::Union{Vector,Nothing}

    # Skip empty edge sets
    if isempty(cache)
        part += 1
        idx = 1
        cache = nothing
        cache_meta = nothing
        @goto start
    end
    cache::Vector{Tuple{T,T}}

    # Get the current edge
    value = Edge(cache[idx])
    if M
        value_meta = cache_meta[idx]
    end
    idx += 1
    cur_part = part

    # Reset if this partition/AdjList is exhausted
    if idx > length(cache)
        part += 1
        idx = 1
        cache = nothing
    end

    # Restart if this edge isn't "owned" by this AdjList
    # FIXME: Don't use src(value) for undirected
    if adj && !(src(value) in g.parts_nv[cur_part])
        @goto start
    end

    # Restart if this edge has already been seen (undirected case)
    if seen !== nothing
        if value in seen
            @goto start
        end
        value_rev = Edge(dst(value), src(value))
        if value_rev in seen
            @goto start
        end
        push!(seen, value)
    end

    return (M ? (value, value_meta) : value,
            DGraphEdgeIterState{T}(adj, part, idx, cache, cache_meta, seen))
end
