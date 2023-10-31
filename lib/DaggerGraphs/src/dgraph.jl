const ELTYPE = Union{Dagger.EagerThunk, Chunk}

struct DGraphState{T,D}
    # A set of locally-connected SimpleDiGraphs
    parts::Vector{ELTYPE}
    # The range of vertices within each of `parts`
    parts_nv::Vector{UnitRange{T}}
    # The number of edges in each of `parts`
    parts_ne::Vector{T}
    # The maximum number of nodes for each of `parts`
    parts_v_max::Int

    # A set of `AdjList` for each of `parts`
    # An edge is present here if either src or dst (but not both) is in
    # the respective `parts` graph
    ext_adjs::Vector{ELTYPE}
    # The number of edges in each of `ext_adjs`
    ext_adjs_ne::Vector{T}
    # The number of edges in each of `ext_adjs` where the source is this partition
    ext_adjs_ne_src::Vector{T}
end
struct DGraph{T,D,F} <: Graphs.AbstractGraph{T}
    state::Dagger.Chunk{DGraphState{T,D}}
    function DGraph{T}(; chunksize::Integer=8, directed::Bool=false) where {T}
        D = directed
        state = DGraphState{T,D}(ELTYPE[],
                                 UnitRange{T}[],
                                 T[],
                                 chunksize,
                                 ELTYPE[],
                                 T[],
                                 T[])
        return new{T,D}(Dagger.tochunk(state))
    end
end
DGraph(; kwargs...) = DGraph{Int}(; kwargs...)
function DGraph{T}(n::Integer; kwargs...) where T
    g = DGraph{T}(; kwargs...)
    add_vertices!(g, n)
    return g
end
DGraph(n::Integer; kwargs...) = DGraph{Int}(n; kwargs...)
function DGraph(sg::AbstractGraph{T}; directed::Bool=is_directed(sg), kwargs...) where T
    g = DGraph{T}(nv(sg); directed, kwargs...)
    foreach(edges(sg)) do edge
        add_edge!(g, edge)
        if !is_directed(sg) && directed
            add_edge!(g, dst(edge), src(edge))
        end
    end
    return g
end
function DGraph(dg::DGraph{T,D,F}; directed::Bool=D, freeze::Bool=false, chunksize::Integer=0) where {T,D}
    state = fetch(dg.state)
    # FIXME: Create g.state on same node as dg.state
    if chunksize == 0
        chunksize = state.parts_v_max
    end
    g = DGraph{T}(; directed, chunksize)
    @assert g.state.handle.owner == dg.state.handle.owner
    new_state = fetch(g.state)
    # TODO: Use streaming
    # FIXME: Support directed != D
    @assert directed == D "Changing directedness not yet supported"
    for part in 1:length(state.parts)
        # FIXME: Create on same nodes
        push!(new_state.parts, Dagger.@spawn copy(state.parts[part]))
        push!(new_state.parts_nv, state.parts_nv[part])
        push!(new_state.parts_ne, state.parts_ne[part])

        push!(new_state.ext_adjs, Dagger.@spawn copy(state.ext_adjs[part]))
        push!(new_state.ext_adjs_ne, state.ext_adjs_ne[part])
        push!(new_state.ext_adjs_ne_src, state.ext_adjs_ne_src[part])
    end
    #=
    foreach(edges(dg)) do edge
        add_edge!(g, edge)
        if !is_directed(dg) && directed
            add_edge!(g, dst(edge), src(edge))
        end
    end
    =#
    return g
end

freeze(g::DGraph{T,D,false}) where {T,D} = DGraph(g; freeze=true)

function Base.show(io::IO, g::DGraph{T,D}) where {T,D}
    print(io, "{$(nv(g)), $(ne(g))} $(D ? "" : "un")directed Dagger $T graph")
end

Base.eltype(::DGraph{T}) where T = T
Graphs.edgetype(::DGraph{T}) where T = Tuple{T,T}
Graphs.nv(g::DGraph) = fetch(Dagger.@spawn nv(g.state))::Int
function Graphs.nv(g::DGraphState)
    if !isempty(g.parts_nv)
        return last(g.parts_nv).stop
    else
        return 0
    end
end
Graphs.ne(g::DGraph) = fetch(Dagger.@spawn ne(g.state))::Int
Graphs.ne(g::DGraphState) = sum(g.parts_ne; init=0) + sum(g.ext_adjs_ne_src; init=0)
Graphs.has_vertex(g::DGraph, v::Integer) = 1 <= v <= nv(g)
Graphs.has_edge(g::DGraph, src::Integer, dst::Integer) =
    fetch(Dagger.@spawn has_edge(g.state, src, dst))::Bool
function Graphs.has_edge(g::DGraphState{T,D}, src::Integer, dst::Integer) where {T,D}
    src_part_idx = findfirst(span->src in span, g.parts_nv)
    src_part_idx !== nothing || return false
    dst_part_idx = findfirst(span->dst in span, g.parts_nv)
    dst_part_idx !== nothing || return false

    if src_part_idx == dst_part_idx
        # The edge will be within a graph partition
        part = g.parts[src_part_idx]
        return fetch(Dagger.@spawn has_edge(part, src, dst))
    else
        # The edge will be in an AdjList
        adj = g.ext_adjs[src_part_idx]
        return fetch(Dagger.@spawn has_ext_adj(adj, src, dst, D))
    end
end
Graphs.is_directed(::DGraph{T,D}) where {T,D} = D
Graphs.vertices(g::DGraph) = Base.OneTo(nv(g))
Graphs.edges(g::DGraph) = DGraphEdgeIter(g)
Graphs.zero(::Type{<:DGraph}) = DGraph()
function Graphs.add_vertex!(g::DGraph)
    fetch(Dagger.@spawn add_vertices!(g.state, 1))
    return
end
Graphs.add_vertices!(g::DGraph, n::Integer) =
    fetch(Dagger.@spawn add_vertices!(g.state, n))
function Graphs.add_vertices!(g::DGraphState, n::Integer)
    for _ in 1:n
        if fld(nv(g), g.parts_v_max) == length(g.parts)
            # We need to create a new partition for this vertex
            add_partition!(g, 1)
        else
            # We will add this vertex to the last partition
            part = last(g.parts)
            fetch(Dagger.@spawn add_vertex!(part))
            span = g.parts_nv[end]
            g.parts_nv[end] = UnitRange{Int}(span.start, span.stop+1)
        end
    end
    return n
end
add_partition!(g::DGraph, n::Integer) =
    fetch(Dagger.@spawn add_partition!(g.state, n))
function add_partition!(g::DGraphState{T,D}, n::Integer) where {T,D}
    if n < 1
        throw(ArgumentError("n must be >= 1"))
    end
    push!(g.parts, Dagger.spawn(n) do n
        g = D ? SimpleDiGraph() : SimpleGraph()
        add_vertices!(g, n)
        g
    end)
    num_v = nv(g)
    push!(g.parts_nv, (num_v+1):(num_v+n))
    push!(g.parts_ne, 0)
    push!(g.ext_adjs, Dagger.@spawn AdjList())
    push!(g.ext_adjs_ne, 0)
    push!(g.ext_adjs_ne_src, 0)
    return length(g.parts)
end
Graphs.add_edge!(g::DGraph, src::Integer, dst::Integer) =
    fetch(Dagger.@spawn add_edge!(g.state, src, dst))
Graphs.add_edge!(g::DGraph, edge::Edge) =
    add_edge!(g, src(edge), dst(edge))
function Graphs.add_edge!(g::DGraphState{T,D}, src::Integer, dst::Integer) where {T,D}
    src_part_idx = findfirst(span->src in span, g.parts_nv)
    @assert src_part_idx !== nothing "Source vertex $src does not exist"

    dst_part_idx = findfirst(span->dst in span, g.parts_nv)
    @assert dst_part_idx !== nothing "Destination vertex $dst does not exist"

    if src_part_idx == dst_part_idx
        # Edge exists within a single partition
        part = g.parts[src_part_idx]
        src_shift = src - (g.parts_nv[src_part_idx].start - 1)
        dst_shift = dst - (g.parts_nv[dst_part_idx].start - 1)
        if fetch(Dagger.@spawn add_edge!(part, src_shift, dst_shift))
            g.parts_ne[src_part_idx] += 1
        else
            return false
        end
    else
        # Edge spans two partitions
        src_ext_adj = g.ext_adjs[src_part_idx]
        dst_ext_adj = g.ext_adjs[dst_part_idx]
        src_t = Dagger.@spawn add_ext_adj!(src_ext_adj, src, dst, D)
        dst_t = Dagger.@spawn add_ext_adj!(dst_ext_adj, src, dst, D)
        if !fetch(src_t) || !fetch(dst_t)
            return false
        end
        if D
            # TODO: This will cause imbalance for many outgoing edges from a few vertices
            g.ext_adjs_ne_src[src_part_idx] += 1
        else
            owner_part_idx = edge_owner(src, dst, src_part_idx, dst_part_idx)
            g.ext_adjs_ne_src[owner_part_idx] += 1
        end
        g.ext_adjs_ne[src_part_idx] += 1
        g.ext_adjs_ne[dst_part_idx] += 1
    end

    return true
end
edge_owner(src::Int, dst::Int, src_part_idx::Int, dst_part_idx::Int) =
    iseven(hash(Base.unsafe_trunc(UInt, src+dst))) ? src_part_idx : dst_part_idx
Graphs.inneighbors(g::DGraph, v::Integer) =
    fetch(Dagger.@spawn inneighbors(g.state, v))
function Graphs.inneighbors(g::DGraphState, v::Integer)
    part_idx = findfirst(span->v in span, g.parts_nv)
    if part_idx === nothing
        throw(BoundsError(g, v))
    end

    neighbors = Int[]
    shift = g.parts_nv[part_idx].start - 1

    # Check against local edges
    v_shift = v - shift
    for local_neigh in fetch(Dagger.@spawn inneighbors(g.parts[part_idx], v_shift))
        push!(neighbors, local_neigh + shift)
    end

    # Check against external edges
    for ext_neigh in fetch(Dagger.@spawn inneighbors(g.ext_adjs[part_idx], v))
        push!(neighbors, ext_neigh)
    end

    return neighbors
end
Graphs.outneighbors(g::DGraph, v::Integer) =
    fetch(Dagger.@spawn outneighbors(g.state, v))
function Graphs.outneighbors(g::DGraphState, v::Integer)
    part_idx = findfirst(span->v in span, g.parts_nv)
    if part_idx === nothing
        throw(BoundsError(g, v))
    end

    neighbors = Int[]
    shift = g.parts_nv[part_idx].start - 1

    # Check against local edges
    v_shift = v - shift
    for local_neigh in fetch(Dagger.@spawn outneighbors(g.parts[part_idx], v_shift))
        push!(neighbors, local_neigh + shift)
    end

    # Check against external edges
    for ext_neigh in fetch(Dagger.@spawn outneighbors(g.ext_adjs[part_idx], v))
        push!(neighbors, ext_neigh)
    end

    return neighbors
end
