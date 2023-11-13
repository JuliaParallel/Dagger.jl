const ELTYPE = Union{Dagger.EagerThunk, Chunk}

struct DGraphState{T,D}
    # Whether the graph is "frozen" (immutable) or mutable
    frozen::Ref{Bool}

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
    # the respective `parts` graph (the so-called "background graph")
    bg_adjs::Vector{ELTYPE}
    # The number of edges in each of `bg_adjs`
    bg_adjs_ne::Vector{T}
    # The number of edges in each of `bg_adjs` where the source is this partition
    bg_adjs_ne_src::Vector{T}
end
mutable struct DGraph{T,D} <: Graphs.AbstractGraph{T}
    # The internal graph state
    state::Union{Dagger.Chunk{DGraphState{T,D}},
                 DGraphState{T,D}}
    # Whether the graph is known to be frozen
    frozen::Ref{Bool}

    function DGraph{T}(;chunksize::Integer=8,
                        directed::Bool=true) where {T}
        D = directed
        state = DGraphState{T,D}(Ref(false),
                                 ELTYPE[],
                                 UnitRange{T}[],
                                 T[],
                                 chunksize,
                                 ELTYPE[],
                                 T[],
                                 T[])
        return new{T,D}(Dagger.tochunk(state), Ref(false))
    end
end
DGraph(; kwargs...) = DGraph{Int}(; kwargs...)
function DGraph{T}(n::Integer; freeze::Bool=false, kwargs...) where T
    g = DGraph{T}(; kwargs...)
    add_vertices!(g, n)
    freeze && freeze!(g)
    return g
end
DGraph(n::Integer; kwargs...) = DGraph{Int}(n; kwargs...)
function DGraph(sg::AbstractGraph{T}; directed::Bool=is_directed(sg), freeze::Bool=false, kwargs...) where T
    g = DGraph{T}(nv(sg); directed, kwargs...)
    foreach(edges(sg)) do edge
        add_edge!(g, edge)
        if !is_directed(sg) && directed
            add_edge!(g, dst(edge), src(edge))
        end
    end
    freeze && freeze!(g)
    return g
end
function DGraph(dg::DGraph{T,D}; chunksize::Integer=0, directed::Bool=D, freeze::Bool=false) where {T,D}
    state = fetch(dg.state)
    # FIXME: Create g.state on same node as dg.state
    if chunksize == 0
        chunksize = state.parts_v_max
    end
    g = DGraph{T}(; chunksize, directed)
    @assert isfrozen(dg) || g.state.handle.owner == dg.state.handle.owner
    new_state = fetch(g.state)
    # TODO: Use streaming
    # FIXME: Support directed != D
    @assert directed == D "Changing directedness not yet supported"
    for part in 1:length(state.parts)
        # FIXME: Create on same nodes
        push!(new_state.parts, Dagger.@spawn copy(state.parts[part]))
        push!(new_state.parts_nv, state.parts_nv[part])
        push!(new_state.parts_ne, state.parts_ne[part])

        push!(new_state.bg_adjs, Dagger.@spawn copy(state.bg_adjs[part]))
        push!(new_state.bg_adjs_ne, state.bg_adjs_ne[part])
        push!(new_state.bg_adjs_ne_src, state.bg_adjs_ne_src[part])
    end
    freeze && freeze!(g)
    return g
end
function with_state(g::DGraph, f, args...; kwargs...)
    if g.frozen[]
        @assert !any(x->x isa ELTYPE, args)
        return f(g.state, args...; kwargs...)
    else
        return fetch(Dagger.@spawn f(g.state, args...; kwargs...))
    end
end
function exec_fast(f, args...; kwargs...)
    # FIXME: Ensure that `EagerThunk` result is also local
    if any(x->(x isa Dagger.EagerThunk && !isready(x)) ||
              (x isa Dagger.Chunk && x.handle.owner != myid()), args)
        return Base.fetch(Dagger.@spawn f(args...; kwargs...))
    else
        fetched_args = ntuple(i->args[i] isa ELTYPE ? Base.fetch(args[i]) : args[i], length(args))
        return f(fetched_args...; kwargs...)
    end
end
function exec_fast_nofetch(f, args...; kwargs...)
    # FIXME: Ensure that `EagerThunk` result is also local
    if any(x->(x isa Dagger.EagerThunk && !isready(x)) ||
              (x isa Dagger.Chunk && x.handle.owner != myid()), args)
        return Dagger.@spawn f(args...; kwargs...)
    else
        fetched_args = ntuple(i->args[i] isa ELTYPE ? Base.fetch(args[i]) : args[i], length(args))
        return f(fetched_args...; kwargs...)
    end
end

isfrozen(g::DGraph) = g.frozen[] || fetch(Dagger.@spawn isfrozen(g.state))
isfrozen(g::DGraphState) = g.frozen[]
function freeze!(g::DGraph)
    if g.frozen[] || !fetch(Dagger.@spawn freeze!(g.state))
        throw(ArgumentError("DGraph is already frozen"))
    end
    state = fetch(g.state)
    g.state = state
    g.frozen[] = true
    return
end
function freeze!(g::DGraphState)
    if isfrozen(g)
        return false
    end
    g.frozen[] = true
    return true
end
struct FrozenGraphException <: Exception end
Base.showerror(io::IO, ex::FrozenGraphException) =
    print(io, "Graph is frozen (immutable)")
function check_not_frozen(g)
    if g.frozen[]
        throw(FrozenGraphException())
    end
end

function Base.show(io::IO, g::DGraph{T,D}) where {T,D}
    print(io, "{$(nv(g)), $(ne(g))} $(D ? "" : "un")directed Dagger $T graph$(isfrozen(g) ? " (frozen)" : "")")
end

nparts(g::DGraph) = with_state(g, nparts)
nparts(g::DGraphState) = length(g.parts)
Base.eltype(::DGraph{T}) where T = T
Graphs.edgetype(::DGraph{T}) where T = Tuple{T,T}
Graphs.nv(g::DGraph) = with_state(g, nv)::Int
function Graphs.nv(g::DGraphState)
    if !isempty(g.parts_nv)
        return last(g.parts_nv).stop
    else
        return 0
    end
end
Graphs.ne(g::DGraph) = with_state(g, ne)::Int
Graphs.ne(g::DGraphState) = sum(g.parts_ne; init=0) + sum(g.bg_adjs_ne_src; init=0)
Graphs.has_vertex(g::DGraph, v::Integer) = 1 <= v <= nv(g)
Graphs.has_edge(g::DGraph, edge::Tuple) = has_edge(g, edge[1], edge[2])
Graphs.has_edge(g::DGraph, src::Integer, dst::Integer) =
    with_state(g, has_edge, src, dst)::Bool
function Graphs.has_edge(g::DGraphState{T,D}, src::Integer, dst::Integer) where {T,D}
    src_part_idx = findfirst(span->src in span, g.parts_nv)
    src_part_idx !== nothing || return false
    dst_part_idx = findfirst(span->dst in span, g.parts_nv)
    dst_part_idx !== nothing || return false

    if src_part_idx == dst_part_idx
        # The edge will be within a graph partition
        part = g.parts[src_part_idx]
        return exec_fast(has_edge, part, src, dst)
    else
        # The edge will be in an AdjList
        adj = g.bg_adjs[src_part_idx]
        return exec_fast(has_edge, adj, src, dst)
    end
end
Graphs.is_directed(::DGraph{T,D}) where {T,D} = D
Graphs.vertices(g::DGraph) = Base.OneTo(nv(g))
Graphs.edges(g::DGraph) = DGraphEdgeIter(g)
Graphs.zero(::Type{<:DGraph}) = DGraph()
function Graphs.add_vertex!(g::DGraph)
    check_not_frozen(g)
    with_state(g, add_vertices!, 1)
    return
end
function Graphs.add_vertices!(g::DGraph, n::Integer)
    check_not_frozen(g)
    return with_state(g, add_vertices!, n)
end
function Graphs.add_vertices!(g::DGraphState, n::Integer)
    check_not_frozen(g)

    n_rem = n
    chunksize = g.parts_v_max
    while n_rem > 0
        max_add = chunksize - rem(nv(g), chunksize)
        to_add = min(max_add, n_rem)
        if rem(nv(g), chunksize) == 0
            # We need to create a new partition for this vertex
            add_partition!(g, to_add)
        else
            # We will add this vertex to the last partition
            part = last(g.parts)
            exec_fast(add_vertices!, part, to_add)
            span = g.parts_nv[end]
            g.parts_nv[end] = UnitRange{Int}(span.start, span.stop+1)
        end
        n_rem -= to_add
    end

    return n
end
function add_partition!(g::DGraph, n::Integer)
    check_not_frozen(g)
    return with_state(g, add_partition!, n)
end
function add_partition!(g::DGraphState{T,D}, n::Integer) where {T,D}
    check_not_frozen(g)
    if n < 1
        throw(ArgumentError("n must be >= 1"))
    end
    push!(g.parts, Dagger.spawn(n) do n
        D ? SimpleDiGraph(n) : SimpleGraph(n)
    end)
    num_v = nv(g)
    push!(g.parts_nv, (num_v+1):(num_v+n))
    push!(g.parts_ne, 0)
    push!(g.bg_adjs, Dagger.@spawn AdjList{T,D}())
    push!(g.bg_adjs_ne, 0)
    push!(g.bg_adjs_ne_src, 0)
    return length(g.parts)
end
function add_partition!(g::DGraph, sg::AbstractGraph)
    check_not_frozen(g)
    return with_state(g, add_partition!, sg)
end
function add_partition!(g::DGraphState{T,D}, sg::AbstractGraph; all::Bool=true) where {T,D}
    check_not_frozen(g)
    shift = nv(g)
    part = add_partition!(g, nv(sg))
    part_edges = map(edge->(src(edge)+shift, dst(edge)+shift), collect(edges(sg)))
    count = add_edges!(g, part_edges; all)
    @assert !all || count == length(part_edges)
    return part
end
function Graphs.add_edge!(g::DGraph, src::Integer, dst::Integer)
    check_not_frozen(g)
    return with_state(g, add_edge!, src, dst)
end
function Graphs.add_edge!(g::DGraph, edge::Edge)
    check_not_frozen(g)
    return add_edge!(g, src(edge), dst(edge))
end
function Graphs.add_edge!(g::DGraphState{T,D}, src::Integer, dst::Integer) where {T,D}
    check_not_frozen(g)

    src_part_idx = findfirst(span->src in span, g.parts_nv)
    @assert src_part_idx !== nothing "Source vertex $src does not exist"

    dst_part_idx = findfirst(span->dst in span, g.parts_nv)
    @assert dst_part_idx !== nothing "Destination vertex $dst does not exist"

    if src_part_idx == dst_part_idx
        # Edge exists within a single partition
        part = g.parts[src_part_idx]
        src_shift = src - (g.parts_nv[src_part_idx].start - 1)
        dst_shift = dst - (g.parts_nv[dst_part_idx].start - 1)
        if exec_fast(add_edge!, part, src_shift, dst_shift)
            g.parts_ne[src_part_idx] += 1
        else
            return false
        end
    else
        # Edge spans two partitions
        src_bg_adj = g.bg_adjs[src_part_idx]
        dst_bg_adj = g.bg_adjs[dst_part_idx]
        src_t = exec_fast(add_edge!, src_bg_adj, (src, dst); fetch=false)
        dst_t = exec_fast(add_edge!, dst_bg_adj, (src, dst); fetch=false)
        if !fetch(src_t) || !fetch(dst_t)
            return false
        end
        if D
            # TODO: This will cause imbalance for many outgoing edges from a few vertices
            g.bg_adjs_ne_src[src_part_idx] += 1
        else
            owner_part_idx = edge_owner(src, dst, src_part_idx, dst_part_idx)
            g.bg_adjs_ne_src[owner_part_idx] += 1
        end
        g.bg_adjs_ne[src_part_idx] += 1
        g.bg_adjs_ne[dst_part_idx] += 1
    end

    return true
end
function add_edges!(g::DGraph, iter; all::Bool=true)
    check_not_frozen(g)
    return with_state(g, add_edges!, iter; all)
end
function add_edges!(g::DGraphState{T,D}, iter; all::Bool=true) where {T,D}
    check_not_frozen(g)

    # Determine edge partition/background
    part_edges = Dict{Int,Vector{Tuple{T,T}}}(part=>Tuple{T,T}[] for part in 1:nparts(g))
    back_edges = Dict{Int,Vector{Tuple{T,T}}}(part=>Tuple{T,T}[] for part in 1:nparts(g))
    nedges = 0
    for edge in iter
        nedges += 1
        src, dst = Tuple(edge)

        src_part_idx = findfirst(span->src in span, g.parts_nv)
        @assert src_part_idx !== nothing "Source vertex $src does not exist"

        dst_part_idx = findfirst(span->dst in span, g.parts_nv)
        @assert dst_part_idx !== nothing "Destination vertex $dst does not exist"

        if src_part_idx == dst_part_idx
            push!(part_edges[src_part_idx], (src, dst))
        else
            owner_part_idx = D ? src_part_idx : edge_owner(src, dst, src_part_idx, dst_part_idx)
            push!(back_edges[owner_part_idx], (src, dst))
        end
    end

    # Add edges concurrently
    part_tasks = Dict(part=>exec_fast_nofetch(add_edges!, g.parts[part], g.parts_nv[part].start-1, edges; all) for (part, edges) in part_edges)
    back_tasks = Dict(part=>exec_fast_nofetch(add_edges!, g.bg_adjs[part], edges; all) for (part, edges) in back_edges)

    # Update edge counters
    for (part, edge_count) in part_tasks
        g.parts_ne[part] += fetch(edge_count)
    end
    for (part, edge_count) in back_tasks
        g.bg_adjs_ne_src[part] += fetch(edge_count)
        g.bg_adjs_ne[part] = exec_fast(ne, g.bg_adjs[part])
    end

    # Validate that all edges were successfully added
    return sum(fetch, values(part_tasks)) + sum(fetch, values(back_tasks))
end
function add_edges!(g::Graphs.AbstractSimpleGraph, shift, edges; all::Bool=true)
    count = 0
    for edge in edges
        src, dst = Tuple(edge)
        if add_edge!(g, src-shift, dst-shift)
            count += 1
        elseif all
            return count
        end
    end
    return count
end
edge_owner(src::Int, dst::Int, src_part_idx::Int, dst_part_idx::Int) =
    iseven(hash(Base.unsafe_trunc(UInt, src+dst))) ? src_part_idx : dst_part_idx
Graphs.inneighbors(g::DGraph, v::Integer) = with_state(g, inneighbors, v)
function Graphs.inneighbors(g::DGraphState, v::Integer)
    part_idx = findfirst(span->v in span, g.parts_nv)
    if part_idx === nothing
        throw(BoundsError(g, v))
    end

    neighbors = Int[]
    shift = g.parts_nv[part_idx].start - 1

    # Check against local edges
    v_shift = v - shift
    local_neighs = exec_fast(inneighbors, g.parts[part_idx], v_shift)
    append!(neighbors, Iterators.map(neigh->neigh + shift, local_neighs))

    # Check against background edges
    append!(neighbors, exec_fast(inneighbors, g.bg_adjs[part_idx], v))

    return neighbors
end
Graphs.outneighbors(g::DGraph, v::Integer) = with_state(g, outneighbors, v)
function Graphs.outneighbors(g::DGraphState, v::Integer)
    part_idx = findfirst(span->v in span, g.parts_nv)
    if part_idx === nothing
        throw(BoundsError(g, v))
    end

    neighbors = Int[]
    shift = g.parts_nv[part_idx].start - 1

    # Check against local edges
    v_shift = v - shift
    local_neighs = exec_fast(outneighbors, g.parts[part_idx], v_shift)
    append!(neighbors, Iterators.map(neigh->neigh + shift, local_neighs))

    # Check against background edges
    append!(neighbors, exec_fast(outneighbors, g.bg_adjs[part_idx], v))

    return neighbors
end
Graphs.weights(g::DGraph) = Graphs.DefaultDistance(nv(g))

get_partition(g::DGraph, part::Integer) =
    with_state(g, get_partition, part)
get_partition(g::DGraphState, part::Integer) = fetch(g.parts[part])
get_background(g::DGraph, part::Integer) =
    with_state(g, get_background, part)
get_background(g::DGraphState, part::Integer) = fetch(g.bg_adjs[part])
