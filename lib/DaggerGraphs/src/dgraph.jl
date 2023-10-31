const ELTYPE = Union{Dagger.EagerThunk, Chunk}
const META_ELTYPE = Union{ELTYPE,Nothing}

"""
Stores the `DGraph`'s state, where `T` is the type of the graph's vertices and
`D` determines whether the graph is directed or undirected.
"""
mutable struct DGraphState{T<:Integer,D}
    # Whether the graph is "frozen" (immutable) or mutable
    frozen::Ref{Bool}

    # Vector of locally-connected SimpleDiGraphs
    parts::Vector{ELTYPE}
    # The range of vertices within each of `parts`
    parts_nv::Vector{UnitRange{T}}
    # The number of edges in each of `parts`
    parts_ne::Vector{T}
    # The maximum number of nodes for each of `parts`
    parts_v_max::Int
    # The vertex metadata for each of `parts`
    parts_v_meta::Vector{META_ELTYPE}
    # The edge metadata for each of `parts`
    parts_e_meta::Vector{META_ELTYPE}

    # FIXME we are iggnoring AbstractMetaGraph form MetaGraphs.jl
    # perhaps DGraph should implement this interface since we are supporting
    # metadata by definition?

    # Matrix of `AdjList` for each of `parts`
    # An edge is present in a given `AdjList` if either the src or dst vertex
    # is in the respective `parts` graph (the so-called "background graph")
    bg_adjs::Matrix{ELTYPE}
    # The number of edges in each of `bg_adjs`
    bg_adjs_ne::Matrix{T}
    # The number of edges in each of `bg_adjs` where the source is this partition
    bg_adjs_ne_src::Matrix{T}
    # The edge metadata for each of `bg_adjs`
    bg_adjs_e_meta::Matrix{META_ELTYPE}
end

"""
Represents graph's state where `T` is the type of the graph's vertices, chunk size
`chunksize`  where `D` determines whether the graph is directed or undirected.
"""
function DGraphState{T,D}(chunksize::Integer) where {T<:Integer,D}
    return DGraphState{T,D}(
        Ref(false),                       # frozen
        ELTYPE[],                         # parts
        UnitRange{T}[],                   # parts_nv
        T[],                              # parts_ne
        chunksize,                        # parts_v_max
        META_ELTYPE[],                    # parts_v_meta
        META_ELTYPE[],                    # parts_e_meta
        Matrix{ELTYPE}(undef, 0, 0),      # bg_adjs
        Matrix{T}(undef, 0, 0),           # bg_adjs_ne
        Matrix{T}(undef, 0, 0),           # bg_adjs_ne_src
        Matrix{META_ELTYPE}(undef, 0, 0)) # bg_adjs_e_meta
end

"""
Represents a distributed graph where `T` is the type of the graph's vertices and
where `D` determines whether the graph is directed or undirected.

    DGraph(n::T; freeze::Bool=false) where T < Integer

Create a new `DGraph` with `n` vertices and optionally freeze it.

    DGraph(sg::AbstractGraph{T}; directed::Bool=is_directed(sg), freeze::Bool=false, kwargs...) where {T<:Integer}

Create a new `DGraph` from any `AbstractGraph` and optionally freeze it.

    DGraph(dg::DGraph{T,D}; chunksize::T=0, directed::Bool=D, freeze::Bool=false) where {T<:Integer, D}

Create a new `DGraph` from a `DGraph` and optionally freeze it.
"""
mutable struct DGraph{T<:Integer, D} <: Graphs.AbstractGraph{T}
    # The internal graph state
    state::Union{Dagger.Chunk{DGraphState{T,D}}, DGraphState{T,D}}
    # Whether the graph is known to be frozen
    frozen::Ref{Bool}

    # Cache of metadata queries
    meta_cache::LockedObject{Dict{Symbol,Dict{Tuple{T,T},Any}}}
    # Cache of queries of arbitrary graph data
    query_cache::Dict{Function,LRU{Any,Any}}

    function DGraph{T}(state=nothing;
                       chunksize::T=T(128),
                       directed::Bool=true) where {T<:Integer}
        if state === nothing
            D = directed
            state = DGraphState{T,D}(chunksize)
        else
            D = is_directed(state)
        end
        return new{T,D}(Dagger.tochunk(state), # state
                        Ref(false), # frozen
                        LockedObject(Dict{Symbol,Dict{Tuple{T,T},Any}}()), # meta_cache
                        Dict()) # query_cache
    end
end
DGraph(; kwargs...) = DGraph{Int}(; kwargs...)
DGraph(x::T; kwargs...) where {T<:Integer} = DGraph{T}(x; kwargs...)
DGraph(x::AbstractGraph{T}; kwargs...) where {T<:Integer} = DGraph{T}(x; kwargs...)
DGraph{T}(n::S; kwargs...) where {T<:Integer,S<:Integer} =
    DGraph{T}(T(n); kwargs...)
function DGraph{T}(n::T; freeze::Bool=false, kwargs...) where {T<:Integer}
    g = DGraph{T}(; kwargs...)
    add_vertices!(g, n)
    freeze && freeze!(g)
    return g
end
function DGraph{T}(sg::AbstractGraph{U}; directed::Bool=is_directed(sg), freeze::Bool=false, weights::Bool=true, kwargs...) where {T<:Integer, U<:Integer}
    g = DGraph{T}(T(nv(sg)); directed, kwargs...)
    add_edges!(g, edges(sg))
    if weights
        sg_w = Graphs.weights(sg)
        if !(sg_w isa Graphs.DefaultDistance)
            set_edge_metadata!(g, sg_w)
        end
    end
    freeze && freeze!(g)
    return g
end
function DGraph{T}(dg::DGraph{T,D}; chunksize::T=0, directed::Bool=D, freeze::Bool=false) where {T<:Integer, D}
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

    new_state.bg_adjs = Matrix{ELTYPE}(undef, length(state.parts), length(state.parts))
    new_state.bg_adjs_ne = Matrix{T}(undef, length(state.parts), length(state.parts))
    new_state.bg_adjs_ne_src = Matrix{T}(undef, length(state.parts), length(state.parts))
    new_state.bg_adjs_e_meta = Matrix{META_ELTYPE}(undef, length(state.parts), length(state.parts))
    for part in 1:length(state.parts)
        # FIXME: Create on same nodes
        push!(new_state.parts, Dagger.@spawn copy(state.parts[part]))
        push!(new_state.parts_nv, state.parts_nv[part])
        push!(new_state.parts_ne, state.parts_ne[part])
        push!(new_state.parts_v_meta, Dagger.@spawn copymeta(state.parts_v_meta[part]))
        push!(new_state.parts_e_meta, Dagger.@spawn copymeta(state.parts_e_meta[part]))

        for opart in 1:length(state.parts)
            new_state.bg_adjs[part, opart] = Dagger.@spawn copy(state.bg_adjs[part, opart])
            new_state.bg_adjs_ne[part, opart] = state.bg_adjs_ne[part, opart]
            new_state.bg_adjs_ne_src[part, opart] = state.bg_adjs_ne_src[part, opart]
            new_state.bg_adjs_e_meta[part, opart] = Dagger.@spawn copymeta(state.bg_adjs_e_meta[part, opart])
        end
    end
    freeze && freeze!(g)
    return g
end

"""
    with_state(g::DGraph, f, args...; kwargs...)

Execute `f` on the graph's chunk local to the calling worker,
passing `args` and `kwargs` to `f`.
If the graph is frozen, `f` is executed locally on the state,
otherwise the execution is deferred to the worker owning the chunk.
"""
function with_state(g::DGraph, f, args...; kwargs...)
    if g.frozen[]
        @assert !any(x->x isa ELTYPE, args)
        return f(g.state, args...; kwargs...)
    else
        return fetch(Dagger.@spawn f(g.state, args...; kwargs...))
    end
end

"""
    with_state_cached(g::DGraph, f, args...; kwargs...)

Execute `f` on the graph's chunk local to the calling worker,
passing `args` and `kwargs` to `f`; the result of this call will be cached in
`g` and returned for future identical queries.
If the graph is frozen, `f` is executed locally on the state,
otherwise the execution is deferred to the worker owning the chunk.
"""
function with_state_cached(g::DGraph, f, args...; kwargs...)
    if g.frozen[]
        # Try to get the object from the cache
        key = (args, kwargs)
        if !haskey(g.query_cache, f)
            g.query_cache[f] = LRU{Any,Any}(;maxsize=128)
        end
        return get!(g.query_cache[f], key) do
            return with_state(g, f, args...; kwargs...)
        end
    else
        return with_state(g, f, args...; kwargs...)
    end
end

"""
    exec_fast(f, args...; kwargs...)

Executes `f` on the graph's chunk local to the calling worker,
passing `args` and `kwargs`.
"""
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

"""
    exec_fast_nofetch(f, args...; kwargs...)

Executes `f` on the graph's chunk optionally passing `args` and `kwargs`.
The execution is deferred to the worker owning the chunk.
"""
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

"""
    isfrozen(g::DGraph)

Check whether the graph is frozen (immutable).
"""
isfrozen(g::DGraph) = g.frozen[] || fetch(Dagger.@spawn isfrozen(g.state))

"""
    isfrozen(g::DGraphState)

Check whether the graph state is frozen (immutable).
"""
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

"""
    freeze!(g::DGraphState)

Freeze the graph state (make it immutable).
"""
function freeze!(g::DGraphState)
    if isfrozen(g)
        return false
    end
    g.frozen[] = true
    for part in 1:nparts(g)
        if Dagger.istask(g.parts[part])
            g.parts[part] = fetch(g.parts[part]; raw=true)
        end
        if Dagger.istask(g.parts_v_meta[part])
            g.parts_v_meta[part] = fetch(g.parts_v_meta[part]; raw=true)
        end
        if Dagger.istask(g.parts_e_meta[part])
            g.parts_e_meta[part] = fetch(g.parts_e_meta[part]; raw=true)
        end
        for opart in 1:nparts(g)
            if Dagger.istask(g.bg_adjs[part, opart])
                g.bg_adjs[part, opart] = fetch(g.bg_adjs[part, opart]; raw=true)
            end
            if Dagger.istask(g.bg_adjs_e_meta[part, opart])
                g.bg_adjs_e_meta[part, opart] = fetch(g.bg_adjs_e_meta[part, opart]; raw=true)
            end
        end
    end
    return true
end

"""
    DGraphException <: Exception

Exception thrown when an operation is attempted on a frozen graph.
"""
struct FrozenGraphException <: Exception end

Base.showerror(io::IO, ex::FrozenGraphException) =
    print(io, "Graph is frozen (immutable)")
function check_not_frozen(g)
    if g.frozen[]
        throw(FrozenGraphException())
    end
end

"""
    get_meta_cached(f, g::DGraph, key::Symbol, src::Integer, dst::Integer) -> Any

Fetches the metadata object at `key` from the cache for the partition
containing the edge `src => dst`; if not present, `f()` is called to retrieve
the object, which will then be placed into the cache for future queries.
"""
function get_meta_cached(f, g::DGraph{T}, key::Symbol, src::Integer, dst::Integer) where T
    # Lookup the relevant partition
    src_part, dst_part = partition_for(g, src, dst)

    # Try to get the object from the cache
    obj = nothing
    @safe_lock1 g.meta_cache cache begin
        if haskey(cache, key)
            if haskey(cache[key], (src_part, dst_part))
                obj = Some{Any}(cache[key][(src_part, dst_part)])
            end
        end
    end
    if obj !== nothing
        # Cache hit, return the cached object
        return f(g, something(obj))
    end

    # Cache miss
    obj_task = with_state(g, _get_meta, key, src_part, dst_part)
    if true # FIXME: Provide configurable caching strategies
        # Fetch the metadata directly
        obj = fetch(obj_task)

        # Insert the metadata into the cache for future queries
        @safe_lock1 g.meta_cache cache begin
            if !haskey(cache, key)
                cache[key] = Dict{Tuple{T,T},Any}()
            end
            cache[key][(src_part, dst_part)] = obj
        end

        return f(g, obj)
    else
        # Fetch the object directly
        return with_state(g, f, obj_task)
    end
end
function _get_meta(g::DGraphState, key, src_part, dst_part)
    if src_part == dst_part
        meta = g.parts_e_meta[src_part]
    else
        meta = g.bg_adjs_e_meta[src_part, dst_part]
    end
    if meta === nothing
        throw(ArgumentError("DGraph has no edge metadata\nPlease add some with `set_edge_metadata!`"))
    end
    return exec_fast(getindex, meta, key)
end

"""
    has_metadata(g::DGraph)

Check whether the graph has metadata.
"""
has_metadata(g::DGraph) = with_state_cached(g, has_metadata)

"""
    has_vertex_metadata(g::DGraph)

Check whether the graph has vertex metadata.
"""
has_vertex_metadata(g::DGraph) = with_state_cached(g, has_vertex_metadata)
"""
    has_edge_metadata(g::DGraph)

Check whether the graph has edge metadata.
"""
has_edge_metadata(g::DGraph) = with_state_cached(g, has_edge_metadata)

"""
    has_metadata(g::DGraphState)

Check whether the graph state has metadata.
"""
has_metadata(g::DGraphState) =
    has_vertex_metadata(g) ||
    has_edge_metadata(g)

"""
    has_vertex_metadata(g::DGraphState)

Check whether the graph state has vertex metadata.
"""
has_vertex_metadata(g::DGraphState) =
    any(!isnothing, g.parts_v_meta)

"""
    has_edge_metadata(g::DGraphState)

Check whether the graph state has edge metadata.
"""
has_edge_metadata(g::DGraphState) =
    any(!isnothing, g.parts_e_meta) ||
    any(!isnothing, g.bg_adjs_e_meta)

edge_metadata_keys(g::DGraph) = with_state_cached(g, edge_metadata_keys)
function edge_metadata_keys(g::DGraphState)
    if nv(g) == 0 || g.parts_e_meta[1] === nothing
        return Symbol[]
    end
    return exec_fast(metadata_keys, g.parts_e_meta[1])
end

"""
    set_vertex_metadata!(g::DGraph, meta, key::Union{Symbol,Nothing}=nothing)

Set the vertex metadata for the graph `g` to `meta`, which will be
automatically partitioned by calling `partition_vertex_metadata`. If `key` is
provided, then `meta` will be accessible using that key. If `key` is `nothing`,
then the metadata will be queried using `metadata_keys` to determine if the
metadata is already split into multiple kinds of metadata; if so, then the
metadata will be accessible using those keys, otherwise, the key `default` will
be used to access the metadata.
"""
function set_vertex_metadata!(g::DGraph, meta, key::Union{Symbol,Nothing}=nothing)
    check_not_frozen(g)
    # Create vertex metadata for each partition, being careful not to transfer
    # `meta` itself, which may be large
    for part in 1:nparts(g)
        part_vs = partition_vertices(g, part)
        submeta = partition_vertex_metadata(meta, part_vs)
        with_state(g, set_vertex_metadata!, part, submeta)
    end
end
function set_vertex_metadata!(g::DGraphState, part::Integer, key, submeta)
    check_not_frozen(g)
    if key !== nothing
        submeta = Dict(key=>submeta)
    elseif !isempty(metadata_keys(submeta))
        # Leave as-is
    else
        submeta = Dict(:default=>submeta)
    end
    g.parts_v_meta[part] = Dagger.tochunk(submeta)
    return
end

"""
    set_edge_metadata!(g::DGraph, meta, key::Union{Symbol,Nothing}=nothing)

Set the edge metadata for the graph `g` to `meta`, which will be automatically
partitioned by calling `split_edge_metadata`. If `key` is provided, then
`meta` will be accessible using that key. If `key` is `nothing`, then the
metadata will be queried using `metadata_keys` to determine if the metadata is
already split into multiple kinds of metadata; if so, then the metadata
will be accessible using those keys, otherwise, the key `default` will be used
to access the metadata.
"""
function set_edge_metadata!(g::DGraph, meta, key::Union{Symbol,Nothing}=nothing)
    check_not_frozen(g)
    # Create edge metadata for each partition and background,
    # being careful not to transfer `meta` itself, which may be large
    for part in 1:nparts(g)
        part_edges = partition_edges(g, part)
        if length(part_edges) > 0
            part_submeta = split_edge_metadata(meta, part_edges)
            with_state(g, set_partition_edge_metadata!, part, key, part_submeta)
        end

        for opart in 1:nparts(g)
            part == opart && continue
            back_edges = background_edges(g, part, opart)
            if length(back_edges) > 0
                back_submeta = split_edge_metadata(meta, back_edges)
                with_state(g, set_background_edge_metadata!, part, opart, key, back_submeta)
            end
        end
    end
end
function set_partition_edge_metadata!(g::DGraphState, part::Integer, key, submeta)
    check_not_frozen(g)
    if submeta !== nothing
        if key !== nothing
            submeta = Dict(key=>submeta)
        elseif !isempty(metadata_keys(submeta))
            # Leave as-is
        else
            submeta = Dict(:default=>submeta)
        end
        g.parts_e_meta[part] = Dagger.tochunk(submeta)
    end
    return
end
function set_background_edge_metadata!(g::DGraphState, src_part::Integer, dst_part::Integer, key, submeta)
    check_not_frozen(g)
    if submeta !== nothing
        if key !== nothing
            submeta = Dict(key=>submeta)
        elseif !isempty(metadata_keys(submeta))
            # Leave as-is
        else
            submeta = Dict(:default=>submeta)
        end
        g.bg_adjs_e_meta[src_part, dst_part] = Dagger.tochunk(submeta)
    end
    return
end
metadata_keys(::Any) = Symbol[]
metadata_keys(x::AbstractDict{Symbol}) = collect(keys(x))

split_vertex_metadata(meta, part_nv) = error("Must define `split_vertex_metadata` for `$(typeof(meta))`")
split_edge_metadata(meta, edges) = error("Must define `split_edge_metadata` for `$(typeof(meta))`")
split_vertex_metadata(meta::AbstractVector, part_vs) =
    OffsetArray(meta[part_vs], part_vs)

"""
    split_edge_metadata(meta::AbstractMatrix{T}, edges)

Returns partition edge metadata `meta` for the edges `edges`.
"""
function split_edge_metadata(meta::AbstractMatrix{T}, edges) where T
    if isempty(edges)
        return fill(one(T), 0, 0)
    end
    #=
    vs_min = minimum(x->min(src(x), dst(x)), edges)
    vs_max = maximum(x->max(src(x), dst(x)), edges)
    vs_span = vs_min:vs_max
    =#
    src_span = range(extrema(Iterators.map(src, edges))...)
    dst_span = range(extrema(Iterators.map(dst, edges))...)
    meta_slice = meta[src_span, dst_span]
    return OffsetArray(meta_slice, src_span, dst_span)
end

"""
    get_partition_vertex_metadata(g::DGraph, part::Integer) -> Any

Get the vertex metadata for the partition `part` of the graph `g`.
"""
get_partition_vertex_metadata(g::DGraph, part::Integer) =
    fetch(with_state(g, get_partition_vertex_metadata, part))
function get_partition_vertex_metadata(g::DGraphState, part::Integer)
    return g.parts_v_meta[part]
end

"""
    get_partition_edge_metadata(g::DGraph, part::Integer) -> Any

Get the edge metadata for the partition `part` of the graph `g`.
"""
get_partition_edge_metadata(g::DGraph, part::Integer) =
    fetch(with_state(g, get_partition_edge_metadata, part))
function get_partition_edge_metadata(g::DGraphState, part::Integer)
    return g.parts_e_meta[part]
end

"""
    get_background_vertex_metadata(g::DGraph, part::Integer)

Get the vertex metadata for the background (intercluster) graph of the partition `part` of the graph `g`.
"""
get_background_edge_metadata(g::DGraph, part::Integer) =
    fetch(with_state(g, get_background_edge_metadata, part))
function get_background_edge_metadata(g::DGraphState, part::Integer)
    return g.bg_adjs_e_meta[part]
end
copymeta(x) = x
copymeta(x::AbstractArray) = copy(x)

"""
    Graphs.weights(g::DGraph)

Get the weights of the graph `g` - uses the edge metadata if present,
otherwise yields a matrix of ones as `Graphs.DefaultDistance`.

By default, if there is only one kind of metadata, it is used as the weights.
If there is more than one kind of metadata, then the desired metadata key must
be set using [`with_weights`](@ref), or else trying to access the result of
`weights` will throw an error.
"""
function Graphs.weights(g::DGraph)
    meta_key = weights_metadata_key(g)
    if meta_key === nothing
        # No metadata, so just provide default weights
        return Graphs.DefaultDistance(nv(g))
    end
    return LazyWeights(g, meta_key)
end

"""
    with_weights(f, key::Symbol)

FIXME
"""
with_weights(f, key::Symbol) = with(f, WEIGHTS_METADATA_KEY=>key)
function weights_metadata_key(g)
    key = WEIGHTS_METADATA_KEY[]
    if key !== nothing
        return key
    end
    keys = edge_metadata_keys(g)
    nkeys = length(keys)
    if nkeys > 1
        throw(ArgumentError("Weights have not been configured, but multiple kinds of metadata are available\nPlease use `with_weights` to select which metadata to use as weights"))
    elseif nkeys == 1
        return first(keys)
    else
        return nothing
    end
end

const WEIGHTS_METADATA_KEY = ScopedValue{Union{Symbol,Nothing}}(nothing)
struct LazyWeights{T,D<:DGraph{T}} <: AbstractMatrix{T}
    g::D
    meta_key::Union{Symbol,Nothing}
end
function Base.collect(w::LazyWeights)
    W = ones(Float64, nv(w.g), nv(w.g))
    for (edge, w) in edges_with_weights(w.g)
        src, dst = Tuple(edge)
        W[src,dst] = w
    end
    return W
end
function Base.getindex(w::LazyWeights, src::Integer, dst::Integer)
    try
        return get_meta_cached(w.g, w.meta_key, src, dst) do g, meta
            return meta[src, dst]
        end
    catch
        @warn "Failed on $src=>$dst"
        rethrow()
    end
end

function Base.show(io::IO, g::DGraph{T,D}) where {T,D}
    print(io, "{$(nv(g)), $(ne(g))} $(D ? "" : "un")directed Dagger $T $(has_metadata(g) ? "meta-" : "")graph$(isfrozen(g) ? " (frozen)" : "")")
end

partition_size(g::DGraph) = with_state_cached(g, partition_size)
partition_size(g::DGraphState) = g.parts_v_max

"""
    nparts(g::DGraph)

Get the number of partitions in the graph `g`.
"""
nparts(g::DGraph) = with_state_cached(g, nparts)

"""
    nparts(g::DGraphState)

Get the number of partitions in the graph state `g`.
"""
nparts(g::DGraphState) = length(g.parts)
Base.eltype(::DGraph{T}) where T = T
Graphs.edgetype(::DGraph{T}) where T = Edge{T}
Graphs.nv(g::DGraph{T}) where T <: Integer = with_state_cached(g, nv)::T
function Graphs.nv(g::DGraphState{T}) where T
    if !isempty(g.parts_nv)
        return T(last(g.parts_nv).stop)
    else
        return zero(T)
    end
end
Graphs.ne(g::DGraph) = with_state_cached(g, ne)::Int
Graphs.ne(g::DGraphState) = Int(sum(g.parts_ne; init=0) + sum(g.bg_adjs_ne_src; init=0))
Graphs.has_vertex(g::DGraph, v::Integer) = 1 <= v <= nv(g)
Graphs.has_edge(g::DGraph, edge::Tuple) = has_edge(g, edge[1], edge[2])
Graphs.has_edge(g::DGraph, src::Integer, dst::Integer) =
    with_state_cached(g, has_edge, src, dst)::Bool
function Graphs.has_edge(g::DGraphState{T,D}, src::Integer, dst::Integer) where {T,D}
    src_part_idx = findfirst(span->src in span, g.parts_nv)
    src_part_idx !== nothing || return false
    dst_part_idx = findfirst(span->dst in span, g.parts_nv)
    dst_part_idx !== nothing || return false

    if src_part_idx == dst_part_idx
        # The edge will be within a graph partition
        part = g.parts[src_part_idx]
        src_shift = src - (g.parts_nv[src_part_idx].start - 1)
        dst_shift = dst - (g.parts_nv[dst_part_idx].start - 1)
        return exec_fast(has_edge, part, src_shift, dst_shift)
    else
        # The edge will be in an AdjList
        adj = g.bg_adjs[src_part_idx, dst_part_idx]
        return exec_fast(has_edge, adj, src, dst)
    end
end
Graphs.is_directed(::DGraph{T,D}) where {T,D} = D
Graphs.is_directed(::Type{<:DGraph{T,D}}) where {T,D} = D
Graphs.is_directed(::DGraphState{T,D}) where {T,D} = D
Graphs.is_directed(::Type{<:DGraphState{T,D}}) where {T,D} = D
Graphs.vertices(g::DGraph{T}) where T = Base.OneTo{T}(nv(g))
Graphs.edges(g::DGraph) = DGraphEdgeIter(g)

"""
    edges_with_metadata(f, g::DGraph)

Iterate over the edges of the graph `g`, optionally passing the edge metadata to `f`.
"""
edges_with_metadata(f, g::DGraph) = DGraphEdgeIter(g; metadata=true, meta_f=f)

"""
    edges_with_weights(g::DGraph)

Iterate over the weights of edges of the graph `g`.
"""
edges_with_weights(g::DGraph) = edges_with_metadata(weights, g)
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

"""
    add_partition!(g::DGraph, n::Integer)

Add a partition of `n` vertices to the graph `g`.
"""
function add_partition!(g::DGraph, n::Integer)
    check_not_frozen(g)
    return with_state(g, add_partition!, n)
end

"""
    add_partition!(g::DGraphState{T,D}, n::T) where {T <: Integer, D}

Add a partition of `n` vertices to the graph state `g`.
"""
function add_partition!(g::DGraphState{T,D}, n::Integer) where {T <: Integer, D}
    check_not_frozen(g)
    if n < 1
        throw(ArgumentError("n must be >= 1"))
    end
    push!(g.parts, Dagger.spawn(T, n) do T, n
        D ? SimpleDiGraph{T}(n) : SimpleGraph{T}(n)
    end)
    num_v = nv(g)
    push!(g.parts_nv, (num_v+1):(num_v+n))
    push!(g.parts_ne, 0)
    push!(g.parts_v_meta, nothing)
    push!(g.parts_e_meta, nothing)
    old_bg_adjs = g.bg_adjs
    old_bg_adjs_ne = g.bg_adjs_ne
    old_bg_adjs_ne_src = g.bg_adjs_ne_src
    old_bg_adjs_e_meta = g.bg_adjs_e_meta
    nparts = length(g.parts)
    g.bg_adjs = Matrix{ELTYPE}(undef, nparts, nparts)
    g.bg_adjs_ne = Matrix{T}(undef, nparts, nparts)
    g.bg_adjs_ne_src = Matrix{T}(undef, nparts, nparts)
    g.bg_adjs_e_meta = Matrix{META_ELTYPE}(undef, nparts, nparts)
    g.bg_adjs[1:(nparts-1), 1:(nparts-1)] .= old_bg_adjs
    g.bg_adjs_ne[1:(nparts-1), 1:(nparts-1)] .= old_bg_adjs_ne
    g.bg_adjs_ne_src[1:(nparts-1), 1:(nparts-1)] .= old_bg_adjs_ne_src
    g.bg_adjs_e_meta[1:(nparts-1), 1:(nparts-1)] .= old_bg_adjs_e_meta
    for part in 1:(nparts-1)
        g.bg_adjs[part, nparts] = Dagger.@spawn AdjList{T,D}()
        g.bg_adjs[nparts, part] = Dagger.@spawn AdjList{T,D}()
        g.bg_adjs_ne[part, nparts] = 0
        g.bg_adjs_ne[nparts, part] = 0
        g.bg_adjs_ne_src[part, nparts] = 0
        g.bg_adjs_ne_src[nparts, part] = 0
        g.bg_adjs_e_meta[part, nparts] = nothing
        g.bg_adjs_e_meta[nparts, part] = nothing
    end
    g.bg_adjs[nparts, nparts] = Dagger.@spawn AdjList{T,D}()
    g.bg_adjs_ne[nparts, nparts] = 0
    g.bg_adjs_ne_src[nparts, nparts] = 0
    g.bg_adjs_e_meta[nparts, nparts] = nothing
    return length(g.parts)
end

"""
    add_partition!(g::DGraph, sg::AbstractGraph)

Add a partition consisting of a subgraph `sg` to the graph `g`.
"""
function add_partition!(g::DGraph, sg::AbstractGraph)
    check_not_frozen(g)
    return with_state(g, add_partition!, sg)
end

"""
    add_partition!(g::DGraphState{T,D}, sg::AbstractGraph; all::Bool=true) where {T <: Integer, D}

Add a partition consisting of a subgraph `sg` to the graph state `g`.
"""
function add_partition!(g::DGraphState{T,D}, sg::AbstractGraph; all::Bool=true) where {T <: Integer, D}
    check_not_frozen(g)
    shift = nv(g)
    part = add_partition!(g, nv(sg))
    part_edges = map(edge->(src(edge)+shift, dst(edge)+shift), collect(edges(sg)))
    count = add_edges!(g, part_edges; all)
    @assert !all || count == length(part_edges)
    return part
end


function add_partition!(g::DGraph, part_data::ELTYPE, back_data::ELTYPE,
                        part_vert_meta_data::META_ELTYPE,
                        part_edge_meta_data::META_ELTYPE,
                        back_edge_meta_data::META_ELTYPE,
                        n_verts::Integer, n_part_edges::Integer,
                        n_back_edges::Integer, n_back_own_edges::Integer)
    check_not_frozen(g)
    return with_state(g, add_partition!, Ref(part_data), Ref(back_data),
                      Ref(part_vert_meta_data),
                      Ref(part_edge_meta_data),
                      Ref(back_edge_meta_data),
                      n_verts, n_part_edges,
                      n_back_edges, n_back_own_edges)
end
function add_partition!(g::DGraphState{T,D}, part_data::Ref, back_data::Ref,
                        part_vert_meta_data::Ref,
                        part_edge_meta_data::Ref,
                        back_edge_meta_data::Ref,
                        n_verts::Integer, n_part_edges::Integer,
                        n_back_edges::Integer, n_back_own_edges::Integer) where {T,D}
    check_not_frozen(g)
    if n_verts < 1
        throw(ArgumentError("n_verts must be >= 1"))
    end
    num_v = nv(g)
    push!(g.parts, part_data[])
    push!(g.parts_nv, (num_v+1):(num_v+n_verts))
    push!(g.parts_ne, n_part_edges)
    push!(g.parts_v_meta, part_vert_meta_data[])
    push!(g.parts_e_meta, part_edge_meta_data[])
    push!(g.bg_adjs, back_data[])
    push!(g.bg_adjs_ne, n_back_edges)
    push!(g.bg_adjs_ne_src, n_back_own_edges)
    push!(g.bg_adjs_e_meta, back_edge_meta_data[])
    return length(g.parts)
end
function Graphs.add_edge!(g::DGraph{T}, src::Integer, dst::Integer) where T
    check_not_frozen(g)
    return with_state(g, add_edge!, T(src), T(dst))
end
function Graphs.add_edge!(g::DGraph{T}, edge::Edge) where T
    check_not_frozen(g)
    return add_edge!(g, T(src(edge)), T(dst(edge)))
end
function Graphs.add_edge!(g::DGraphState{T,D}, src::Integer, dst::Integer) where {T,D}
    check_not_frozen(g)

    src_part_idx = T(findfirst(span->src in span, g.parts_nv))
    @assert src_part_idx !== nothing "Source vertex $src does not exist"

    dst_part_idx = T(findfirst(span->dst in span, g.parts_nv))
    @assert dst_part_idx !== nothing "Destination vertex $dst does not exist"

    if src_part_idx == dst_part_idx
        # Edge exists within a single partition
        part = g.parts[src_part_idx]
        src_shift = src - (g.parts_nv[src_part_idx].start - one(T))
        dst_shift = dst - (g.parts_nv[dst_part_idx].start - one(T))
        if exec_fast(add_edge!, part, src_shift, dst_shift)
            g.parts_ne[src_part_idx] += one(T)
        else
            return false
        end
    else
        error("FIXME")
        # Edge spans two partitions
        src_bg_adj = g.bg_adjs[src_part_idx]
        dst_bg_adj = g.bg_adjs[dst_part_idx]
        src_t = exec_fast_nofetch(add_edge!, src_bg_adj, (src, dst))
        dst_t = exec_fast_nofetch(add_edge!, dst_bg_adj, (src, dst))
        if !fetch(src_t) || !fetch(dst_t)
            return false
        end
        if D
            # TODO: This will cause imbalance for many outgoing edges from a few vertices
            g.bg_adjs_ne_src[src_part_idx] += one(T)
        else
            owner_part_idx = edge_owner(src, dst, src_part_idx, dst_part_idx)
            g.bg_adjs_ne_src[owner_part_idx] += one(T)
        end
        g.bg_adjs_ne[src_part_idx] += one(T)
        g.bg_adjs_ne[dst_part_idx] += one(T)
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
    back_edges = Dict{Tuple{Int,Int},Vector{Tuple{T,T}}}((src_part, dst_part)=>Tuple{T,T}[] for src_part in 1:nparts(g), dst_part in 1:nparts(g))
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
            #owner_part_idx = D ? src_part_idx : edge_owner(src, dst, src_part_idx, dst_part_idx)
            push!(back_edges[src_part_idx, dst_part_idx], (src, dst))
        end
    end

    # Add edges concurrently
    part_tasks = Dict(part=>exec_fast_nofetch(add_edges!, g.parts[part], g.parts_nv[part].start-1, edges; all) for (part, edges) in part_edges)
    back_tasks = Dict((src_part, dst_part)=>exec_fast_nofetch(add_edges!, g.bg_adjs[src_part, dst_part], edges; all) for ((src_part, dst_part), edges) in back_edges)

    # Update edge counters
    for (part, edge_count) in part_tasks
        g.parts_ne[part] += fetch(edge_count)
    end
    for ((src_part, dst_part), edge_count) in back_tasks
        g.bg_adjs_ne_src[src_part, dst_part] += fetch(edge_count)
        g.bg_adjs_ne[src_part, dst_part] = exec_fast(ne, g.bg_adjs[src_part, dst_part])
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

"""
    edge_owner(src::T, dst::T, src_part_idx::T, dst_part_idx::T) where {T<:Integer}

Determine which partition owns the edge `(src, dst)`.
"""
edge_owner(src::T, dst::T, src_part_idx::T, dst_part_idx::T) where {T<:Integer} =
    iseven(hash(Base.unsafe_trunc(UInt, src+dst))) ? src_part_idx : dst_part_idx

Graphs.inneighbors(g::DGraph{T}, v::Integer) where T =
    with_state_cached(g, inneighbors, T(v))
function Graphs.inneighbors(g::DGraphState{T}, v::T) where T
    part_idx = findfirst(span->v in span, g.parts_nv)
    if part_idx === nothing
        throw(BoundsError(g, v))
    end

    neighbors = T[]
    shift = g.parts_nv[part_idx].start - one(T)

    # Check against local edges
    v_shift = v - shift
    local_neighs = exec_fast(inneighbors, g.parts[part_idx], v_shift)
    append!(neighbors, Iterators.map(neigh->neigh + shift, local_neighs))

    # Check against background edges
    append!(neighbors, exec_fast(inneighbors, g.bg_adjs[part_idx], v))

    return sort!(neighbors)
end
Graphs.outneighbors(g::DGraph{T}, v::Integer) where T =
    with_state_cached(g, outneighbors, T(v))
function Graphs.outneighbors(g::DGraphState{T}, v::T) where T
    part_idx = findfirst(span->v in span, g.parts_nv)
    if part_idx === nothing
        throw(BoundsError(g, v))
    end

    neighbors = T[]
    shift = g.parts_nv[part_idx].start - one(T)

    # Check against local edges
    v_shift = v - shift
    local_neighs = exec_fast(outneighbors, g.parts[part_idx], v_shift)
    append!(neighbors, Iterators.map(neigh->neigh + shift, local_neighs))

    # Check against background edges
    append!(neighbors, exec_fast(outneighbors, g.bg_adjs[part_idx], v))

    return sort!(neighbors)
end

"""
    get_partition(g::DGraph, part::Integer)

Get the partition `part` of the graph `g`.
"""
get_partition(g::DGraph, part::Integer) =
    with_state(g, get_partition, part)

"""
    get_partition(g::DGraphState, part::Integer)

Get the partition `part` of the graph state `g`.
"""
get_partition(g::DGraphState, part::Integer) = fetch(g.parts[part])

"""
    get_background(g::DGraph, part::Integer)

Get the background (intercluster) graph of the partition `part` of the graph `g`.
"""
get_background(g::DGraph, part::Integer) =
    with_state(g, get_background, part)

"""
    get_background(g::DGraphState, part::Integer)

Get the background (intercluster) graph of the partition `part` of the graph state `g`.
"""
get_background(g::DGraphState, part::Integer) = fetch(g.bg_adjs[part])

"""
    partition_vertices(g::DGraph, part::Integer)

Get the vertices of the partition `part` of the graph `g`.
"""
partition_vertices(g::DGraph, part::Integer) =
    with_state_cached(g, partition_vertices, part)

"""
    partition_vertices(g::DGraphState, part::Integer)

Get the vertices of the partition `part` of the graph state `g`.
"""
partition_vertices(g::DGraphState, part::Integer) = g.parts_nv[part]

"""
    partition_edges(g::DGraph, part::Integer)

Get the edges of the partition `part` of the graph `g`.
"""
partition_edges(g::DGraph, part::Integer) =
    with_state(g, partition_edges, part)
function partition_edges(g::DGraphState{T}, part::Integer) where T
    shift = g.parts_nv[part].start - one(T)
    return map(edge->Edge(src(edge)+shift, dst(edge)+shift), exec_fast(edges, g.parts[part]))
end
"""
    background_edges(g::DGraph, src_part::Integer, dst_part::Integer)

Get the edges of the background partition at `src_part`=>`dst_part` of the graph `g`.
"""
background_edges(g::DGraph, src_part::Integer, dst_part::Integer) =
    with_state(g, background_edges, src_part, dst_part)
function background_edges(g::DGraphState{T}, src_part::Integer, dst_part::Integer) where T
    @assert src_part != dst_part
    return exec_fast(edges, g.bg_adjs[src_part, dst_part])
end

"""
    partition_nv(g::DGraph, part::Integer)

Get the number of vertices in the partition `part` of the graph `g`.
"""
partition_nv(g::DGraph, part::Integer) = length(partition_vertices(g, part))

"""
    partition_ne(g::DGraph, part::Integer)

Get the number of edges in the partition `part` of the graph `g`.
"""
partition_ne(g::DGraph, part::Integer) = with_state_cached(g, partition_ne, part)

"""
    partition_ne(g::DGraphState, part::Integer)

Get the number of edges in the partition `part` of the graph state `g`.
"""
function partition_ne(g::DGraphState, part::Integer)
    return (g.parts_ne[part],
            g.bg_adjs_ne[part],
            g.bg_adjs_ne_src[part])
end

"""
    partition_for(g::DGraph, v::Integer) -> Int

Get the partition ID of the vertex `v` of the graph `g`.
"""
partition_for(g::DGraph, v::Integer) =
    with_state_cached_cached(g, partition_for, v)
function partition_for(g::DGraphState, v::Integer)
    idx = findfirst(span->v in span, g.parts_nv)
    if idx === nothing
        throw(BoundsError(g, v))
    end
    return idx
end

"""
    partition_for(g::DGraph, src::Integer, dst::Integer) -> Int

Get the partition IDs of the edge `(src, dst)` of the graph `g`.
"""
partition_for(g::DGraph, src::Integer, dst::Integer) =
    with_state_cached_cached(g, partition_for, src, dst)
function partition_for(g::DGraphState, src::Integer, dst::Integer)
    src_part_idx = findfirst(span->src in span, g.parts_nv)
    dst_part_idx = findfirst(span->dst in span, g.parts_nv)
    if src_part_idx === nothing || dst_part_idx === nothing
        throw(BoundsError(g, (src, dst)))
    end
    return src_part_idx, dst_part_idx
    #= FIXME: Remove me
    if src_part_idx == dst_part_idx
        return src_part_idx, false
    else
        return edge_owner(src, dst, src_part_idx, dst_part_idx), true
    end
    =#
end

"""
    partitioning(g::DGraph)

Get the partitioning of the graph `g`.
This yields a vector `c` such that `c[v]` is the partition of vertex `v`.
The length of the vector is equal to the number of vertices in the graph.
"""
partitioning(g::DGraph) = with_state_cached(g, partitioning)

"""
    partitioning(g::DGraphState)

Get the partitioning of the graph state `g`.
This yields a vector `c` such that `c[v]` is the partition of vertex `v`.
The length of the vector is equal to the number of vertices in the graph state.
"""
function partitioning(g::DGraphState)
    c = fill(0, nv(g))
    for part in 1:nparts(g)
        span = g.parts_nv[part]
        c[span] .= part
    end
    return c
end
