## Adjacency list storage

abstract type AbstractAdjListStorage{T,D} end
Base.IteratorSize(::Type{<:AbstractAdjListStorage}) = Base.HasLength()
Base.IteratorEltype(::Type{<:AbstractAdjListStorage}) = Base.HasEltype()
Base.eltype(::Type{<:AbstractAdjListStorage{T}}) where T = Edge{T}
function add_edges!(adjlist::AbstractAdjListStorage, edges; all::Bool=true)
    count = 0
    for edge in edges
        if add_edge!(adjlist, edge)
            count += 1
        elseif all
            return count
        end
    end
    return count
end

# Storage matching Graphs.SimpleGraph for high edge counts
struct SimpleAdjListStorage{T,D} <: AbstractAdjListStorage{T,D}
    fadjlist::Vector{Vector{T}}
    badjlist::Vector{Vector{T}}
end
SimpleAdjListStorage{T,D}() where {T,D} =
    SimpleAdjListStorage{T,D}(Vector{Vector{T}}(),
                              Vector{Vector{T}}())
Base.copy(adjlist::SimpleAdjListStorage{T,D}) where {T,D} =
    SimpleAdjListStorage{T,D}(copy(adjlist.fadjlist),
                              copy(adjlist.badjlist))
Base.length(adjlist::SimpleAdjListStorage) =
    sum(length, adjlist.fadjlist; init=0)
function Base.iterate(adjlist::SimpleAdjListStorage{T}) where T
    idx = findfirst(x->!isempty(x), adjlist.fadjlist)
    if idx === nothing
        return nothing
    end
    edge_idx = something(findfirst(x->!isempty(x), adjlist.fadjlist[idx]))
    state = (T(idx), T(edge_idx))
    return Base.iterate(adjlist, state)
end
function Base.iterate(adjlist::SimpleAdjListStorage{T}, state::Tuple{T,T}) where T
    src, dst = state
    if src > length(adjlist.fadjlist)
        return nothing
    elseif dst > length(adjlist.fadjlist[src])
        src += one(T)
        dst = one(T)
        head = src
        src = findfirst(x->!isempty(x), @view(adjlist.fadjlist[head:end]))
        if src === nothing
            return nothing
        end
        src = T(src)
        # Shift by offset from @view
        src += head - one(T)
    end
    value = (src, adjlist.fadjlist[src][dst])
    dst += one(T)
    return (Edge(value), (src, dst))
end
function Graphs.add_edge!(adjlist::SimpleAdjListStorage{T,D}, edge) where {T,D}
    src, dst = Tuple(edge)
    if !D
        src, dst = minmax(src, dst)
    end

    has_edge(adjlist, edge) && return false

    # If necessary, allocate more inner vectors
    nv = max(src, dst)
    if nv > length(adjlist.fadjlist)
        #= FIXME: Resize more efficiently, and use undef elements
        resize!(adjlist.fadjlist, nv)
        if D
            resize!(adjlist.badjlist, nv)
        end
        isassigned(adjlist.fadjlist, src) || (adjlist.fadjlist[src] = Vector{T}())
        isassigned(adjlist.fadjlist, dst) || (adjlist.fadjlist[dst] = Vector{T}())
        if D
            isassigned(adjlist.badjlist, src) || (adjlist.badjlist[src] = Vector{T}())
            isassigned(adjlist.badjlist, dst) || (adjlist.badjlist[dst] = Vector{T}())
        end
        =#
        idx = length(adjlist.fadjlist)+1
        for _ in idx:nv
            push!(adjlist.fadjlist, Vector{T}())
            if D
                push!(adjlist.badjlist, Vector{T}())
            end
        end
    end

    # Add edges
    if D
        # Directed graphs have only forward edges
        push!(adjlist.fadjlist[src], dst)
        push!(adjlist.badjlist[dst], src)
    else
        # Undirected graphs have both forward and backward edges
        push!(adjlist.fadjlist[src], dst)
        push!(adjlist.fadjlist[dst], src)
    end

    return true
end
function Graphs.has_edge(adjlist::SimpleAdjListStorage{T,D}, edge) where {T,D}
    src, dst = Tuple(edge)
    if !D
        src, dst = (min(src, dst), max(src, dst))
    end
    if length(adjlist.fadjlist) >= src && dst in adjlist.fadjlist[src]
        return true
    end
    return false
end

# Storage for sparse background graphs
struct SparseAdjListStorage{T,D} <: AbstractAdjListStorage{T,D}
    adjlist::Vector{Tuple{T,T}}
end
SparseAdjListStorage{T,D}() where {T,D} =
    SparseAdjListStorage{T,D}(Vector{Tuple{T,T}}())
Base.copy(adjlist::SparseAdjListStorage{T,D}) where {T,D} =
    SparseAdjListStorage{T,D}(copy(adjlist.adjlist))
Base.length(adjlist::SparseAdjListStorage) = length(adjlist.adjlist)
function Base.iterate(adjlist::SparseAdjListStorage{T}, state=one(T)) where T
    if state > length(adjlist.adjlist)
        return nothing
    end
    value = adjlist.adjlist[state]
    return (Edge(value), state+one(T))
end
function Graphs.add_edge!(adjlist::SparseAdjListStorage, edge)
    if findfirst(==(Tuple(edge)), adjlist.adjlist) !== nothing
        return false
    end
    push!(adjlist.adjlist, Tuple(edge))
    return true
end
function add_edges!(adjlist::SparseAdjListStorage, edges; all::Bool=true)
    # FIXME: Account for non-directedness
    edge_set = Set(map(Tuple, edges))
    for edge in adjlist.adjlist
        if edge in edge_set
            if all
                return 0
            else
                pop!(edge_set, edge)
            end
        end
    end
    append!(adjlist.adjlist, collect(edge_set))
    return length(edge_set)
end
function Graphs.has_edge(adjlist::SparseAdjListStorage{T,D}, edge) where {T,D}
    src, dst = Tuple(edge)
    if !D
        src, dst = (min(src, dst), max(src, dst))
    end
    return findfirst(x->x==(src, dst), adjlist.adjlist) !== nothing
end

## Adjacency list implementation

struct AdjList{T,D,A<:AbstractAdjListStorage{T,D}}
    data::A
end
AdjList{T,D}(adjlist::AbstractAdjListStorage{T,D}) where {T,D} =
    AdjList{T,D,typeof(adjlist)}(adjlist)
#AdjList{T,D}() where {T,D} = AdjList{T,D}(SimpleAdjListStorage{T,D}())
AdjList{T,D}() where {T,D} = AdjList{T,D}(SparseAdjListStorage{T,D}())
AdjList() = AdjList{Int,true}()
Base.copy(adj::AdjList{T,D,A}) where {T,D,A} = AdjList{T,D,A}(copy(adj.data))
Graphs.ne(adj::AdjList) = length(adj.data) # TODO: Use ne()
Graphs.has_edge(adj::AdjList{T}, src::Integer, dst::Integer) where T =
    has_edge(adj.data, Edge{T}(src, dst))
Graphs.has_edge(adj::AdjList{T,D}, edge) where {T,D} = has_edge(adj.data, edge)
Graphs.add_edge!(adj::AdjList{T}, src::Integer, dst::Integer) where T =
    add_edge!(adj, Edge{T}(src, dst))
Graphs.add_edge!(adj::AdjList, edge) = add_edge!(adj.data, edge)
add_edges!(adj::AdjList, edges; all::Bool=true) = add_edges!(adj.data, edges; all)
Graphs.edges(adj::AdjList) = copy(adj.data)
function Graphs.inneighbors(adj::AdjList{T,D}, v::Integer) where {T,D}
    neighbors = Int[]
    for edge in adj.data
        src, dst = Tuple(edge)
        if dst == v
            push!(neighbors, src)
        elseif !D && src == v
            push!(neighbors, dst)
        end
    end
    sort!(neighbors)
    unique!(neighbors)
    return neighbors
end
function Graphs.inneighbors(adj::AdjList{T,D,SimpleAdjListStorage{T,D}}, v::Integer) where {T,D}
    if D
        return length(adj.data.badjlist) >= v ? copy(adj.data.badjlist[v]) : T[]
    else
        if length(adj.data.fadjlist) >= v
            neighbors = copy(adj.data.fadjlist[v])
            sort!(neighbors)
            unique!(neighbors)
            return neighbors
        else
            return T[]
        end
    end
end
function Graphs.outneighbors(adj::AdjList{T,D}, v::Integer) where {T,D}
    neighbors = Int[]
    for edge in adj.data
        src, dst = Tuple(edge)
        if src == v
            push!(neighbors, dst)
        elseif !D && dst == v
            push!(neighbors, src)
        end
    end
    sort!(neighbors)
    unique!(neighbors)
    return neighbors
end
function Graphs.outneighbors(adj::AdjList{T,SimpleAdjListStorage{T,D}}, v::Integer) where {T,D}
    if D
        return length(adj.data.fadjlist) >= v ? copy(adj.data.fadjlist[v]) : T[]
    else
        if length(adj.data.fadjlist) >= v
            neighbors = copy(adj.data.fadjlist[v])
            sort!(neighbors)
            unique!(neighbors)
            return neighbors
        else
            return T[]
        end
    end
end
