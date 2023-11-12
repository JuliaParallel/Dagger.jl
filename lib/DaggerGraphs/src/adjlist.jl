## Adjacency list storage

abstract type AbstractAdjListStorage{T,D} end
Base.IteratorSize(::Type{<:AbstractAdjListStorage}) = Base.HasLength()
Base.IteratorEltype(::Type{<:AbstractAdjListStorage}) = Base.HasEltype()
Base.eltype(::Type{<:AbstractAdjListStorage{T}}) where T = Edge{T}

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
    state = (idx, edge_idx)
    return Base.iterate(adjlist, state)
end
function Base.iterate(adjlist::SimpleAdjListStorage{T}, state::Tuple{T,T}) where T
    src, dst = state
    if src > length(adjlist.fadjlist)
        return nothing
    elseif dst > length(adjlist.fadjlist[src])
        src += 1
        dst = 1
        head = src
        src = findfirst(x->!isempty(x), @view(adjlist.fadjlist[head:end]))
        if src === nothing
            return nothing
        end
        # Shift by offset from @view
        src += head - 1
    end
    value = (src, adjlist.fadjlist[src][dst])
    dst += 1
    return (Edge(value), (src, dst))
end
function Base.push!(adjlist::SimpleAdjListStorage{T,D}, edge) where {T,D}
    src, dst = Tuple(edge)
    if !D
        src, dst = (min(src, dst), max(src, dst))
    end

    # If necessary, allocate more inner vectors
    nv = max(src, dst)
    if nv > length(adjlist.fadjlist)
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
        push!(adjlist.fadjlist[src], dst)
        push!(adjlist.badjlist[dst], src)
    else
        push!(adjlist.fadjlist[src], dst)
    end

    return adjlist
end
function Base.in(edge, adjlist::SimpleAdjListStorage{T,D}) where {T,D}
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
function Base.push!(adjlist::SparseAdjListStorage, edge)
    push!(adjlist.adjlist, Tuple(edge))
    return adjlist
end
function Base.in(edge, adjlist::SparseAdjListStorage{T,D}) where {T,D}
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
AdjList{T,D}() where {T,D} = AdjList{T,D}(SimpleAdjListStorage{T,D}())
AdjList() = AdjList{Int,true}()
Base.copy(adj::AdjList{T,D,A}) where {T,D,A} = AdjList{T,D,A}(copy(adj.data))
Base.in(adj::AdjList{T,D}, edge) where {T,D} = edge in adj.data
function Graphs.add_edge!(adj::AdjList{T,D}, edge) where {T,D}
    if edge in adj.data
        return false
    end
    push!(adj.data, edge)
    return true
end
Graphs.edges(adj::AdjList) = copy(adj.data)
function Graphs.inneighbors(adj::AdjList, v::Integer)
    neighbors = Int[]
    for edge in adj.data
        src, dst = Tuple(edge)
        if dst == v
            push!(neighbors, src)
        end
    end
    return neighbors
end
function Graphs.inneighbors(adj::AdjList{T,D,SimpleAdjListStorage{T}}, v::Integer) where {T,D}
    if D
        return copy(adj.data.badjlist[v])
    else
        return invoke(inneighbors, Tuple{AdjList, Integer}, adj, v)
    end
end
function Graphs.outneighbors(adj::AdjList, v::Integer)
    neighbors = Int[]
    for edge in adj.data
        src, dst = Tuple(edge)
        if src == v
            push!(neighbors, dst)
        end
    end
    return neighbors
end
function Graphs.outneighbors(adj::AdjList{T,SimpleAdjListStorage{T,D}}, v::Integer) where {T,D}
    if D
        return copy(adj.data.fadjlist[v])
    else
        return invoke(outneighbors, Tuple{AdjList, Integer}, adj, v)
    end
end
