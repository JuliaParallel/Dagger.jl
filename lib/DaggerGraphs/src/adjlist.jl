struct AdjList{T}
    adj::Vector{Tuple{T,T}}
end
AdjList() = AdjList{Int}(Tuple{Int,Int}[])
Base.copy(adj::AdjList) = AdjList(copy(adj.adj))
function has_bg_adj(adj::AdjList, src::Int, dst::Int, directed::Bool)
    idx = findfirst(edge->(edge == (src, dst)) ||
                          (!directed && (edge == (dst, src))),
                    adj.adj)
    if idx !== nothing
        return true
    end
    return false
end
function add_bg_adj!(adj::AdjList, src::Int, dst::Int, directed::Bool)
    if has_bg_adj(adj, src, dst, directed)
        return false
    end
    push!(adj.adj, (src, dst))
    return true
end
Graphs.edges(adj::AdjList) = adj.adj
function Graphs.inneighbors(adj::AdjList, v::Integer)
    neighbors = Int[]
    for (src, dst) in adj.adj
        if dst == v
            push!(neighbors, src)
        end
    end
    return neighbors
end
function Graphs.outneighbors(adj::AdjList, v::Integer)
    neighbors = Int[]
    for (src, dst) in adj.adj
        if src == v
            push!(neighbors, dst)
        end
    end
    return neighbors
end
