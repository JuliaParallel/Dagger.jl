export distgraph, distdigraph, num_vertices, vertices, adj
###
# Graph data structure definitions.
###

# Allowed Graph datastructures:
typealias AdjacencyList Array{Array{Int, 1}, 1}
typealias AdjacencyMatrix AbstractArray{Int, 2}                      # Covers sparse matrices
typealias GraphStruct Union{AdjacencyList, AdjacencyMatrix}


# Generic (undirected or directed, up to the user to decide) Graph
type DistGraph{S<:GraphStruct}
    nv::Int                            # not really required, but kept for convenience
    vertices::UnitRange{Int}
    adj::S
end

"""
Creates a Graph representation. Requires inputs:
- Number of vertices
- Range of vertices
- A Graph Data Structure which may be an Adjacency List or a Adjacency Matrix
"""
distgraph(nv, vertices, adj) = DistGraph(nv, vertices, adj)
distgraph(nv, adj) = DistGraph(nv, UnitRange{Int}(1, nv), adj)


"""Returns the number of vertices in the graph representation."""
num_vertices(x::DistGraph) = x.nv

"""Returns the range of vertices in the graph representation."""
vertices(x::DistGraph) = x.vertices

"""Returns the datastructure in the graph representation."""
adj(x::DistGraph) = x.adj


###
# Graph layouts
###

immutable GraphLayout{S<:GraphStruct} <: AbstractLayout end

"""
Given an input vertex range, slice it into sub-ranges.
"""
function slice(ctx, vrange::UnitRange{Int}, ::GraphLayout, targets)
    index_splits(endof(vrange)-start(vrange)+1, length(targets))
end

"""
Slice the individual componenets of the input graph and commpose sub-graphs.
"""
function slice{S<:AdjacencyList}(ctx, graph::DistGraph{S}, layout::GraphLayout, targets)
    vertex_chunks = slice(ctx, vertices(graph), layout, targets)
    nv_chunks = [length(x) for x in vertex_chunks]
    adj_chunks = slice(ctx, adj(graph), RowLayout(), targets)
    [distgraph(t...) for t in zip(nv_chunks, vertex_chunks, adj_chunks)]
end

function slice{S<:AdjacencyMatrix}(ctx, graph::DistGraph{S}, layout::GraphLayout, targets)
    vertex_chunks = slice(ctx, vertices(graph), layout, targets)
    nv_chunks = [length(x) for x in vertex_chunks]
    adj_chunks = slice(ctx, adj(graph), ColumnLayout(), targets)
    [distgraph(t...) for t in zip(nv_chunks, vertex_chunks, adj_chunks)]
end

"""
Combine the individual subgraphs to produce the original graph.
"""
function gather{S<:AdjacencyList}(ctx, layout::GraphLayout{S}, graphs::Vector)
    nv = round(Int, mapreduce(num_vertices, +, 0, graphs))
    adj_aggregate = gather(ctx, RowLayout(), map(adj, graphs))
    distgraph(nv, UnitRange(1, nv), adj_aggregate)
end

function gather{S<:AdjacencyMatrix}(ctx, layout::GraphLayout{S}, graphs::Vector)
    nv = round(Int, mapreduce(num_vertices, +, 0, graphs))
    adj_aggregate = gather(ctx, ColumnLayout(), map(adj, graphs))
    distgraph(nv, UnitRange(1, nv), adj_aggregate)
end

"""
Generic method for the distribute function.
"""
function distribute{S<:GraphStruct}(graph::DistGraph{S})
    distribute(graph, GraphLayout{S}())
end
