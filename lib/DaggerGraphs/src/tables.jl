function fromtable(T, x; cluster_col=1, meta_cols=())
    clusters = Tables.getcolumn(x, cluster_col)

    g = DGraph{T}(;chunksize=typemax(Int))
    clusters_uniq = unique(clusters)
    custom_parts = Vector{eltype(clusters_uniq)}()
    vertex_meta = Vector{Dict{Union{Symbol,Int},Vector}}()
    for (idx,c) in enumerate(clusters_uniq)
        c_elems = count(co->co===c, clusters)
        part = add_partition!(g, c_elems)
        push!(custom_parts, c)
        @assert part == length(custom_parts)
        error("Update vertex_meta")
    end
    return MetaDGraph(g, custom_parts)
end
struct MetaDGraph{T,P} <: AbstractGraph{T}
    state::Chunk{DGraphState{T}}
    custom_parts::Vector{P}
    vertex_meta # FIXME: Concrete type
end
Base.eltype(::MetaDGraph{T}) where T = T
Graphs.edgetype(::MetaDGraph{T}) where T = Tuple{T,T}
Graphs.nv(g::MetaDGraph) = fetch(Dagger.@spawn nv(g.state))::Int
Graphs.ne(g::MetaDGraph) = fetch(Dagger.@spawn ne(g.state))::Int
Graphs.has_vertex(g::MetaDGraph, v::Integer) = 1 <= v <= nv(g)
Graphs.has_edge(g::MetaDGraph, src::Integer, dst::Integer) =
    fetch(Dagger.@spawn has_edge(g.state, src, dst))
Graphs.is_directed(::MetaDGraph) = true
Graphs.vertices(g::MetaDGraph) = Base.OneTo(nv(g))
Graphs.edges(g::MetaDGraph) = DGraphEdgeIter(g)
Graphs.zero(::Type{<:MetaDGraph}) = MetaDGraph()
