using DaggerGraphs, Graphs
using Test

function generate_random_dgraph(N)
    g = DGraph()
    n_ctr = 1
    add_vertex!(g)
    n_edges = 0
    for i in 1:N
        src = rand() < 0.1 ? n_ctr+1 : rand(1:n_ctr)
        if src > n_ctr
            add_vertex!(g)
            n_ctr += 1
        end
        dst = rand() < 0.1 ? n_ctr+1 : rand(1:n_ctr)
        if dst > n_ctr
            add_vertex!(g)
            n_ctr += 1
        end
        if add_edge!(g, src, dst)
            n_edges += 1
        end
    end
    @test nv(g) == n_ctr
    @test ne(g) == n_edges
    return g
end

@testset "Basics" begin
    g = generate_random_dgraph(1000)
    @test vertices(g) == Base.OneTo(nv(g))
    @test length(edges(g)) == ne(g)
    e = collect(edges(g))
    @test e isa Vector{Tuple{Int,Int}}
    @test all(edge->has_edge(g, edge[1], edge[2]), e)
end
