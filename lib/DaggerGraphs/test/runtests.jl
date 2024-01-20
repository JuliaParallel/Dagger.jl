using DaggerGraphs, Graphs
using Interfaces
using GraphsInterfaceChecker
using Test
import Graphs: AbstractSimpleGraph

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

function test_part_lengths(dg)
    np = DaggerGraphs.nparts(dg)
    state = fetch(dg.state)
    @test length(state.parts) ==
          length(state.parts_nv) ==
          length(state.parts_ne) ==
          length(state.bg_adjs) ==
          length(state.bg_adjs_ne) ==
          length(state.bg_adjs_ne_src) ==
          np
end

function test_eltype(dg, T)
    @test dg isa DGraph{T}
    @test eltype(dg) == T
    @test nv(dg) isa T
    @test vertices(dg) isa Base.OneTo{T}
    @test edgetype(dg) == Edge{T}
    @test all(edge->edge isa Edge{T}, edges(dg))
    @test inneighbors(dg, 1) isa Vector{T}
    @test outneighbors(dg, 1) isa Vector{T}

    state = fetch(dg.state)
    D = is_directed(dg)
    if D
        @test all(part->fetch(part) isa SimpleDiGraph{T}, state.parts)
    else
        @test all(part->fetch(part) isa SimpleGraph{T}, state.parts)
    end
    @test all(adj->fetch(adj) isa DaggerGraphs.AdjList{T,D}, state.bg_adjs)
end

function test_equal(sg::AbstractSimpleGraph{T}, dg::DGraph{U}; check_type::Bool=true, check_directedness::Bool=true) where {T,U}
    if check_type
        @test T === U
    end
    @test nv(sg) == nv(dg)
    @test vertices(sg) == vertices(dg)
    if check_directedness
        @test ne(sg) == ne(dg)
        @test is_directed(sg) == is_directed(dg)
        @test length(edges(sg)) == length(edges(dg))
        @test sort(edges(sg)) == sort(edges(dg))
    end
    all_edges = Set(vcat(collect(edges(sg)),
                         collect(edges(dg))))
    @test all(edge->has_edge(sg, edge), all_edges)
    @test all(edge->has_edge(dg, edge), all_edges)
    @test all(v->sort(inneighbors(sg, v)) == sort(inneighbors(dg, v)), vertices(sg))
    @test all(v->sort(outneighbors(sg, v)) == sort(outneighbors(dg, v)), vertices(sg))
end

function test_frozen(sg, dg)
    test_equal(sg, dg)
    @test DaggerGraphs.isfrozen(dg)
    @test_throws ArgumentError DaggerGraphs.freeze!(dg)
    @test DaggerGraphs.isfrozen(dg)
end
function test_freeze!(sg, dg)
    test_equal(sg, dg)
    @test !DaggerGraphs.isfrozen(dg)
    DaggerGraphs.freeze!(dg)
    test_frozen(sg, dg)
end

function test_mutation_edges(sg, dg, N=2*nv(sg))
    nvg = nv(sg)
    for i in 1:N
        src = rand(1:nvg)
        dst = rand(1:nvg)
        @test add_edge!(dg) == add_edge!(sg)
        @test ne(dg) == ne(sg)
        @test sort(edges(dg)) == sort(edges(sg))
    end
end

test_graphs = [
    DGraph(),
    DGraph(smallgraph(:house), chunksize=3),
    DGraph(smallgraph(:karate), chunksize=12),
    generate_random_dgraph(500),
]

Interfaces.@implements AbstractGraphInterface DGraph test_graphs

@testset "DaggerGraphs" begin
    @testset "Interface" begin
        @test Interfaces.test(AbstractGraphInterface, DGraph)
    end

    @testset "API Basics" begin
        g = generate_random_dgraph(1000)
        @test vertices(g) == Base.OneTo(nv(g))
        @test length(edges(g)) == ne(g)
        e = collect(edges(g))
        @test e isa Vector{Edge{Int}}
        @test all(edge->has_edge(g, edge), e)
    end

    @testset "Non-conversion ctors" begin
        dg = DGraph()
        @test dg isa DGraph{Int}
        @test nv(dg) == 0
        @test ne(dg) == 0
        @test is_directed(dg) == true
        @test eltype(dg) == Int
        @test edgetype(dg) == Edge{Int}

        dg = DGraph(;directed=false)
        @test is_directed(dg) == false

        # Default chunksize is 8
        for (vs, np) in [(0,0),
                         (1,1),
                         (2,1),
                         (8,1),
                         (9,2),
                         (10,2),
                         (16,2),
                         (17,3)]
            dg = DGraph(vs)
            @test nv(dg) == vs
            @test ne(dg) == 0
            @test vertices(dg) == 1:vs
            @test DaggerGraphs.nparts(dg) == np
            test_part_lengths(dg)
        end

        # Non-default chunksize
        for (vs, np) in [(10,1),
                         (11,1),
                         (12,2),
                         (22,2),
                         (23,3)]
            dg = DGraph(vs; chunksize=11)
            @test nv(dg) == vs
            @test ne(dg) == 0
            @test vertices(dg) == 1:vs
            @test DaggerGraphs.nparts(dg) == np
            test_part_lengths(dg)
        end

        # Different eltype
        dg = DGraph{Int32}(16)
        @test dg isa DGraph{Int32}
        @test nv(dg) === Int32(16)
        test_part_lengths(dg)
        test_eltype(dg, Int32)
    end

    @testset "Conversion ctors" begin
        # Directed
        sg = complete_digraph(10)
        dg = DGraph(sg)
        test_equal(sg, dg)

        # Undirected
        sg = clique_graph(10, 20)
        dg = DGraph(sg)
        test_equal(sg, dg)

        @testset "Chunksize $chunksize" for chunksize in [1, 2, 3, 8, 11, 100]
            # Directed
            sg = complete_digraph(10)
            dg = DGraph(sg; chunksize)
            @test DaggerGraphs.nparts(dg) == cld(nv(sg), chunksize)
            test_equal(sg, dg)

            # Undirected
            sg = clique_graph(10, 20)
            dg = DGraph(sg; chunksize)
            @test DaggerGraphs.nparts(dg) == cld(nv(sg), chunksize)
            test_equal(sg, dg)
        end

        @testset "Directedness" begin
            # Manual directedness, empty
            for directed in (true, false)
                dg = DGraph(;directed)
                @test is_directed(dg) == directed
                @test is_directed(typeof(dg)) == directed
            end

            # Directed->Directed, manual directedness
            sg = complete_digraph(10)
            dg = DGraph(sg; directed=true)
            @test is_directed(dg)
            @test is_directed(typeof(dg))
            test_equal(sg, dg)

            # Undirected->Undirected, manual directedness
            sg = SimpleGraph()
            dg = DGraph(sg; directed=false)
            @test !is_directed(dg)
            @test !is_directed(typeof(dg))
            test_equal(sg, dg)

            # Directed->Directed, auto directedness
            sg = complete_digraph(10)
            dg = DGraph(sg)
            @test is_directed(dg)
            @test is_directed(typeof(dg))
            test_equal(sg, dg)

            # Undirected->Undirected, auto directedness
            sg = clique_graph(10, 10)
            dg = DGraph(sg)
            @test !is_directed(dg)
            @test !is_directed(typeof(dg))
            test_equal(sg, dg)

            # Directed->Undirected
            sg = complete_digraph(10)
            dg = DGraph(sg; directed=false)
            @test !is_directed(dg)
            @test !is_directed(typeof(dg))
            test_equal(sg, dg; check_directedness=false)

            # Undirected->Directed
            sg = clique_graph(10, 10)
            dg = DGraph(sg; directed=true)
            @test is_directed(dg)
            @test is_directed(typeof(dg))
            test_equal(sg, dg; check_directedness=false)
        end

        @testset "Freezing" begin
            # Directed, empty, post-ctor
            sg = SimpleDiGraph()
            dg = DGraph(;directed=true)
            test_freeze!(sg, dg)

            # Undirected, empty, post-ctor
            sg = SimpleGraph()
            dg = DGraph(;directed=false)
            test_freeze!(sg, dg)

            # Directed, manual build, post-ctor
            sg = complete_digraph(10)
            dg = DGraph(nv(sg); directed=true)
            for edge in edges(sg)
                @assert add_edge!(dg, edge)
            end
            test_freeze!(sg, dg)

            # Undirected, manual build, post-ctor
            sg = clique_graph(10, 10)
            dg = DGraph(nv(sg); directed=false)
            for edge in edges(sg)
                @assert add_edge!(dg, edge)
            end
            test_freeze!(sg, dg)

            # Directed, auto build, in-ctor
            sg = complete_digraph(10)
            dg = DGraph(sg; freeze=true)
            test_frozen(sg, dg)

            # Undirected, auto build, in-ctor
            sg = clique_graph(10, 10)
            dg = DGraph(sg; freeze=true)
            test_frozen(sg, dg)

            # Directed, auto build, post-ctor
            sg = complete_digraph(10)
            dg = DGraph(sg)
            test_freeze!(sg, dg)

            # Undirected, auto build, post-ctor
            sg = clique_graph(10, 10)
            dg = DGraph(sg)
            test_freeze!(sg, dg)
        end

        @testset "Vertex eltype $T -> $OT" for T in (Int32, Int64), OT in (Int32, Int64)
            # Directed, manual build
            sg = SimpleDiGraph{T}(complete_digraph(10))
            dg = DGraph{OT}(nv(sg))
            for edge in edges(sg)
                @assert add_edge!(dg, edge)
            end
            test_eltype(dg, OT)

            # Undirected, manual build
            sg = SimpleGraph{T}(clique_graph(10, 10))
            dg = DGraph{OT}(nv(sg))
            for edge in edges(sg)
                @assert add_edge!(dg, edge)
            end
            test_eltype(dg, OT)

            # Directed, auto build (manual eltype)
            sg = SimpleDiGraph{T}(complete_digraph(10))
            dg = DGraph{OT}(sg)
            test_eltype(dg, OT)

            # Undirected, auto build (manual eltype)
            sg = SimpleGraph{T}(clique_graph(10, 10))
            dg = DGraph{OT}(sg)
            test_eltype(dg, OT)

            if T === OT
                # Directed, auto build (auto eltype), same eltype
                sg = SimpleDiGraph{T}(complete_digraph(10))
                dg = DGraph(sg)
                test_eltype(dg, T)

                # Undirected, auto build (auto eltype), same eltype
                sg = SimpleGraph{T}(clique_graph(10, 10))
                dg = DGraph(sg)
                test_eltype(dg, T)
            end
        end
    end
end
