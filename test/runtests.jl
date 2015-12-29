addprocs(1)

using ComputeFramework
using Base.Test

x = rand(100, 100)
@test gather(Context(), map(-, distribute(x))) == -x


# Graph-Layouts tests
g = distgraph(10, [ones(Int, 10) for i in 1:10])
dg = compute(Context(), distribute(g))
ng = gather(Context(), dg)
@test ng.nv == g.nv && ng.vertices == g.vertices && ng.adj == g.adj


g = distgraph(10, ones(Int, 10, 10))
dg = compute(Context(), distribute(g))
ng = gather(Context(), dg)
@test ng.nv == g.nv && ng.vertices == g.vertices && ng.adj == g.adj


g = distgraph(10, sparse(ones(Int, 10, 10)))
dg = compute(Context(), distribute(g))
ng = gather(Context(), dg)
@test ng.nv == g.nv && ng.vertices == g.vertices && ng.adj == g.adj
