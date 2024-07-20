mapvertices(f, g::DGraph) = with_state(g, mapvertices, g, f)
function mapvertices(g::DGraphState{T}, dg::DGraph, f) where T
    tasks = [Dagger.spawn(_mapvertices, f, dg, vs) for vs in g.parts_nv]
    ET = Base.promote_op(f, DGraphState, T)
    d = ArrayDomain((1:nv(g),))
    part = Blocks(partition_size(g))
    ds = partition(part, d)
    return Dagger.DArray(ET, d, ds, tasks, part)
end
_mapvertices(f, g::DGraph, vs) = [f(g, v) for v in vs]
