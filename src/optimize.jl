# Construction-time DAG optimizations
import Base: IdFun

immutable Comp{F, G}
    f::F
    g::G
end
Comp(::IdFun, f::IdFun) = f
Comp(f, ::IdFun) = f
Comp(::IdFun, f) = f

call(c::Comp, args...) = c.f(c.g(args...))

# Fuse 2 maps together
MapNode{N<:MapNode}(f, input::Tuple{N}) = MapNode(Comp(f, input[1].f), input[1].input)

# Fuse map & reduce into mapreduce
MapReduceNode(f, op, v0, input::MapNode) = MapReduceNode(Comp(f, input.f), op, v0, input.input)

# Fuse reduceby key and map
MapReduceByKey(f, op, v0, input::MapNode) = MapReduceByKey(Comp(f, input.f), op, v0, input.input)

