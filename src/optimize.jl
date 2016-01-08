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
map(f, input::Tuple{Map}) = Map(Comp(f, input[1].f), input[1].input)

# Fuse map & reduce into mapreduce
MapReduce(f, op, v0, input::Tuple{Map}) = MapReduce(Comp(f, input[1].f), op, v0, input[1].input)

# Fuse reduceby key and map
MapReduceByKey(f, op, v0, input::Tuple{Map}) = MapReduceByKey(Comp(f, input[1].f), op, v0, input[1].input)
