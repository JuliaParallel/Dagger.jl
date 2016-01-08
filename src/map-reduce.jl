
import Base: map, reduce, mapreduce, filter, IdFun
if VERSION >= v"0.5.0-"
import Base: foreach
end

export reducebykey, foreach

### ForEach node ###

immutable Foreach{T<:Tuple} <: ComputeNode
    f
    input::T
end

foreach(f, xs::AbstractNode...) = Foreach(f, xs)

function foreach_seq(f, args...)
    for i=1:length(args[1])
        f([a[i] for a in args]...)
    end
end

function compute(ctx, node::Foreach)
    compute(ctx, mappart(part -> foreach_seq(node.f, part), node.input))
end

### Map ###

immutable Map{T<:Tuple} <: ComputeNode
    f
    input::T
end

map(f, ns::AbstractNode...) = Map(f, ns)

function compute(ctx, node::Map)
    compute(ctx, mappart((localparts...) -> map(node.f, localparts...), node.input))
end

### Fused Map-reduce ###

immutable MapReduce{T<:Tuple} <: ComputeNode
    f
    op
    v0
    input::T
end

mapreduce(f, op, v0, input::AbstractNode...) = MapReduce(f, op, v0, input)
reduce(op, v0, node::AbstractNode...) = mapreduce(IdFun(), op, v0, node...)

# Mapreduce on multiple arguments
function mapreduce(f, op, v0, X...)
    acc = v0
    for args in zip(X...)
        acc = op(acc, f(args...))
    end
end

function compute(ctx, node::MapReduce)
    mapped = gather(ctx, mappart((parts...) -> mapreduce(node.f, node.op, node.v0, parts...), node.input))
    reduce(node.op, node.v0, mapped)
end

### Filter ###

immutable Filter{N<:AbstractNode} <: ComputeNode
    f
    input::N
end

filter(f, x::AbstractNode) = Filter(f, x)

function compute(ctx, node::Filter)
    compute(ctx, mappart(part -> filter(node.f, part), node.input))
end

### GroupBy ###

immutable MapReduceByKey{N<:Tuple} <: ComputeNode
    f
    op
    v0
    input::N
end

reducebykey(op, v0, input...) = MapReduceByKey(IdFun(), op, v0, input)
mapreducebykey(f, op, v0, input...) = MapReduceByKey(f, op, v0, input)

function mapreducebykey_seq(f, op,  v0, itr, dict=Dict())
    for x in itr
        y = f(x)
        dict[y[1]] = op(get(dict, y[1], v0), y[2])
    end
    dict
end

reducebykey_seq(op, v0, itr,dict=Dict()) = mapreducebykey_seq(IdFun(), op, v0, itr, dict)

object(x) = x
object(x::Chunks) = x.xs

function compute(ctx, node::MapReduceByKey)
    parts = mappart((part) -> mapreducebykey_seq(node.f, node.op, node.v0, part), node.input)
    reduce((acc, chunk) -> reducebykey_seq(node.op, node.v0, chunk, acc), Dict(), gather(ctx, parts) |> object)
end
